package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const evalJob = "00000000-0000-4000-8000-000000000001"
const evalRequest = "00000000-0000-4000-8000-000000000002"
const evalAttempt = "00000000-0000-4000-8000-000000000003"

func TestEvaluationStructuredProbeRejectsAmbiguousObjects(t *testing.T) {
	for _, test := range []struct {
		body string
		want bool
	}{
		{`{"ok":true}`, true}, {` { "ok" : true } `, true},
		{`{"ok":false,"ok":true}`, false}, {`{"ok":true,"extra":0}`, false},
		{`true`, false}, {`{"ok":"true"}`, false}, {`{"ok":true} null`, false},
	} {
		if got := evaluationScore("structured-output", test.body); got != test.want {
			t.Errorf("body=%q got=%v want=%v", test.body, got, test.want)
		}
	}
}

func TestEvaluationDefaultsClosed(t *testing.T) {
	t.Setenv("NEWNOA_EVALUATOR_TOKEN", "")
	g := newGateway("http://127.0.0.1:1", strings.Repeat("i", 32), "eval-test", nil)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("POST", "/internal/evaluations/"+evalJob+"/greeting", strings.NewReader(`{}`)))
	if w.Code != 503 {
		t.Fatalf("disabled evaluation status=%d", w.Code)
	}
}

func TestEvaluationUsesFixedBodyAndSettlesOnce(t *testing.T) {
	for _, reason := range []string{"stop", "length", "content_filter", "tool_calls", ""} {
		t.Run("finish="+reason, func(t *testing.T) { testEvaluationTermination(t, reason, false) })
	}
	t.Run("personal-node", func(t *testing.T) { testEvaluationTermination(t, "stop", true) })
}

func testEvaluationTermination(t *testing.T, reason string, routed bool) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-fixture")
	wantState, wantHTTP := "failed", 200
	if reason == "stop" {
		wantState = "succeeded"
	} else if reason == "" {
		wantState, wantHTTP = "unknown", 502
	}
	token := strings.Repeat("e", 32)
	internal := strings.Repeat("i", 32)
	t.Setenv("NEWNOA_EVALUATOR_TOKEN", token)
	var calls, consumed, finished atomic.Int32
	fixed := `{"model":"mock-eval","stream":false,"max_tokens":16,"messages":[{"role":"user","content":"Reply with exactly OK."}]}`
	sum := sha256.Sum256([]byte(fixed))
	digest := hex.EncodeToString(sum[:])
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body map[string]any
		wantAuth := ""
		if routed {
			wantAuth = "Bearer " + internal
			if r.URL.Path != "/tunnels/"+evalJob+"/7/evaluation/v1/chat/completions" || r.Header.Get("X-Newnoa-Permit") != "eval-test-permit" || r.Header.Get("X-Newnoa-Epoch") != "7" {
				t.Error("node scope not forwarded")
			}
		}
		if r.Header.Get("Authorization") != wantAuth || json.NewDecoder(r.Body).Decode(&body) != nil || body["model"] != "mock-eval" {
			t.Error("unexpected upstream credentials or body")
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": "OK"}, "finish_reason": reason}}, "usage": map[string]int{"prompt_tokens": 12, "completion_tokens": 2}})
	}))
	defer upstream.Close()
	t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", upstream.URL)
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+internal {
			t.Error("wrong control credential")
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/dispatch"):
			permit := map[string]any{"requestId": evalRequest, "attemptId": evalAttempt, "purpose": "evaluation", "permit": "eval-test-permit", "endpoint": upstream.URL, "body": fixed, "bodyDigest": digest, "reserveTokens": 128}
			if routed {
				permit["nodeId"] = evalJob
				permit["ownerId"] = "owner-fixture"
				permit["epoch"] = 7
				permit["route"] = upstream.URL + "/tunnels/" + evalJob + "/7/evaluation"
				permit["endpoint"] = "http://unreachable.invalid"
			}
			json.NewEncoder(w).Encode(permit)
		case strings.HasSuffix(r.URL.Path, "/consume"):
			if routed {
				t.Error("gateway consumed a device permit")
			}
			consumed.Add(1)
			w.Write([]byte(`{"consumed":true}`))
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			var result map[string]any
			if json.NewDecoder(r.Body).Decode(&result) != nil || result["status"] != wantState || result["inputTokens"] != float64(12) || result["outputTokens"] != float64(2) {
				t.Errorf("wrong settlement: %v", result)
			}
			finished.Add(1)
			w.Write([]byte(`{"resultDigest":"stored"}`))
		default:
			t.Error("unexpected control route")
			w.WriteHeader(404)
		}
	}))
	defer control.Close()
	g := newGateway(control.URL, internal, "eval-test", []string{"127.0.0.1"})
	raw := []byte(`{"workerId":"worker","fencingToken":1,"requestId":"` + evalRequest + `","attemptId":"` + evalAttempt + `"}`)
	request := httptest.NewRequest("POST", "/internal/evaluations/"+evalJob+"/greeting", bytes.NewReader(raw))
	request.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, request)
	wantConsumed := int32(1)
	if routed {
		wantConsumed = 0
	}
	if w.Code != wantHTTP || calls.Load() != 1 || consumed.Load() != wantConsumed || finished.Load() != 1 {
		t.Fatalf("status=%d calls=%d consumed=%d finished=%d body=%s", w.Code, calls.Load(), consumed.Load(), finished.Load(), w.Body.String())
	}
	if wantHTTP != 200 {
		return
	}
	var result map[string]any
	if json.Unmarshal(w.Body.Bytes(), &result) != nil || result["status"] != wantState || result["output"] != "OK" {
		t.Fatalf("bad result %s", w.Body.String())
	}
	rawResponse, ok := result["responseBody"].(string)
	if !ok {
		t.Fatal("raw artifact bytes missing from evaluation report")
	}
	artifact := sha256.Sum256([]byte(rawResponse))
	if result["artifactDigest"] != hex.EncodeToString(artifact[:]) {
		t.Fatal("artifact digest does not match report bytes")
	}
	bad := httptest.NewRequest("POST", "/internal/evaluations/"+evalJob+"/greeting", strings.NewReader(`{"endpoint":"https://example.invalid"}`))
	bad.Header.Set("Authorization", "Bearer "+token)
	denied := httptest.NewRecorder()
	g.ServeHTTP(denied, bad)
	if denied.Code != 400 || calls.Load() != 1 {
		t.Fatalf("arbitrary body accepted: %d", denied.Code)
	}
}

func TestEvaluationChangedBodyCannotConsumeOrExecute(t *testing.T) {
	token := strings.Repeat("e", 32)
	internal := strings.Repeat("i", 32)
	t.Setenv("NEWNOA_EVALUATOR_TOKEN", token)
	var calls, consumed, finished atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer upstream.Close()
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/dispatch"):
			json.NewEncoder(w).Encode(map[string]any{"requestId": evalRequest, "attemptId": evalAttempt, "purpose": "evaluation", "permit": "eval-test-permit", "endpoint": upstream.URL, "body": "{}", "bodyDigest": strings.Repeat("0", 64), "reserveTokens": 128})
		case strings.HasSuffix(r.URL.Path, "/consume"):
			consumed.Add(1)
			w.Write([]byte(`{"consumed":true}`))
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			var result map[string]any
			json.NewDecoder(r.Body).Decode(&result)
			if result["status"] != "failed" || result["inputTokens"] != float64(0) || result["outputTokens"] != float64(0) {
				t.Errorf("invalid preflight result: %v", result)
			}
			finished.Add(1)
			w.Write([]byte(`{"resultDigest":"stored"}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer control.Close()
	g := newGateway(control.URL, internal, "eval-test", []string{"127.0.0.1"})
	r := httptest.NewRequest("POST", "/internal/evaluations/"+evalJob+"/greeting", strings.NewReader(`{"workerId":"worker","fencingToken":1,"requestId":"`+evalRequest+`","attemptId":"`+evalAttempt+`"}`))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 502 || calls.Load() != 0 || consumed.Load() != 0 || finished.Load() != 1 {
		t.Fatalf("changed body status=%d calls=%d consume=%d finish=%d", w.Code, calls.Load(), consumed.Load(), finished.Load())
	}
}
