package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddingValidation(t *testing.T) {
	for _, tc := range []struct {
		input string
		count int
	}{
		{`"text"`, 1}, {`["a","b"]`, 2}, {`[1,2]`, 1}, {`[[1],[2,3]]`, 2},
		{`null`, 0}, {`[]`, 0}, {`[""]`, 0}, {`[1,"a"]`, 0}, {`["a",[1]]`, 0}, {`[[-1]]`, 0}, {`[[1.5]]`, 0},
	} {
		var value any
		json.Unmarshal([]byte(tc.input), &value)
		if got := embeddingInput(value); got != tc.count {
			t.Fatalf("%s: %d", tc.input, got)
		}
	}
	good := `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}]}`
	if !validEmbeddings([]byte(good), 1, 2) {
		t.Fatal("valid vector rejected")
	}
	for _, bad := range []string{strings.Replace(good, `"index":0,`, "", 1), strings.Replace(good, "0.1", "null", 1), strings.Replace(good, `"index":0`, `"index":1`, 1), `{"object":"list","data":[]}`} {
		if validEmbeddings([]byte(bad), 1, 2) {
			t.Fatal("invalid vector accepted", bad)
		}
	}
	if validEmbeddings([]byte(good), 2, 2) || validEmbeddings([]byte(good), 1, 3) {
		t.Fatal("count/dimensions mismatch accepted")
	}
}

func TestEmbeddingExecutionAndUnknownResponse(t *testing.T) {
	for _, valid := range []bool{true, false} {
		t.Run(map[bool]string{true: "success", false: "malformed"}[valid], func(t *testing.T) {
			calls := 0
			state := ""
			dispatched := false
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != "/v1/embeddings" {
					t.Error("wrong upstream path")
				}
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				if body["model"] != "upstream-model" {
					t.Error("model not rewritten")
				}
				if valid {
					w.Write([]byte(`{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":2,"total_tokens":2}}`))
				} else {
					w.Write([]byte(`{"data":[]}`))
				}
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					w.Write([]byte(`{"model":"upstream-model"}`))
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					dispatched = true
					if body["inferencePath"] != "/v1/embeddings" {
						t.Error("missing type binding")
					}
					json.NewEncoder(w).Encode(map[string]any{"requestId": "embedding-test", "permit": "fixture", "endpoint": upstream.URL})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					state, _ = body["status"].(string)
					if _, metered := body["meterSequence"]; metered {
						t.Error("embedding legacy path unexpectedly metered")
					}
					for _, key := range []string{"inputTokens", "outputTokens", "cachedInputTokens", "cacheWriteTokens"} {
						if _, present := body[key]; !present {
							t.Errorf("legacy raw counter key removed: %s", key)
						}
					}
					if valid && (body["inputTokens"] != float64(2) || body["outputTokens"] != float64(0)) {
						t.Error("legacy upstream usage lost")
					}
					w.Write([]byte(`{}`))
				default:
					w.Write([]byte(`{}`))
				}
			}))
			defer api.Close()
			gateway := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			request := httptest.NewRequest("POST", "/v1/embeddings", strings.NewReader(`{"model":"deployment","input":"text","dimensions":2}`))
			request.Header.Set("X-Api-Key", "nn_fixture")
			request.Header.Set("Authorization", "Bearer ses_client_login")
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, request)
			want, code := "unknown", 502
			if valid {
				want, code = "succeeded", 200
			}
			if calls != 1 || !dispatched || state != want || response.Code != code {
				t.Fatalf("calls=%d dispatched=%v state=%s status=%d", calls, dispatched, state, response.Code)
			}
		})
	}
}
