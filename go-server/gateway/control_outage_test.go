package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestControlOutageStopsBeforeUpstream(t *testing.T) {
	for _, stage := range []string{"authorize", "dispatches", "consume"} {
		for _, failure := range []string{"disconnect", "server-error"} {
			t.Run(stage+"/"+failure, func(t *testing.T) {
				var upstreamCalls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { upstreamCalls.Add(1); w.Write([]byte(`{}`)) }))
				defer upstream.Close()
				var mu sync.Mutex
				calls := map[string]int{}
				finalState := ""
				control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					operation := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
					mu.Lock()
					calls[operation]++
					mu.Unlock()
					if operation == stage {
						if failure == "server-error" {
							http.Error(w, "fixture unavailable", 500)
							return
						}
						connection, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						connection.Close()
						return
					}
					switch operation {
					case "abort":
						w.Write([]byte(`{"accepted":true}`))
					case "authorize":
						w.Write([]byte(`{"model":"fixture"}`))
					case "dispatches":
						json.NewEncoder(w).Encode(map[string]any{"requestId": "outage-fixture", "permit": "fixture", "endpoint": upstream.URL})
					case "consume":
						w.Write([]byte(`{}`))
					case "finalize":
						var body map[string]any
						if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
							t.Error(err)
							return
						}
						mu.Lock()
						finalState, _ = body["status"].(string)
						mu.Unlock()
						w.Write([]byte(`{}`))
					default:
						t.Errorf("unexpected control operation %s", operation)
					}
				}))
				defer control.Close()
				g := newGateway(control.URL, "internal-fixture", "outage-fixture", []string{"127.0.0.1"})
				request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","max_tokens":16,"messages":[{"role":"user","content":"test"}]}`))
				request.Header.Set("X-Api-Key", "fixture-key")
				request.Header.Set("Authorization", "Bearer ses_client_login")
				request.Header.Set("Idempotency-Key", "outage-request")
				response := httptest.NewRecorder()
				g.ServeHTTP(response, request)
				if response.Code != 503 || upstreamCalls.Load() != 0 {
					t.Fatalf("status=%d upstream calls=%d", response.Code, upstreamCalls.Load())
				}
				mu.Lock()
				defer mu.Unlock()
				if calls[stage] != 1 {
					t.Fatalf("failed operation retried: %v", calls)
				}
				if stage == "authorize" && (calls["dispatches"] != 0 || calls["consume"] != 0 || calls["finalize"] != 0) {
					t.Fatalf("continued after authorization failure: %v", calls)
				}
				if stage == "dispatches" && (calls["consume"] != 0 || calls["finalize"] != 0) {
					t.Fatalf("continued after dispatch failure: %v", calls)
				}
				if stage == "consume" && (calls["finalize"] != 1 || finalState != "failed") {
					t.Fatalf("unexecuted request finalization: calls=%v state=%s", calls, finalState)
				}
			})
		}
	}
}

func TestFinalizeOutageNeverReexecutesUpstream(t *testing.T) {
	var upstreamCalls, finalizeCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":2}}`))
	}))
	defer upstream.Close()
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			w.Write([]byte(`{"model":"fixture"}`))
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(map[string]any{"requestId": "finalize-outage", "permit": "fixture", "endpoint": upstream.URL})
		case strings.HasSuffix(r.URL.Path, "/consume"):
			w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/transport"):
			w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			finalizeCalls.Add(1)
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			body = meterFixtureSelectedEvidence(r, body)
			if body["status"] != "succeeded" || body["inputTokens"] != float64(12) || body["outputTokens"] != float64(2) {
				t.Errorf("lost completion evidence: %v", body)
			}
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			connection.Close()
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer control.Close()
	g := newGateway(control.URL, "internal-fixture", "finalize-fixture", []string{"127.0.0.1"})
	request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","max_tokens":16,"messages":[{"role":"user","content":"test"}]}`))
	request.Header.Set("X-Api-Key", "fixture-key")
	request.Header.Set("Authorization", "Bearer ses_client_login")
	response := httptest.NewRecorder()
	g.ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), "OK") || upstreamCalls.Load() != 1 || finalizeCalls.Load() != 3 {
		t.Fatalf("status=%d upstream=%d finalize=%d", response.Code, upstreamCalls.Load(), finalizeCalls.Load())
	}
}

func TestFinalizeTransientFailureRetriesSameEvidence(t *testing.T) {
	var upstreamCalls, finalizeCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":2}}`))
	}))
	defer upstream.Close()
	var first string
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			w.Write([]byte(`{"model":"fixture"}`))
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(map[string]any{"requestId": "retry-fixture", "permit": "fixture", "endpoint": upstream.URL})
		case strings.HasSuffix(r.URL.Path, "/consume"), strings.HasSuffix(r.URL.Path, "/transport"):
			w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			if finalizeCalls.Add(1) == 1 {
				first = string(data)
				http.Error(w, "deadlock", 500)
				return
			}
			if string(data) != first {
				t.Errorf("finalization evidence changed")
			}
			w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer control.Close()
	g := newGateway(control.URL, "internal-fixture", "retry-fixture", []string{"127.0.0.1"})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","max_tokens":16,"messages":[{"role":"user","content":"test"}]}`))
	req.Header.Set("X-Api-Key", "fixture-key")
	req.Header.Set("Authorization", "Bearer ses_client_login")
	response := httptest.NewRecorder()
	g.ServeHTTP(response, req)
	if response.Code != 200 || upstreamCalls.Load() != 1 || finalizeCalls.Load() != 2 {
		t.Fatalf("status=%d upstream=%d finalize=%d", response.Code, upstreamCalls.Load(), finalizeCalls.Load())
	}
}

func TestFinalizeDoesNotRetryPermanentClientError(t *testing.T) {
	var calls atomic.Int32
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "conflicting finalization", http.StatusConflict)
	}))
	defer control.Close()
	g := newGateway(control.URL, "internal-fixture", "retry-fixture", nil)
	if err := g.finalize(context.Background(), "/requests/test/finalize", map[string]any{"status": "failed"}); err == nil {
		t.Fatal("expected conflict")
	}
	if calls.Load() != 1 {
		t.Fatalf("conflict was retried %d times", calls.Load())
	}
}

func TestFinalizeRetriesAfterStalledAttemptWithinTotalDeadline(t *testing.T) {
	var calls atomic.Int32
	var first atomic.Value
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if calls.Add(1) == 1 {
			first.Store(string(body))
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
			return
		}
		if string(body) != first.Load().(string) {
			t.Error("retry changed settlement evidence")
		}
		io.WriteString(w, `{}`)
	}))
	defer control.Close()
	g := newGateway(control.URL, "internal-fixture", "retry-fixture", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if err := g.finalize(ctx, "/requests/fixture/finalize", map[string]any{"status": "succeeded", "inputTokens": 12, "outputTokens": 2}); err != nil {
		t.Fatalf("stalled first attempt prevented recovery: %v", err)
	}
	if calls.Load() != 2 || ctx.Err() != nil {
		t.Fatalf("calls=%d context=%v", calls.Load(), ctx.Err())
	}
}

func TestFinalizeCancellationStopsRetries(t *testing.T) {
	var calls atomic.Int32
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer control.Close()
	g := newGateway(control.URL, "internal-fixture", "retry-fixture", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.finalize(ctx, "/requests/fixture/finalize", map[string]any{"status": "failed"}); err == nil || calls.Load() != 0 {
		t.Fatalf("cancelled finalization sent traffic: err=%v calls=%d", err, calls.Load())
	}
}
