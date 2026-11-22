package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The API fixture holds each checkpoint independently: useful output must wait
// for an exact acknowledgment, not merely for the POST to be attempted.
func TestHostedMeterPersistsBeforeUpstreamAndUsefulOutput(t *testing.T) {
	for _, public := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			name := "native-json"
			if public {
				name = "public-json"
			}
			if stream {
				name += "-stream"
			}
			t.Run(name, func(t *testing.T) {
				checkpoints := make(chan int64, 8)
				release := make(chan struct{}, 8)
				upstreamStarted := make(chan struct{}, 1)
				var finish map[string]any
				var acceptedSnapshot map[string]any
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					upstreamStarted <- struct{}{}
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n")
						w.(http.Flusher).Flush()
						io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
					} else {
						io.WriteString(w, `{"choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`)
					}
				}))
				defer upstream.Close()
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case strings.HasSuffix(r.URL.Path, "/meter"):
						if r.Header.Get("Authorization") != "Bearer "+hostedFixtureReportToken || r.Header.Get("X-Api-Key") != "" {
							t.Error("checkpoint caller identity leaked")
						}
						var v struct {
							Sequence int64           `json:"sequence"`
							Snapshot json.RawMessage `json:"snapshot"`
						}
						if json.NewDecoder(r.Body).Decode(&v) != nil || len(v.Snapshot) == 0 {
							t.Error("invalid checkpoint payload")
						}
						if strings.Contains(string(v.Snapshot), "hello") || strings.Contains(string(v.Snapshot), "upstream-secret") {
							t.Error("plaintext/credential in checkpoint")
						}
						checkpoints <- v.Sequence
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
						if err := json.Unmarshal(v.Snapshot, &acceptedSnapshot); err != nil {
							t.Error(err)
						}
						json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": hostedFixtureID, "sequence": v.Sequence})
					case strings.HasSuffix(r.URL.Path, "/finish"):
						json.NewDecoder(r.Body).Decode(&finish)
						io.WriteString(w, `{"finalized":true}`)
					case r.URL.Path == "/api/v1/me":
						io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
					case strings.HasSuffix(r.URL.Path, "/dispatches"):
						json.NewEncoder(w).Encode(hostedDispatch{RequestID: hostedFixtureID, AccountID: hostedFixtureID, KeyID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual", Endpoint: "http://upstream.test/v1", APIKey: "upstream-secret", Revision: 1})
					default:
						t.Errorf("unexpected control route %s", r.URL.Path)
						w.WriteHeader(404)
					}
				}))
				defer api.Close()
				c := newCore(t.TempDir(), "http://127.0.0.1:1")
				c.control, c.onlineSupply, c.publicStateless = api.URL, fixturePublicPolicy(upstream), public
				defer c.shutdown()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				raw := `{"model":"` + hostedFixtureID + `","messages":[{"role":"user","content":"test"}],"stream":false}`
				if stream {
					raw = strings.Replace(raw, `"stream":false`, `"stream":true`, 1)
				}
				r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(raw)).WithContext(ctx)
				r.Header.Set("Authorization", "Bearer ses_fixture")
				r.Header.Set("X-Api-Key", "nn_fixture")
				r.Header.Set("Content-Type", "application/json")
				w := &meterBarrierWriter{ResponseRecorder: httptest.NewRecorder(), useful: make(chan struct{}, 8)}
				done := make(chan struct{})
				go func() { defer close(done); c.executeHosted(w, r, hostedFixtureID, []byte(raw)) }()
				defer func() {
					cancel()
					for range 8 {
						select {
						case release <- struct{}{}:
						default:
						}
					}
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Error("hosted fixture did not stop")
					}
				}()
				select {
				case seq := <-checkpoints:
					if seq != 1 {
						t.Fatalf("start sequence=%d", seq)
					}
				case <-done:
					t.Fatal("no durable start checkpoint before execution")
				case <-time.After(2 * time.Second):
					t.Fatal("start checkpoint not attempted")
				}
				select {
				case <-upstreamStarted:
					t.Fatal("provider invoked before start acknowledgment")
				default:
				}
				release <- struct{}{}
				select {
				case <-upstreamStarted:
				case <-time.After(2 * time.Second):
					t.Fatal("provider never started")
				}
				select {
				case seq := <-checkpoints:
					if seq != 2 {
						t.Fatalf("output sequence=%d", seq)
					}
				case <-done:
					t.Fatal("no output checkpoint")
				case <-time.After(2 * time.Second):
					t.Fatal("output checkpoint not attempted")
				}
				select {
				case <-w.useful:
					t.Fatal("useful output escaped checkpoint barrier")
				default:
				}
				for range 8 {
					select {
					case release <- struct{}{}:
					default:
					}
				}
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatal("hosted request did not complete")
				}
				reported, _ := acceptedSnapshot["reported"].(map[string]any)
				if w.Code != 200 || !strings.Contains(w.Body.String(), "hello") || finish["meterSequence"] == nil || finish["executionClosed"] != true || reported["inputTokens"] != float64(2) {
					t.Fatalf("response=%d %q finish=%v", w.Code, w.Body.String(), finish)
				}
				for _, field := range []string{"inputTokens", "outputTokens", "cachedInputTokens", "cacheWriteTokens", "usageSource"} {
					if _, present := finish[field]; present {
						t.Errorf("lifecycle-only closure sent %s", field)
					}
				}
				if public {
					entries, _ := os.ReadDir(c.home)
					if len(entries) != 0 || c.history != nil {
						t.Fatal("stateless hosted meter opened SQLite")
					}
				}
			})
		}
	}
}

type meterBarrierWriter struct {
	*httptest.ResponseRecorder
	useful chan struct{}
}

func (w *meterBarrierWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "hello") {
		w.useful <- struct{}{}
	}
	return w.ResponseRecorder.Write(p)
}

func TestHostedMeterRejectsBadStartAcknowledgmentsWithoutInference(t *testing.T) {
	for _, ack := range []string{`{}`, `{"accepted":false}`, `{"accepted":true,"requestId":"wrong","sequence":1}`, `{"accepted":true,"requestId":"` + hostedFixtureID + `","sequence":2}`} {
		t.Run(ack, func(t *testing.T) {
			var executions atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				executions.Add(1)
				io.WriteString(w, `{"choices":[{"message":{"content":"hello"}}]}`)
			}))
			defer upstream.Close()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/meter"):
					io.WriteString(w, ack)
				case strings.HasSuffix(r.URL.Path, "/finish"):
					io.WriteString(w, `{"finalized":true}`)
				case r.URL.Path == "/api/v1/me":
					io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
				default:
					json.NewEncoder(w).Encode(hostedDispatch{RequestID: hostedFixtureID, AccountID: hostedFixtureID, KeyID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual", Endpoint: "http://upstream.test/v1", Revision: 1})
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control, c.onlineSupply = api.URL, fixturePublicPolicy(upstream)
			defer c.shutdown()
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture"}`))
			r.Header.Set("Authorization", "Bearer ses_fixture")
			w := httptest.NewRecorder()
			c.executeHosted(w, r, hostedFixtureID, []byte(`{"model":"fixture"}`))
			if executions.Load() != 0 || strings.Contains(w.Body.String(), "hello") || w.Code < 400 {
				t.Fatalf("bad ack released inference: calls=%d status=%d body=%s", executions.Load(), w.Code, w.Body.String())
			}
		})
	}
}
