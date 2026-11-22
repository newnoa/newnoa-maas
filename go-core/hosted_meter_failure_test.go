package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHostedOutputCheckpointFailureWithholdsUsefulBytesAndClosesProvider(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "public"}[public], func(t *testing.T) {
			cancelled := make(chan struct{})
			var calls, checkpoints atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"uncheckpointed-secret-output\"}}],\"usage\":{\"prompt_tokens\":12}}\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(cancelled)
			}))
			defer upstream.Close()
			var finish map[string]any
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/meter"):
					checkpoints.Add(1)
					var v struct {
						Sequence int64 `json:"sequence"`
					}
					json.NewDecoder(r.Body).Decode(&v)
					if v.Sequence == 1 {
						json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": hostedFixtureID, "sequence": 1})
					} else {
						// An explicit permanent API rejection differs from an ambiguous
						// corrupt ack; the latter is covered by the committed-bad-ack test.
						w.WriteHeader(http.StatusConflict)
						io.WriteString(w, `{"error":"[TEST] closed checkpoint"}`)
					}
				case strings.HasSuffix(r.URL.Path, "/finish"):
					select {
					case <-cancelled:
					case <-time.After(time.Second):
						t.Error("provider still open during failure completion")
					}
					json.NewDecoder(r.Body).Decode(&finish)
					io.WriteString(w, `{"finalized":true}`)
				case r.URL.Path == "/api/v1/me":
					io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
				default:
					json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, RequestID: hostedFixtureID, KeyID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual", Endpoint: "http://upstream.test/v1", Revision: 1})
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control, c.onlineSupply, c.publicStateless = api.URL, fixturePublicPolicy(upstream), public
			defer c.shutdown()
			raw := []byte(`{"model":"fixture","stream":true,"messages":[]}`)
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw)))
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			c.executeHosted(w, r, hostedFixtureID, raw)
			if w.Code != 503 || strings.Contains(w.Body.String(), "uncheckpointed-secret-output") || calls.Load() != 1 || checkpoints.Load() != 2 || finish["meterSequence"] != float64(1) || finish["executionClosed"] != true || finish["inputTokens"] != nil {
				t.Fatalf("checkpoint failure released/replayed or misreported: status=%d body=%s executions=%d checkpoints=%d finish=%v", w.Code, w.Body.String(), calls.Load(), checkpoints.Load(), finish)
			}
		})
	}
}
