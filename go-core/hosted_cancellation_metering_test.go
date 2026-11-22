package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHostedCancelStopsProviderBeforeFinishRetryAndKeepsPartialUsage(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "public"}[public], func(t *testing.T) {
			providerCancelled := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}],\"usage\":{\"prompt_tokens\":12}}\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(providerCancelled)
			}))
			defer upstream.Close()
			var report map[string]any
			var acceptedSnapshot map[string]any
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/meter"):
					var v struct {
						Sequence int64          `json:"sequence"`
						Snapshot map[string]any `json:"snapshot"`
					}
					json.NewDecoder(r.Body).Decode(&v)
					acceptedSnapshot = v.Snapshot
					json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": hostedFixtureID, "sequence": v.Sequence})
				case strings.HasSuffix(r.URL.Path, "/finish"):
					select {
					case <-providerCancelled:
					case <-time.After(time.Second):
						t.Error("finish reporting held provider open")
					}
					json.NewDecoder(r.Body).Decode(&report)
					w.WriteHeader(503)
				case r.URL.Path == "/api/v1/me":
					io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
				default:
					json.NewEncoder(w).Encode(hostedDispatch{RequestID: hostedFixtureID, AccountID: hostedFixtureID, KeyID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual", Endpoint: "http://upstream.test/v1", Revision: 1})
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control, c.onlineSupply, c.publicStateless = api.URL, fixturePublicPolicy(upstream), public
			defer c.shutdown()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			raw := []byte(`{"model":"fixture","stream":true,"messages":[]}`)
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw))).WithContext(ctx)
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("Content-Type", "application/json")
			w := &meterBarrierWriter{ResponseRecorder: httptest.NewRecorder(), useful: make(chan struct{}, 8)}
			done := make(chan struct{})
			go func() { defer close(done); c.executeHosted(w, r, hostedFixtureID, raw) }()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("hosted fixture did not stop")
				}
			}()
			select {
			case <-w.useful:
			case <-time.After(2 * time.Second):
				t.Fatal("hosted content never arrived")
			}
			cancel()
			select {
			case <-providerCancelled:
			case <-time.After(time.Second):
				t.Fatal("hosted provider not promptly cancelled")
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("finish reporting blocked execution close")
			}
			reported, _ := acceptedSnapshot["reported"].(map[string]any)
			if reported["inputTokens"] != float64(12) || reported["outputTokens"] != nil || report["status"] != "cancelled" || report["executionClosed"] != true || report["meterSequence"] == nil {
				t.Fatalf("partial usage was erased or fabricated after cancellation: report=%v snapshot=%v", report, acceptedSnapshot)
			}
			for _, field := range []string{"inputTokens", "outputTokens", "cachedInputTokens", "cacheWriteTokens", "usageSource"} {
				if _, present := report[field]; present {
					t.Errorf("lifecycle-only closure sent %s", field)
				}
			}
			if !public {
				h, _ := c.historyStore()
				var raw string
				if err := h.db.QueryRow("SELECT payload FROM hosted_usage_outbox").Scan(&raw); err != nil || !strings.Contains(raw, `"inputTokens":12`) {
					t.Fatalf("failed finish lost native outbox: %s %v", raw, err)
				}
			}
		})
	}
}
