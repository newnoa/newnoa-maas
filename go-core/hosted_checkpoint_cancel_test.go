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

// An API may commit an output snapshot just before downstream cancellation loses
// its acknowledgment. Recover that identical checkpoint only after closing the
// provider; never leave native outbox finish stuck behind its stale sequence.
func TestHostedCancelledCheckpointRecoversAckAfterProviderClose(t *testing.T) {
	providerClosed := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}],\"usage\":{\"prompt_tokens\":12}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(providerClosed)
	}))
	defer upstream.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var saved, finish map[string]any
	attempts := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/meter"):
			var b map[string]any
			json.NewDecoder(r.Body).Decode(&b)
			if b["sequence"] == float64(2) {
				attempts++
				if saved == nil {
					saved = b
					cancel()
					<-r.Context().Done()
					return
				}
				select {
				case <-providerClosed:
				case <-time.After(time.Second):
					t.Error("ambiguous-ack recovery kept provider open")
				}
				before, _ := json.Marshal(saved)
				after, _ := json.Marshal(b)
				if string(before) != string(after) {
					t.Error("ambiguous retry changed sequence/snapshot")
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": hostedFixtureID, "sequence": b["sequence"]})
		case strings.HasSuffix(r.URL.Path, "/finish"):
			json.NewDecoder(r.Body).Decode(&finish)
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
	raw := []byte(`{"model":"fixture","stream":true,"messages":[]}`)
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw))).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer ses_fixture")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	c.executeHosted(w, r, hostedFixtureID, raw)
	if attempts != 2 || finish["meterSequence"] != float64(2) || finish["inputTokens"] != nil || finish["outputTokens"] != nil || finish["status"] != "cancelled" || strings.Contains(w.Body.String(), "hello") {
		t.Fatalf("ambiguous checkpoint lost/escaped: attempts=%d finish=%v body=%s", attempts, finish, w.Body.String())
	}
	snapshot, _ := saved["snapshot"].(map[string]any)
	usage, _ := snapshot["reported"].(map[string]any)
	if usage["inputTokens"] != float64(12) || usage["outputTokens"] != nil {
		t.Fatal("durable partial evidence lost", saved)
	}
}
