package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestHostedCheckpointThrottleAndTimeoutRemainAmbiguous(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(status) }))
			defer api.Close()
			c := &core{control: api.URL}
			err := c.checkpointHosted(context.Background(), hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken}, 2, json.RawMessage(`{"responseStarted":true,"terminalUsage":false}`))
			if !errors.Is(err, errHostedCheckpointUnavailable) || calls != 2 {
				t.Fatalf("temporary response discarded retryable observation: status=%d calls=%d err=%v", status, calls, err)
			}
		})
	}
}

func TestHostedCommittedCheckpointBadAckRetainsExactPendingUntilOutboxRetry(t *testing.T) {
	var providerCalls atomic.Int32
	providerClosed := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"withheld-unacknowledged-text\"}}],\"usage\":{\"prompt_tokens\":12}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(providerClosed)
	}))
	defer upstream.Close()
	var allowAck atomic.Bool
	var mu sync.Mutex
	latest := int64(0)
	var committed json.RawMessage
	finishes := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/dispatch-attempts/abort"):
			t.Error("valid admission should not be aborted")
			io.WriteString(w, `{"accepted":true}`)
		case strings.HasSuffix(r.URL.Path, "/meter"):
			var body struct {
				Sequence int64           `json:"sequence"`
				Snapshot json.RawMessage `json:"snapshot"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			mu.Lock()
			defer mu.Unlock()
			if body.Sequence == 1 && latest == 0 {
				latest = 1
			} else if body.Sequence == 2 {
				if committed == nil {
					committed = append(json.RawMessage(nil), body.Snapshot...)
					latest = 2
				} else if string(body.Snapshot) != string(committed) {
					t.Error("same-sequence retry changed committed payload")
					w.WriteHeader(409)
					return
				}
			} else {
				t.Errorf("unexpected sequence %d latest=%d", body.Sequence, latest)
				w.WriteHeader(409)
				return
			}
			if body.Sequence == 2 && !allowAck.Load() {
				io.WriteString(w, `{"accepted":true,"requestId":"wrong","sequence":2}`)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": hostedFixtureID, "sequence": body.Sequence})
		case strings.HasSuffix(r.URL.Path, "/finish"):
			mu.Lock()
			defer mu.Unlock()
			finishes++
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if !allowAck.Load() || body["meterSequence"] != float64(latest) || body["inputTokens"] != nil || body["outputTokens"] != nil {
				t.Errorf("stale finish stranded committed checkpoint or invented usage: %v latest=%d", body, latest)
				w.WriteHeader(409)
				return
			}
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
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw)))
	r.Header.Set("Authorization", "Bearer ses_fixture")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	c.executeHosted(w, r, hostedFixtureID, raw)
	if strings.Contains(w.Body.String(), "withheld-unacknowledged-text") || providerCalls.Load() != 1 {
		t.Fatal("unacked bytes released or inference replayed")
	}
	h, _ := c.historyStore()
	var saved string
	if err := h.db.QueryRow("SELECT payload FROM hosted_usage_outbox").Scan(&saved); err != nil {
		t.Fatal(err)
	}
	var report usageReport
	json.Unmarshal([]byte(saved), &report)
	mu.Lock()
	beforeFinishes := finishes
	mu.Unlock()
	if len(report.PendingMeter) == 0 || report.MeterSequence != 1 || report.Input != nil || beforeFinishes != 0 {
		t.Fatalf("bad ack not retained as ambiguous: report=%s finishCalls=%d", saved, beforeFinishes)
	}
	allowAck.Store(true)
	h.db.Exec("UPDATE hosted_usage_outbox SET next_attempt=0")
	c.flushHosted(context.Background(), h)
	var pending int
	h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox").Scan(&pending)
	mu.Lock()
	defer mu.Unlock()
	if pending != 0 || finishes != 1 || latest != 2 || providerCalls.Load() != 1 {
		t.Fatalf("exact committed checkpoint retry stranded: rows=%d finishes=%d latest=%d providers=%d", pending, finishes, latest, providerCalls.Load())
	}
}

func TestHostedExpiredPendingRejectFinishesLastAcceptedEvidenceOnly(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	d := hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken}
	c := &core{}
	report := usageReport{Status: "unknown", MeterSequence: 1, Input: ptrUsage(7), Output: ptrUsage(1), PendingMeter: json.RawMessage(`{"estimator":"text-bpe-v1","encoding":"unknown","estimatedInputTokens":123,"estimatedOutputTokens":456,"responseStarted":true,"reported":{"inputTokens":12},"terminalUsage":false}`)}
	if err = c.beginHosted(h, "original", d, usageReport{Status: "unknown"}); err != nil {
		t.Fatal(err)
	}
	if err = c.completeHosted(h, "original", d, report); err != nil {
		t.Fatal(err)
	}
	meterCalls, finishes := 0, 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+hostedFixtureReportToken {
			t.Error("lost original report authority")
		}
		if strings.HasSuffix(r.URL.Path, "/meter") {
			meterCalls++
			w.WriteHeader(409)
			io.WriteString(w, `{"error":"[TEST] request expired in provisional phase"}`)
			return
		}
		finishes++
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["meterSequence"] != float64(1) || body["inputTokens"] != nil || body["outputTokens"] != nil || body["cachedInputTokens"] != nil || body["cacheWriteTokens"] != nil || body["status"] != "unknown" || body["pendingMeter"] != nil {
			t.Errorf("unaccepted pending evidence replaced last ack: %v", body)
		}
		var saved string
		h.db.QueryRow("SELECT payload FROM hosted_usage_outbox").Scan(&saved)
		if !strings.Contains(saved, `"inputTokens":7`) || !strings.Contains(saved, `"outputTokens":1`) {
			t.Error("last accepted partial observation was replaced by rejected pending")
		}
		if finishes == 1 {
			w.WriteHeader(503)
			return
		}
		io.WriteString(w, `{"finalized":true}`)
	}))
	defer api.Close()
	c.control = api.URL
	c.flushHosted(context.Background(), h)
	var saved, lastError string
	h.db.QueryRow("SELECT payload,last_error FROM hosted_usage_outbox").Scan(&saved, &lastError)
	if meterCalls != 1 || finishes != 1 || strings.Contains(saved, "pendingMeter") {
		t.Fatalf("permanent reject still blocks outbox: meter=%d finish=%d saved=%s diagnostic=%s", meterCalls, finishes, saved, lastError)
	}
	h.db.Exec("UPDATE hosted_usage_outbox SET next_attempt=0")
	c.flushHosted(context.Background(), h)
	var pending int
	h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox").Scan(&pending)
	if pending != 0 || meterCalls != 1 || finishes != 2 {
		t.Fatalf("expiry fallback stuck/reposted rejected checkpoint: meter=%d finish=%d rows=%d", meterCalls, finishes, pending)
	}
}
