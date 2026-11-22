package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHostedOutboxRetriesPendingCheckpointBeforeFinish(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	const snapshot = `{"estimator":"text-bpe-v1","encoding":"unknown","responseStarted":true,"reported":{"inputTokens":12},"terminalUsage":false}`
	var report usageReport
	if err = json.Unmarshal([]byte(`{"status":"cancelled","meterSequence":1,"pendingMeter":`+snapshot+`}`), &report); err != nil {
		t.Fatal(err)
	}
	d := hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken}
	c := &core{}
	if err = c.beginHosted(h, "original", d, usageReport{Status: "unknown"}); err != nil {
		t.Fatal(err)
	}
	if err = c.completeHosted(h, "original", d, report); err != nil {
		t.Fatal(err)
	}
	meterCalls, finishes := 0, 0
	allow := false
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+hostedFixtureReportToken || r.Header.Get("X-Api-Key") != "" {
			t.Error("retry rebound original identity")
		}
		if strings.HasSuffix(r.URL.Path, "/meter") {
			meterCalls++
			var v struct {
				Sequence int64           `json:"sequence"`
				Snapshot json.RawMessage `json:"snapshot"`
			}
			json.NewDecoder(r.Body).Decode(&v)
			if v.Sequence != 2 || string(v.Snapshot) != snapshot {
				t.Errorf("retry changed frozen pending checkpoint: %d %s", v.Sequence, v.Snapshot)
			}
			if !allow {
				w.WriteHeader(503)
				return
			}
			io.WriteString(w, `{"accepted":true,"requestId":"`+hostedFixtureID+`","sequence":2}`)
			return
		}
		finishes++
		var v map[string]any
		json.NewDecoder(r.Body).Decode(&v)
		if !allow || v["meterSequence"] != float64(2) || v["inputTokens"] != nil || v["outputTokens"] != nil || v["pendingMeter"] != nil {
			t.Errorf("stale/invalid completion before pending checkpoint ack: %v", v)
		}
		var saved string
		h.db.QueryRow("SELECT payload FROM hosted_usage_outbox").Scan(&saved)
		if !strings.Contains(saved, `"inputTokens":12`) {
			t.Error("accepted partial observation absent in saved native outbox")
		}
		io.WriteString(w, `{"finalized":true}`)
	}))
	defer api.Close()
	c.control = api.URL
	c.flushHosted(context.Background(), h)
	if finishes != 0 {
		t.Fatal("finish bypassed unacknowledged durable checkpoint")
	}
	allow = true
	h.db.Exec("UPDATE hosted_usage_outbox SET next_attempt=0")
	c.flushHosted(context.Background(), h)
	var pending int
	h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox").Scan(&pending)
	if meterCalls != 3 || finishes != 1 || pending != 0 {
		t.Fatalf("outbox checkpoint recovery lost/stuck: meter=%d finish=%d pending=%d", meterCalls, finishes, pending)
	}
}
