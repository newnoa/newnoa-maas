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

func TestHostedCheckpointRetriesIdenticalPayloadAndOnlyReportToken(t *testing.T) {
	var bodies []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/client/hosted/dispatches/"+hostedFixtureID+"/meter" || r.Header.Get("Authorization") != "Bearer "+hostedFixtureReportToken || r.Header.Get("X-Api-Key") != "" {
			t.Error("checkpoint trust boundary changed")
		}
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(raw))
		if len(bodies) == 1 {
			w.WriteHeader(503)
			return
		}
		io.WriteString(w, `{"accepted":true,"requestId":"`+hostedFixtureID+`","sequence":4}`)
	}))
	defer api.Close()
	c := &core{control: api.URL}
	d := hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken}
	if err := c.checkpointHosted(context.Background(), d, 4, json.RawMessage(`{"estimator":"text-heuristic-v1","encoding":"unknown","responseStarted":false,"terminalUsage":false}`)); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || bodies[0] != bodies[1] || !strings.Contains(bodies[0], `"sequence":4`) {
		t.Fatalf("checkpoint retry changed identity/payload: %v", bodies)
	}
}

func TestHostedCheckpointDoesNotAcceptIncompleteAckOrCancelledContext(t *testing.T) {
	for _, ack := range []string{`{}`, `{"accepted":true}`, `{"accepted":true,"requestId":"` + hostedFixtureID + `","sequence":0}`, `{"accepted":true,"requestId":"wrong","sequence":1}`, `{"accepted":true,"requestId":"` + hostedFixtureID + `","sequence":1} {}`} {
		t.Run(ack, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, ack) }))
			defer api.Close()
			c := &core{control: api.URL}
			if err := c.checkpointHosted(context.Background(), hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken}, 1, json.RawMessage(`{}`)); err == nil {
				t.Fatal("invalid checkpoint acknowledgment accepted")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &core{control: "http://127.0.0.1:1"}
	if err := c.checkpointHosted(ctx, hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken}, 1, json.RawMessage(`{}`)); err == nil {
		t.Fatal("cancelled checkpoint accepted")
	}
}

func TestHostedCheckpointValidatesWireBoundsBeforeSending(t *testing.T) {
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		io.WriteString(w, `{"accepted":true,"requestId":"`+hostedFixtureID+`","sequence":9007199254740992}`)
	}))
	defer api.Close()
	c := &core{control: api.URL}
	d := hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken}
	for _, tc := range []struct {
		seq      int64
		snapshot string
	}{
		{9007199254740992, `{}`},
		{1, `{"encoding":"` + strings.Repeat("x", 4096) + `"}`},
	} {
		if err := c.checkpointHosted(context.Background(), d, tc.seq, json.RawMessage(tc.snapshot)); err == nil {
			t.Fatal("out-of-contract checkpoint accepted")
		}
	}
	if calls != 0 {
		t.Fatalf("invalid checkpoint sent to API %d times", calls)
	}
}

func TestHostedOutboxRetainsMeterSequenceAcrossRetry(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	d := hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, DeploymentID: hostedFixtureID, Revision: 1}
	var report usageReport
	if err = json.Unmarshal([]byte(`{"status":"unknown","meterSequence":7,"inputTokens":12,"outputTokens":3}`), &report); err != nil {
		t.Fatal(err)
	}
	c := &core{}
	if err = c.beginHosted(h, "owner", d, report); err != nil {
		t.Fatal(err)
	}
	if err = c.completeHosted(h, "owner", d, report); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		var v map[string]any
		json.NewDecoder(r.Body).Decode(&v)
		if v["meterSequence"] != float64(7) || v["inputTokens"] != nil || v["executionClosed"] != true {
			t.Errorf("lost persisted partial report/sequence: %v", v)
		}
		var persisted string
		h.db.QueryRow("SELECT payload FROM hosted_usage_outbox").Scan(&persisted)
		if !strings.Contains(persisted, `"inputTokens":12`) {
			t.Error("local outbox lost actual counters before lifecycle closure")
		}
		if attempts == 1 {
			w.WriteHeader(503)
			return
		}
		io.WriteString(w, `{"finalized":true}`)
	}))
	defer api.Close()
	c.control = api.URL
	c.flushHosted(context.Background(), h)
	h.db.Exec("UPDATE hosted_usage_outbox SET next_attempt=0")
	c.flushHosted(context.Background(), h)
	var pending int
	h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox").Scan(&pending)
	if attempts != 2 || pending != 0 {
		t.Fatalf("finish attempts=%d pending=%d", attempts, pending)
	}
}

func TestHostedProgressKeepsOutboxActiveAndBoundToOriginalIdentity(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	c := &core{}
	d := hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, DeploymentID: hostedFixtureID, KeyID: hostedFixtureID, Revision: 2}
	if err = c.beginHosted(h, "original-owner", d, usageReport{Status: "unknown"}); err != nil {
		t.Fatal(err)
	}
	report := usageReport{Status: "unknown", MeterSequence: 2, Input: ptrUsage(12)}
	if err = c.persistHostedProgress(context.Background(), h, "other-owner", d, report); err == nil {
		t.Fatal("progress overwrote another account's outbox")
	}
	if err = c.persistHostedProgress(context.Background(), h, "original-owner", d, report); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err = h.db.QueryRow("SELECT payload FROM hosted_usage_outbox WHERE request_id=?", d.RequestID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var saved usageReport
	if json.Unmarshal([]byte(raw), &saved) != nil || saved.MeterSequence != 2 || saved.Input == nil || *saved.Input != 12 || !c.hostedActive[d.RequestID] {
		t.Fatalf("checkpoint progress lost or prematurely finalized: %s", raw)
	}
}
