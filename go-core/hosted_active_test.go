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
)

func TestHostedActiveExpiredOutboxWaitsForActualCompletion(t *testing.T) {
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		io.WriteString(w, `{"choices":[{"message":{"content":"fixture"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_write_tokens":2,"cached_tokens":4}}}`)
	}))
	defer upstream.Close()
	var finishes atomic.Int32
	reported := make(chan usageReport, 10)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/finish"):
			finishes.Add(1)
			var report usageReport
			decodeHostedFixtureClosure(t, r, &report)
			reported <- report
			io.WriteString(w, `{"finalized":true}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: "hreport_" + strings.Repeat("a", 43), Model: "actual", Endpoint: "http://upstream.test/v1", Revision: 1})
		case r.URL.Path == "/api/v1/me":
			io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
		default:
			t.Errorf("unexpected API path %s", r.URL.Path)
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control = api.URL
	c.onlineSupply = fixturePublicPolicy(upstream)
	defer c.shutdown()
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"`+hostedFixtureID+`"}`))
	r.Header.Set("Authorization", "Bearer ses_fixture")
	r.Header.Set("X-Api-Key", "nn_fixture")
	r.Header.Set("Content-Type", "application/json")
	go func() {
		defer close(done)
		c.executeHosted(httptest.NewRecorder(), r, hostedFixtureID, []byte(`{"model":"`+hostedFixtureID+`"}`))
	}()
	<-started
	if _, err = h.db.Exec("UPDATE hosted_usage_outbox SET next_attempt=0"); err != nil {
		t.Fatal(err)
	}
	c.flushHosted(context.Background(), h)
	if finishes.Load() != 0 {
		once.Do(func() { close(release) })
		<-done
		t.Fatal("active inference was finalized as unknown")
	}
	var racers sync.WaitGroup
	for range 8 {
		racers.Go(func() { c.flushHosted(context.Background(), h) })
	}
	once.Do(func() { close(release) })
	<-done
	racers.Wait()
	c.flushHosted(context.Background(), h)
	if finishes.Load() != 1 {
		t.Fatalf("completion calls=%d", finishes.Load())
	}
	report := <-reported
	if report.Status != "succeeded" || report.Input == nil || *report.Input != 12 || report.Write == nil || *report.Write != 2 {
		t.Fatalf("lost actual completion: %+v", report)
	}
	var pending int
	if err = h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox").Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("pending=%d err=%v", pending, err)
	}
}

func TestHostedCompletionRestoresMissingRowAndRejectsChangedBinding(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	c := &core{}
	d := hostedDispatch{RequestID: hostedFixtureID, DeploymentID: hostedFixtureID, KeyID: hostedFixtureID, Revision: 2, ReportToken: hostedFixtureReportToken}
	if err = c.beginHosted(h, "owner", d, usageReport{Status: "unknown"}); err != nil {
		t.Fatal(err)
	}
	if err = c.completeHosted(h, "other-account", d, usageReport{Status: "succeeded"}); err == nil {
		t.Fatal("completion overwrote another account")
	}
	if c.hostedActive[d.RequestID] {
		t.Fatal("failed completion remained permanently active")
	}
	if _, err = h.db.Exec("DELETE FROM hosted_usage_outbox WHERE request_id=?", d.RequestID); err != nil {
		t.Fatal(err)
	}
	if err = c.completeHosted(h, "owner", d, usageReport{Status: "succeeded", Input: ptrUsage(12), Output: ptrUsage(3), Write: ptrUsage(2)}); err != nil {
		t.Fatal(err)
	}
	if c.hostedActive[d.RequestID] {
		t.Fatal("persisted completion stayed active")
	}
	var raw, owner string
	var next int64
	if err = h.db.QueryRow("SELECT payload,account,next_attempt FROM hosted_usage_outbox WHERE request_id=?", d.RequestID).Scan(&raw, &owner, &next); err != nil {
		t.Fatal(err)
	}
	var report usageReport
	if err = json.Unmarshal([]byte(raw), &report); err != nil {
		t.Fatal(err)
	}
	if owner != "owner" || next != 0 || report.Status != "succeeded" || report.Input == nil || *report.Input != 12 || report.Write == nil || *report.Write != 2 {
		t.Fatalf("terminal outcome lost: owner=%s next=%d report=%+v", owner, next, report)
	}
	h.db.Close()
	if err = c.completeHosted(h, "owner", d, report); err == nil {
		t.Fatal("closed outbox accepted completion")
	}
}

func TestHostedHandlerPersistenceFailureReleasesActiveAndRetriesUnknownWithoutRestart(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	defer c.shutdown()
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := h.db.Exec(`CREATE TRIGGER fail_hosted_completion BEFORE INSERT ON hosted_usage_outbox
		 WHEN NEW.payload LIKE '%"status":"succeeded"%' BEGIN SELECT RAISE(FAIL,'fixture transient storage failure'); END`)
		if err != nil {
			t.Error(err)
		}
		io.WriteString(w, `{"choices":[{"message":{"content":"fixture"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":3}}`)
	}))
	defer upstream.Close()
	var finishes atomic.Int32
	reported := make(chan usageReport, 2)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/finish"):
			var report usageReport
			decodeHostedFixtureClosure(t, r, &report)
			reported <- report
			if finishes.Add(1) == 1 {
				http.Error(w, "retry", 503)
				return
			}
			io.WriteString(w, `{"finalized":true}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual", Endpoint: "http://upstream.test/v1", Revision: 1})
		case r.URL.Path == "/api/v1/me":
			io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
		default:
			t.Errorf("unexpected API path %s", r.URL.Path)
		}
	}))
	defer api.Close()
	c.control, c.onlineSupply = api.URL, fixturePublicPolicy(upstream)
	raw := `{"model":"` + hostedFixtureID + `"}`
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(raw))
	r.Header.Set("Authorization", "Bearer ses_fixture")
	r.Header.Set("X-Api-Key", "nn_fixture")
	r.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	c.executeHosted(response, r, hostedFixtureID, []byte(raw))
	if response.Code != 200 {
		t.Fatalf("upstream result lost: %d", response.Code)
	}
	if len(c.hostedActive) != 0 {
		t.Fatal("finished handler left permanent active protection")
	}
	var payload string
	if err = h.db.QueryRow("SELECT payload FROM hosted_usage_outbox WHERE request_id=?", hostedFixtureID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, `"status":"unknown"`) {
		t.Fatalf("crash-safe fallback lost: %s", payload)
	}
	if _, err = h.db.Exec("DROP TRIGGER fail_hosted_completion"); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err = h.db.Exec("UPDATE hosted_usage_outbox SET next_attempt=0"); err != nil {
			t.Fatal(err)
		}
		c.flushHosted(context.Background(), h)
		report := <-reported
		// Completion failed, but the earlier acknowledged checkpoint and native
		// progress row already saved these real counters before useful output.
		if report.Status != "unknown" || report.Input == nil || *report.Input != 12 || report.Output == nil || *report.Output != 3 || report.MeterSequence != 2 {
			t.Fatalf("checkpointed evidence lost or unsaved outcome presented as complete: %+v", report)
		}
	}
	var pending int
	if err = h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox").Scan(&pending); err != nil || pending != 0 || finishes.Load() != 2 {
		t.Fatalf("recovery pending=%d calls=%d err=%v", pending, finishes.Load(), err)
	}
}
