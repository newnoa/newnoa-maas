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

func TestPrivateUsageCaptureReadsJSONAndChunkedSSEWithoutDoubleCounting(t *testing.T) {
	for _, sse := range []bool{false, true} {
		c := newUsageCapture(sse)
		raw := `{"usage":{"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":4}}}`
		if sse {
			raw = "data: " + raw + "\n\ndata: [DONE]\n\n"
		}
		for _, part := range []string{raw[:10], raw[10:]} {
			c.feed([]byte(part))
		}
		v := c.counts()
		if v.Input == nil || *v.Input != 12 || v.Output == nil || *v.Output != 3 || v.Cached == nil || *v.Cached != 4 {
			t.Fatalf("usage: %+v", v)
		}
	}
	c := newUsageCapture(true)
	c.feed([]byte("data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":4,\"cache_creation_input_tokens\":2,\"output_tokens\":0}}}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":3}}\n\n"))
	v := c.counts()
	if v.Input == nil || *v.Input != 16 || v.Cached == nil || *v.Cached != 4 || v.Output == nil || *v.Output != 3 {
		t.Fatalf("native usage: %+v", v)
	}
}
func TestUsageOutboxSurvivesReopenAndNeverFlushesAnotherNode(t *testing.T) {
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	report := usageReport{RequestID: "request-a", NodeID: "node-a", Epoch: 1, GatewayID: "server-a", Input: ptrUsage(12)}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/connector/usage" || r.Header.Get("Authorization") != "Bearer node_secret" {
			t.Error("wrong request")
		}
		json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
	}))
	defer api.Close()

	if err = h.queueUsage(report); err != nil {
		t.Fatal(err)
	}
	h.db.Close()
	h, err = openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	n := &connector{nodeID: "node-b", credential: "node_secret", control: api.URL, client: api.Client(), usageStore: h}
	n.flushUsage(context.Background())
	var pending int
	h.db.QueryRow("SELECT count(*) FROM private_usage_outbox").Scan(&pending)
	if pending != 1 {
		t.Fatal("flushed another node")
	}
	n.nodeID = "node-a"
	n.flushUsage(context.Background())
	h.db.QueryRow("SELECT count(*) FROM private_usage_outbox").Scan(&pending)
	if pending != 0 {
		t.Fatal("acknowledged usage not cleared")
	}
}
func ptrUsage(v int64) *int64 { return &v }
func TestPrivateCompletionOutboxFinalizesBeforeUsageAndRetries(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	calls := []string{}
	fail := true
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		if r.URL.Path == "/api/v1/connector/dispatches/request-a/private-finish" {
			if fail {
				w.WriteHeader(503)
				return
			}
			json.NewEncoder(w).Encode(map[string]bool{"finalized": true})
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
	}))
	defer api.Close()
	if err = h.queueUsage(usageReport{RequestID: "request-a", NodeID: "node-a", Epoch: 1, GatewayID: "fixture", Status: "unknown"}); err != nil {
		t.Fatal(err)
	}
	n := &connector{nodeID: "node-a", credential: "node_secret", control: api.URL, client: api.Client(), usageStore: h}
	n.flushUsage(context.Background())
	if len(calls) != 1 {
		t.Fatal("usage sent before completion acknowledgement")
	}
	fail = false
	h.db.Exec("UPDATE private_usage_outbox SET next_attempt=0")
	n.flushUsage(context.Background())
	var count int
	h.db.QueryRow("SELECT count(*) FROM private_usage_outbox").Scan(&count)
	if count != 0 || len(calls) != 3 || calls[1] != "/api/v1/connector/dispatches/request-a/private-finish" || calls[2] != "/api/v1/connector/usage" {
		t.Fatalf("pending=%d calls=%v", count, calls)
	}
}
func TestCloudUsageRetriesWithoutDependingOnGoServer(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	var calls int
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(503)
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
	}))
	defer api.Close()
	h.queueUsage(usageReport{RequestID: "cloud-a", NodeID: "node-a", GatewayID: "server-a", Epoch: 1, Input: ptrUsage(5)})
	n := &connector{nodeID: "node-a", credential: "node_secret", control: api.URL, client: api.Client(), usageStore: h}
	n.flushUsage(context.Background())
	var pending int
	h.db.QueryRow("SELECT count(*) FROM private_usage_outbox").Scan(&pending)
	if pending != 1 {
		t.Fatal("failed cloud upload lost")
	}
	h.db.Exec("UPDATE private_usage_outbox SET next_attempt=0")
	n.flushUsage(context.Background())
	h.db.QueryRow("SELECT count(*) FROM private_usage_outbox").Scan(&pending)
	if pending != 0 || calls != 2 {
		t.Fatalf("pending=%d calls=%d", pending, calls)
	}
}

func TestPrivateUsageMarksProviderCompletionEvents(t *testing.T) {
	for _, raw := range []string{`{"type":"message_stop"}`, `{"type":"response.completed","response":{"usage":{"input_tokens":7,"output_tokens":2}}}`, `{"choices":[{"finish_reason":"stop"}]}`} {
		capture := newUsageCapture(false)
		capture.feed([]byte(raw))
		capture.counts()
		if !capture.completed {
			t.Fatal("provider completion marker lost", raw)
		}
	}
}

func TestConnectorPrivateUsageCompletesThroughAuthenticatedOutbox(t *testing.T) {
	for _, mode := range []string{"success", "upstream-failure", "confirmation-pending", "multiline-stream", "incomplete-stream"} {
		t.Run(mode, func(t *testing.T) {
			store, err := openChatHistory(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer store.db.Close()
			uploaded := make(chan usageReport, 1)
			confirmations := 0
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer node_fixture" {
					t.Error("private usage lost node identity")
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/consume"):
					json.NewEncoder(w).Encode(map[string]any{"endpoint": "http://127.0.0.1:12345", "model": "fixture", "edition": "domestic", "usageReporting": true})
				case strings.HasSuffix(r.URL.Path, "/private-transport"):
					confirmations++
					if mode == "confirmation-pending" {
						w.WriteHeader(503)
						return
					}
					io.WriteString(w, `{"confirmed":true}`)
				case strings.HasSuffix(r.URL.Path, "/private-finish"):
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					status := "succeeded"
					if mode == "upstream-failure" {
						status = "failed"
					}
					if mode == "incomplete-stream" {
						status = "unknown"
					}
					if body["status"] != status || body["transportConfirmed"] != true {
						t.Error("private finish does not bind actual provider outcome", body)
					}
					io.WriteString(w, `{"finalized":true}`)
				case strings.HasSuffix(r.URL.Path, "/usage"):
					var report usageReport
					json.NewDecoder(r.Body).Decode(&report)
					uploaded <- report
					io.WriteString(w, `{"accepted":true}`)
				default:
					t.Error("unexpected private usage route", r.URL.Path)
				}
			}))
			defer control.Close()
			n := &connector{nodeID: "node-fixture", control: control.URL, credential: "node_fixture", client: control.Client(), usageStore: store, upstream: &http.Client{Transport: connectorFlushTransport(func(r *http.Request) (*http.Response, error) {
				status := 200
				if mode == "upstream-failure" {
					status = 500
				}
				body := `{"choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"prompt_tokens_details":{"cache_write_tokens":1}}}`
				contentType := "application/json"
				if mode == "multiline-stream" || mode == "incomplete-stream" {
					contentType = "text/event-stream"
					body = "data: {\"usage\":\ndata: {\"prompt_tokens\":3,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cache_write_tokens\":1}}}\n\ndata: [DONE]\n\n"
					if mode == "incomplete-stream" {
						body += "data: {"
					}
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture"}`))
			r.Header.Set("X-Newnoa-Request", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
			r.Header.Set("X-Newnoa-Permit", "permit_fixture")
			r.Header.Set("X-Newnoa-Gateway", "fixture")
			r.Header.Set("X-Newnoa-Epoch", "1")
			w := httptest.NewRecorder()
			n.ServeHTTP(w, r)
			select {
			case report := <-uploaded:
				if report.Input == nil || *report.Input != 3 || report.Output == nil || *report.Output != 2 || report.Write == nil || *report.Write != 1 {
					t.Fatal("private usage counts changed", report)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("private completion/usage not flushed")
			}
			n.usageMu.Lock()
			n.usageMu.Unlock()
			if confirmations != 1 {
				t.Fatal("missing provider transport acknowledgement")
			}
		})
	}
}

func TestUsageBacklogPausesAndRecoversWithoutDroppingRecords(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	_, err = h.db.Exec("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<?) INSERT INTO private_usage_outbox(request_id,node_id,payload) SELECT x,'fixture','{}' FROM n", usageBacklogHighWater)
	if err != nil {
		t.Fatal(err)
	}
	n := &connector{usageStore: h, purpose: "serving"}
	n.checkUsageBacklog(context.Background())
	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	r.Header.Set("X-Newnoa-Request", "12345678-1234-1234-1234-123456789abc")
	r.Header.Set("X-Newnoa-Permit", "permit_fixture")
	r.Header.Set("X-Newnoa-Gateway", "fixture")
	r.Header.Set("X-Newnoa-Epoch", "1")
	w := httptest.NewRecorder()
	n.ServeHTTP(w, r)
	if w.Code != 503 || w.Header().Get("X-Newnoa-Outcome") != "not-started" {
		t.Fatalf("backlog admission: %d %s", w.Code, w.Body.String())
	}
	var count int
	if err := h.db.QueryRow("SELECT count(*) FROM private_usage_outbox").Scan(&count); err != nil || count != usageBacklogHighWater {
		t.Fatalf("pending reports: %d %v", count, err)
	}
	if _, err := h.db.Exec("DELETE FROM private_usage_outbox WHERE request_id='1'"); err != nil {
		t.Fatal(err)
	}
	n.checkUsageBacklog(context.Background())
	if n.usageQueuePaused {
		t.Fatal("admission did not resume below threshold")
	}
}
