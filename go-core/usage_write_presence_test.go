package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCacheWritePresencePersistsAndReachesPrivateAndHostedWire(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		want        any
	}{
		{"missing", `"prompt_tokens":12,"completion_tokens":3`, nil},
		{"zero", `"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_write_tokens":0}`, float64(0)},
		{"positive", `"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_write_tokens":2}`, float64(2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := newUsageCapture(false)
			capture.feed([]byte(`{"usage":{` + tc.usage + `}}`))
			report := capture.counts()
			report.RequestID = "request"
			report.NodeID = "node"
			home := t.TempDir()
			h, err := openChatHistory(home)
			if err != nil {
				t.Fatal(err)
			}
			if err = h.queueUsage(report); err != nil {
				t.Fatal(err)
			}
			if err = h.queueHosted("[TEST] account", hostedDispatch{RequestID: "request", DeploymentID: "[TEST] model", KeyID: "[TEST] key", Revision: 1, ReportToken: "[TEST] report"}, report); err != nil {
				t.Fatal(err)
			}
			h.db.Close()
			h, err = openChatHistory(home)
			if err != nil {
				t.Fatal(err)
			}
			defer h.db.Close()
			var wire []map[string]any
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var value map[string]any
				json.NewDecoder(r.Body).Decode(&value)
				wire = append(wire, value)
				json.NewEncoder(w).Encode(map[string]bool{"accepted": true, "finalized": true})
			}))
			defer api.Close()
			n := &connector{nodeID: "node", credential: "[TEST] credential", control: api.URL, client: api.Client(), usageStore: h}
			n.flushUsage(context.Background())
			h.db.Exec("UPDATE hosted_usage_outbox SET next_attempt=0")
			c := &core{control: api.URL}
			c.flushHosted(context.Background(), h)
			if len(wire) != 2 {
				t.Fatal("reports missing", wire)
			}
			for _, value := range wire {
				if value["cacheWriteTokens"] != tc.want {
					t.Fatalf("%s fabricated/lost presence: %#v", tc.name, value)
				}
			}
		})
	}
}

func TestUsageReportReadsLegacyOutboxNumbers(t *testing.T) {
	for _, raw := range []string{`{"cacheWriteTokens":0}`, `{"cacheWriteTokens":2}`} {
		var report usageReport
		if err := json.Unmarshal([]byte(raw), &report); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]any
		json.Unmarshal(encoded, &value)
		if value["cacheWriteTokens"] == nil {
			t.Fatal("old numeric outbox lost compatibility", string(encoded))
		}
	}
}
