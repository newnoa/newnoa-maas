package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestUsageCaptureRejectsMalformedCountsWithoutKeepingOldUsage(t *testing.T) {
	cases := []struct{ name, protocol, body string }{
		{"invalid-json", "/v1/chat/completions", `{"usage":`},
		{"non-object", "/v1/chat/completions", `null`},
		{"invalid-usage", "/v1/chat/completions", `{"usage":[]}`},
		{"string-count", "/v1/chat/completions", `{"usage":{"prompt_tokens":"12","completion_tokens":3}}`},
		{"fractional-count", "/v1/chat/completions", `{"usage":{"prompt_tokens":12.5,"completion_tokens":3}}`},
		{"negative-count", "/v1/chat/completions", `{"usage":{"prompt_tokens":12,"completion_tokens":-1}}`},
		{"overflow-count", "/v1/chat/completions", `{"usage":{"prompt_tokens":1000000001,"completion_tokens":3}}`},
		{"write-exceeds-input", "/v1/chat/completions", `{"usage":{"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_write_tokens":13}}}`},
		{"cache-subsets-overlap", "/v1/chat/completions", `{"usage":{"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_write_tokens":8,"cached_tokens":5}}}`},
		{"missing-input-with-cache", "/v1/chat/completions", `{"usage":{"prompt_tokens_details":{"cached_tokens":5}}}`},
		{"native-invalid-delta", "/v1/messages", `{"type":"message_delta","usage":{"output_tokens":"2"}}`},
		{"native-total-overflow", "/v1/messages", `{"usage":{"input_tokens":1000000000,"cache_creation_input_tokens":1}}`},
	}
	for _, tc := range cases {
		for _, stream := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/json", true: "/sse"}[stream], func(t *testing.T) {
				c := newUsageCapture(stream)
				c.protocol = tc.protocol
				if stream {
					c.feed([]byte("data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n"))
					c.feed([]byte("data: " + tc.body + "\n\ndata: {\"type\":\"message_stop\"}\n\ndata: [DONE]\n\n"))
				} else {
					c.feed([]byte(tc.body))
				}
				report := c.counts()
				if !c.invalid || c.completed || report.Input != nil || report.Output != nil || report.Cached != nil || report.Write != nil || report.Status != "failed" {
					t.Fatalf("invalid usage accepted: %+v capture=%+v", report, c)
				}
			})
		}
	}
}

func TestHostedInvalidUsageFailsAndMissingUsageRetainsUnknownCounts(t *testing.T) {
	for _, tc := range []struct {
		name, path, body, status string
		stream                   bool
	}{
		{"invalid-json", "/v1/chat/completions", `{"usage":`, "failed", false},
		{"invalid-count", "/v1/chat/completions", `{"usage":{"prompt_tokens":1,"completion_tokens":"2"}}`, "failed", false},
		{"invalid-native-delta", "/v1/messages", "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":\"2\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n", "failed", true},
		{"missing-usage", "/v1/chat/completions", `{"choices":[]}`, "succeeded", false},
		{"missing-stream-usage", "/v1/chat/completions", "data: {\"choices\":[]}\n\ndata: [DONE]\n\n", "succeeded", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.stream {
					w.Header().Set("Content-Type", "text/event-stream")
				}
				io.WriteString(w, tc.body)
			}))
			defer upstream.Close()
			reported := make(chan usageReport, 1)
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hostedMeterFixtureAck(t, w, r) {
					return
				}
				switch {
				case r.URL.Path == "/api/v1/me":
					io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual", Endpoint: "http://upstream.test/v1", Revision: 1})
				case strings.HasSuffix(r.URL.Path, "/finish"):
					var report usageReport
					decodeHostedFixtureClosure(t, r, &report)
					reported <- report
					io.WriteString(w, `{"finalized":true}`)
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control = api.URL
			c.onlineSupply = fixturePublicPolicy(upstream)
			defer c.shutdown()
			raw := []byte(`{"model":"` + hostedFixtureID + `"}`)
			r := httptest.NewRequest("POST", tc.path, strings.NewReader(string(raw)))
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("X-Api-Key", "nn_fixture")
			r.Header.Set("Content-Type", "application/json")
			c.executeHosted(httptest.NewRecorder(), r, hostedFixtureID, raw)
			select {
			case report := <-reported:
				if tc.name == "invalid-native-delta" {
					// A later invalid output counter cannot erase the valid initial
					// input/output snapshot; these remain partial, never terminal.
					if report.Status != "failed" || report.Input == nil || *report.Input != 10 || report.Output == nil || *report.Output != 0 || report.Cached != nil || report.Write != nil {
						t.Fatalf("initial partial usage erased or malformed delta accepted: %+v", report)
					}
					return
				}
				if report.Status != tc.status || report.Input != nil || report.Output != nil || report.Cached != nil || report.Write != nil {
					t.Fatalf("completion fabricated usage: %+v", report)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("no completion")
			}
		})
	}
}
func TestUsageCaptureMissingUsageKeepsUnknownCounts(t *testing.T) {
	for _, body := range []string{`{"data":[]}`, `{"usage":null}`, `{"usage":{}}`, `{"usage":{"prompt_tokens":0,"completion_tokens":0}}`} {
		for _, stream := range []bool{false, true} {
			c := newUsageCapture(stream)
			if stream {
				c.feed([]byte("data: " + body + "\n\ndata: [DONE]\n\n"))
			} else {
				c.feed([]byte(body))
			}
			report := c.counts()
			if c.invalid {
				t.Fatalf("valid response rejected: %s", body)
			}
			if strings.Contains(body, "prompt_tokens") {
				if report.Input == nil || *report.Input != 0 || report.Output == nil || *report.Output != 0 {
					t.Fatalf("explicit zero lost: %+v", report)
				}
			} else if report.Input != nil || report.Output != nil || report.Cached != nil {
				t.Fatalf("missing usage fabricated: %+v", report)
			}
		}
	}
}

func TestUsageCaptureRetainsExplicitOutputWhenInputIsMissing(t *testing.T) {
	for _, stream := range []bool{false, true} {
		c := newUsageCapture(stream)
		raw := `{"usage":{"completion_tokens":3}}`
		if stream {
			raw = "data: " + raw + "\n\ndata: [DONE]\n\n"
		}
		c.feed([]byte(raw))
		report := c.counts()
		if c.invalid || report.Input != nil || report.Output == nil || *report.Output != 3 {
			t.Fatalf("partial evidence changed: %+v", report)
		}
	}
}
