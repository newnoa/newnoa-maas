package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestExecuteHostedClassifiesJSONFallbackAndLargeResponses(t *testing.T) {
	const secret = "fixture-upstream-secret"
	const chat = `{"choices":[{"message":{"content":"normal error text fixture-upstream-secret"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":3}}`
	const response = `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"normal error text fixture-upstream-secret"}]}],"usage":{"input_tokens":12,"output_tokens":3}}`
	large := `{"type":"response.completed","response":{"status":"completed","output":[{"text":"` + strings.Repeat("x", 300*1024) + `"}],"usage":{"input_tokens":12,"output_tokens":70000,"input_tokens_details":{"cached_tokens":4,"cache_write_tokens":2}}}}`
	for _, tc := range []struct {
		name, path, media, body string
		failure                 bool
		output                  int64
	}{
		{"chat JSON error", "/v1/chat/completions", "application/json", `{"error":{"message":"` + secret + `"}}`, true, 0},
		{"pretty JSON error", "/v1/chat/completions", "application/json; charset=utf-8", " \n{\n  \"error\": {\"message\":\"" + secret + "\"}\n}\n", true, 0},
		{"BOM JSON error", "/v1/chat/completions", "application/json", "\xef\xbb\xbf" + `{"error":"` + secret + `"}`, true, 0},
		{"mislabeled SSE JSON error", "/v1/chat/completions", "text/event-stream", `{"error":{"message":"` + secret + `"}}`, true, 0},
		{"missing content type JSON error", "/v1/chat/completions", "", `{"error":"` + secret + `"}`, true, 0},
		{"Messages JSON error", "/v1/messages", "application/json", `{"type":"error","error":{"type":"api_error","message":"` + secret + `"}}`, true, 0},
		{"Responses failed JSON", "/v1/responses", "application/json", `{"status":"failed","error":{"message":"` + secret + `"}}`, true, 0},
		{"Responses incomplete JSON", "/v1/responses", "application/json", `{"status":"incomplete","incomplete_details":{"reason":"` + secret + `"}}`, true, 0},
		{"unclassified plain error", "/v1/chat/completions", "text/plain", secret, true, 0},
		{"unterminated error field", "/v1/chat/completions", "text/plain", "error: " + secret, true, 0},
		{"unterminated invalid key text", "/v1/chat/completions", "text/plain", "invalid API key: " + secret, true, 0},
		{"error field complete", "/v1/chat/completions", "text/event-stream", "error: " + secret + "\n\n", true, 0},
		{"unknown field after comment", "/v1/chat/completions", "text/plain", ": heartbeat\n\ninvalid API key: " + secret + "\n\n", true, 0},
		{"unclassified JSON error", "/v1/chat/completions", "application/json", `{"type":"invalid_request_error","message":"` + secret + `"}`, true, 0},
		{"malformed JSON", "/v1/chat/completions", "application/json", `{"error":"` + secret, true, 0},
		{"truncated JSON transport", "/v1/chat/completions", "application/json", `{"choices":[{"message":{"content":"` + secret, true, 0},
		{"Chat legitimate JSON", "/v1/chat/completions", "application/json", chat, false, 3},
		{"Chat pretty JSON", "/v1/chat/completions", "application/json", "\n  " + chat + "\n", false, 3},
		{"Chat SSE mislabeled JSON", "/v1/chat/completions", "text/event-stream", chat, false, 3},
		{"SSE comment and valid fields", "/v1/chat/completions", "text/plain", ": heartbeat\n\nid: fixture\nretry: 100\nevent: message\ndata: {\"choices\":[{\"delta\":{\"content\":\"normal error text " + secret + "\"}}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n", false, 3},
		{"Responses legitimate JSON", "/v1/responses", "application/json", response, false, 3},
		{"Responses JSON 300KiB", "/v1/responses", "application/json", strings.TrimSuffix(strings.TrimPrefix(large, `{"type":"response.completed","response":`), `}`), false, 70000},
		{"Responses SSE 300KiB", "/v1/responses", "text/event-stream", "event: response.completed\ndata: " + large + "\n\n", false, 70000},
		{"Responses SSE above bound", "/v1/responses", "text/event-stream", "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[{\"text\":\"" + strings.Repeat("x", historyBodyLimit) + secret + "\"}]}}\n\n", true, 0},
		{"JSON above bound", "/v1/chat/completions", "application/json", `{"choices":[{"message":{"content":"` + strings.Repeat("x", historyBodyLimit) + secret + `"}}]}`, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if tc.media != "" {
					w.Header().Set("Content-Type", tc.media)
				}
				if tc.name == "truncated JSON transport" {
					w.Header().Set("Content-Length", strconv.Itoa(len(tc.body)+100))
				}
				for body := tc.body; len(body) > 0; {
					n := min(8191, len(body))
					if _, err := io.WriteString(w, body[:n]); err != nil {
						return
					}
					body = body[n:]
				}
			}))
			defer upstream.Close()
			reports := make(chan usageReport, 1)
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hostedMeterFixtureAck(t, w, r) {
					return
				}
				switch {
				case r.URL.Path == "/api/v1/me":
					io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual", Endpoint: "http://upstream.test/v1", APIKey: secret, Revision: 1})
				case strings.HasSuffix(r.URL.Path, "/finish"):
					var report usageReport
					if err := decodeHostedFixtureClosure(t, r, &report); err != nil {
						t.Error(err)
					}
					reports <- report
					io.WriteString(w, `{"finalized":true}`)
				default:
					t.Errorf("unexpected API path %s", r.URL.Path)
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control, c.onlineSupply = api.URL, fixturePublicPolicy(upstream)
			defer c.shutdown()
			raw := []byte(`{"model":"` + hostedFixtureID + `","stream":true,"messages":[{"role":"user","content":"hi"}],"input":"hi"}`)
			r := httptest.NewRequest("POST", tc.path, strings.NewReader(string(raw)))
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("X-Api-Key", "nn_fixture")
			r.Header.Set("Content-Type", "application/json")
			out := httptest.NewRecorder()
			c.executeHosted(out, r, hostedFixtureID, raw)
			report := <-reports
			if calls.Load() != 1 {
				t.Fatal("request replayed or never executed", calls.Load())
			}
			if tc.failure {
				wantStatus := "failed"
				if tc.name == "truncated JSON transport" || strings.HasPrefix(tc.name, "unterminated") {
					wantStatus = "unknown"
				}
				if strings.Contains(out.Body.String(), secret) || report.Status != wantStatus || !strings.Contains(out.Body.String(), "Hosted upstream") {
					t.Fatalf("unsafe response: HTTP=%d status=%s bytes=%d secret=%v", out.Code, report.Status, out.Body.Len(), strings.Contains(out.Body.String(), secret))
				}
			} else {
				if out.Code != 200 || out.Body.String() != tc.body || report.Status != "succeeded" {
					t.Fatalf("normal response changed: HTTP=%d status=%s got=%d want=%d", out.Code, report.Status, out.Body.Len(), len(tc.body))
				}
				if report.Input == nil || *report.Input != 12 || report.Output == nil || *report.Output != tc.output {
					t.Fatalf("actual usage lost: %+v", report)
				}
				if strings.Contains(tc.name, "300KiB") && (report.Cached == nil || *report.Cached != 4 || report.Write == nil || *report.Write != 2) {
					t.Fatalf("large response cache usage lost: %+v", report)
				}
				if strings.Contains(tc.name, "JSON") && !strings.HasPrefix(out.Header().Get("Content-Type"), "application/json") {
					t.Fatal("JSON fallback kept SSE content type", out.Header())
				}
			}
		})
	}
}
