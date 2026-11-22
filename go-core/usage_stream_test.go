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

const streamUsage = `{"usage":{"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":4}}}`

func TestUsageStreamEventBoundariesAndCompletion(t *testing.T) {
	for _, tc := range []struct {
		name, protocol, body string
		complete, invalid    bool
	}{
		{"chat", "/v1/chat/completions", "data: " + streamUsage + "\n\ndata: [DONE]\n\n", true, false},
		{"multiline CRLF", "/v1/chat/completions", "event: chunk\r\ndata: {\"usage\":\r\ndata: {\"prompt_tokens\":12,\"completion_tokens\":3,\"prompt_tokens_details\":{\"cached_tokens\":4}}}\r\n\r\ndata: [DONE]\r\n\r\n: keepalive\r\n\r\n", true, false},
		{"finish reason without sentinel", "/v1/chat/completions", "data: " + streamUsage + "\n\ndata: {\"choices\":[{\"finish_reason\":\"stop\"}]}\n\n", false, false},
		{"missing blank line", "/v1/chat/completions", "data: " + streamUsage + "\n\ndata: [DONE]\n", false, false},
		{"partial line after done", "/v1/chat/completions", "data: " + streamUsage + "\n\ndata: [DONE]\n\ndata: {", false, false},
		{"partial event after done", "/v1/chat/completions", "data: " + streamUsage + "\n\ndata: [DONE]\n\ndata: {}\n", false, false},
		{"data after done", "/v1/chat/completions", "data: " + streamUsage + "\n\ndata: [DONE]\n\ndata: {}\n\n", false, true},
		{"foreign message stop", "/v1/chat/completions", "data: {\"type\":\"message_stop\"}\n\n", false, false},
		{"responses sentinel", "/v1/responses", "data: [DONE]\n\n", false, true},
		{"messages sentinel", "/v1/messages", "data: [DONE]\n\n", false, true},
		{"asr sentinel without native end", "/v1/audio/transcriptions", "data: [DONE]\n\n", false, true},
		{"responses incomplete status", "/v1/responses", "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"incomplete\"}}\n\n", false, true},
		{"responses complete", "/v1/responses", "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n", true, false},
		{"native message end", "/v1/messages", "data: {\"type\":\"message_stop\"}\n\n", true, false},
		{"native asr end and optional sentinel", "/v1/audio/transcriptions", "data: {\"type\":\"transcript.text.done\"}\n\ndata: [DONE]\n\n", true, false},
	} {
		for _, size := range []int{1, 7, len(tc.body)} {
			t.Run(tc.name+"/chunk="+strconv.Itoa(size), func(t *testing.T) {
				capture := newUsageCapture(true)
				capture.protocol = tc.protocol
				for left := tc.body; len(left) > 0; {
					n := min(size, len(left))
					capture.feed([]byte(left[:n]))
					left = left[n:]
				}
				report := capture.counts()
				if capture.completed != tc.complete || capture.invalid != tc.invalid {
					t.Fatalf("completed=%v invalid=%v want %v/%v", capture.completed, capture.invalid, tc.complete, tc.invalid)
				}
				if tc.complete && strings.Contains(tc.body, "prompt_tokens") && (report.Input == nil || *report.Input != 12 || report.Output == nil || *report.Output != 3 || report.Cached == nil || *report.Cached != 4) {
					t.Fatalf("lost multiline usage: %+v", report)
				}
			})
		}
	}
}

func TestUsageStreamBoundsWholeEvents(t *testing.T) {
	for _, body := range []string{
		"data: " + strings.Repeat("x", 256<<10) + "\n\n",
		strings.Repeat("data: "+strings.Repeat("x", 1024)+"\n", 257),
	} {
		capture := newUsageCapture(true)
		for left := body; len(left) > 0; {
			n := min(4096, len(left))
			capture.feed([]byte(left[:n]))
			left = left[n:]
		}
		if !capture.overflow || !capture.invalid || capture.completed || len(capture.pending) != 0 || len(capture.event) != 0 {
			t.Fatalf("unbounded event retained or accepted: overflow=%v invalid=%v pending=%d", capture.overflow, capture.invalid, len(capture.pending))
		}
	}
}

func TestHostedStreamIntegrityReportsObservedCompletionWithoutReplay(t *testing.T) {
	for _, tc := range []struct{ name, body, status, output string }{
		{"multiline", "data: {\"usage\":\ndata: {\"prompt_tokens\":12,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n", "succeeded", ""},
		{"unfinished sentinel", "data: " + streamUsage + "\n\ndata: [DONE]\n", "unknown", ""},
		// A truncated JSON event may contain an upstream error: emit a safe error, retaining observed usage.
		{"unfinished tail", "data: " + streamUsage + "\n\ndata: [DONE]\n\ndata: {", "unknown", "data: " + streamUsage + "\n\ndata: [DONE]\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Hosted upstream stream failed\"}}\n\n"},
		{"finish without sentinel", "data: " + streamUsage + "\n\ndata: {\"choices\":[{\"finish_reason\":\"stop\"}]}\n\n", "unknown", ""},
		{"event after end", "data: " + streamUsage + "\n\ndata: [DONE]\n\ndata: {}\n\n", "failed", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var executions atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				executions.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, tc.body)
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
					json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual", Endpoint: "http://upstream.test/v1", Revision: 1})
				case strings.HasSuffix(r.URL.Path, "/finish"):
					var reported usageReport
					if err := decodeHostedFixtureClosure(t, r, &reported); err != nil {
						t.Error(err)
					}
					reports <- reported
					io.WriteString(w, `{"finalized":true}`)
				default:
					t.Errorf("unexpected API request %s", r.URL.Path)
				}
			}))
			defer api.Close()
			core := newCore(t.TempDir(), "http://127.0.0.1:1")
			core.control, core.onlineSupply = api.URL, fixturePublicPolicy(upstream)
			defer core.shutdown()
			raw := `{"model":"` + hostedFixtureID + `","stream":true}`
			request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(raw))
			request.Header.Set("Authorization", "Bearer ses_fixture")
			request.Header.Set("X-Api-Key", "nn_fixture")
			response := httptest.NewRecorder()
			core.executeHosted(response, request, hostedFixtureID, []byte(raw))
			var reported usageReport
			select {
			case reported = <-reports:
			default:
				t.Fatal("no completion report")
			}
			wantBody := tc.body
			if tc.output != "" {
				wantBody = tc.output
			}
			if tc.name == "unfinished sentinel" || tc.name == "event after end" {
				// Only complete checkpointed events can precede the fixed safe error.
				// A pending sentinel or invalid post-terminal event is never passed through.
				wantBody = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Hosted upstream stream failed\"}}\n\n"
				if tc.name == "unfinished sentinel" {
					wantBody = "data: " + streamUsage + "\n\n" + wantBody
				}
				if response.Body.String() != wantBody || response.Code != 200 {
					t.Fatalf("uncheckpointed/invalid event escaped: %d %q", response.Code, response.Body.String())
				}
			} else if response.Body.String() != wantBody || response.Code != 200 {
				t.Fatalf("upstream stream changed: %d %q", response.Code, response.Body.String())
			}
			if reported.Status != tc.status || executions.Load() != 1 {
				t.Fatalf("status=%s executions=%d want=%s/1", reported.Status, executions.Load(), tc.status)
			}
			if reported.Input == nil || *reported.Input != 12 || reported.Output == nil || *reported.Output != 3 || reported.MeterSequence < 2 {
				t.Fatalf("observed usage lost: %+v", reported)
			}
		})
	}
}
