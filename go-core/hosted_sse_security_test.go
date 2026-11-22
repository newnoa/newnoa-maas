package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHostedSSEProtocolErrorsAreSanitizedWithoutChangingModelText(t *testing.T) {
	const secret = "upstream-secret"
	const chatText = "data: {\"choices\":[{\"delta\":{\"content\":\"model text with upstream-secret and error\"}}]}\n\n"
	const chatUsage = "data: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3,\"prompt_tokens_details\":{\"cached_tokens\":4,\"cache_write_tokens\":2}}}\n\n"
	for _, tc := range []struct {
		name, path, body string
		failure          bool
	}{
		{"chat error", "/v1/chat/completions", "data: {\"error\":{\"message\":\"upstream-secret\"}}\n\n", true},
		{"message error", "/v1/messages", "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"upstream-secret\"}}\n\n", true},
		{"named error", "/v1/responses", "event:error\ndata: {\"message\":\"upstream-secret\"}\n\n", true},
		{"response failed", "/v1/responses", "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"upstream-secret\"}}}\n\n", true},
		{"response incomplete", "/v1/responses", "data: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"upstream-secret\"}}}\n\n", true},
		{"response completed with error status", "/v1/responses", "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"failed\",\"incomplete_details\":{\"reason\":\"upstream-secret\"}}}\n\n", true},
		{"audio error", "/v1/audio/transcriptions", "data: {\"type\":\"error\",\"message\":\"upstream-secret\"}\n\n", true},
		{"multiline crlf error", "/v1/chat/completions", "data: {\r\ndata: \"error\":\"upstream-secret\"}\r\n\r\n", true},
		{"bom error", "/v1/chat/completions", "\xef\xbb\xbfdata: {\"error\":\"upstream-secret\"}\n\n", true},
		{"bare cr error", "/v1/chat/completions", "event:error\rdata: {\"message\":\"upstream-secret\"}\r\r", true},
		{"partial error", "/v1/chat/completions", "data: {\"error\":{\"message\":\"upstream-secret", true},
		{"malformed event", "/v1/chat/completions", "data: upstream-secret\n\n", true},
		{"oversized event", "/v1/chat/completions", "data: {\"error\":\"upstream-secret" + strings.Repeat("x", 256<<10) + "\"}\n\n", true},
		{"chat text and usage", "/v1/chat/completions", chatText + chatUsage + "data: [DONE]\n\n", false},
		{"responses text", "/v1/responses", "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"upstream-secret and error\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":12,\"output_tokens\":3}}}\n\n", false},
		{"message tool text", "/v1/messages", "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":12,\"output_tokens\":0}}}\n\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"error\\\":\\\"upstream-secret\\\"}\"}}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":3}}\n\ndata: {\"type\":\"message_stop\"}\n\n", false},
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
			raw := `{"model":"` + hostedFixtureID + `","stream":true,"input":"hello","messages":[]}`
			contentType := "application/json"
			if tc.path == "/v1/audio/transcriptions" {
				var body bytes.Buffer
				writer := multipart.NewWriter(&body)
				writer.WriteField("model", hostedFixtureID)
				writer.WriteField("stream", "true")
				file, err := writer.CreateFormFile("file", "[TEST].wav")
				if err != nil {
					t.Fatal(err)
				}
				file.Write([]byte("RIFF[TEST]"))
				writer.Close()
				raw, contentType = body.String(), writer.FormDataContentType()
			}
			request := httptest.NewRequest("POST", tc.path, strings.NewReader(raw))
			request.Header.Set("Authorization", "Bearer ses_fixture")
			request.Header.Set("X-Api-Key", "nn_fixture")
			request.Header.Set("Content-Type", contentType)
			response := httptest.NewRecorder()
			c.executeHosted(response, request, hostedFixtureID, []byte(raw))
			report := <-reports
			if executions.Load() != 1 {
				t.Fatalf("executions=%d want=1", executions.Load())
			}
			if response.Code != 200 {
				t.Fatalf("unexpected HTTP status %d: %s", response.Code, response.Body.String())
			}
			if tc.failure {
				wantStatus := "failed"
				if tc.name == "partial error" {
					wantStatus = "unknown"
				}
				if strings.Contains(response.Body.String(), secret) || !strings.Contains(response.Body.String(), "Hosted upstream stream failed") || report.Status != wantStatus {
					t.Fatalf("unsanitized protocol error: status=%s body=%q", report.Status, response.Body.String()[:min(response.Body.Len(), 400)])
				}
			} else {
				if response.Body.String() != tc.body || report.Status != "succeeded" {
					t.Fatalf("model text changed or completion lost: status=%s body=%s", report.Status, response.Body.String())
				}
				if report.Input == nil || *report.Input != 12 || report.Output == nil || *report.Output != 3 {
					t.Fatalf("observed usage changed: %+v", report)
				}
				if tc.name == "chat text and usage" && (report.Cached == nil || *report.Cached != 4 || report.Write == nil || *report.Write != 2) {
					t.Fatalf("cache usage lost: %+v", report)
				}
			}
		})
	}
}

func TestHostedSSEFilterHandlesBoundariesWithoutWholeStreamBuffering(t *testing.T) {
	for _, separator := range []string{"\n", "\r\n", "\r"} {
		for _, size := range []int{1, 7, 32768} {
			safe := ": heartbeat" + separator + separator + "data: {\"choices\":[{\"delta\":{\"content\":\"text with error and upstream-secret\"}}]}" + separator + separator
			f := hostedSSEFilter{protocol: "/v1/chat/completions"}
			// Many complete events must be delivered before EOF; only the current event may wait.
			for event := 0; event < 200; event++ {
				var output []byte
				for raw := []byte(safe); len(raw) > 0; {
					n := min(size, len(raw))
					output = append(output, f.feed(raw[:n])...)
					raw = raw[n:]
					if len(f.pending) > hostedSSEEventLimit {
						t.Fatal("unbounded pending event")
					}
				}
				if string(output) != safe || len(f.pending) != 0 {
					t.Fatalf("safe events changed separator=%q chunk=%d", separator, size)
				}
			}
		}
	}
}

func TestHostedSSEFilterNeverLeaksFragmentedProtocolErrors(t *testing.T) {
	for _, separator := range []string{"\n", "\r\n", "\r"} {
		for _, size := range []int{1, 7, 32768} {
			body := "\xef\xbb\xbfevent: error" + separator + "data: {\"message\":\"upstream-secret\"}" + separator + separator + "data: {\"leaked_after_error\":true}" + separator + separator
			f := hostedSSEFilter{protocol: "/v1/responses"}
			var output []byte
			for raw := []byte(body); len(raw) > 0; {
				n := min(size, len(raw))
				output = append(output, f.feed(raw[:n])...)
				raw = raw[n:]
				if strings.Contains(string(output), "upstream-secret") {
					t.Fatalf("error bytes escaped before event finished: separator=%q size=%d", separator, size)
				}
			}
			output = append(output, f.finish()...)
			if !f.failed || !strings.Contains(string(output), "Hosted upstream stream failed") || strings.Contains(string(output), "leaked_after_error") {
				t.Fatalf("invalid terminal error output %q", output)
			}
		}
	}
}
