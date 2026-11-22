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

func TestExecuteHostedNonstreamJSONRequiresKnownResultEnvelope(t *testing.T) {
	const secret = "fixture-upstream-secret"
	const usage = `,"usage":{"input_tokens":12,"output_tokens":3}`
	for _, tc := range []struct {
		name, path, media, body string
		valid                   bool
	}{
		{"Chat typed error", "/v1/chat/completions", "application/json", `{"type":"invalid_request_error","message":"` + secret + `"}`, false},
		{"Chat generic message", "/v1/chat/completions", "application/json", `{"message":"` + secret + `"}`, false},
		{"Chat BOM error", "/v1/chat/completions", "application/json", "\xef\xbb\xbf" + `{"code":"bad_key","message":"` + secret + `"}`, false},
		{"Chat mislabeled error", "/v1/chat/completions", "text/plain", `{"type":"rate_limit_error","message":"` + secret + `"}`, false},
		{"Chat missing media error", "/v1/chat/completions", "", `{"detail":"` + secret + `"}`, false},
		{"Chat wrong result type", "/v1/chat/completions", "application/json", `{"choices":"` + secret + `"}`, false},
		{"Chat null result", "/v1/chat/completions", "application/json", `{"choices":null,"message":"` + secret + `"}`, false},
		{"Responses unknown error", "/v1/responses", "application/json", `{"type":"invalid_request_error","message":"` + secret + `"}`, false},
		{"Messages unknown error", "/v1/messages", "application/json", `{"type":"authentication_error","message":"` + secret + `"}`, false},
		{"ASR unknown error", "/v1/audio/transcriptions", "application/json", `{"code":"bad_key","message":"` + secret + `"}`, false},
		{"ASR null text", "/v1/audio/transcriptions", "application/json", `{"text":null,"message":"` + secret + `"}`, false},
		{"Images unknown error", imageGenerationPath, "application/json", `{"type":"invalid_request_error","message":"` + secret + `"}`, false},
		{"Images null data", imageGenerationPath, "application/json", `{"data":null,"message":"` + secret + `"}`, false},
		{"Chat normal model text", "/v1/chat/completions", "application/json", `{"choices":[{"message":{"content":"invalid_request_error and ` + secret + `"},"finish_reason":"stop"}]` + usage + `}`, true},
		{"Responses normal output", "/v1/responses", "application/json", `{"status":"completed","output":[{"text":"invalid API key: ` + secret + `"}]` + usage + `}`, true},
		{"Messages normal content", "/v1/messages", "application/json", `{"type":"message","content":[{"type":"text","text":"error ` + secret + `"}]` + usage + `}`, true},
		{"ASR normal text", "/v1/audio/transcriptions", "application/json", `{"text":"error ` + secret + `"` + usage + `}`, true},
		{"Images normal data", imageGenerationPath, "application/json", `{"data":[{"url":"https://images.example.test/` + secret + `.png"}]` + usage + `}`, true},
		{"Embeddings compatibility", "/v1/embeddings", "application/json", `{"data":[{"embedding":[0.1,0.2],"text":"normal error text ` + secret + `"}]` + usage + `}`, true},
		{"Rerank compatibility", "/v1/rerank", "application/json", `{"results":[{"index":0,"relevance_score":0.9,"document":{"text":"normal error text ` + secret + `"}}]` + usage + `}`, true},
		{"Decision compatibility", "/v1/systemone", "application/json", `{"result":[{"decision":"normal error text ` + secret + `"}]` + usage + `}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if tc.media != "" {
					w.Header().Set("Content-Type", tc.media)
				}
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
					json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Endpoint: "http://upstream.test/v1", Model: "actual", Revision: 1, APIKey: secret})
				case strings.HasSuffix(r.URL.Path, "/finish"):
					var report usageReport
					if err := decodeHostedFixtureClosure(t, r, &report); err != nil {
						t.Error(err)
					}
					reports <- report
					io.WriteString(w, `{"finalized":true}`)
				default:
					t.Error("unexpected API route", r.URL.Path)
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control, c.onlineSupply = api.URL, fixturePublicPolicy(upstream)
			defer c.shutdown()
			raw := []byte(`{"model":"` + hostedFixtureID + `","stream":false,"messages":[],"input":"hello","prompt":"fixture image"}`)
			media := "application/json"
			if tc.path == "/v1/audio/transcriptions" {
				var body bytes.Buffer
				form := multipart.NewWriter(&body)
				form.WriteField("model", hostedFixtureID)
				file, err := form.CreateFormFile("file", "[TEST].wav")
				if err != nil {
					t.Fatal(err)
				}
				file.Write([]byte("RIFF[TEST]"))
				form.Close()
				raw, media = body.Bytes(), form.FormDataContentType()
			}
			r := httptest.NewRequest("POST", tc.path, bytes.NewReader(raw))
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("X-Api-Key", "nn_fixture")
			r.Header.Set("Content-Type", media)
			out := httptest.NewRecorder()
			c.executeHosted(out, r, hostedFixtureID, raw)
			report := <-reports
			if calls.Load() != 1 {
				t.Fatal("upstream replayed or not called", calls.Load())
			}
			if !tc.valid {
				if out.Code != 502 || strings.Contains(out.Body.String(), secret) || report.Status != "failed" || !report.TransportConfirmed {
					t.Fatalf("nonstream protocol error escaped: HTTP=%d secret=%v status=%s", out.Code, strings.Contains(out.Body.String(), secret), report.Status)
				}
				return
			}
			if out.Code != 200 || out.Body.String() != tc.body || report.Status != "succeeded" {
				t.Fatalf("valid protocol body changed: HTTP=%d status=%s bytes=%d want=%d", out.Code, report.Status, out.Body.Len(), len(tc.body))
			}
			if report.Input == nil || *report.Input != 12 || report.Output == nil || *report.Output != 3 {
				t.Fatalf("actual usage changed: %+v", report)
			}
		})
	}
}
