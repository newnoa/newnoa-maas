package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	metering "github.com/newnoa/newnoa-maas/go-server/network/metering"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Exercise actual request termination; a wrong branch, leaked error or relaxed
// completion/usage gate must fail these assertions.
func TestInferenceAbortCauseFollowsActualBranchWithoutSecrets(t *testing.T) {
	const delta = "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"private-output\",\"obfuscation\":\"private-obfuscation\"}\n\n"
	const done = "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":12,\"output_tokens\":2}}}\n\n"
	for _, tc := range []struct{ name, cause string }{
		{"complete", "none"}, {"missing terminal", "read-eof-before-terminal"}, {"read error", "read-error"},
		{"malformed", "evidence-invalid"}, {"invalid index", "meter-invalid"}, {"meter rejected", "meter-checkpoint-failed"},
		{"meter ack mismatch", "meter-checkpoint-failed"}, {"activity rejected", "activity-failed"},
		{"write failed", "downstream-write-failed"}, {"caller cancelled", "context-cancelled"},
		{"activity and write failed", "activity-failed"},
		{"activity cancelled during bandwidth read", "context-cancelled"}, {"upstream rejected", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(previous)
			var final, lastSnapshot map[string]any
			upstreamCalls := 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			body := &diagnosticBody{parts: []string{delta}}
			switch tc.name {
			case "complete", "write failed", "meter rejected", "meter ack mismatch", "activity rejected":
				body.parts = append(body.parts, done)
			case "read error":
				body.err = errors.New("private-read-error secret-key https://secret-endpoint.test")
			case "malformed":
				body.parts = []string{"data: {private-malformed\n\n"}
			case "invalid index":
				body.parts = []string{strings.Replace(delta, "\"output_index\":0", "\"output_index\":-1", 1)}
			case "caller cancelled":
				body.before = cancel
			}
			activityDuringBandwidthRead := tc.name == "activity cancelled during bandwidth read"
			g := newGateway("https://secret-api-endpoint.test", "secret-internal", "fixture", nil)
			g.client = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				payload, code := "{}", 200
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					payload = `{"model":"fixture","deploymentId":"deployment"}`
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					payload = `{"requestId":"diagnostic-request","permit":"secret-permit","endpoint":"https://secret-endpoint.test","apiKey":"secret-upstream","pooledCredential":true}`
					if activityDuringBandwidthRead {
						payload = strings.TrimSuffix(payload, "}") + `,"membership":{"applies":true}}`
					}
				case strings.HasSuffix(r.URL.Path, "/bandwidth"):
					payload = `{"waitMillis":0}`
				case strings.HasSuffix(r.URL.Path, "/meter"):
					var m struct {
						Sequence int64          `json:"sequence"`
						Snapshot map[string]any `json:"snapshot"`
					}
					if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
						t.Fatal(err)
					}
					if m.Sequence > 1 && tc.name == "meter rejected" {
						code, payload = 403, `{"error":"private-meter-error secret-key"}`
					} else {
						id := "diagnostic-request"
						if m.Sequence > 1 && tc.name == "meter ack mismatch" {
							id = "secret-wrong-request"
						} else {
							lastSnapshot = m.Snapshot
						}
						raw, _ := json.Marshal(map[string]any{"accepted": true, "requestId": id, "sequence": m.Sequence})
						payload = string(raw)
					}
				case strings.HasSuffix(r.URL.Path, "/activity"):
					if activityDuringBandwidthRead {
						cancel()
					}
					if tc.name == "activity rejected" || tc.name == "activity and write failed" || activityDuringBandwidthRead {
						code, payload = 409, `{"error":"private-activity-error secret-key"}`
					}
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					if err := json.NewDecoder(r.Body).Decode(&final); err != nil {
						t.Fatal(err)
					}
				}
				return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(payload))}, nil
			})}
			g.egress = &http.Client{Transport: gatewayRoundTripFunc(func(*http.Request) (*http.Response, error) {
				upstreamCalls++
				status := 200
				if tc.name == "upstream rejected" {
					status = 429
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
			})}
			req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"fixture","input":"private-prompt","stream":true}`)).WithContext(ctx)
			req.Header.Set("Authorization", "Bearer ses_secret-caller")
			req.Header.Set("X-Api-Key", "nn_secret-caller-key")
			recorder := httptest.NewRecorder()
			var out http.ResponseWriter = recorder
			if tc.name == "write failed" || tc.name == "activity and write failed" {
				out = diagnosticWriteFailure{recorder}
			}
			aborted := serveGatewayCapturingAbort(g, out, req)
			wantAbort := tc.name == "missing terminal" || tc.name == "read error" || tc.name == "malformed" || tc.name == "invalid index" || tc.name == "meter rejected" || tc.name == "meter ack mismatch" || tc.name == "activity rejected"
			if aborted != wantAbort {
				t.Fatalf("abort=%v want=%v", aborted, wantAbort)
			}
			if upstreamCalls != 1 || !body.closed {
				t.Fatalf("calls=%d closed=%v status=%d body=%s", upstreamCalls, body.closed, recorder.Code, recorder.Body.String())
			}
			var row map[string]any
			scan := bufio.NewScanner(bytes.NewReader(logs.Bytes()))
			for scan.Scan() {
				var item map[string]any
				if json.Unmarshal(scan.Bytes(), &item) == nil && item["msg"] == "inference.completed" {
					row = item
				}
			}
			if err := scan.Err(); err != nil {
				t.Fatal(err)
			}
			if row == nil {
				t.Fatal("missing completion log")
			}
			if row["abortCause"] != tc.cause && !(activityDuringBandwidthRead && row["abortCause"] == "activity-failed") {
				t.Errorf("cause=%v want=%s", row["abortCause"], tc.cause)
			}
			if tc.name == "complete" {
				if row["outcome"] != "succeeded" || final["status"] != "succeeded" || aborted {
					t.Fatal("completion changed", row, final, aborted)
				}
				if row["input_tokens"] != float64(12) || row["output_tokens"] != float64(2) {
					t.Fatal("terminal usage lost", row)
				}
			} else {
				if row["outcome"] == "succeeded" || final["status"] == "succeeded" {
					t.Fatal("failure became success", row, final)
				}
				if tc.name != "write failed" && row["input_tokens"] != nil {
					t.Fatal("invented usage", row)
				}
			}
			if tc.name == "meter rejected" || tc.name == "meter ack mismatch" {
				if recorder.Body.Len() != 0 {
					t.Fatal("unacknowledged content escaped")
				}
			}
			if tc.name == "missing terminal" && lastSnapshot["terminalUsage"] != false {
				t.Fatal("missing terminal confirmed", lastSnapshot)
			}
			raw, _ := json.Marshal(row)
			for _, secret := range []string{"secret-", "private-", "https://secret-endpoint.test"} {
				if bytes.Contains(raw, []byte(secret)) {
					t.Fatal("sensitive sentinel leaked")
				}
			}
		})
	}
}

type diagnosticBody struct {
	parts  []string
	before func()
	err    error
	closed bool
}

func (b *diagnosticBody) Read(p []byte) (int, error) {
	if b.before != nil {
		f := b.before
		b.before = nil
		f()
		return 0, context.Canceled
	}
	if len(b.parts) > 0 {
		n := copy(p, b.parts[0])
		b.parts[0] = b.parts[0][n:]
		if b.parts[0] == "" {
			b.parts = b.parts[1:]
		}
		return n, nil
	}
	if b.err != nil {
		return 0, b.err
	}
	return 0, io.EOF
}
func (b *diagnosticBody) Close() error { b.closed = true; return nil }

type diagnosticWriteFailure struct{ *httptest.ResponseRecorder }

func (diagnosticWriteFailure) Write([]byte) (int, error) {
	return 0, errors.New("private-write-error secret-key https://secret-endpoint.test")
}

func TestMeterCheckpointClassificationPreservesOriginalError(t *testing.T) {
	original := errors.New("private-checkpoint secret-key")
	g := newGateway("https://secret-api-endpoint.test", "secret-internal", "fixture", nil)
	g.client = &http.Client{Transport: gatewayRoundTripFunc(func(*http.Request) (*http.Response, error) { return nil, original })}
	m, err := metering.New("fixture", "/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	reporter := &meterCheckpoint{gateway: g, request: "fixture", permit: "secret-permit"}
	_, err = checkpointMeterBatch(context.Background(), m, reporter, []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"private-text\"}\n\n"), false)
	if !errors.Is(err, errMeterCheckpoint) || !errors.Is(err, original) {
		t.Fatal("classification lost underlying error")
	}
}
