package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	metering "github.com/newnoa/newnoa-maas/go-server/network/metering"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestMeterRejectionCompletionDetailsAreBoundedAndAccurate(t *testing.T) {
	const plain = "synthetic-output"
	prefix := []byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"" + plain + "\"}\n\n")
	runMeterRejectionLogCases(t, prefix, plain)
}
func TestMeterRejectionSavedPrefixOffline(t *testing.T) {
	p := os.Getenv("NEWNOA_OFFLINE_METER_PREFIX")
	if p == "" {
		t.Skip("explicit private offline prefix fixture only")
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal("PRIVATE_PREFIX_READ_FAILED")
	}
	h := sha256.Sum256(b)
	if len(b) != 2549 || hex.EncodeToString(h[:]) != "b4bd2ab61de78e29093286fd3723ad4f8fe4c84812db0474395a03bd6ca9d69b" {
		t.Fatal("PRIVATE_PREFIX_HASH_MISMATCH")
	}
	var text string
	for _, line := range bytes.Split(b, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("data:")) {
			var e struct{ Type, Delta string }
			if json.Unmarshal(bytes.TrimSpace(line[5:]), &e) != nil {
				t.Fatal("PRIVATE_PREFIX_JSON")
			}
			if e.Type == "response.output_text.delta" {
				text += e.Delta
			}
		}
	}
	runMeterRejectionLogCases(t, b, text)
}
func runMeterRejectionLogCases(t *testing.T, prefix []byte, plain string) {
	event := func(v any) string {
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal("FIXTURE_JSON")
		}
		return "data: " + string(b) + "\n\n"
	}
	metadata := event(map[string]any{"type": "response.metadata", "metadata": map[string]string{"private-data": "private-provider-metadata"}})
	terminal := event(map[string]any{"type": "response.completed", "response": map[string]any{"status": "completed", "output": []any{map[string]any{"type": "message", "content": []any{map[string]any{"type": "output_text", "text": plain}}}}, "usage": map[string]any{"input_tokens": 12, "output_tokens": 5}}})
	for _, tc := range []struct {
		name, tail, stage, class, site, event string
		success                               bool
	}{
		{name: "prefix-only", stage: "", class: "", site: "", event: ""},
		{name: "matching-terminal", tail: terminal, success: true},
		{name: "provider-metadata-and-real-terminal", tail: metadata + terminal, success: true},
		{name: "provider-metadata-without-terminal", tail: metadata},
		{name: "unsupported-known", tail: event(map[string]any{"type": "response.output_text.annotation.added", "output_index": 0, "content_index": 0}), stage: "feed", class: "protocol", site: "unsupported-event", event: "response.output_text.annotation.added"},
		{name: "malicious-event", tail: event(map[string]any{"type": "secret-event-value", "private-key-sentinel": "private-value-sentinel"}), stage: "feed", class: "protocol", site: "unsupported-event", event: "other"},
		{name: "snapshot-mismatch", tail: event(map[string]any{"type": "response.output_text.done", "output_index": 0, "content_index": 0, "text": plain + "private-mismatch-sentinel"}), stage: "feed", class: "protocol", site: "snapshot-mismatch", event: "response.output_text.done"},
		{name: "invalid-index", tail: event(map[string]any{"type": "response.output_text.done", "output_index": -1, "text": plain}), stage: "feed", class: "protocol", site: "invalid-index", event: "response.output_text.done"},
		{name: "invalid-usage", tail: event(map[string]any{"type": "response.completed", "response": map[string]any{"status": "completed", "usage": map[string]any{"input_tokens": -1, "output_tokens": 5}}}), stage: "feed", class: "protocol", site: "invalid-usage", event: "response.completed"},
		{name: "partial-tail", tail: "data: {\"type\":\"private-unparsed-sentinel", stage: "finish", class: "truncated", site: "other", event: "none"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(old)
			var final map[string]any
			calls := 0
			body := &diagnosticBody{parts: []string{string(prefix), tc.tail}}
			g := newGateway("https://private-endpoint.test", "secret-internal", "fixture", nil)
			g.client = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				value := "{}"
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					value = `{"model":"unknown-fixture","deploymentId":"deployment"}`
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					value = `{"requestId":"meter-detail-fixture","permit":"secret-permit","endpoint":"https://private-endpoint.test","apiKey":"secret-upstream"}`
				case strings.HasSuffix(r.URL.Path, "/meter"):
					var m struct {
						Sequence int64 `json:"sequence"`
					}
					if json.NewDecoder(r.Body).Decode(&m) != nil {
						t.Fatal("CHECKPOINT_FIXTURE")
					}
					b, _ := json.Marshal(map[string]any{"accepted": true, "requestId": "meter-detail-fixture", "sequence": m.Sequence})
					value = string(b)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					if json.NewDecoder(r.Body).Decode(&final) != nil {
						t.Fatal("FINALIZE_FIXTURE")
					}
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(value))}, nil
			})}
			g.egress = &http.Client{Transport: gatewayRoundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
			})}
			req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"fixture","input":"private-prompt","stream":true}`))
			req.Header.Set("X-Api-Key", "nn_secret-caller")
			req.Header.Set("Authorization", "Bearer ses_secret-caller")
			out := httptest.NewRecorder()
			aborted := serveGatewayCapturingAbort(g, out, req)
			if calls != 1 || !body.closed {
				t.Fatal("INVOCATION_OR_CLEANUP_CHANGED")
			}
			var row map[string]any
			scan := bufio.NewScanner(bytes.NewReader(logs.Bytes()))
			for scan.Scan() {
				var m map[string]any
				if json.Unmarshal(scan.Bytes(), &m) == nil && m["msg"] == "inference.completed" {
					row = m
				}
			}
			if row == nil {
				t.Fatal("COMPLETION_LOG_MISSING")
			}
			if tc.stage != "" {
				if row["abortCause"] != "meter-invalid" || row["meterStage"] != tc.stage || row["meterErrorClass"] != tc.class || row["meterRejectSite"] != tc.site || row["meterEvent"] != tc.event {
					t.Errorf("SAFE_DIAGNOSTIC_MISMATCH case=%s stage=%v class=%v site=%v event=%v", tc.name, row["meterStage"], row["meterErrorClass"], row["meterRejectSite"], row["meterEvent"])
				}
			} else {
				for _, key := range []string{"meterStage", "meterErrorClass", "meterRejectSite", "meterEvent"} {
					if _, ok := row[key]; ok {
						t.Error("DETAIL_ON_NON_METER_FAILURE_OR_SUCCESS")
					}
				}
			}
			if bytes.Contains(out.Body.Bytes(), []byte("private-provider-metadata")) {
				t.Fatal("provider metadata was forwarded")
			}
			if tc.success {
				if final["status"] != "succeeded" || aborted {
					t.Fatal("VALID_COMPLETION_CHANGED")
				}
			} else {
				if final["status"] == "succeeded" || !aborted {
					t.Fatal("FAILURE_RELAXED")
				}
				if !bytes.Equal(out.Body.Bytes(), prefix) {
					t.Fatal("REJECTED_BYTES_RELEASED_OR_PREFIX_CHANGED")
				}
			}
			encoded, _ := json.Marshal(row)
			for _, secret := range []string{"private-", "secret-", plain} {
				if bytes.Contains(encoded, []byte(secret)) {
					t.Fatal("SENSITIVE_METADATA_LEAK")
				}
			}
		})
	}
}
func TestMeterBatchStageRetainsExactOriginalError(t *testing.T) {
	m, _ := metering.New("unknown-fixture", "/v1/responses", nil)
	_, _ = m.Finish()
	reporter := &meterCheckpoint{}
	_, err := checkpointMeterBatch(context.Background(), m, reporter, nil, false)
	if err != metering.ErrFinished || !errors.Is(err, metering.ErrFinished) || reporter.failureStage != "feed" {
		t.Fatal("ORIGINAL_ERROR_OR_STAGE_CHANGED")
	}
}

func TestMeterErrorClassNeverUsesErrorText(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{metering.ErrProtocol, "protocol"}, {metering.ErrLimit, "limit"}, {metering.ErrTruncated, "truncated"}, {metering.ErrFinished, "finished"},
		{errors.New("private-tokenizer-or-unknown-error secret-key"), "other"},
	} {
		if got := meterErrorClass(tc.err); got != tc.want {
			t.Errorf("CLASS_MISMATCH got=%s want=%s", got, tc.want)
		}
	}
}
