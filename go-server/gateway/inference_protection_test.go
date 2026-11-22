package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestInferenceValidationReportsOnlyCallerViolations(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(map[bool]string{false: "recorded", true: "isolated"}[blocked], func(t *testing.T) {
			reports := 0
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/internal/inference-protection/events" {
					t.Errorf("invalid request reached %s", r.URL.Path)
				}
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				if body["code"] != "invalid_output_limit" || body["secret"] != "nn_fixture" || body["session"] != "ses_fixture" || body["gatewayId"] != "fixture" {
					t.Errorf("invalid attribution: %v", body)
				}
				if _, err := uuid.Parse(body["eventId"].(string)); err != nil {
					t.Error(err)
				}
				reports++
				json.NewEncoder(w).Encode(map[string]any{"blocked": blocked, "count": 10})
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", nil)
			req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"fixture","input":"hi","max_output_tokens":0}`))
			req.Header.Set("Authorization", "Bearer ses_fixture")
			req.Header.Set("X-Api-Key", "nn_fixture")
			res := httptest.NewRecorder()
			g.ServeHTTP(res, req)
			want := 400
			if blocked {
				want = 403
			}
			if res.Code != want || reports != 1 {
				t.Fatalf("HTTP=%d reports=%d", res.Code, reports)
			}
		})
	}
}

func TestUpstreamFailuresNeverReportCallerViolations(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		body         string
		stream       bool
		transportErr bool
	}{
		{"rate limited", 429, "", false, false},
		{"server error", 503, "", false, false},
		{"malformed SSE", 200, "data: not-json\n\n", true, false},
		{"truncated SSE", 200, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n", true, false},
		{"transport timeout", 0, "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reports, finalized := 0, false
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/inference-protection/events"):
					reports++
					io.WriteString(w, `{}`)
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					io.WriteString(w, `{"requestId":"upstream-fault","permit":"fixture","endpoint":"https://upstream.example.test"}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					finalized = true
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", nil)
			g.egress.Transport = gatewayRoundTripFunc(func(*http.Request) (*http.Response, error) {
				if tc.transportErr {
					return nil, context.DeadlineExceeded
				}
				return &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			body := `{"model":"fixture","input":"hi","stream":false}`
			if tc.stream {
				body = strings.Replace(body, "false", "true", 1)
			}
			req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer ses_fixture")
			req.Header.Set("X-Api-Key", "nn_fixture")
			serveGatewayCapturingAbort(g, httptest.NewRecorder(), req)
			if reports != 0 || !finalized {
				t.Fatalf("provider error reported as user violation: reports=%d finalized=%v", reports, finalized)
			}
		})
	}
}

func TestForgedExecutionMetadataIsReportedBeforeExecution(t *testing.T) {
	var code string
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/inference-protection/events") {
			t.Errorf("attack reached %s", r.URL.Path)
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		code, _ = body["code"].(string)
		json.NewEncoder(w).Encode(map[string]any{"blocked": true})
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", nil)
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"fixture","input":"hi"}`))
	req.Header.Set("Authorization", "Bearer ses_fixture")
	req.Header.Set("X-Api-Key", "nn_fixture")
	req.Header.Set("X-Newnoa-Permit", "forged")
	res := httptest.NewRecorder()
	g.ServeHTTP(res, req)
	if res.Code != 403 || code != "forged_execution_metadata" {
		t.Fatalf("HTTP=%d code=%s", res.Code, code)
	}
}
