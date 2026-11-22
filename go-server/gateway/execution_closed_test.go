package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type executionClosedBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *executionClosedBody) Close() error { b.closed.Store(true); return nil }

func TestExecutionClosureIsConfirmedAfterUpstreamCleanup(t *testing.T) {
	for _, connected := range []bool{false, true} {
		t.Run(map[bool]string{true: "incomplete stream", false: "connection failed"}[connected], func(t *testing.T) {
			body := &executionClosedBody{Reader: strings.NewReader("data: {\"type\":\"response.created\"}\n\n")}
			var execution context.Context
			var final map[string]any
			calls := 0
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					io.WriteString(w, `{"requestId":"closed-fixture","permit":"fixture","endpoint":"https://upstream.example.test"}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					json.NewDecoder(r.Body).Decode(&final)
					if execution == nil || execution.Err() != context.Canceled || connected && !body.closed.Load() {
						t.Error("execution finalized before upstream cleanup")
					}
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", nil)
			g.egress.Transport = gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				execution = r.Context()
				if !connected {
					return nil, errors.New("fixture connection refused")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
			})
			req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"fixture","input":"hello","stream":true}`))
			req.Header.Set("Authorization", "Bearer ses_fixture")
			req.Header.Set("X-Api-Key", "nn_fixture")
			serveGatewayCapturingAbort(g, httptest.NewRecorder(), req)
			if calls != 1 || final["status"] != "unknown" || final["executionClosed"] != true || final["inputTokens"] != nil {
				t.Fatalf("calls=%d final=%v", calls, final)
			}
		})
	}
}
