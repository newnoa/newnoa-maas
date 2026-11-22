package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	rpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
)

func TestModelAndSVGWrappersDoNotCutAPIExecutionWindow(t *testing.T) {
	for _, mode := range []string{"model", "SVG"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := svgFixture(t, nil)
			remaining := make(chan time.Duration, 1)
			server := rpc.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				deadline, ok := r.Context().Deadline()
				if !ok {
					t.Error("wrapper unbounded")
				}
				remaining <- time.Until(deadline)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"<svg></svg>\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
			}))
			defer server.Stop()
			gateway, ca := gatewayTLSFixture(t, server, false)
			c.gatewaySelector = &gatewaySelector{discover: func(context.Context, string, string) ([]gatewayEndpoint, error) {
				return []gatewayEndpoint{{ID: "deadline", Address: gateway.URL, TrustedCA: ca, Transport: "grpc"}}, nil
			}, probe: func(context.Context, gatewayEndpoint) (time.Duration, error) { return time.Millisecond, nil }}
			var jobID string
			if mode == "model" {
				h, err := c.modelTestHandler()
				if err != nil {
					t.Fatal(err)
				}
				out := httptest.NewRecorder()
				h.ServeHTTP(out, modelTestRequest(modelTestBody()))
				if out.Code != 200 {
					t.Fatal(out.Code, out.Body.String())
				}
			} else {
				jobID = svgCreate(t, c, uuid.NewString())["id"].(string)
			}
			select {
			case left := <-remaining:
				if left < 35*time.Minute || left > 36*time.Minute {
					t.Fatalf("%s wrapper budget %v cuts approved admission/execution window", mode, left)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("request never reached gateway")
			}
			if jobID != "" {
				waitSVG(t, func() bool { return svgGet(t, c, jobID)["status"] != "running" })
			}
		})
	}
}

func TestModelTestServerKeepsUploadLimitNotShortExecutionWriteLimit(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	defer c.shutdown()
	if _, err := c.modelTestEndpoint(); err != nil {
		t.Fatal(err)
	}
	if c.modelTestServer.ReadTimeout != 10*time.Second {
		t.Fatal("upload timeout changed")
	}
	if c.modelTestServer.WriteTimeout != 0 && c.modelTestServer.WriteTimeout < 36*time.Minute {
		t.Fatalf("model-test write deadline %v cuts execution", c.modelTestServer.WriteTimeout)
	}
}

func TestPublicWrapperDoesNotCutExecutionWindow(t *testing.T) {
	observed := make(chan time.Duration, 1)
	server := rpc.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadline, ok := r.Context().Deadline()
		if !ok {
			t.Error("unbounded public wrapper")
		}
		observed <- time.Until(deadline)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Stop()
	gateway, ca := gatewayTLSFixture(t, server, false)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"hosted": false})
	}))
	defer api.Close()
	c := newCore(t.TempDir(), gateway.URL)
	c.control = api.URL
	defer c.shutdown()
	c.gatewaySelector = &gatewaySelector{discover: func(context.Context, string, string) ([]gatewayEndpoint, error) {
		return []gatewayEndpoint{{ID: "deadline", Address: gateway.URL, TrustedCA: ca, Transport: "grpc"}}, nil
	}, probe: func(context.Context, gatewayEndpoint) (time.Duration, error) { return time.Millisecond, nil }}
	handler, err := c.publicProxyHandler(publicTestSecret, "fixture-internal")
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	r.Body = io.NopCloser(strings.NewReader(`{"model":"fixture","stream":true}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer ses_fixture_aaaaaaaa")
	r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, r)
	if out.Code != 200 {
		t.Fatal(out.Code, out.Body.String())
	}
	select {
	case left := <-observed:
		if left < 35*time.Minute || left > 36*time.Minute {
			t.Fatalf("public wrapper %v cuts admission/execution window", left)
		}
	case <-time.After(time.Second):
		t.Fatal("missing gateway request")
	}
}
