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
)

func TestSVGThinkingUsesAuthorizedCapability(t *testing.T) {
	for _, public := range []bool{false, true} {
		for _, capability := range []string{"missing", "false", "true"} {
			for _, thinking := range []bool{false, true} {
				t.Run(strings.Join([]string{map[bool]string{false: "local", true: "public"}[public], capability, map[bool]string{false: "off", true: "on"}[thinking]}, "/"), func(t *testing.T) {
					control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						switch r.URL.Path {
						case "/api/v1/me/model-tests/credential":
							value := map[string]any{"secret": "nn_test_01234567890123456789012345678901", "expiresAt": time.Now().Add(time.Minute).Format(time.RFC3339), "modelType": "llm"}
							if capability != "missing" {
								value["supportsReasoningNone"] = capability == "true"
							}
							json.NewEncoder(w).Encode(value)
						case "/api/v1/client/history/identity":
							io.WriteString(w, `{"accountId":"`+testModelID+`","keyId":"`+testModelID+`"}`)
						case "/api/v1/client/history/policy":
							io.WriteString(w, `{"mode":"off"}`)
						default:
							io.WriteString(w, `{"available":false}`)
						}
					}))
					defer control.Close()
					calls := 0
					inference := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls++
						var body map[string]any
						if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
							t.Fatal(err)
						}
						want := capability == "true" && !thinking
						value, exists := body["reasoning_effort"]
						if exists != want || (want && value != "none") {
							t.Errorf("capability=%s thinking=%v: effort=%v present=%v", capability, thinking, value, exists)
						}
						w.Header().Set("Content-Type", "text/event-stream")
						io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"<svg/>\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
					})
					gateway := httptest.NewServer(inference)
					defer gateway.Close()
					c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
					defer c.shutdown()
					c.control, c.server = control.URL, &http.Server{}
					var handler http.Handler
					if public {
						c.publicSVGInference = inference
						handler = c.publicSVGModelTestHandler()
					} else {
						var err error
						handler, err = c.modelTestHandler()
						if err != nil {
							t.Fatal(err)
						}
					}
					raw := strings.Replace(modelTestBody(), `"maxTokens":512`, `"maxTokens":512,"thinking":`+map[bool]string{false: "false", true: "true"}[thinking], 1)
					r := modelTestRequest(raw)
					r = r.WithContext(context.WithValue(r.Context(), svgModelTestContext{}, true))
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, r)
					if w.Code != 200 || calls != 1 {
						t.Fatalf("status=%d calls=%d body=%s", w.Code, calls, w.Body.String())
					}
				})
			}
		}
	}
}
