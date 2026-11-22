package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRecommendationTestUsesAuthorizedChatAndSampling(t *testing.T) {
	for _, mode := range []string{"false", "true", "default"} {
		thinking := mode != "false"
		control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/api/v1/me/model-tests/credential" {
				json.NewEncoder(w).Encode(map[string]any{"secret": "nn_test_01234567890123456789012345678901", "expiresAt": time.Now().Add(time.Minute).Format(time.RFC3339), "modelType": "recommendation"})
				return
			}
			if strings.Contains(r.URL.Path, "/targets/") {
				io.WriteString(w, `{"available":false}`)
				return
			}
			if r.URL.Path == "/api/v1/client/history/identity" {
				io.WriteString(w, `{"accountId":"`+testModelID+`","keyId":"`+testModelID+`"}`)
				return
			}
			if r.URL.Path == "/api/v1/client/history/policy" {
				io.WriteString(w, `{"mode":"off"}`)
				return
			}
			t.Error("unexpected control request", r.URL.Path)
			w.WriteHeader(404)
		}))
		called := false
		gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if r.URL.Path != "/v1/chat/completions" || body["model"] != testModelID || body["temperature"] != 0.75 || body["top_p"] != 0.95 || body["top_k"] != float64(20) {
				t.Error("invalid recommendation request", body)
			}
			options, _ := body["chat_template_kwargs"].(map[string]any)
			format, _ := json.Marshal(body["response_format"])
			if mode == "true" && !strings.Contains(string(format), `"minItems":2`) {
				t.Fatal("video example did not require two candidates")
			}
			if options["enable_thinking"] != thinking {
				t.Error("thinking option not bound", options)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"A\"}}]}\n\ndata: [DONE]\n\n")
		}))
		c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
		c.server = &http.Server{}
		c.control = control.URL
		handler, err := c.modelTestHandler()
		if err != nil {
			t.Fatal(err)
		}
		raw := modelTestBody()
		if mode != "default" {
			raw = strings.Replace(raw, `"maxTokens":512`, `"maxTokens":512,"thinking":`+mode, 1)
		}
		if mode == "true" {
			raw = strings.Replace(raw, `"thinking":true`, `"thinking":true,"recommendationFormat":"video-pair"`, 1)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, modelTestRequest(raw))
		if w.Code != 200 || !called {
			t.Fatalf("status=%d called=%v response=%s", w.Code, called, w.Body.String())
		}
		c.shutdown()
		gateway.Close()
		control.Close()
	}
}

func TestRecommendationOutputFormats(t *testing.T) {
	for _, format := range []string{"product-ranking", "text-answer", "outdoor-video"} {
		raw := strings.Replace(modelTestBody(), `"maxTokens":512`, `"maxTokens":512,"recommendationFormat":"`+format+`"`, 1)
		input, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(raw))
		if err != nil || !strings.Contains(string(recommendationFormats[input.RecommendationFormat]), strings.ReplaceAll(format, "-", "_")) {
			t.Fatalf("unavailable format %s: %v", format, err)
		}
	}
}

func TestRecommendationThinkingCannotCrossInputOrModelBoundary(t *testing.T) {
	for _, value := range []string{`null`, `"true"`, `1`, `{}`} {
		raw := strings.Replace(modelTestBody(), `"maxTokens":512`, `"maxTokens":512,"thinking":`+value, 1)
		if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(raw)); err == nil {
			t.Fatalf("accepted non-boolean thinking %s", value)
		}
	}
	for _, kind := range []string{"llm", "asr", "decision", "embedding"} {
		control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"secret": "nn_test_01234567890123456789012345678901", "expiresAt": time.Now().Add(time.Minute).Format(time.RFC3339), "modelType": kind})
		}))
		gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unauthorized thinking reached inference") }))
		c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
		c.server = &http.Server{}
		c.control = control.URL
		handler, err := c.modelTestHandler()
		if err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, modelTestRequest(strings.Replace(modelTestBody(), `"maxTokens":512`, `"maxTokens":512,"thinking":false`, 1)))
		if w.Code != 422 {
			t.Fatalf("type=%s status=%d", kind, w.Code)
		}
		c.shutdown()
		gateway.Close()
		control.Close()
	}
}
