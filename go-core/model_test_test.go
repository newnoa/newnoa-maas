package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testModelID = "00000000-0000-0000-0000-000000000001"

func TestModelTestTenMiBBoundary(t *testing.T) {
	header, _ := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")
	for _, size := range []int{10 * 1024 * 1024, 10*1024*1024 + 1} {
		data := make([]byte, size)
		copy(data, header)
		raw := json.RawMessage(`[{"type":"image_url","image_url":{"url":"data:image/png;base64,` + base64.StdEncoding.EncodeToString(data) + `"}}]`)
		_, err := validateTestContent(raw, "user")
		if (err == nil) != (size == 10*1024*1024) {
			t.Fatalf("size=%d err=%v", size, err)
		}
	}
}

func modelTestBody() string {
	return `{"keyId":"` + testModelID + `","model":"` + testModelID + `","conversationId":"` + testModelID + `","requestId":"` + testModelID + `","maxTokens":512,"messages":[{"role":"user","content":"你好"}]}`
}
func modelTestRequest(body string) *http.Request {
	r := httptest.NewRequest("POST", "/api/local/model-test", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer session-test-only")
	return r
}
func TestModelTestValidation(t *testing.T) {
	for _, body := range []string{
		strings.Replace(modelTestBody(), `"keyId"`, `"secret"`, 1), strings.Replace(modelTestBody(), `"user"`, `"system"`, 1), strings.Replace(modelTestBody(), `512`, `65536`, 1), strings.Replace(modelTestBody(), `"content":"你好"`, `"content":"你好","url":"https://evil.invalid"`, 1), strings.Replace(modelTestBody(), `"content":"你好"`, `"content":[]`, 1), modelTestBody() + `{}`, strings.Replace(modelTestBody(), testModelID, "not-an-id", 1), strings.Replace(modelTestBody(), `"你好"`, `"`+strings.Repeat("a", 32769)+`"`, 1),
	} {
		if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(body)); err == nil {
			t.Fatal("accepted invalid model test")
		}
	}
	if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(modelTestBody())); err != nil {
		t.Fatal(err)
	}
}
func TestModelTestStreamsEphemeralAuthorizationAndHistory(t *testing.T) {
	const secret = "nn_test_01234567890123456789012345678901"
	var inferenceCalls, credentialCalls atomic.Int32
	for _, mode := range []string{"off", "local"} {
		t.Run(mode, func(t *testing.T) {
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/api/v1/me/model-tests/credential" {
					credentialCalls.Add(1)
					if r.Header.Get("Authorization") != "Bearer session-test-only" {
						t.Error("missing session")
					}
					var body map[string]string
					json.NewDecoder(r.Body).Decode(&body)
					if len(body) != 2 || body["keyId"] != testModelID || body["model"] != testModelID {
						t.Error("invalid credential body")
					}
					json.NewEncoder(w).Encode(map[string]string{"secret": secret, "expiresAt": time.Now().Add(time.Minute).Format(time.RFC3339)})
					return
				}
				if (strings.HasPrefix(r.URL.Path, "/api/") && r.Header.Get("Authorization") != "Bearer "+secret) || (!strings.HasPrefix(r.URL.Path, "/api/") && r.Header.Get("X-Api-Key") != secret) {
					t.Error("wrong inference authorization")
				}
				switch r.URL.Path {
				case "/api/v1/client/history/identity":
					io.WriteString(w, `{"accountId":"`+testModelID+`","keyId":"`+testModelID+`"}`)
				case "/api/v1/client/history/policy":
					io.WriteString(w, `{"mode":"`+mode+`","id":"`+testModelID+`"}`)
				case "/api/v1/client/targets/" + testModelID:
					io.WriteString(w, `{"available":false,"private":false}`)
				default:
					t.Error("unexpected control route", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer control.Close()
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				inferenceCalls.Add(1)
				if r.URL.Path != "/v1/chat/completions" || r.Header.Get("X-Api-Key") != secret || r.Header.Get("X-Newnoa-Conversation-Id") != testModelID || r.Header.Get("Idempotency-Key") != testModelID {
					t.Error("incorrect inference request")
				}
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				options, _ := body["stream_options"].(map[string]any)
				if len(body) != 5 || body["stream"] != true || body["max_tokens"] != float64(512) || len(options) != 1 || options["include_usage"] != true {
					t.Error("unsafe inference body", body)
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Authorization", secret)
				io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"你好\"}}]}\n\n")
				w.(http.Flusher).Flush()
				io.WriteString(w, "data: [DONE]\n\n")
			}))
			defer gateway.Close()
			c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
			configureTestHistorySQLite(t, c.home, testModelID)
			c.server = &http.Server{}
			c.control = control.URL
			defer c.shutdown()
			if mode == "off" {
				h, err := c.historyStore()
				if err != nil {
					t.Fatal(err)
				}
				if _, err = h.db.Exec("INSERT INTO history_local_settings(viewer,enabled) VALUES(?,0)", testModelID); err != nil {
					t.Fatal(err)
				}
			}
			handler, err := c.modelTestHandler()
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, modelTestRequest(modelTestBody()))
			if response.Code != 200 || !strings.Contains(response.Body.String(), "你好") || !strings.Contains(response.Body.String(), "[DONE]") || response.Header().Get("Authorization") != "" {
				t.Fatal(response.Code, response.Body.String())
			}
			rows, err := c.history.list(testModelID, "", 20)
			if err != nil {
				t.Fatal(err)
			}
			expected := 0
			if mode == "local" {
				expected = 1
			}
			if len(rows) != expected {
				t.Fatal("history policy bypassed", rows)
			}
			if c.server == nil || c.proxyPort != 0 || c.pending != nil {
				t.Fatal("test mutated proxy or agent configuration")
			}
			c.history.db.Close()
		})
	}
	if inferenceCalls.Load() != 2 || credentialCalls.Load() != 2 {
		t.Fatal("model test replayed")
	}
}
func TestModelTestWriterSanitizesAndLimits(t *testing.T) {
	response := httptest.NewRecorder()
	writer := &modelTestWriter{ResponseWriter: response, header: make(http.Header), cancel: func() {}}
	writer.Header().Set("Content-Type", "text/plain")
	writer.WriteHeader(500)
	writer.Write([]byte("nn_secret-private-body"))
	if response.Code != 500 || strings.Contains(response.Body.String(), "nn_secret") {
		t.Fatal("upstream error leaked")
	}
	response = httptest.NewRecorder()
	cancelled := false
	writer = &modelTestWriter{ResponseWriter: response, header: make(http.Header), cancel: func() { cancelled = true }}
	writer.Header().Set("Content-Type", "text/event-stream")
	if _, err := writer.Write(make([]byte, modelTestResponseLimit+1)); err == nil || !cancelled {
		t.Fatal("stream limit missing")
	}
}
func TestModelTestEndpointIsPrivateAndCloses(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.invalid")
	value, err := c.handle("model.test.endpoint", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := value.(map[string]string)
	request, _ := http.NewRequest("POST", endpoint["url"], strings.NewReader(modelTestBody()))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 403 {
		t.Fatal("unauthenticated endpoint")
	}
	request, _ = http.NewRequest("POST", endpoint["url"], strings.NewReader(modelTestBody()))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Newnoa-Model-Test", endpoint["token"])
	request.Header.Set("Origin", "http://127.0.0.1")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 403 {
		t.Fatal("browser reached native endpoint")
	}
	c.shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request, _ = http.NewRequestWithContext(ctx, "POST", endpoint["url"], nil)
	if response, err = http.DefaultClient.Do(request); err == nil {
		response.Body.Close()
		t.Fatal("endpoint survived shutdown")
	}
}

func TestModelTestCancellationStopsUpstreamWithoutReplay(t *testing.T) {
	secret := "nn_test_01234567890123456789012345678901"
	var calls atomic.Int32
	cancelled := make(chan struct{})
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/me/model-tests/credential":
			json.NewEncoder(w).Encode(map[string]string{"secret": secret, "expiresAt": time.Now().Add(time.Minute).Format(time.RFC3339)})
		case "/api/v1/client/targets/" + testModelID:
			io.WriteString(w, `{"available":false,"private":false}`)
		default:
			w.WriteHeader(503)
		}
	}))
	defer control.Close()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: started\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(cancelled)
		case <-time.After(3 * time.Second):
		}
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	c.server = &http.Server{}
	c.control = control.URL
	defer c.shutdown()
	handler, err := c.modelTestHandler()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/api/local/model-test", strings.NewReader(modelTestBody()))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer session-test-only")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	part := make([]byte, 15)
	if _, err = io.ReadFull(response.Body, part); err != nil {
		t.Fatal(err)
	}
	cancel()
	response.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("upstream was not cancelled")
	}
	if calls.Load() != 1 {
		t.Fatal("cancelled test replayed")
	}
	if c.history != nil {
		c.history.db.Close()
	}
}

func TestModelTestImageLimitsAndOutput(t *testing.T) {
	body := strings.Replace(modelTestBody(), `512`, `65535`, 1)
	if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(body)); err != nil {
		t.Fatal(err)
	}
	png := "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII="
	image := `[{"type":"text","text":"描述"},{"type":"image_url","image_url":{"url":"data:image/png;base64,` + png + `"}}]`
	body = strings.Replace(body, `"你好"`, image, 1)
	if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(body)); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{strings.Replace(body, "data:image/png;base64,"+png, "https://example.com/image.png", 1), strings.Replace(body, "image/png", "image/jpeg", 1), strings.Replace(body, `"type":"text"`, `"type":"file"`, 1)} {
		if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(invalid)); err == nil {
			t.Fatal("accepted invalid image")
		}
	}
}

func TestModelTestPictureSelection(t *testing.T) {
	const ref = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII="
	for _, tc := range []struct {
		picture string
		valid   bool
	}{
		{`{"prompt":"icon"}`, true}, {`{"prompt":"edit","size":"96x128","reference":"` + ref + `"}`, true},
		{`{"prompt":" "}`, false}, {`{"prompt":"edit","reference":"https://example.test/private"}`, false},
		{`{"prompt":"image","size":"0x128"}`, false},
	} {
		body := map[string]any{"model": testModelID, "keyId": testModelID, "conversationId": testModelID, "picture": json.RawMessage(tc.picture)}
		raw, _ := json.Marshal(body)
		_, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(string(raw)))
		if (err == nil) != tc.valid {
			t.Errorf("picture accepted=%t, want %t", err == nil, tc.valid)
		}
	}
}
