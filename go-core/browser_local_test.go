package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBrowserLocalErrorsFollowAccountLanguage(t *testing.T) {
	for _, language := range []string{"zh-CN", "en"} {
		t.Run(language, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","language":"` + language + `"}`))
			}))
			defer api.Close()
			c := newCore(t.TempDir(), api.URL)
			defer c.shutdown()
			r := httptest.NewRequest("POST", "/api/local/core", strings.NewReader(`{"method":"model.list","params":{"endpoint":"invalid"}}`))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Accept-Language", map[string]string{"zh-CN": "en", "en": "zh-CN"}[language])
			w := httptest.NewRecorder()
			browserLocal(c, api.URL).ServeHTTP(w, r)
			if w.Code != 400 {
				t.Fatalf("status=%d", w.Code)
			}
			want := map[string]string{"zh-CN": "模型服务地址无效", "en": "invalid model service address"}[language]
			if !strings.Contains(w.Body.String(), want) {
				t.Fatalf("language=%s body=%s", language, w.Body.String())
			}
		})
	}
}
func TestBrowserLocalRequiresSessionAndRestrictsMethods(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture" {
			w.WriteHeader(401)
			return
		}
		w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	handler := browserLocal(c, api.URL)
	for _, row := range []struct {
		method, token string
		want          int
	}{{"proxy.status", "", 401}, {"agent.apply", "fixture", 400}, {"unknown.command", "fixture", 403}, {"proxy.status", "fixture", 200}} {
		request := httptest.NewRequest("POST", "/api/local/core", strings.NewReader(`{"method":"`+row.method+`","params":{}}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+row.token)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != row.want {
			t.Fatalf("%s %d", row.method, response.Code)
		}
	}
}

func TestBrowserLocalReadsModelsThroughTheAuthenticatedGoCore(t *testing.T) {
	models := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != "Bearer upstream-key" {
			t.Fatalf("unexpected upstream request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"model-one"}]}`))
	}))
	defer models.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer account-session" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/api/v1/me/edition" {
			w.Write([]byte(`{"edition":"domestic"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	c.control = api.URL
	handler := browserLocal(c, api.URL)
	body, _ := json.Marshal(map[string]any{"method": "model.list", "params": map[string]any{"endpoint": models.URL + "/v1", "apiKey": "upstream-key"}})
	request := httptest.NewRequest(http.MethodPost, "/api/local/core", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer account-session")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "model-one") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestBrowserLocalAgentConfigurationUsesAuthenticatedSession(t *testing.T) {
	const key = "11111111-1111-4111-8111-111111111111"
	const model = "22222222-2222-4222-8222-222222222222"
	const secret = "nn_selected_authorized_key_123456789"
	for _, agent := range []string{"codex", "claude-cli"} {
		t.Run(agent, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer account-session" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				switch r.URL.Path {
				case "/api/v1/me":
					json.NewEncoder(w).Encode(map[string]string{"id": key})
				case "/api/v1/me/api-keys":
					json.NewEncoder(w).Encode([]any{map[string]any{"id": key, "deploymentIds": []string{model}}})
				case "/api/v1/me/api-keys/" + key + "/copy-secret":
					json.NewEncoder(w).Encode(map[string]string{"secret": secret})
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), api.URL)
			c.control = api.URL
			c.proxyPort = availableProxyPort(t)
			defer c.shutdown()
			handler := browserLocal(c, api.URL)
			call := func(method string, params map[string]any) *httptest.ResponseRecorder {
				t.Helper()
				// Renderer-supplied sessions must never override the authenticated header.
				params["session"] = "forged-session"
				params["ſession"] = "forged-unicode-session"
				body, _ := json.Marshal(map[string]any{"method": method, "params": params})
				request := httptest.NewRequest(http.MethodPost, "/api/local/core", strings.NewReader(string(body)))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", "Bearer account-session")
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				return response
			}
			draft := agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: model, DisplayName: "Fixture"}}}
			if agent == "claude-cli" {
				draft = agentExtendedConfiguration{Roles: []agentRoleConfiguration{{Role: "sonnet", ModelID: model}}}
			}
			response := call("agent.preview", map[string]any{"agent": agent, "keyId": key, "apiKey": secret, "configuration": draft})
			if response.Code != http.StatusOK {
				t.Fatalf("preview status=%d body=%s", response.Code, response.Body.String())
			}
			var preview struct{ Digest string }
			if err := json.Unmarshal(response.Body.Bytes(), &preview); err != nil || preview.Digest == "" {
				t.Fatal("missing preview digest", err)
			}
			if strings.Contains(response.Body.String(), secret) {
				t.Fatal("preview exposed API key")
			}
			if err := c.startProxyAt(secret, c.proxyPort); err != nil {
				t.Fatal(err)
			}
			response = call("agent.apply", map[string]any{"digest": preview.Digest})
			if response.Code != http.StatusOK {
				t.Fatalf("apply status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestBrowserLocalBoundsWholeRemoteEnvelope(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	for _, size := range []int{16384, 65536} {
		body, _ := json.Marshal(map[string]any{"method": "remote.command", "params": map[string]any{"command": map[string]any{"payload": map[string]any{"content": strings.Repeat("a", size)}}}})
		r := httptest.NewRequest(http.MethodPost, "/api/local/core", strings.NewReader(string(body)))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		browserLocal(c, api.URL).ServeHTTP(w, r)
		want := http.StatusUnauthorized
		if size == 65536 {
			want = http.StatusBadRequest
		}
		if w.Code != want {
			t.Fatalf("contentBytes=%d status=%d want=%d", size, w.Code, want)
		}
	}
}

func TestBrowserLocalSessionLimitHasSpecificChineseFeedback(t *testing.T) {
	const want = "此电脑的远程会话已达上限（8 条）。请打开已有会话继续。"
	if got := browserLocalMessage("remote.command", nil, errors.New("remote session limit reached (8)")).Chinese; got != want {
		t.Fatalf("got generic/localization error %q", got)
	}
}
