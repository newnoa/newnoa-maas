package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const publicTestSecret = "public-service-secret-at-least-32-chars"

func TestPublicProxyRejectsServiceAndHeaderSpoofing(t *testing.T) {
	c := &core{control: "http://127.0.0.1:1", gateway: "http://127.0.0.1:1"}
	h, err := c.publicProxyHandler(publicTestSecret, "internal-test")
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"", "wrong"} {
		r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
		r.Header.Set("Authorization", "Bearer ses_user_aaaaaaaa")
		r.Header.Set("X-Newnoa-Public-Proxy", token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatalf("unauthenticated service status=%d", w.Code)
		}
	}
	for _, address := range []string{"0.0.0.0:18223", "[::]:18223", "8.8.8.8:18223", "localhost:18223"} {
		if validatePublicProxyAddress(address) == nil {
			t.Errorf("accepted non-private bind %s", address)
		}
	}
	for _, address := range []string{"127.0.0.1:18223", "[::1]:18223", "10.2.0.2:18223"} {
		if err := validatePublicProxyAddress(address); err != nil {
			t.Errorf("private bind: %v", err)
		}
	}
}

func TestPublicProxyConcurrentBearersStayIsolatedWithoutLocalFiles(t *testing.T) {
	home := t.TempDir()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, header := range []string{"X-Newnoa-Public-Proxy", "X-Newnoa-Caller-IP", "X-Newnoa-Session", "X-Newnoa-Internal-Token", "X-Api-Key", "Forwarded"} {
			if r.Header.Get(header) != "" {
				t.Errorf("internal header leaked: %s", header)
			}
		}
		io.WriteString(w, r.Header.Get("Authorization"))
	}))
	defer gateway.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		if r.Header.Get("Authorization") != "Bearer internal-test" {
			t.Error("missing API service credential")
		}
		var v map[string]any
		json.NewDecoder(r.Body).Decode(&v)
		if !strings.HasPrefix(v["secret"].(string), "ses_user_") {
			t.Errorf("caller identity lost: %v", v)
		}
		if r.URL.Path == "/api/v1/internal/public/models" {
			io.WriteString(w, `[{"id":"m","model":"upstream-m","modelType":"llm","apiKey":"must-not-leak","endpoint":"must-not-leak"}]`)
			return
		}
		if r.URL.Path != "/api/v1/internal/public/authorize" {
			t.Errorf("unexpected API %s", r.URL.Path)
		}
		io.WriteString(w, `{"hosted":false}`)
	}))
	defer api.Close()
	c := &core{home: home, control: api.URL, gateway: gateway.URL, proxySession: "ses_wrong_saved_session"}
	h, err := c.publicProxyHandler(publicTestSecret, "internal-test")
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for _, bearer := range []string{"ses_user_aaaaaaaa", "ses_user_bbbbbbbb"} {
		group.Add(1)
		go func(bearer string) {
			defer group.Done()
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
			r.Header.Set("Authorization", "Bearer "+bearer)
			r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
			r.Header.Set("X-Newnoa-Caller-IP", "203.0.113.8")
			r.Header.Set("X-Newnoa-Session", "spoof")
			r.Header.Set("X-Newnoa-Internal-Token", "spoof")
			r.Header.Set("Forwarded", "for=spoof")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 200 || w.Body.String() != "Bearer "+bearer {
				t.Errorf("identity crossed: %d %s", w.Code, w.Body.String())
			}
		}(bearer)
	}
	group.Wait()
	r := httptest.NewRequest("GET", "/v1/models", nil)
	r.Header.Set("Authorization", "Bearer ses_user_aaaaaaaa")
	r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || strings.Contains(w.Body.String(), "must-not-leak") || !strings.Contains(w.Body.String(), `"id":"upstream-m"`) || !strings.Contains(w.Body.String(), `"deploymentId":"m"`) {
		t.Fatalf("model listing=%d %s", w.Code, w.Body.String())
	}
	entries, _ := os.ReadDir(home)
	if len(entries) != 0 || c.history != nil || c.connector != nil {
		t.Fatal("public proxy started local state")
	}
}

func TestPublicProxyStreamsAndCancelsUpstream(t *testing.T) {
	cancelled := make(chan struct{})
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(cancelled)
	}))
	defer gateway.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"hosted":false}`) }))
	defer api.Close()
	c := &core{control: api.URL, gateway: gateway.URL}
	h, _ := c.publicProxyHandler(publicTestSecret, "internal-test")
	server := httptest.NewServer(h)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/responses", strings.NewReader(`{"model":"m","stream":true}`))
	r.Header.Set("Authorization", "Bearer ses_user_aaaaaaaa")
	r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 13)
	if _, err = io.ReadFull(response.Body, raw); err != nil || !bytes.Equal(raw, []byte("data: first\n\n")) {
		t.Fatalf("stream=%q err=%v", raw, err)
	}
	cancel()
	response.Body.Close()
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream not cancelled")
	}
}

func TestPublicProxyHostedUsesAPIAuthorizationAndNoSQLite(t *testing.T) {
	home := t.TempDir()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer upstream-secret" || r.Header.Get("X-Newnoa-Public-Proxy") != "" {
			t.Error("hosted upstream credentials crossed trust boundary")
		}
		io.WriteString(w, `{"choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`)
	}))
	defer upstream.Close()
	finishes := 0
	api := privateControlTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		var v map[string]any
		if strings.HasSuffix(r.URL.Path, "/finish") {
			decodeHostedFixtureClosure(t, r, &v)
		} else {
			json.NewDecoder(r.Body).Decode(&v)
		}
		if strings.HasSuffix(r.URL.Path, "/finish") {
			finishes++
			if r.Header.Get("Authorization") != "Bearer "+hostedFixtureReportToken || v["status"] != "succeeded" || v["inputTokens"] != float64(2) {
				t.Errorf("completion lost=%v", v)
			}
			io.WriteString(w, `{"finalized":true}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer internal-test" || v["secret"] != "ses_user_aaaaaaaa" {
			t.Error("public control authorization lost")
		}
		switch r.URL.Path {
		case "/api/v1/internal/public/authorize":
			io.WriteString(w, `{"hosted":true,"deploymentId":"`+hostedFixtureID+`"}`)
		case "/api/v1/internal/public/hosted/dispatches":
			if v["model"] != hostedFixtureID {
				t.Errorf("hosted model not normalized by API: %v", v)
			}
			if v["clientIp"] != "203.0.113.9" {
				t.Errorf("trusted Java peer not retained: %v", v)
			}
			json.NewEncoder(w).Encode(map[string]any{"accountId": hostedFixtureID, "keyId": hostedFixtureID, "requestId": hostedFixtureID, "reportToken": hostedFixtureReportToken, "model": "real-model", "modelType": "llm", "endpoint": "http://upstream.test/custom", "apiKey": "upstream-secret", "revision": 2})
		default:
			t.Errorf("unexpected public API path=%s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	c := &core{home: home, control: api.URL, gateway: "http://127.0.0.1:1", onlineSupply: fixturePublicPolicy(upstream)}
	h, err := c.publicProxyHandler(publicTestSecret, "internal-test")
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"public-callable-name","messages":[]}`))
	r.Header.Set("Authorization", "Bearer ses_user_aaaaaaaa")
	r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
	r.Header.Set("X-Newnoa-Caller-IP", "203.0.113.9")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 || finishes != 1 {
		t.Fatalf("hosted response=%d %s finishes=%d", w.Code, w.Body.String(), finishes)
	}
	entries, _ := os.ReadDir(home)
	if len(entries) != 0 || c.history != nil {
		t.Fatal("hosted public call opened persistent storage")
	}
}

func TestPublicProxyBoundsAndProtocolPaths(t *testing.T) {
	forwarded := 0
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded++; io.WriteString(w, r.URL.Path) }))
	defer gateway.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"hosted":false}`) }))
	defer api.Close()
	c := &core{control: api.URL, gateway: gateway.URL}
	h, _ := c.publicProxyHandler(publicTestSecret, "internal-test")
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/embeddings", "/v1/rerank", "/v1/systemone", "/v1/audio/transcriptions"} {
		body := `{"model":"m"}`
		contentType := "application/json"
		if path == "/v1/audio/transcriptions" {
			contentType = "multipart/form-data; boundary=example"
			body = "--example\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nm\r\n--example--\r\n"
		}
		r := httptest.NewRequest("POST", path, strings.NewReader(body))
		r.Header.Set("Content-Type", contentType)
		r.Header.Set("Authorization", "Bearer ses_user_aaaaaaaa")
		r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 || w.Body.String() != path {
			t.Errorf("path %s failed=%d %s", path, w.Code, w.Body.String())
		}
	}
	for _, tc := range []struct {
		method, path, body, caller string
		want                       int
	}{
		{"POST", "/rpc", `{"model":"m"}`, "", 404},
		{"POST", "/v1/responses?target=evil", `{"model":"m"}`, "", 404},
		{"POST", "/v1/embeddings", strings.Repeat("x", (1<<20)+1), "", 413},
		{"POST", "/v1/responses", `{"model":"m"}`, "203.0.113.1, 127.0.0.1", 400},
		{"POST", "/v1/responses", `{}`, "", 400},
	} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer ses_user_aaaaaaaa")
		r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
		if tc.caller != "" {
			r.Header.Set("X-Newnoa-Caller-IP", tc.caller)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Errorf("invalid %s status=%d wanted=%d", tc.path, w.Code, tc.want)
		}
	}
	if forwarded != 7 {
		t.Fatalf("invalid request reached gateway; forwarded=%d", forwarded)
	}
}

func TestPublicProxyRejectsMalformedAuthorizationResponse(t *testing.T) {
	reached := false
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true }))
	defer gateway.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{}`) }))
	defer api.Close()
	c := &core{control: api.URL, gateway: gateway.URL}
	h, _ := c.publicProxyHandler(publicTestSecret, "internal-test")
	r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"m"}`))
	r.Header.Set("Authorization", "Bearer ses_user_aaaaaaaa")
	r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if reached || w.Code != 403 {
		t.Fatalf("malformed authorization admitted: status=%d reached=%v", w.Code, reached)
	}
}

func TestLocalProxyTrustedJavaBearerOverridesOnlyItsOwnRequest(t *testing.T) {
	t.Setenv("NEWNOA_PUBLIC_PROXY_SECRET", publicTestSecret)
	t.Setenv("NEWNOA_INTERNAL_TOKEN", "internal-test")
	publicAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/internal/public/authorize" || r.Header.Get("Authorization") != "Bearer internal-test" {
			t.Errorf("unexpected private API request %s", r.URL.Path)
		}
		io.WriteString(w, `{"hosted":false}`)
	}))
	defer publicAPI.Close()
	t.Setenv("NEWNOA_PUBLIC_CONTROL_URL", publicAPI.URL)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		if r.Header.Get("Authorization") == "Bearer internal-test" {
			t.Error("internal service credential leaked to ordinary API origin")
		}
		switch r.URL.Path {
		case "/api/v1/me":
			io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
		case "/api/v1/client/hosted/models":
			io.WriteString(w, `[]`)
		default:
			t.Errorf("unexpected control path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Newnoa-Public-Proxy") != "" {
			t.Error("service secret leaked")
		}
		io.WriteString(w, r.Header.Get("Authorization")+"/"+r.Header.Get("X-Api-Key"))
	}))
	defer gateway.Close()
	local := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	local.control = api.URL
	local.setProxySession("ses_local_saved_session")
	defer local.shutdown()
	if err := local.startProxyAt("", availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for _, bearer := range []string{"ses_public_aaaaaaaa", "ses_public_bbbbbbbb"} {
		group.Add(1)
		go func(bearer string) {
			defer group.Done()
			r, _ := http.NewRequest("POST", local.proxyBaseURL()+"/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
			r.Header.Set("Authorization", "Bearer "+bearer)
			r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
			response, err := http.DefaultClient.Do(r)
			if err != nil {
				t.Error(err)
				return
			}
			defer response.Body.Close()
			body, _ := io.ReadAll(response.Body)
			if response.StatusCode != 200 || string(body) != "Bearer "+bearer+"/" {
				t.Errorf("trusted Java identity rebound: %d %s", response.StatusCode, body)
			}
		}(bearer)
	}
	group.Wait()
	for _, service := range []string{"", "wrong"} {
		r, _ := http.NewRequest("GET", local.proxyBaseURL()+"/v1/models", nil)
		r.Header.Set("Authorization", "Bearer nn_local_agent_key_aaaa")
		if service != "" {
			r.Header.Set("X-Newnoa-Public-Proxy", service)
		}
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if service == "" {
			if response.StatusCode != 200 || string(body) != "Bearer ses_local_saved_session/nn_local_agent_key_aaaa" {
				t.Fatalf("local identity changed: %d %s", response.StatusCode, body)
			}
		} else if response.StatusCode != 401 {
			t.Fatalf("invalid service secret fell back to local identity: %d %s", response.StatusCode, body)
		}
	}
	if local.publicStateless || local.publicInternalToken != "" || local.proxySession != "ses_local_saved_session" {
		t.Fatal("Java requests changed local Core state")
	}
}

func TestPublicProxyRejectsNonPrivateControlOrigin(t *testing.T) {
	for _, origin := range []string{"https://api.example.invalid", "http://8.8.8.8", "http://localhost:18090", "http://127.0.0.1:18090/path", "http://127.0.0.1:18090?x=y", "http://user:pass@127.0.0.1:18090", "ftp://127.0.0.1:18090"} {
		c := &core{control: origin, gateway: "http://127.0.0.1:1"}
		if _, err := c.publicProxyHandler(publicTestSecret, "internal-test"); err == nil {
			t.Errorf("non-private control origin accepted: %s", origin)
		}
	}
}
