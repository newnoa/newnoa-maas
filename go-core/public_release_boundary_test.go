package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublicControlRejectsUnavailableAndUnboundedResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"denied", 403, `{}`}, {"invalid JSON", 200, `{`}, {"oversized", 200, strings.Repeat("x", (1<<20)+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status); io.WriteString(w, tc.body) }))
			defer api.Close()
			c := &core{control: api.URL, publicInternalToken: "internal-test"}
			var result map[string]any
			if err := c.publicControl(context.Background(), "/models", map[string]string{"secret": "ses_website_only"}, &result); err == nil {
				t.Fatal("invalid control response accepted")
			}
		})
	}
	c := &core{control: "http://127.0.0.1:1"}
	var result map[string]any
	if err := c.publicControl(context.Background(), "/models", make(chan int), &result); err == nil {
		t.Fatal("unencodable control body accepted")
	}
	if err := c.publicControl(context.Background(), "/models", nil, &result); err == nil {
		t.Fatal("unreachable control accepted")
	}
	c.control = ":"
	if err := c.publicControl(context.Background(), "/models", nil, &result); err == nil {
		t.Fatal("invalid control URL accepted")
	}
}

func TestPublicProxyRejectsAmbiguousCallerAndServiceIdentity(t *testing.T) {
	c := &core{control: "http://127.0.0.1:1", gateway: "http://127.0.0.1:1"}
	for _, tc := range []struct{ secret, internal string }{{"", "internal"}, {publicTestSecret, ""}, {strings.Repeat("x", 4097), "internal"}, {publicTestSecret + "\n", "internal"}} {
		if _, err := c.publicProxyHandler(tc.secret, tc.internal); err == nil {
			t.Fatal("invalid service identity accepted")
		}
	}
	h, err := c.publicProxyHandler(publicTestSecret, "internal-test")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		headers http.Header
		want    int
	}{
		{"missing bearer", http.Header{}, 401},
		{"duplicate bearer", http.Header{"Authorization": []string{"Bearer ses_website_only", "Bearer ses_website_only"}}, 401},
		{"duplicate caller session", http.Header{"X-Newnoa-Caller-Session": []string{"ses_website_only", "ses_website_only"}}, 401},
		{"invalid caller session", http.Header{"X-Newnoa-Caller-Session": []string{"not_a_session"}}, 401},
		{"duplicate caller IP", http.Header{"X-Newnoa-Caller-Ip": []string{"203.0.113.1", "203.0.113.2"}}, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/v1/models", nil)
			r.Header = tc.headers
			if tc.name != "missing bearer" && r.Header.Get("Authorization") == "" {
				r.Header.Set("Authorization", "Bearer ses_website_only")
			}
			r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	c.gateway = "not-a-gateway"
	if _, err := c.publicProxyHandler(publicTestSecret, "internal-test"); err == nil {
		t.Fatal("invalid gateway accepted")
	}
}

func TestPublicProxyModelListingPreservesTestSessionAndCallableFallback(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["session"] != "ses_website_only" || body["secret"] != "nn_test_website_only" {
			t.Error("model discovery lost test-key/session binding")
		}
		io.WriteString(w, `[{"id":"deployment-only","name":"[TEST] fallback"},{"name":"missing id"}]`)
	}))
	defer api.Close()
	c := &core{control: api.URL, gateway: "http://127.0.0.1:1"}
	h, err := c.publicProxyHandler(publicTestSecret, "internal-test")
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/v1/models", nil)
	r.Header.Set("Authorization", "Bearer nn_test_website_only")
	r.Header.Set("X-Newnoa-Caller-Session", "ses_website_only")
	r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var list struct {
		Data []struct {
			ID string `json:"id"`
		}
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &list) != nil || len(list.Data) != 1 || list.Data[0].ID != "deployment-only" {
		t.Fatalf("invalid discovery: %d %s", w.Code, w.Body.String())
	}
	api.Close()
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 502 {
		t.Fatalf("discovery failure did not fail closed: %d", w.Code)
	}
}

func TestPublicProxyStartupRejectsInvalidOrOccupiedPrivateListener(t *testing.T) {
	t.Setenv("NEWNOA_PUBLIC_PROXY_LISTEN", "0.0.0.0:18223")
	if err := servePublicProxy(); err == nil {
		t.Fatal("public listener accepted")
	}
	t.Setenv("NEWNOA_PUBLIC_PROXY_LISTEN", "")
	t.Setenv("NEWNOA_CONTROL_URL", "")
	t.Setenv("NEWNOA_GATEWAY_URL", "")
	t.Setenv("NEWNOA_PUBLIC_PROXY_SECRET", publicTestSecret)
	t.Setenv("NEWNOA_INTERNAL_TOKEN", "internal-test")
	t.Setenv("NEWNOA_CORE_TEST_HOME", t.TempDir())
	if err := servePublicProxy(); err == nil {
		t.Fatal("default non-private control accepted")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("NEWNOA_PUBLIC_PROXY_LISTEN", listener.Addr().String())
	t.Setenv("NEWNOA_CONTROL_URL", "http://127.0.0.1:1")
	t.Setenv("NEWNOA_GATEWAY_URL", "http://127.0.0.1:1")
	if err := servePublicProxy(); err == nil || !strings.Contains(err.Error(), "listener unavailable") {
		t.Fatalf("occupied bind should fail without taking listener: %v", err)
	}
}
