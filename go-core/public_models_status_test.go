package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPublicModelsPreservesOnlyAuthenticationStatusWithoutPrivateBodyOrInference(t *testing.T) {
	for _, code := range []int{200, 401, 403, 302, 400, 409, 429, 500, 503} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var apiCalls, serverCalls atomic.Int32
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { serverCalls.Add(1); w.WriteHeader(500) }))
			defer gateway.Close()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				apiCalls.Add(1)
				if r.URL.Path != "/api/v1/internal/public/models" || r.Method != "POST" {
					t.Error("unexpected control request")
				}
				w.Header().Set("Location", gateway.URL)
				w.WriteHeader(code)
				if code == 200 {
					io.WriteString(w, `[{"id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","model":"fixture"}]`)
				} else {
					io.WriteString(w, `{"message":"private API body with secret fixture"}`)
				}
			}))
			defer api.Close()
			c := &core{control: api.URL, gateway: gateway.URL}
			h, err := c.publicProxyHandler(publicTestSecret, publicTestSecret)
			if err != nil {
				t.Fatal(err)
			}
			defer c.publicSVGCancel()
			r := httptest.NewRequest("GET", "/v1/models", nil)
			r.Header.Set("Authorization", "Bearer sk-"+strings.Repeat("A", 43))
			r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
			out := httptest.NewRecorder()
			h.ServeHTTP(out, r)
			want := 502
			if code == 200 || code == 401 || code == 403 {
				want = code
			}
			if out.Code != want || strings.Contains(out.Body.String(), "private API body") || apiCalls.Load() != 1 || serverCalls.Load() != 0 {
				t.Fatalf("status=%d want=%d APIcalls=%d serverCalls=%d body=%s", out.Code, want, apiCalls.Load(), serverCalls.Load(), out.Body.String())
			}
			if code == 200 && !strings.Contains(out.Body.String(), `"id":"fixture"`) {
				t.Fatal("successful model listing changed")
			}
		})
	}
}

func TestPublicModelsInvalidResponsesStayUnavailable(t *testing.T) {
	for _, body := range []string{`not-json`, strings.Repeat("x", (1<<20)+1)} {
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
		assertPublicModelsUnavailable(t, api.URL)
		api.Close()
	}
	api := httptest.NewServer(http.NotFoundHandler())
	address := api.URL
	api.Close()
	assertPublicModelsUnavailable(t, address)
}

func assertPublicModelsUnavailable(t *testing.T, address string) {
	t.Helper()
	c := &core{control: address, gateway: "http://127.0.0.1:1"}
	h, err := c.publicProxyHandler(publicTestSecret, publicTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	defer c.publicSVGCancel()
	r := httptest.NewRequest("GET", "/v1/models", nil)
	r.Header.Set("Authorization", "Bearer sk-"+strings.Repeat("A", 43))
	r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
	out := httptest.NewRecorder()
	h.ServeHTTP(out, r)
	if out.Code != 502 || out.Body.String() != "model authorization unavailable\n" {
		t.Fatalf("invalid response changed: %d %s", out.Code, out.Body.String())
	}
}
