package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestWebsiteCoreHealthRequiresServiceIdentityWithoutUserSession(t *testing.T) {
	c := &core{control: "http://127.0.0.1:1", gateway: "http://127.0.0.1:1", proxySession: "ses_must_not_be_used"}
	handler, err := c.publicProxyHandler(publicTestSecret, "internal-test")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, secret string
		want                 int
	}{
		{"GET", "/internal/www/health", publicTestSecret, 200},
		{"GET", "/internal/www/health", "", 401},
		{"GET", "/internal/www/health", "wrong", 401},
		{"POST", "/internal/www/health", publicTestSecret, 404},
		{"GET", "/internal/www/health?probe=anything", publicTestSecret, 404},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		r.Header.Set("X-Newnoa-Public-Proxy", tc.secret)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s %s: got %d, want %d", tc.method, tc.path, w.Code, tc.want)
		}
		if tc.want == 200 {
			var value map[string]any
			if json.Unmarshal(w.Body.Bytes(), &value) != nil || value["service"] != "website-go-core" || value["status"] != "ready" {
				t.Fatalf("unexpected health response: %s", w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("health response must not be cached by intermediaries")
			}
		}
	}
	r := httptest.NewRequest("GET", "/internal/www/health", nil)
	r.Header["X-Newnoa-Public-Proxy"] = []string{publicTestSecret, publicTestSecret}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("duplicate service credentials accepted")
	}
}
