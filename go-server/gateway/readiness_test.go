package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadinessRequiresControlReady(t *testing.T) {
	for _, body := range []string{`{"ready":true}`, `{"ready":false}`, `{"ready":"true"}`, `invalid`} {
		t.Run(body, func(t *testing.T) {
			control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/ready" || r.Header.Get("Authorization") != "" {
					t.Error("Unexpected probe request")
				}
				w.Write([]byte(body))
			}))
			defer control.Close()
			g := newGateway(control.URL, "private-token", "readiness", nil)
			w := httptest.NewRecorder()
			g.ServeHTTP(w, httptest.NewRequest("GET", "/ready", nil))
			expected := 503
			if body == `{"ready":true}` {
				expected = 200
			}
			if w.Code != expected {
				t.Fatalf("status %d, want %d", w.Code, expected)
			}
		})
	}
}

func TestReadinessFailureIsBoundedAndDoesNotRelayControlBody(t *testing.T) {
	for _, mode := range []string{"status", "redirect", "oversized", "timeout", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				switch mode {
				case "status":
					w.WriteHeader(503)
					w.Write([]byte("private control diagnostics"))
				case "redirect":
					w.Header().Set("Location", "/leaked")
					w.WriteHeader(302)
				case "oversized":
					w.Write([]byte(`{"ready":true,"extra":"` + strings.Repeat("x", 4096) + `"}`))
				case "timeout":
					<-r.Context().Done()
				}
			}))
			defer control.Close()
			if mode == "unavailable" {
				control.Close()
			}
			g := newGateway(control.URL, "private-token", "readiness", nil)
			started := time.Now()
			w := httptest.NewRecorder()
			g.ServeHTTP(w, httptest.NewRequest("GET", "/ready", nil))
			if w.Code != 503 || strings.TrimSpace(w.Body.String()) != `{"ready":false}` {
				t.Fatalf("Unsanitized readiness: %d %s", w.Code, w.Body.String())
			}
			if time.Since(started) > 2500*time.Millisecond {
				t.Fatal("Probe exceeded timeout budget")
			}
			if calls.Load() > 1 {
				t.Fatal("Probe retried or followed a redirect")
			}
			w = httptest.NewRecorder()
			g.ServeHTTP(w, httptest.NewRequest("GET", "/health", nil))
			if w.Code != 200 {
				t.Fatal("Liveness depends on control availability")
			}
		})
	}
}
