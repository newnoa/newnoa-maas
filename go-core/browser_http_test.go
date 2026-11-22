package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBrowserAPIForwardsSessionAndLogout(t *testing.T) {
	active := true
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer session" {
			t.Error("session missing")
		}
		if r.URL.Path == "/api/v1/auth/logout" {
			active = false
		}
		if r.URL.Path == "/api/v1/me" && !active {
			w.WriteHeader(401)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer upstream.Close()
	handler, err := browserAPI(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	for i, path := range []string{"/api/v1/me", "/api/v1/auth/logout", "/api/v1/me"} {
		request := httptest.NewRequest("POST", path, strings.NewReader(`{}`))
		request.Header.Set("Authorization", "Bearer session")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		want := 200
		if i == 2 {
			want = 401
		}
		if response.Code != want {
			t.Fatalf("%s: got %d, want %d", path, response.Code, want)
		}
	}
	for _, path := range []string{"/api/v1/admin/accounts", "/internal/permits", "/api/v1/internal/permits", "/api/v1/me/../admin/accounts"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != 404 {
			t.Fatalf("must reject %s: %d", path, response.Code)
		}
	}
	for _, row := range []struct {
		method string
		want   int
	}{{http.MethodPost, http.StatusOK}, {http.MethodGet, http.StatusNotFound}} {
		request := httptest.NewRequest(row.method, "/api/v1/model-descriptions/translate", strings.NewReader(`{"description":"fixture"}`))
		request.Header.Set("Authorization", "Bearer session")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != row.want {
			t.Fatalf("translation route method=%s got %d want %d", row.method, response.Code, row.want)
		}
	}
}

func TestBrowserBoundary(t *testing.T) {
	handler := browserBoundary(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, tc := range []struct {
		host, origin string
		want         int
	}{
		{"127.0.0.1:18092", "", 204},
		{"[::1]:18092", "http://[::1]:15179", 204},
		{"localhost:18092", "http://localhost:15179", 204},
		{"h5.example.invalid", "https://h5.example.invalid", 204},
		{"h5.example.invalid", "", 204},
		{"attacker.example", "", 403},
		{"127.0.0.1:18092", "https://attacker.example", 403},
		{"h5.example.invalid", "null", 403},
		{"h5.example.invalid", "https://h5.example.invalid.attacker.example", 403},
		{"h5.example.invalid", "https://h5.example.invalid/path", 403},
		{"127.0.0.1:bad:port", "", 403},
	} {
		r := httptest.NewRequest("POST", "/api/local/core", nil)
		r.Host = tc.host
		if tc.origin != "" {
			r.Header.Set("Origin", tc.origin)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Errorf("host=%s origin=%s: got %d want %d", tc.host, tc.origin, w.Code, tc.want)
		}
	}
}

func TestBrowserBoundaryPublicDomains(t *testing.T) {
	for _, mode := range []string{"", "docker"} {
		t.Run("runtime="+mode, func(t *testing.T) {
			t.Setenv("NEWNOA_RUNTIME_MODE", mode)
			handler := browserBoundary(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
			for _, host := range []string{"h5.example.invalid", "dev-h5.example.invalid", "sit-h5.example.invalid", "dev-h5-cn.example.invalid"} {
				origins := []string{"", "https://" + host, "http://" + host, "https://" + host + ".attacker.example", "https://" + host + "/path", "https://" + host + "?query=1", "https://" + host + "?", "https://" + host + "#", "https://user@" + host, "https://" + host + ":6060", "null", "http://localhost:15179", "https://h5.example.invalid", "https://dev-h5.example.invalid", "https://sit-h5.example.invalid", "https://dev-h5-cn.example.invalid"}
				for _, origin := range origins {
					t.Run(host+"/"+origin, func(t *testing.T) {
						r := httptest.NewRequest("POST", "/api/local/core", nil)
						r.Host = host
						r.Header.Set("Origin", origin)
						w := httptest.NewRecorder()
						handler.ServeHTTP(w, r)
						want := http.StatusForbidden
						if origin == "" || origin == "https://"+host {
							want = http.StatusNoContent
						}
						if w.Code != want {
							t.Errorf("got %d want %d", w.Code, want)
						}
					})
				}
				for _, host := range []string{host + ".attacker.example", "attacker-" + host} {
					r := httptest.NewRequest("GET", "/", nil)
					r.Host = host
					r.Header.Set("X-Forwarded-Host", "sit-h5.example.invalid")
					w := httptest.NewRecorder()
					handler.ServeHTTP(w, r)
					if w.Code != http.StatusForbidden {
						t.Errorf("lookalike host %s accepted", host)
					}
				}
			}
			for _, origin := range []string{"https://h5.example.invalid", "https://dev-h5.example.invalid", "https://sit-h5.example.invalid", "https://dev-h5-cn.example.invalid"} {
				r := httptest.NewRequest("POST", "/api/local/core", nil)
				r.Host = "localhost:18092"
				r.Header.Set("Origin", origin)
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				if w.Code != http.StatusNoContent {
					t.Errorf("loopback host with trusted H5 origin %s got %d", origin, w.Code)
				}
			}
		})
	}
}
