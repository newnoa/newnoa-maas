package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDockerBrowserBoundaryRequiresTrustedLiteralHostAndMatchingOrigin(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	handler := browserBoundary(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, row := range []struct {
		host, origin string
		want         int
	}{
		{"192.168.1.8:6060", "http://192.168.1.8:6060", 204},
		{"192.168.1.8:6060", "", 204},
		{"[fd00::8]:6060", "http://[fd00::8]:6060", 204},
		{"192.168.1.8:6060", "http://192.168.1.9:6060", 403},
		{"192.168.1.8:6060", "http://192.168.1.8:6061", 403},
		{"192.168.1.8:6060", "https://192.168.1.8:6060", 403},
		{"192.168.1.8:6060", "http://localhost:6060", 403},
		{"attacker.example:6060", "http://attacker.example:6060", 403},
		{"0.0.0.0:6060", "", 403},
		{"192.168.1.8:6060", "null", 403},
	} {
		r := httptest.NewRequest("POST", "/api/local/core", nil)
		r.Host = row.host
		r.Header.Set("Origin", row.origin)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != row.want {
			t.Errorf("host=%s origin=%s: got %d want %d", row.host, row.origin, w.Code, row.want)
		}
	}
	t.Setenv("NEWNOA_RUNTIME_MODE", "")
	r := httptest.NewRequest("GET", "http://192.168.1.8:6060/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("desktop browser boundary accepted a LAN host")
	}
}

func TestDockerProxyStatusAdvertisesRequestHostNotLoopback(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	c.proxyLAN = true
	handler := browserLocal(c, api.URL)
	for _, host := range []string{"192.168.1.8:6060", "localhost:6060", "h5.example.invalid", "dev-h5.example.invalid", "sit-h5.example.invalid", "dev-h5-cn.example.invalid"} {
		r := httptest.NewRequest("POST", "/api/local/core", strings.NewReader(`{"method":"proxy.status","params":{}}`))
		r.Host = host
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Forwarded-Host", "attacker.example")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		var result struct {
			RuntimeMode string   `json:"runtimeMode"`
			BaseURLs    []string `json:"baseUrls"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		if result.RuntimeMode != "docker" {
			t.Fatalf("missing Docker runtime: %s", w.Body.String())
		}
		if host == "192.168.1.8:6060" {
			if len(result.BaseURLs) != 1 || result.BaseURLs[0] != "https://192.168.1.8:18222/v1" {
				t.Fatalf("bad advertised URL: %v", result.BaseURLs)
			}
		} else if len(result.BaseURLs) != 0 {
			t.Fatalf("must not advertise loopback/domain: %v", result.BaseURLs)
		}
	}
}
