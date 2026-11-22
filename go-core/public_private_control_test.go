package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPublicProxyUsesConfiguredPrivateControlWithoutWeakeningDesktop(t *testing.T) {
	for _, origin := range []string{"http://10.219.77.28:8090", "http://172.16.2.3:8090", "http://192.168.2.3:8090", "http://[fd00::2]:8090"} {
		t.Run(origin, func(t *testing.T) {
			if _, err := controlBase(origin); err == nil {
				t.Fatal("desktop HTTP control policy widened")
			}
			if _, err := privateInferenceWithSelector(origin, "https://server.example.test", nil); err == nil {
				t.Fatal("desktop inference accepted private HTTP control")
			}
			c := &core{control: origin, gateway: "https://server.example.test"}
			handler, err := c.publicProxyHandler(publicTestSecret, "private-api-token")
			if err != nil {
				t.Fatalf("configured private public-mode control rejected: %v", err)
			}
			t.Cleanup(c.publicSVGCancel)
			r := httptest.NewRequest("GET", "/internal/www/health", nil)
			r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"ready"`) {
				t.Fatalf("private control readiness: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestPublicProxyPrivateControlStillRejectsUnsafeOrigins(t *testing.T) {
	for _, origin := range []string{"http://169.254.1.2:8090", "http://[fe80::1]:8090", "http://0.0.0.0:8090", "http://8.8.8.8:8090", "https://api.example.test", "http://10.219.77.28:8090?", "http://10.219.77.28:8090#fragment", "http://10.219.77.28:8090/path", "http://user@10.219.77.28:8090"} {
		t.Run(origin, func(t *testing.T) {
			c := &core{control: origin, gateway: "https://server.example.test"}
			if _, err := c.publicProxyHandler(publicTestSecret, "private-api-token"); err == nil {
				t.Cleanup(c.publicSVGCancel)
				t.Fatal("unsafe private control accepted")
			}
		})
	}
	c := &core{control: "http://10.219.77.28:8090", gateway: "http://8.8.8.8:6666"}
	if _, err := c.publicProxyHandler(publicTestSecret, "private-api-token"); err == nil {
		t.Cleanup(c.publicSVGCancel)
		t.Fatal("public endpoint lost TLS requirement")
	}
}
