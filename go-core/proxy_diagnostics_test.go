package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProxyDiagnosticsFixedCatalogAndSanitizedResults(t *testing.T) {
	status, body, calls := 200, `{"data":[]}`, 0
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" || r.URL.Path != "/v1/models" || r.Header.Get("X-Api-Key") != "nn_"+strings.Repeat("x", 40) {
			t.Error("unexpected probe")
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	defer c.close()
	if c.diagnoseProxy()["status"] != "stopped" || calls != 0 {
		t.Fatal("stopped proxy probed")
	}
	if err := startProxyRPC(t, c, availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		code       int
		body, want string
	}{{200, `{"data":[]}`, "reachable"}, {401, "private-secret", "authorization_rejected"}, {403, "private-secret", "authorization_rejected"}, {503, "private-secret", "unavailable"}, {302, "private-secret", "unavailable"}, {200, `{"data":null}`, "invalid_response"}, {200, `{"data":{}}`, "invalid_response"}, {200, strings.Repeat("x", maxFrame+1), "invalid_response"}} {
		status, body = test.code, test.body
		got := c.diagnoseProxy()
		if got["status"] != test.want {
			t.Fatalf("%v", got)
		}
		raw, _ := json.Marshal(got)
		if strings.Contains(string(raw), "private-secret") || strings.Contains(string(raw), c.localKey) {
			t.Fatal("probe leaked content")
		}
	}
	if calls != 8 {
		t.Fatal("unexpected request count", calls)
	}
	if _, err := c.handle("proxy.diagnose", json.RawMessage(`{"url":"http://other"}`)); err == nil {
		t.Fatal("caller network target accepted")
	}
}
