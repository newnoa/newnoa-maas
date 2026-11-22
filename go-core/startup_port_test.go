package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStartupPortConflictKeepsCoreAvailableAndAllowsExplicitRetry(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	port := occupied.Addr().(*net.TCPAddr).Port
	if err := c.bindStartupPort(port); err == nil {
		t.Fatal("accepted occupied port")
	}
	state, err := c.handle("startup.status", json.RawMessage(`{}`))
	if err != nil || state.(map[string]any)["ready"] != false {
		t.Fatalf("status unavailable: %v %v", state, err)
	}
	if _, err := c.handle("handshake", json.RawMessage(`{"nonce":"still-alive"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.handle("startup.bind", json.RawMessage(fmt.Sprintf(`{"port":%d}`, port))); err == nil {
		t.Fatal("retry accepted occupied port")
	}
	free := availableProxyPort(t)
	state, err = c.handle("startup.bind", json.RawMessage(fmt.Sprintf(`{"port":%d}`, free)))
	if err != nil || state.(map[string]any)["ready"] != true {
		t.Fatalf("retry failed: %v %v", state, err)
	}
	if c.proxyListener == nil || c.proxyListener.Addr().(*net.TCPAddr).Port != free {
		t.Fatal("selected port was not bound")
	}
	if c.server == nil {
		t.Fatal("startup did not start the local proxy")
	}
	connection, err := net.Dial("tcp", occupied.Addr().String())
	if err != nil {
		t.Fatal("unrelated listener was changed", err)
	}
	connection.Close()
}

func TestStartupHTTPAllowsOnlyBoundedPortSetupBeforeLogin(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	handler := browserStartup(c)
	for _, row := range []struct {
		method, body string
		want         int
	}{
		{"GET", "", 200},
		{"DELETE", "", 405},
		{"POST", `{"port":1023}`, 400},
		{"POST", `{"port":65536}`, 400},
		{"POST", `{"port":18222,"secret":"forged"}`, 400},
		{"POST", `{"port":"18222"}`, 400},
		{"POST", `{}`, 400},
		{"POST", `{"port":18000} {}`, 400},
	} {
		request := httptest.NewRequest(row.method, "http://127.0.0.1/api/local/startup", strings.NewReader(row.body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != row.want {
			t.Errorf("%s %s: %d want %d", row.method, row.body, response.Code, row.want)
		}
	}
}
