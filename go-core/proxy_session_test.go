package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProxyForwardsAPIKeyWithCurrentLoginSession(t *testing.T) {
	var key, auth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, auth = r.Header.Get("X-Api-Key"), r.Header.Get("Authorization")
		w.WriteHeader(418)
	}))
	defer upstream.Close()
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `[]`) }))
	defer control.Close()
	c := fixedGatewayTestCore(t.TempDir(), upstream.URL)
	c.control = control.URL
	defer c.shutdown()
	if err := c.bindStartupPort(availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	for _, session := range []string{"", "ses_current_login", "ses_next_login", ""} {
		if _, err := c.handle("proxy.session", json.RawMessage(`{"session":"`+session+`"}`)); err != nil {
			t.Fatal(err)
		}
		for _, apiKey := range []string{"custom-key", "", "invalid-key"} {
			request, _ := http.NewRequest("GET", c.proxyBaseURL()+"/v1/models", nil)
			if apiKey != "" {
				request.Header.Set("Authorization", "Bearer "+apiKey)
			}
			request.Header.Set("X-Newnoa-Session", "spoofed")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			wantAuth := ""
			if session != "" {
				wantAuth = "Bearer " + session
			}
			if response.StatusCode != 418 || key != apiKey || auth != wantAuth {
				t.Fatalf("forwarding mismatch: status=%d key=%q session matches=%v", response.StatusCode, key, auth == wantAuth)
			}
		}
	}
}

func TestBrowserLoginSwitchAndLogoutUpdateForwardedSession(t *testing.T) {
	const first, second = "ses_first_login", "ses_second_login"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/client/hosted/models" {
			io.WriteString(w, `[]`)
			return
		}
		id := "00000000-0000-0000-0000-000000000001"
		if r.Header.Get("Authorization") == "Bearer "+second {
			id = "00000000-0000-0000-0000-000000000002"
		}
		json.NewEncoder(w).Encode(map[string]string{"id": id})
	}))
	defer control.Close()
	var auth string
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { auth = r.Header.Get("Authorization"); w.WriteHeader(418) }))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	c.control = control.URL
	defer c.shutdown()
	if err := c.bindStartupPort(availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	account := &browserAccount{core: c}
	for _, token := range []string{first, second, ""} {
		if token != "" {
			if err := account.authenticate(httptest.NewRequest("POST", "/api/local/account", nil), token, control.URL); err != nil {
				t.Fatal(err)
			}
		} else if err := account.logout(second, control.URL); err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequest("GET", c.proxyBaseURL()+"/v1/models", nil)
		req.Header.Set("X-Api-Key", "application-key")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		want := ""
		if token != "" {
			want = "Bearer " + token
		}
		if res.StatusCode != 418 || auth != want {
			t.Fatalf("login identity not propagated: status=%d matches=%v", res.StatusCode, auth == want)
		}
	}
}

func TestBrowserObserverLogoutCannotRebindNativeInference(t *testing.T) {
	const host, observer = "ses_host_login", "ses_observer_login"
	denied := make(chan struct{})
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer "+observer {
			select {
			case <-denied:
				w.WriteHeader(401)
				return
			default:
			}
		}
		if r.URL.Path == "/api/v1/client/hosted/models" {
			io.WriteString(w, `[]`)
			return
		}
		io.WriteString(w, `{"id":"00000000-0000-0000-0000-000000000001"}`)
	}))
	defer control.Close()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+host {
			t.Error("proxy forwarded a different browser session")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	c.control = control.URL
	defer c.shutdown()
	if err := c.bindStartupPort(availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	a := &browserAccount{core: c}
	if err := a.authenticate(httptest.NewRequest("POST", "/api/local/account", nil), host, control.URL); err != nil {
		t.Fatal(err)
	}
	handler := a.handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }), control.URL)
	for _, body := range []string{`{"method":"agent.remote.status","params":{}}`, `{"method":"remote.snapshot","params":{}}`, `{"method":"proxy.status","params":{}}`, `{"method":"agent.detect","params":{"agent":"codex"}}`, `{"method":"history.database.status","params":{}}`} {
		r := httptest.NewRequest("POST", "/api/local/core", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+observer)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 204 {
			t.Fatalf("valid observer could not read: %d", w.Code)
		}
	}
	close(denied)
	req, _ := http.NewRequest("POST", c.proxyBaseURL()+"/v1/responses", strings.NewReader(`{"model":"00000000-0000-0000-0000-000000000042","input":"fixture"}`))
	req.Header.Set("Authorization", "Bearer nn_fixture_native_key_123456789")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("observer revoked a valid native inference route: HTTP %d", res.StatusCode)
	}
}
