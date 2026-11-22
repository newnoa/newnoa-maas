package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMessagesProxyReplacesLocalNativeKey(t *testing.T) {
	calls := 0
	secret := "nn_" + strings.Repeat("x", 40)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/v1/messages" || r.Header.Get("Authorization") != "" || (r.Header.Get("X-Api-Key") != secret && r.Header.Get("X-Api-Key") != "wrong") {
			t.Error("local key leaked or credential not replaced")
		}
		io.WriteString(w, `{}`)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	port := availableProxyPort(t)
	if err := c.startProxyAt(secret, port); err != nil {
		t.Fatal(err)
	}
	defer c.close()
	for _, tc := range []struct {
		key, auth string
		code      int
	}{{"wrong", "", 200}, {c.localKey, "", 200}, {"", "Bearer " + c.localKey, 200}, {c.localKey, "Bearer " + c.localKey, 200}} {
		req, _ := http.NewRequest("POST", c.proxyBaseURL()+"/v1/messages", strings.NewReader(`{}`))
		req.Header.Set("X-Api-Key", tc.key)
		req.Header.Set("Authorization", tc.auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != tc.code {
			t.Fatal("auth boundary", res.StatusCode, tc.code)
		}
	}
	if calls != 4 {
		t.Fatal("unexpected upstream calls", calls)
	}
}

func TestMessagesProxyPreservesBodyWithoutManagedModel(t *testing.T) {
	secret := "nn_" + strings.Repeat("x", 40)
	seen := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		seen <- string(body)
		io.WriteString(w, `{}`)
	}))
	defer upstream.Close()
	c := fixedGatewayTestCore(t.TempDir(), upstream.URL)
	port := availableProxyPort(t)
	if err := c.startProxyAt(secret, port); err != nil {
		t.Fatal(err)
	}
	defer c.close()
	req, _ := http.NewRequest("POST", c.proxyBaseURL()+"/v1/messages", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+secret)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	if body := <-seen; body != `{}` {
		t.Fatalf("upstream body = %q", body)
	}
}
