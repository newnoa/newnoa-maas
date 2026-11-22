package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLocalProxyForwardsNonLLMPathsAndMultipart(t *testing.T) {
	secret := "nn_" + strings.Repeat("x", 40)
	const raw = "multipart-fixture"
	const contentType = "multipart/form-data; boundary=fixture"
	calls := 0
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		data, _ := io.ReadAll(r.Body)
		if string(data) != raw || r.Header.Get("Content-Type") != contentType || r.Header.Get("X-Api-Key") != secret {
			t.Error("proxy changed payload or credential boundary")
		}
		io.WriteString(w, `{"ok":true}`)
	}))
	defer gateway.Close()
	core := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	if err := core.startProxyAt(secret, availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	defer core.close()
	for _, path := range []string{"/v1/embeddings", "/v1/rerank", "/v1/audio/transcriptions", "/v1/systemone"} {
		for _, authorized := range []bool{false, true} {
			req, _ := http.NewRequest("POST", core.proxyBaseURL()+path, strings.NewReader(raw))
			req.Header.Set("Content-Type", contentType)
			if authorized {
				req.Header.Set("Authorization", "Bearer "+core.localKey)
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			want := 200
			if res.StatusCode != want {
				t.Fatalf("path %s: got %d want %d", path, res.StatusCode, want)
			}
		}
	}
	if calls != 8 {
		t.Fatalf("upstream executions %d", calls)
	}
}

func TestLocalProxyPreservesApplicationKey(t *testing.T) {
	secret := "ses_" + strings.Repeat("s", 43)
	forwarded := ""
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded = r.Header.Get("X-Api-Key")
		io.WriteString(w, `{"data":[]}`)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	if err := c.startProxyAt("", availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	defer c.close()
	req, _ := http.NewRequest("GET", c.proxyBaseURL()+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+secret)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || forwarded != secret {
		t.Fatalf("status=%d; session identity not forwarded", response.StatusCode)
	}
}

func TestLocalProxyAPIKeyAliasRejectsConflicts(t *testing.T) {
	secret := "nn_" + strings.Repeat("a", 40)
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("X-Api-Key") != secret || r.Header.Get("api-key") != "" {
			t.Error("credential alias leaked or changed")
		}
		io.WriteString(w, `{"data":[]}`)
	}))
	defer upstream.Close()
	c := fixedGatewayTestCore(t.TempDir(), upstream.URL)
	if err := c.startProxyAt("", availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	defer c.close()
	for _, tc := range []struct {
		alias, key, auth string
		code             int
	}{{secret, "", "", 200}, {secret, secret, "Bearer " + secret, 200}, {secret, "different", "", 401}, {secret, "", "Bearer different", 401}} {
		req, _ := http.NewRequest("GET", c.proxyBaseURL()+"/v1/models", nil)
		req.Header.Set("api-key", tc.alias)
		if tc.key != "" {
			req.Header.Set("X-Api-Key", tc.key)
		}
		if tc.auth != "" {
			req.Header.Set("Authorization", tc.auth)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != tc.code {
			t.Errorf("status=%d want=%d", res.StatusCode, tc.code)
		}
	}
	if calls != 2 {
		t.Fatalf("forwarded contradictory credentials: %d", calls)
	}
}

func TestLocalProxyRejectsBrowserOriginsAndReboundHosts(t *testing.T) {
	calls := 0
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		io.WriteString(w, `{"data":[]}`)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	if err := c.startProxyAt("nn_"+strings.Repeat("x", 40), availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	defer c.close()
	for _, tc := range []struct {
		host, origin string
		want         int
	}{
		{"", "", 200},
		{"", "https://evil.example", 403},
		{"", c.proxyBaseURL(), 403},
		{"evil.example", "", 403},
		{"evil.example", "http://evil.example", 403},
	} {
		req, _ := http.NewRequest("GET", c.proxyBaseURL()+"/v1/models", nil)
		if tc.host != "" {
			req.Host = tc.host
		}
		if tc.origin != "" {
			req.Header.Set("Origin", tc.origin)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != tc.want {
			t.Errorf("host=%q origin=%q: got %d want %d", tc.host, tc.origin, res.StatusCode, tc.want)
		}
	}
	if calls != 1 {
		t.Fatalf("unexpected upstream executions: %d", calls)
	}
}
