package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLANProxyRequiresCredentialsAndTrustedHost(t *testing.T) {
	first, second := "nn_"+strings.Repeat("x", 40), "nn_"+strings.Repeat("y", 40)
	var mu sync.Mutex
	var seen []string
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("X-Api-Key"))
		mu.Unlock()
		io.WriteString(w, `{"data":[]}`)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	defer c.close()
	if err := c.startProxyOn(first, availableProxyPort(t), true); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, host, key, origin string
		want                    int
	}{
		{"rebound without key", "evil.example", "", "", 403},
		{"rebound with key", "evil.example", second, "", 403},
		{"no key", "192.168.1.2", "", "", 401},
		{"loopback no key on LAN listener", "127.0.0.1", "", "", 401},
		{"browser", "192.168.1.2", second, "https://evil.example", 403},
		{"caller first", "192.168.1.2", first, "", 200},
		{"caller second", "192.168.1.2", second, "", 200},
		{"explicit local capability", "192.168.1.2", c.localKey, "", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest("GET", c.proxyBaseURL()+"/v1/models", nil)
			req.Host = tc.host
			if tc.key != "" {
				req.Header.Set("Authorization", "Bearer "+tc.key)
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
				t.Errorf("got %d want %d", res.StatusCode, tc.want)
			}
		})
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(seen, ",") != strings.Join([]string{first, second, first}, ",") {
		t.Fatalf("unexpected forwarded keys: %v", seen)
	}
}

func TestLoopbackProxyEnforcesManagedCredentials(t *testing.T) {
	const lease = "rla_proxy_fixture_012345678901234567890"
	const upstream = "ra_12345678901234567890123456789012"
	const reference = "nref_proxy_fixture_012345678901234567890"
	const session = "fixture-session"
	var mu sync.Mutex
	var forwarded []string
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		forwarded = append(forwarded, r.Method+" "+r.URL.Path+" "+r.Header.Get("X-Api-Key"))
		mu.Unlock()
		io.WriteString(w, `{"data":[]}`)
	}))
	defer gateway.Close()

	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	defer c.close()
	c.remote = &remoteManager{
		session:  session,
		enabled:  true,
		sessions: map[string]*remoteSession{session: {ID: session}},
		siteCredentials: map[string]*remoteAgentCredential{
			hash([]byte(lease)): {
				sessionID: session, accountSession: session, upstreamToken: upstream,
				expiresAt: time.Now().Add(time.Hour),
			},
		},
	}
	c.modelAliases = map[string]agentModelRoute{
		"codex\nfixture-model": {Model: "fixture-deployment", Keys: map[string]bool{managedAgentKeyHash(reference): true}},
	}
	if err := c.startProxyOn("", availableProxyPort(t), false); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	for _, tc := range []struct {
		name, method, path, key, body, message string
		status                                 int
	}{
		{"active lease", http.MethodGet, "/v1/models", lease, "", "", http.StatusOK},
		{"unknown lease", http.MethodGet, "/v1/models", "rla_unknown", "", "Remote agent credential unavailable", http.StatusUnauthorized},
		{"native request missing model", http.MethodPost, "/v1/chat/completions", reference, `{}`, "Native configuration requires a managed model", http.StatusBadRequest},
		{"native request for unbound model", http.MethodPost, "/v1/chat/completions", reference, `{"model":"unbound"}`, "Model is outside this native configuration", http.StatusBadRequest},
		{"native request without account", http.MethodPost, "/v1/chat/completions", reference, `{"model":"fixture-model"}`, "Native configuration authorization failed", http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, c.proxyBaseURL()+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+tc.key)
			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			if res.StatusCode != tc.status || !strings.Contains(string(body), tc.message) {
				t.Fatalf("response status %d and body %q, expected %d and %q", res.StatusCode, body, tc.status, tc.message)
			}
		})
	}

	mu.Lock()
	defer mu.Unlock()
	if len(forwarded) != 1 || forwarded[0] != "GET /v1/models "+upstream {
		t.Fatalf("fixture upstream received unexpected requests: %v", forwarded)
	}
}

func TestLANProxyStartupFailsWithUnusableCertificateStorage(t *testing.T) {
	c := fixedGatewayTestCore(t.TempDir(), "http://127.0.0.1:8091")
	defer c.close()

	port := availableProxyPort(t)
	path := filepath.Join(c.home, ".newnoa", fmt.Sprintf("lan-proxy-%d-certificate.pem", port))
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := c.startProxyOn("", port, true); err == nil || !strings.Contains(err.Error(), "prepare LAN TLS certificate") {
		t.Fatalf("expected certificate preparation to stop startup, got %v", err)
	}
	if c.server != nil || c.proxyServerListener != nil {
		t.Fatal("failed startup left a proxy running")
	}
	listener, err := reserveProxyListener(port, true)
	if err != nil {
		t.Fatalf("failed startup left the port occupied: %v", err)
	}
	listener.Close()
}
