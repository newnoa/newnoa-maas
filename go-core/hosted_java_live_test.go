package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// The root isolated Java/PostgreSQL/ClickHouse fixture supplies a private hosted model and cloud history policy.
func TestHostedCoreJavaLive(t *testing.T) {
	api := os.Getenv("NEWNOA_HOSTED_TEST_API")
	if api == "" {
		t.Skip("set NEWNOA_HOSTED_TEST_API/SESSION/KEY/MODEL from the root isolated hosted fixture")
	}
	session, key, model := os.Getenv("NEWNOA_HOSTED_TEST_SESSION"), os.Getenv("NEWNOA_HOSTED_TEST_KEY"), os.Getenv("NEWNOA_HOSTED_TEST_MODEL")
	if !strings.HasPrefix(session, "ses_") || !validAPIKey(key) || !nodePattern.MatchString(model) {
		t.Fatal("invalid isolated hosted fixture credentials")
	}
	executions := 0
	serve := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executions++
		if r.URL.Path != "/prefix/v1/chat/completions" || r.Header.Get("X-Api-Key") != "" || r.Header.Get("Authorization") == "Bearer "+key || r.Header.Get("Authorization") == "" {
			t.Error("upstream path/credential binding")
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["model"] == model {
			t.Error("upstream model binding missing")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"fixture-response","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"isolated hosted fixture"},"finish_reason":"stop"}],"usage":{"prompt_tokens":118000,"completion_tokens":2000,"prompt_tokens_details":{"cache_write_tokens":8000,"cached_tokens":100000}}}`)
	})
	fixture := httptest.NewUnstartedServer(serve)
	tlsFixture := os.Getenv("NEWNOA_HOSTED_TEST_TLS") == "1"
	if tlsFixture {
		fixture.StartTLS()
	} else {
		fixture.Start()
	}
	defer fixture.Close()
	policy := fixturePublicPolicy(fixture)
	if tlsFixture {
		destination := strings.TrimPrefix(fixture.URL, "https://")
		policy.connect = func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, destination)
		}
		original := http.DefaultTransport
		cloned := http.DefaultTransport.(*http.Transport).Clone()
		roots := x509.NewCertPool()
		roots.AddCert(fixture.Certificate())
		cloned.TLSClientConfig = &tls.Config{RootCAs: roots, ServerName: "example.com", MinVersion: tls.VersionTLS12}
		http.DefaultTransport = cloned
		defer func() { http.DefaultTransport = original; cloned.CloseIdleConnections() }()
	}
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control = api
	c.onlineSupply = policy
	c.setProxySession(session)
	defer c.shutdown()
	raw := `{"model":"` + model + `","messages":[{"role":"user","content":"isolated fixture"}],"stream":false}`
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+key)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "hosted-java-"+randomToken())
	r.Header.Set("X-Newnoa-Agent", "[TEST] Hosted Core Java fixture")
	w := httptest.NewRecorder()
	c.recordInference(c.hostedInference(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("private hosted request reached Go Server") }))).ServeHTTP(w, r)
	if w.Code != 200 || executions != 1 || w.Header().Get("X-Newnoa-Path") != "core-hosted-direct" {
		t.Fatalf("hosted status=%d executions=%d body=%s", w.Code, executions, w.Body.String())
	}
	requestID := w.Header().Get("X-Newnoa-Request")
	if !nodePattern.MatchString(requestID) {
		t.Fatal("request identity missing")
	}
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	var pending int
	if err = h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox").Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("real API completion pending=%d err=%v", pending, err)
	}
	var account struct {
		ID string `json:"id"`
	}
	if err = historyRemote(context.Background(), api, "/me", "Bearer "+session, nil, &account); err != nil {
		t.Fatal(err)
	}
	saved, err := h.get(account.ID, requestID)
	if err != nil {
		t.Fatalf("local/cloud history missing: %v", err)
	}
	if saved["storageMode"] != "cloud" || saved["transportPath"] != "core-hosted-direct" {
		t.Fatalf("history policy/path=%v/%v", saved["storageMode"], saved["transportPath"])
	}
	if err = h.db.QueryRow("SELECT pending FROM exchanges WHERE account=? AND id=?", account.ID, requestID).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("real cloud history pending=%d err=%v", pending, err)
	}
	t.Logf("HOSTED_JAVA_REQUEST_ID=%s route=core-hosted-direct input=118000 output=2000 cacheWrite=8000 cachedInput=100000 upstreamExecutions=1 cloudHistoryPending=0", requestID)
}
