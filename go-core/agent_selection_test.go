package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestAgentSelectionRechecksGrantSecretAndKeyPause(t *testing.T) {
	const key = "11111111-1111-4111-8111-111111111111"
	const model = "22222222-2222-4222-8222-222222222222"
	const secret = "nn_selected_authorized_key_123456789"
	var paused atomic.Bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer account-session" {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/api/v1/me/api-keys":
			json.NewEncoder(w).Encode([]any{map[string]any{"id": key, "paused": paused.Load(), "deploymentIds": []string{model}}})
		case "/api/v1/me/api-keys/" + key + "/copy-secret":
			json.NewEncoder(w).Encode(map[string]string{"secret": secret})
		default:
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	c.control = api.URL
	if err := c.validateAgentSelection(context.Background(), "account-session", key, secret, []string{model}); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		session, key, secret string
		models               []string
	}{{"account-session", key, "nn_wrong_secret_123456789", []string{model}}, {"account-session", key, secret, []string{"33333333-3333-4333-8333-333333333333"}}, {"stale-session", key, secret, []string{model}}} {
		if c.validateAgentSelection(context.Background(), check.session, check.key, check.secret, check.models) == nil {
			t.Fatal("invalid key/model selection accepted")
		}
	}
	paused.Store(true)
	if c.validateAgentSelection(context.Background(), "account-session", key, secret, []string{model}) == nil {
		t.Fatal("paused key accepted on revalidation")
	}
}

func TestOpenCodeRPCRechecksSelectedGrantBeforeWriting(t *testing.T) {
	const model = "22222222-2222-4222-8222-222222222222"
	const key = "11111111-1111-4111-8111-111111111111"
	const secret = "nn_selected_authorized_key_123456789"
	var paused atomic.Bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/me/api-keys":
			json.NewEncoder(w).Encode([]any{map[string]any{"id": key, "paused": paused.Load(), "deploymentIds": []string{model}}})
		case "/api/v1/me/api-keys/" + key + "/copy-secret":
			json.NewEncoder(w).Encode(map[string]string{"secret": secret})
		default:
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	c.control = api.URL
	c.server = &http.Server{}
	params := map[string]string{"agent": "opencode", "session": "account-session", "apiKey": secret, "model": model}
	command := func(method string, p map[string]string) (any, error) {
		raw, _ := json.Marshal(p)
		return c.handle(method, raw)
	}
	if _, err := command("agent.preview", params); err == nil {
		t.Fatal("OpenCode preview accepted without a selected account key")
	}
	params["keyId"] = key
	result, err := command("agent.preview", params)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(result)
	if result.(preview).Content != "" || strings.Contains(string(raw), secret) {
		t.Fatal("OpenCode preview leaked native configuration or key")
	}
	if _, err := command("agent.preview", map[string]string{"agent": "opencode", "session": params["session"], "apiKey": secret, "model": model}); err == nil {
		t.Fatal("invalid replacement preview accepted")
	}
	if _, err := command("agent.apply", map[string]string{"session": params["session"], "digest": result.(preview).Digest}); err == nil || err.Error() != "preview required" {
		t.Fatalf("old preview survived a failed replacement: %v", err)
	}
	result, err = command("agent.preview", params)
	if err != nil {
		t.Fatal(err)
	}
	paused.Store(true)
	if _, err := command("agent.apply", map[string]string{"session": params["session"], "digest": result.(preview).Digest}); err == nil || err.Error() != "Selected API key is paused, expired or revoked" {
		t.Fatalf("apply did not recheck authorization: %v", err)
	}
}

func TestManagedProvidersUseCurrentAccountAndRecheckGrant(t *testing.T) {
	for _, agent := range []string{"opencode", "openclaw", "pi-agent", "hermes-agent", "hana-agent"} {
		t.Run(agent, func(t *testing.T) { testManagedProviderRPC(t, agent) })
	}
}

func testManagedProviderRPC(t *testing.T, agent string) {
	t.Helper()
	const model = "22222222-2222-4222-8222-222222222222"
	const key = "11111111-1111-4111-8111-111111111111"
	const owner = "55555555-5555-4555-8555-555555555555"
	const other = "66666666-6666-4666-8666-666666666666"
	const secret = "nn_selected_authorized_key_123456789"
	var revoke atomic.Bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer other" {
			if r.URL.Path == "/api/v1/me" {
				_ = json.NewEncoder(w).Encode(map[string]string{"id": other})
				return
			}
			w.WriteHeader(403)
			return
		}
		if r.Header.Get("Authorization") != "Bearer one" {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/api/v1/me":
			_ = json.NewEncoder(w).Encode(map[string]string{"id": owner})
		case "/api/v1/me/api-keys":
			_ = json.NewEncoder(w).Encode([]any{map[string]any{"id": key, "paused": revoke.Load(), "deploymentIds": []string{model}}})
		case "/api/v1/me/api-keys/" + key + "/copy-secret":
			_ = json.NewEncoder(w).Encode(map[string]string{"secret": secret})
		default:
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	if agent == "hana-agent" {
		t.Setenv("HANA_HOME", "")
		paths, _ := c.hanaPaths()
		if err := atomicFile(paths[0], []byte(`{"catalogVersion":2,"providers":{}}`)); err != nil {
			t.Fatal(err)
		}
	}
	c.control = api.URL
	command := func(method string, p map[string]any) (any, error) {
		t.Helper()
		raw, _ := json.Marshal(p)
		return c.handle(method, raw)
	}
	params := map[string]any{"agent": agent, "session": "one", "providerAction": "add", "keyId": key, "apiKey": secret, "providerModels": []providerModel{{model, "fixture"}}}
	prepare := func() string {
		t.Helper()
		result, err := command("agent.preview", params)
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(result)
		if strings.Contains(string(encoded), secret) {
			t.Fatal("preview leaked key")
		}
		return result.(preview).Digest
	}
	digest := prepare()
	revoke.Store(true)
	if _, err := command("agent.apply", map[string]any{"session": "one", "digest": digest}); err == nil {
		t.Fatal("write accepted after revocation")
	}
	revoke.Store(false)
	digest = prepare()
	if _, err := command("agent.apply", map[string]any{"session": "other", "digest": digest}); err == nil {
		t.Fatal("write accepted with changed session")
	}
	digest = prepare()
	if _, err := command("agent.apply", map[string]any{"session": "one", "digest": digest}); err != nil {
		t.Fatal(err)
	}
	status, err := command("agent.detect", map[string]any{"agent": agent})
	if err != nil || status.(map[string]any)["model"] != nil || status.(map[string]any)["configuredModels"] != nil {
		t.Fatal("general detection exposed managed models", err)
	}
	for _, session := range []string{"one", "other"} {
		result, err := command("agent.providers", map[string]any{"agent": agent, "session": session})
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(result)
		if strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), key) != (session == "one") {
			t.Fatal("unexpected provider list disclosure")
		}
	}
	if _, err := command("agent.restore", map[string]any{"agent": agent, "session": "other"}); err == nil {
		t.Fatal("cross-account restore")
	}
	if _, err := command("agent.preview", map[string]any{"agent": agent, "session": "other", "providerAction": "remove", "keyId": key}); err == nil {
		t.Fatal("cross-account removal")
	}
	revoke.Store(true)
	result, err := command("agent.preview", map[string]any{"agent": agent, "session": "one", "providerAction": "remove", "keyId": key})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = command("agent.apply", map[string]any{"session": "one", "digest": result.(preview).Digest}); err != nil {
		t.Fatal("could not remove revoked key", err)
	}
	if _, ok := c.modelAliases[agent+"\nfixture"]; ok {
		t.Fatal("removed alias still active")
	}
}
