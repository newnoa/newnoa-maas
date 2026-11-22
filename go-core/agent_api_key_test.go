package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentPreviewUsesSelectedAPIKey(t *testing.T) {
	key := "nn_selected_agent_api_key_1234567890"
	for _, agent := range []string{"codex", "claude-cli", "pi-agent", "openclaw", "hermes-agent"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			preview, err := c.previewWithAPIKey(agent, "deployment-id", key)
			if err != nil {
				t.Fatal(err)
			}
			credentialContent := preview.Content
			for _, file := range preview.companions {
				credentialContent += string(file.Content)
			}
			if strings.Contains(credentialContent, key) || !strings.Contains(credentialContent, managedAgentKey(key)) || !strings.Contains(preview.Content, "http://127.0.0.1:") {
				t.Fatal("preview omitted local proxy credentials or endpoint")
			}
			if err := c.apply(preview.Digest, preview.Content); err != nil {
				t.Fatal(err)
			}
			for _, path := range preview.Paths {
				if path == "" {
					continue
				}
				if _, err := os.Stat(path); err != nil {
					t.Fatal(err)
				}
			}
			if len(preview.Paths) == 0 {
				if _, err := os.Stat(preview.Path); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestAgentPreviewRejectsMissingOrInvalidAPIKey(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	for _, key := range []string{"", "contains spaces", strings.Repeat("x", 257)} {
		if _, err := c.previewWithAPIKey("claude-cli", "deployment-id", key); err == nil {
			t.Fatalf("accepted invalid API key %q", key)
		}
	}
	result, err := c.handle("agent.preview", json.RawMessage(`{"agent":"claude-cli","model":"deployment-id"}`))
	if err == nil {
		t.Fatal("RPC accepted preview without selected API key", result)
	}
	if _, err := os.Stat(filepath.Join(c.home, ".claude", "settings.json")); !os.IsNotExist(err) {
		t.Fatal("invalid preview modified Agent configuration", err)
	}
}

func TestAgentPreviewUsesAliasInNativeConfiguration(t *testing.T) {
	key := "nn_selected_agent_api_key_1234567890"
	for _, agent := range []string{"claude-cli", "pi-agent", "openclaw", "hermes-agent", "opencode"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			preview, err := c.previewWithAPIKey(agent, "deployment-id", key, "agent-model")
			if err != nil {
				t.Fatal(err)
			}
			if preview.Model != "deployment-id" || preview.Alias != "agent-model" {
				t.Fatalf("preview metadata lost deployment or alias: %#v", preview)
			}
			if !strings.Contains(preview.Content, "agent-model") || strings.Contains(preview.Content, "deployment-id") {
				t.Fatalf("native configuration did not use alias: %s", preview.Content)
			}
		})
	}
}

func TestLocalProxyForwardsSelectedAgentAPIKey(t *testing.T) {
	key := "nn_selected_agent_api_key_1234567890"
	model := "00000000-0000-0000-0000-000000000021"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.Header.Get("X-Api-Key") != key || r.URL.Path != "/v1/chat/completions" || body["model"] != model {
			t.Errorf("upstream request lost key or alias route: %s %s model=%s", r.Header.Get("Authorization"), r.URL.Path, body["model"])
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"fixture","choices":[]}`))
	}))
	defer upstream.Close()
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/client/hosted/models":
			json.NewEncoder(w).Encode([]any{})
			return
		case "/api/v1/me/api-keys":
			if r.Header.Get("Authorization") != "Bearer ses_fixture" {
				t.Error("Core did not use the active account session")
			}
			json.NewEncoder(w).Encode([]any{map[string]any{"id": "00000000-0000-4000-8000-000000000042", "prefix": key[:10]}})
			return
		case "/api/v1/me/api-keys/00000000-0000-4000-8000-000000000042/copy-secret":
			json.NewEncoder(w).Encode(map[string]string{"secret": key})
			return
		}
		if r.URL.Path == "/api/v1/client/history/identity" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
			return
		}
		if r.URL.Path == "/api/v1/client/targets/"+model {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"available":false,"private":false}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer control.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	c := fixedGatewayTestCore(t.TempDir(), upstream.URL)
	c.control = control.URL
	c.proxySession = "ses_fixture"
	c.modelAliases = map[string]agentModelRoute{"claude-cli\nalias-model": {Model: model, Keys: map[string]bool{hash([]byte(key)): true}}}
	if err = c.startProxyAt("", port); err != nil {
		t.Fatal(err)
	}
	defer c.close()
	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/v1/chat/completions", port), strings.NewReader(`{"model":"alias-model"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+managedAgentKey(key))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("proxy status=%d", response.StatusCode)
	}
}

func TestAgentAliasRouteReloadsFromJournalWithoutPlaintextKey(t *testing.T) {
	home := t.TempDir()
	key := "nn_selected_agent_api_key_1234567890"
	c := newCore(home, "http://127.0.0.1:8091")
	preview, err := c.previewWithAPIKey("claude-cli", "deployment-id", key, "agent-model")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.apply(preview.Digest, preview.Content); err != nil {
		t.Fatal(err)
	}
	journal, err := os.ReadFile(filepath.Join(home, ".newnoa", "claude-cli-journal.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(journal), key) {
		t.Fatal("journal persisted API key plaintext")
	}
	reloaded := newCore(home, "http://127.0.0.1:8091")
	route, ok := reloaded.modelAliases["claude-cli\nagent-model"]
	if !ok || route.Model != "deployment-id" || !route.Keys[hash([]byte(key))] {
		t.Fatalf("alias route was not restored: %#v", reloaded.modelAliases)
	}
}
