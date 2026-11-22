package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestHermesRoundTrip(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, journalPath, _ := c.agentPaths("hermes-agent")
	original := []byte("# preserved comment\nunknown: 9007199254740993\nmodel:\n  default: old\n  api_key: old-secret\n  openai_runtime: codex_app_server\nfallback_model: old\nsmart_model_routing: true\nproviders:\n  keep:\n    api_key: untouched\n")
	if err := atomicFile(path, original); err != nil {
		t.Fatal(err)
	}
	pr, err := c.preview("hermes-agent", "deployment-id")
	if err != nil {
		t.Fatal(err)
	}
	var value struct {
		Unknown   uint64
		Model     map[string]string
		Providers map[string]map[string]any
	}
	if err = yaml.Unmarshal([]byte(pr.Content), &value); err != nil {
		t.Fatal(err)
	}
	providerID := strings.TrimPrefix(value.Model["provider"], "custom:")
	if value.Unknown != 9007199254740993 || !strings.HasPrefix(providerID, "newnoa-") || value.Model["default"] != "deployment-id" || value.Model["api_key"] != "" || value.Model["openai_runtime"] != "" || value.Providers[providerID]["api_key"] != c.localKey || value.Providers["keep"]["api_key"] != "untouched" {
		t.Fatal("incorrect Hermes configuration")
	}
	if bytes.Contains([]byte(pr.Content), []byte("fallback_model:")) || bytes.Contains([]byte(pr.Content), []byte("smart_model_routing:")) {
		t.Fatal("fallback retained")
	}
	extra, ok := value.Providers[providerID]["extra_body"].(map[string]any)
	if !ok || extra["max_tokens"] != 4096 {
		t.Fatal("missing explicit platform output limit")
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	state, err := c.detectAgent("hermes-agent")
	if err != nil || state["configurationState"] != "applied" {
		t.Fatal(state, err)
	}
	if err = atomicFile(path, []byte("external: true")); err != nil {
		t.Fatal(err)
	}
	if c.restoreAgent("hermes-agent") == nil {
		t.Fatal("external edit overwritten")
	}
	if err = atomicFile(path, []byte(pr.Content)); err != nil {
		t.Fatal(err)
	}
	if err = c.restore(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(raw, original) {
		t.Fatal("restore changed original", err)
	}
	if _, err = os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatal("journal retained")
	}
}

func TestHermesRefusals(t *testing.T) {
	for _, raw := range []string{"", "null", "[]", "model: []", "model: {}\nmodel: {}", "providers:\n  newnoa: {}", "a: &x {}\nmodel: *x", "model: {}\n---\nmodel: {}"} {
		c := newCore(t.TempDir(), "http://127.0.0.1:8091")
		path, _, _ := c.agentPaths("hermes-agent")
		atomicFile(path, []byte(raw))
		if _, err := c.preview("hermes-agent", "model"); err == nil {
			t.Fatal("unsafe YAML accepted", raw)
		}
	}
	for _, key := range []string{"HERMES_HOME", "HERMES_PROFILE", "HERMES_CONFIG", "HERMES_CONFIG_PATH"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "")
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			pr, err := c.preview("hermes-agent", "model")
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(key, "custom")
			if c.apply(pr.Digest, pr.Content) == nil {
				t.Fatal("changed path ignored")
			}
		})
	}
}

func TestHermesProviderAliasConflicts(t *testing.T) {
	for _, raw := range []string{
		"providers:\n  Newnoa: {}\n",
		"providers:\n  old:\n    name: ' Newnoa '\n    base_url: https://old.example.test\n",
		"providers:\n  'custom:newnoa': {}\n",
		"custom_providers:\n  - name: NEWNOA\n    base_url: https://old.example.test\n",
		"custom_providers:\n  - name: old\n    provider_key: custom:newnoa\n",
	} {
		c := newCore(t.TempDir(), "http://127.0.0.1:8091")
		path, _, _ := c.agentPaths("hermes-agent")
		if err := atomicFile(path, []byte(raw)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.preview("hermes-agent", "model"); err == nil {
			t.Fatal("provider alias conflict accepted", raw)
		}
		actual, err := os.ReadFile(path)
		if err != nil || string(actual) != raw {
			t.Fatal("preview changed original", err)
		}
	}
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, _, _ := c.agentPaths("hermes-agent")
	original := "custom_providers:\n  - name: unrelated\n    base_url: https://unrelated.example.test\n"
	if err := atomicFile(path, []byte(original)); err != nil {
		t.Fatal(err)
	}
	pr, err := c.preview("hermes-agent", "model")
	if err != nil || !strings.Contains(pr.Content, "https://unrelated.example.test") {
		t.Fatal("unrelated provider not preserved", err)
	}
}
