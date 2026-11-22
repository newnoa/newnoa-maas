package main

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestCodexReplaceKeepsFirstBackup(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	original := []byte("# original\nmodel = \"old\"\n[projects.\"/safe\"]\ntrust_level = \"trusted\"\n")
	if err := atomicFile(c.configPath(), original); err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"model-one", "model-two", "model-two"} {
		pr, err := c.previewWithAPIKey("codex", model, "nn_selected_agent_api_key_1234567890")
		if err != nil {
			t.Fatal("replacement preview", err)
		}
		if err = c.apply(pr.Digest, pr.Content); err != nil {
			t.Fatal(err)
		}
	}
	raw, _ := os.ReadFile(c.configPath())
	if strings.Contains(string(raw), "model-one") || strings.Count(string(raw), "[model_providers.newnoa]") != 1 {
		t.Fatal("old active connection retained")
	}
	if err := c.restoreAgent("codex"); err != nil {
		t.Fatal(err)
	}
	raw, _ = os.ReadFile(c.configPath())
	if string(raw) != string(original) {
		t.Fatal("first backup lost")
	}
}

func TestHermesAppendsIndependentKeysAndRestores(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, _, _ := c.agentPaths("hermes-agent")
	original := []byte("providers:\n  keep:\n    api_key: untouched\nmodel:\n  default: old\n")
	if err := atomicFile(path, original); err != nil {
		t.Fatal(err)
	}
	keys := []string{"nn_first_agent_api_key_1234567890", "nn_second_agent_api_key_1234567890"}
	for _, index := range []int{0, 1, 0} {
		pr, err := c.previewWithAPIKey("hermes-agent", "deployment-id", keys[index], "agent-model")
		if err != nil {
			t.Fatal("add preview", err)
		}
		if err = c.apply(pr.Digest, pr.Content); err != nil {
			t.Fatal(err)
		}
	}
	raw, _ := os.ReadFile(path)
	var config struct{ Providers map[string]map[string]any }
	if err := yaml.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	if len(config.Providers) != 3 {
		t.Fatalf("provider count=%d, expected keep + two unique keys", len(config.Providers))
	}
	for _, key := range keys {
		if strings.Contains(string(raw), key) || strings.Count(string(raw), managedAgentKey(key)) != 1 {
			t.Fatal("key lost or duplicate")
		}
	}
	reloaded := newCore(c.home, c.gateway)
	route := reloaded.modelAliases["hermes-agent\nagent-model"]
	for _, key := range keys {
		if !route.Keys[hash([]byte(key))] {
			t.Fatal("route lost after restart")
		}
	}
	status, err := c.detectAgent("hermes-agent")
	if err != nil {
		t.Fatal(err)
	}
	public, _ := json.Marshal(status)
	for _, key := range keys {
		if strings.Contains(string(public), key) {
			t.Fatal("status leaked key")
		}
	}
	if err := c.restoreAgent("hermes-agent"); err != nil {
		t.Fatal(err)
	}
	raw, _ = os.ReadFile(path)
	if string(raw) != string(original) {
		t.Fatal("first backup lost")
	}
}

func TestManagedConnectionConflictStopsPreviewAndApply(t *testing.T) {
	for _, agent := range []string{"codex", "hermes-agent"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			pr, err := c.preview(agent, "model-one")
			if err != nil {
				t.Fatal(err)
			}
			if err = c.apply(pr.Digest, pr.Content); err != nil {
				t.Fatal(err)
			}
			next, err := c.preview(agent, "model-two")
			if err != nil {
				t.Fatal(err)
			}
			if err = atomicFile(pr.Path, []byte("# external modification\n"+pr.Content)); err != nil {
				t.Fatal(err)
			}
			if c.apply(next.Digest, next.Content) == nil {
				t.Fatal("apply overwrote external edit")
			}
			if _, err = c.preview(agent, "model-two"); err == nil {
				t.Fatal("preview ignored managed conflict")
			}
			if c.restoreAgent(agent) == nil {
				t.Fatal("restore overwrote external edit")
			}
		})
	}
}

func TestCodexRepeatedReplacementIsIdempotent(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	first, err := c.preview("codex", "same-model")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(first.Digest, first.Content); err != nil {
		t.Fatal(err)
	}
	second, err := c.preview("codex", "same-model")
	if err != nil {
		t.Fatal(err)
	}
	if second.Content != first.Content {
		t.Fatal("repeated replacement changed identical configuration")
	}
}

func TestManagedConnectionInterruptedUpdateRestoresFirstBackup(t *testing.T) {
	for _, agent := range []string{"codex", "hermes-agent"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			first, err := c.preview(agent, "model-one")
			if err != nil {
				t.Fatal(err)
			}
			if err = c.apply(first.Digest, first.Content); err != nil {
				t.Fatal(err)
			}
			_, journalPath, _ := c.agentPaths(agent)
			raw, _ := os.ReadFile(journalPath)
			var j journal
			if err = json.Unmarshal(raw, &j); err != nil {
				t.Fatal(err)
			}
			j.Stage, j.PreviousInstalled, j.Installed = "prepared", j.Installed, hash([]byte("new content never installed"))
			raw, _ = json.Marshal(j)
			if err = atomicFile(journalPath, raw); err != nil {
				t.Fatal(err)
			}
			state, err := c.detectAgent(agent)
			if err != nil || state["configurationState"] != "recovery_required" {
				t.Fatal(state, err)
			}
			if _, err = c.preview(agent, "model-two"); err == nil {
				t.Fatal("unfinished update accepted")
			}
			if err = c.restoreAgent(agent); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(first.Path); !os.IsNotExist(err) {
				t.Fatal("first absent configuration not restored")
			}
		})
	}
}

func TestAgentConnectionCapabilitiesAndPreviewRedaction(t *testing.T) {
	for _, agent := range []string{"codex", "hermes-agent", "claude-cli", "opencode", "openclaw", "pi-agent", "hana-agent"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			state, err := c.detectAgent(agent)
			if err != nil {
				t.Fatal(err)
			}
			want := "configure"
			if agent == "codex" || agent == "claude-cli" {
				want = "replace"
			}
			if agent == "hana-agent" || agent == "hermes-agent" || agent == "opencode" || agent == "pi-agent" || agent == "openclaw" {
				want = "add"
			}
			if state["operation"] != want {
				t.Fatal("missing operation", state)
			}
			capability, ok := state["capabilities"].(agentCapabilities)
			if !ok || !capability.Restore || capability.MultipleKeys != (want == "add") {
				t.Fatal("invalid capabilities", state)
			}
			if (agent == "pi-agent" || agent == "openclaw") && (capability.Replace || !capability.ProviderList || !capability.Add) {
				t.Fatal("managed update capabilities missing", capability)
			}
			if agent == "hana-agent" || agent == "opencode" {
				// OpenCode's authenticated RPC preview is covered by agent_selection_test.go.
				return
			}
			key := "nn_agent_redaction_key_1234567890"
			params, _ := json.Marshal(map[string]string{"agent": agent, "model": "deployment-id", "apiKey": key})
			result, err := c.handle("agent.preview", params)
			if err != nil {
				t.Fatal(err)
			}
			pr := result.(preview)
			if pr.Operation != want || pr.Content != "" {
				t.Fatal("invalid preview summary")
			}
			raw, _ := json.Marshal(result)
			if strings.Contains(string(raw), key) {
				t.Fatal("preview leaked API key")
			}
		})
	}
}

func TestAgentAliasIsolationAndCodexReplacement(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	c.server = &http.Server{}
	apply := func(agent, model, key string) error {
		params, _ := json.Marshal(map[string]string{"agent": agent, "model": model, "alias": "shared-alias", "apiKey": key})
		result, err := c.handle("agent.preview", params)
		if err != nil {
			return err
		}
		pr := result.(preview)
		params, _ = json.Marshal(map[string]string{"digest": pr.Digest})
		_, err = c.handle("agent.apply", params)
		return err
	}
	first, second := "nn_first_agent_api_key_1234567890", "nn_second_agent_api_key_1234567890"
	if err := apply("claude-cli", "claude-model", first); err != nil {
		t.Fatal(err)
	}
	if err := apply("codex", "codex-model", second); err != nil {
		t.Fatal("different agents and keys must stay independent", err)
	}
	if err := apply("codex", "replacement-model", second); err != nil {
		t.Fatal("same active alias replacement", err)
	}
	route := c.modelAliases["codex\nshared-alias"]
	if route.Model != "replacement-model" || !route.Keys[hash([]byte(second))] || route.Keys[hash([]byte(first))] {
		t.Fatal("replacement route retained old identity")
	}
	if err := apply("hermes-agent", "hermes-model", second); err == nil {
		t.Fatal("same key and alias silently rerouted across agents")
	}
}
