package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAgentConfigurationStatus(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			check := func(state string, exists bool) {
				t.Helper()
				result, err := c.detectAgent(agent)
				if err != nil || result["configurationState"] != state || result["exists"] != exists {
					t.Fatal(result, err)
				}
				data, _ := json.Marshal(result)
				if strings.Contains(string(data), c.localKey) || strings.Contains(string(data), "original") {
					t.Fatal("status leaked configuration")
				}
			}
			check("not_applied", false)
			pr, err := c.preview(agent, "deployment-id")
			if err != nil {
				t.Fatal(err)
			}
			if err = c.apply(pr.Digest, pr.Content); err != nil {
				t.Fatal(err)
			}
			check("applied", true)
			_, journalPath, _ := c.agentPaths(agent)
			data, err := os.ReadFile(journalPath)
			if err != nil {
				t.Fatal(err)
			}
			var j journal
			if err = json.Unmarshal(data, &j); err != nil {
				t.Fatal(err)
			}
			j.Stage = "prepared"
			prepared, _ := json.Marshal(j)
			if err = atomicFile(journalPath, prepared); err != nil {
				t.Fatal(err)
			}
			check("recovery_required", true)
			if err = atomicFile(journalPath, data); err != nil {
				t.Fatal(err)
			}
			if err = atomicFile(pr.Path, []byte("external edit")); err != nil {
				t.Fatal(err)
			}
			check("conflict", true)
			if err = os.Remove(pr.Path); err != nil {
				t.Fatal(err)
			}
			check("conflict", false)
			if err = atomicFile(pr.Path, []byte(pr.Content)); err != nil {
				t.Fatal(err)
			}
			if err = c.restoreAgent(agent); err != nil {
				t.Fatal(err)
			}
			check("not_applied", false)
			if err = atomicFile(journalPath, []byte("{}")); err != nil {
				t.Fatal(err)
			}
			if _, err = c.detectAgent(agent); err == nil {
				t.Fatal("invalid journal reported a state")
			}
		})
	}
}

func TestAppliedAgentModelIdentity(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli", "opencode", "openclaw", "pi-agent", "hermes-agent"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			pr, err := c.preview(agent, "deployment-model-a")
			if err != nil {
				t.Fatal(err)
			}
			if pr.Model != "deployment-model-a" {
				t.Fatal("preview lost model identity")
			}
			if err = c.apply(pr.Digest, pr.Content); err != nil {
				t.Fatal(err)
			}
			check := func(want string) {
				t.Helper()
				state, err := c.detectAgent(agent)
				if err != nil {
					t.Fatal(err)
				}
				model, _ := state["model"].(string)
				if model != want {
					t.Fatalf("model=%q, want %q: %v", model, want, state)
				}
			}
			check("deployment-model-a")
			original, err := os.ReadFile(pr.Path)
			if err != nil {
				t.Fatal(err)
			}
			if err = atomicFile(pr.Path, []byte("external edit")); err != nil {
				t.Fatal(err)
			}
			check("")
			if err = atomicFile(pr.Path, original); err != nil {
				t.Fatal(err)
			}
			_, journalPath, err := c.agentPaths(agent)
			if agent == "pi-agent" {
				_, journalPath = c.piPaths()
				err = nil
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(journalPath)
			if err != nil {
				t.Fatal(err)
			}
			var stored map[string]json.RawMessage
			if err = json.Unmarshal(raw, &stored); err != nil {
				t.Fatal(err)
			}
			stored["stage"] = json.RawMessage(`"prepared"`)
			raw, _ = json.Marshal(stored)
			if err = atomicFile(journalPath, raw); err != nil {
				t.Fatal(err)
			}
			check("")
			stored["stage"] = json.RawMessage(`"applied"`)
			delete(stored, "model")
			raw, _ = json.Marshal(stored)
			if err = atomicFile(journalPath, raw); err != nil {
				t.Fatal(err)
			}
			check("")
			if err = c.restoreAgent(agent); err != nil {
				t.Fatal(err)
			}
			check("")
		})
	}
}

func TestAgentDetectionRejectsLocationOverrides(t *testing.T) {
	for _, item := range []struct{ agent, env string }{
		{"opencode", "OPENCODE_CONFIG_DIR"}, {"openclaw", "OPENCLAW_PROFILE"},
		{"hermes-agent", "HERMES_HOME"}, {"pi-agent", "PI_CODING_AGENT_DIR"},
	} {
		t.Run(item.agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			p, err := c.preview(item.agent, "deployment-a")
			if err != nil {
				t.Fatal(err)
			}
			if err = c.apply(p.Digest, p.Content); err != nil {
				t.Fatal(err)
			}
			t.Setenv(item.env, "custom-location")
			if state, err := c.detectAgent(item.agent); err == nil {
				t.Fatalf("overridden location returned model: %v", state)
			}
			if err = c.restoreAgent(item.agent); err != nil {
				t.Fatal("override blocked default config recovery", err)
			}
			t.Setenv(item.env, "")
			state, err := c.detectAgent(item.agent)
			if err != nil || state["configurationState"] != "not_applied" {
				t.Fatal(state, err)
			}
		})
	}
}

func TestCodexLocationChangesPreserveRecovery(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	t.Setenv("CODEX_HOME", filepath.Join(c.home, ".codex")+string(os.PathSeparator))
	p, err := c.preview("codex", "deployment-a")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_HOME", t.TempDir())
	if _, err = c.preview("codex", "deployment-b"); err == nil {
		t.Fatal("custom Codex home accepted")
	}
	if err = c.apply(p.Digest, p.Content); err == nil {
		t.Fatal("Codex home changed after preview but applied")
	}
	if _, err = os.Stat(c.journalPath()); !os.IsNotExist(err) {
		t.Fatal("refused apply created recovery journal", err)
	}
	t.Setenv("CODEX_HOME", "")
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_HOME", t.TempDir())
	for _, agent := range []string{"codex", ""} {
		if state, err := c.detectAgent(agent); err == nil {
			t.Fatalf("default config reported under override: %v", state)
		}
	}
	if err = c.restore(); err != nil {
		t.Fatal("override blocked recovery", err)
	}
	if _, err = os.Stat(c.configPath()); !os.IsNotExist(err) {
		t.Fatal("new config not removed", err)
	}
}
