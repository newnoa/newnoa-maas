package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestClaudeConfigIndependentRecovery(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, journal, _ := c.agentPaths("claude-cli")
	original := []byte(`{ "unknown":9007199254740993, "permissions":{"allow":["Read"]},"env":{"CUSTOM":"keep","ANTHROPIC_AUTH_TOKEN":"old"}}`)
	if err := atomicFile(path, original); err != nil {
		t.Fatal(err)
	}
	p, err := c.preview("claude-cli", "deployment-id")
	if err != nil {
		t.Fatal(err)
	}
	var settings struct {
		Unknown json.Number
		Env     map[string]string
	}
	if err = json.Unmarshal([]byte(p.Content), &settings); err != nil {
		t.Fatal(err)
	}
	if settings.Unknown.String() != "9007199254740993" || settings.Env["CUSTOM"] != "keep" || settings.Env["ANTHROPIC_MODEL"] != "deployment-id" || settings.Env["ANTHROPIC_API_KEY"] != c.localKey || settings.Env["ANTHROPIC_BASE_URL"] != "http://127.0.0.1:18222" {
		t.Fatal("settings lost or incorrect")
	}
	if _, ok := settings.Env["ANTHROPIC_AUTH_TOKEN"]; ok {
		t.Fatal("ambiguous credentials")
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	codex, err := c.preview("codex", "other-model")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(codex.Digest, codex.Content); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("claude-cli"); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(actual, original) {
		t.Fatal("original bytes not restored", err)
	}
	if _, err = os.Stat(journal); !os.IsNotExist(err) {
		t.Fatal("journal retained", err)
	}
	if _, err = os.Stat(c.journalPath()); err != nil {
		t.Fatal("Codex journal affected", err)
	}
	if err = c.restore(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(c.configPath()); !os.IsNotExist(err) {
		t.Fatal("new Codex config not removed")
	}
}

func TestClaudeConfigRejectsInvalidSettings(t *testing.T) {
	for _, content := range []string{"", "null", "[]", "{} {}", `{"env":null}`, `{"env":[]}`, `{"env":"invalid"}`} {
		t.Run(content, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			path, journal, _ := c.agentPaths("claude-cli")
			if err := atomicFile(path, []byte(content)); err != nil {
				t.Fatal(err)
			}
			if _, err := c.preview("claude-cli", "model"); err == nil {
				t.Fatal("invalid settings accepted")
			}
			if _, err := os.Stat(journal); !os.IsNotExist(err) {
				t.Fatal("journal created")
			}
		})
	}
}

func TestClaudeConfigConflictAndSymlink(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, _, _ := c.agentPaths("claude-cli")
	p, err := c.preview("claude-cli", "model")
	if err != nil {
		t.Fatal(err)
	}
	if err = atomicFile(path, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if c.apply(p.Digest, p.Content) == nil {
		t.Fatal("external edit overwritten")
	}
	p, err = c.preview("claude-cli", "model")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	if err = atomicFile(path, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if c.restoreAgent("claude-cli") == nil {
		t.Fatal("restore overwrote edit")
	}
	c = newCore(t.TempDir(), "http://127.0.0.1:8091")
	if err = os.Symlink(t.TempDir(), filepath.Join(c.home, ".claude")); err != nil {
		t.Fatal(err)
	}
	if _, err = c.preview("claude-cli", "model"); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestAgentPreviewTokenAndRPC(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	old, err := c.preview("codex", "model")
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.preview("claude-cli", "model")
	if err != nil {
		t.Fatal(err)
	}
	if c.apply(old.Digest, p.Content) == nil {
		t.Fatal("stale preview applied different agent")
	}
	if _, err = c.handle("agent.apply", json.RawMessage(`{"digest":"`+p.Digest+`"}`)); err == nil {
		t.Fatal("applied without proxy")
	}
	if _, err = c.handle("agent.detect", json.RawMessage(`{"agent":"claude-cli"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = c.handle("agent.detect", json.RawMessage(`{"agent":"unknown"}`)); err == nil {
		t.Fatal("unknown agent detected")
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	// Startup must recover every supported agent, including a newly created file.
	if err = c.restore(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(p.Path); !os.IsNotExist(err) {
		t.Fatal("new settings retained")
	}
}

func TestClaudeCustomConfigLocation(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, journal, _ := c.agentPaths("claude-cli")
	original := []byte(`{"env":{"CUSTOM":"keep"}}`)
	if err := atomicFile(path, original); err != nil {
		t.Fatal(err)
	}
	custom := t.TempDir()
	customPath := filepath.Join(custom, "settings.json")
	if err := os.WriteFile(customPath, []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CONFIG_DIR", custom)
	if _, err := c.preview("claude-cli", "model"); err == nil {
		t.Fatal("custom config location accepted")
	}
	if _, err := c.detectAgent("claude-cli"); err == nil {
		t.Fatal("default location reported under custom config override")
	}
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Dir(path)+string(os.PathSeparator))
	p, err := c.preview("claude-cli", "model")
	if err != nil {
		t.Fatal("explicit default location refused", err)
	}
	t.Setenv("CLAUDE_CONFIG_DIR", custom)
	if err = c.apply(p.Digest, p.Content); err == nil {
		t.Fatal("environment changed after preview but apply succeeded")
	}
	if raw, err := os.ReadFile(path); err != nil || !bytes.Equal(raw, original) {
		t.Fatal("refused apply changed original", err)
	}
	if _, err := os.Stat(journal); !os.IsNotExist(err) {
		t.Fatal("refused apply created journal", err)
	}
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CLAUDE_CONFIG_DIR", custom)
	if _, err := c.detectAgent("claude-cli"); err == nil {
		t.Fatal("overridden default configuration reported as current")
	}
	if err = c.restoreAgent("claude-cli"); err != nil {
		t.Fatal("environment change blocked recovery of default location", err)
	}
	if raw, err := os.ReadFile(path); err != nil || !bytes.Equal(raw, original) {
		t.Fatal("original not restored", err)
	}
	if raw, err := os.ReadFile(customPath); err != nil || string(raw) != "{}" {
		t.Fatal("custom configuration changed", err)
	}
}
