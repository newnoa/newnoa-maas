package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCodexNativeReasoningAndAuthRecovery(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	original := []byte("model = \"old\"\nmodel_reasoning_effort = \"low\"\ncli_auth_credentials_store = \"keyring\"\n[projects.\"/safe\"]\ntrust_level = \"trusted\"\n")
	authPath := filepath.Join(c.home, ".codex", "auth.json")
	oldAuth := []byte(`{"auth_mode":"chatgpt","tokens":{"access_token":"old"},"custom":9007199254740993}`)
	if err := atomicFile(c.configPath(), original); err != nil {
		t.Fatal(err)
	}
	if err := atomicFile(authPath, oldAuth); err != nil {
		t.Fatal(err)
	}
	result, err := c.handle("agent.preview", json.RawMessage(`{"agent":"codex","model":"deployment-id","apiKey":"nn_selected_key_1234567890","alias":"agent-model","reasoningEffort":"high"}`))
	if err != nil {
		t.Fatal(err)
	}
	pr := result.(preview)
	if err = c.apply(pr.Digest, c.pending.Content); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(c.configPath())
	if !strings.Contains(string(raw), `model_reasoning_effort = "high"`) || !strings.Contains(string(raw), `requires_openai_auth = true`) || !strings.Contains(string(raw), `cli_auth_credentials_store = "file"`) || strings.Contains(string(raw), "nn_selected_key") {
		t.Fatal("invalid Codex TOML", string(raw))
	}
	raw, _ = os.ReadFile(authPath)
	var auth map[string]json.RawMessage
	_ = json.Unmarshal(raw, &auth)
	if string(auth["OPENAI_API_KEY"]) != `"`+managedAgentKey("nn_selected_key_1234567890")+`"` || string(auth["auth_mode"]) != `"apikey"` || string(auth["custom"]) != "9007199254740993" {
		t.Fatal("wrong auth", string(raw))
	}
	for _, effort := range []string{"medium", ""} {
		data, _ := json.Marshal(map[string]string{"agent": "codex", "model": "deployment-next", "apiKey": "nn_next_key_1234567890", "reasoningEffort": effort})
		if _, err = c.handle("agent.preview", data); err != nil {
			t.Fatal(err)
		}
		if err = c.apply(c.pending.Digest, c.pending.Content); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.restoreAgent("codex"); err != nil {
		t.Fatal(err)
	}
	raw, _ = os.ReadFile(c.configPath())
	if string(raw) != string(original) {
		t.Fatal("config backup lost")
	}
	raw, _ = os.ReadFile(authPath)
	if string(raw) != string(oldAuth) {
		t.Fatal("auth backup lost")
	}
}

func TestDesktopPlatformPaths(t *testing.T) {
	home := t.TempDir()
	for _, platform := range []string{"linux", "darwin", "windows"} {
		paths, err := desktopPathsFor(home, platform)
		if err != nil {
			t.Fatal(err)
		}
		root := filepath.Join(home, ".config")
		if platform == "darwin" {
			root = filepath.Join(home, "Library", "Application Support")
		}
		if platform == "windows" {
			root = filepath.Join(home, "AppData", "Local")
		}
		if paths[0] != filepath.Join(root, "Claude", "claude_desktop_config.json") || paths[2] != filepath.Join(root, "Claude-3p", "configLibrary", desktopProfileID+".json") {
			t.Fatal(platform, paths)
		}
	}
}

func TestDesktopPreservesUnknownSettingsAndRejectsConflict(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	if runtime.GOOS == "linux" {
		t.Setenv("XDG_CONFIG_HOME", filepath.Join(c.home, ".config"))
	}
	paths, _ := c.desktopPaths()
	originals := [][]byte{[]byte(`{"deploymentMode":"1p","mcpServers":{"keep":{}},"large":9007199254740993}`), []byte(`{"untouched":true}`), []byte(`{"custom":"keep"}`), []byte(`{"entries":[{"id":"other","name":"Other"}],"appliedId":"other"}`)}
	for i, path := range paths {
		if err := atomicFile(path, originals[i]); err != nil {
			t.Fatal(err)
		}
	}
	pr, err := c.previewWithAPIKey("claude-desktop", "deployment-id", "nn_selected_key_1234567890", "claude-sonnet-newnoa")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(paths[0])
	if !strings.Contains(string(raw), "9007199254740993") || !strings.Contains(string(raw), "mcpServers") {
		t.Fatal("unknown settings lost")
	}
	installed, _ := os.ReadFile(paths[3])
	if err = atomicFile(paths[3], []byte(`{"external":true}`)); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("claude-desktop"); err == nil {
		t.Fatal("restore ignored external metadata")
	}
	if err = atomicFile(paths[3], installed); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("claude-desktop"); err != nil {
		t.Fatal(err)
	}
	for i, path := range paths {
		raw, _ := os.ReadFile(path)
		if string(raw) != string(originals[i]) {
			t.Fatal("restore lost original", path)
		}
	}
}

func TestCodexPreparedRecoveryWithAuthAtEachWrite(t *testing.T) {
	for _, written := range []int{0, 1, 2} {
		t.Run(string(rune('0'+written)), func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			pr, err := c.previewWithAPIKey("codex", "deployment-id", "nn_selected_key_1234567890")
			if err != nil {
				t.Fatal(err)
			}
			j := journal{Model: pr.Model, Alias: pr.Alias, Stage: "prepared", Installed: hash([]byte(pr.Content)), Original: nil, Existed: false, Companions: pr.companions}
			raw, _ := json.Marshal(j)
			if err = atomicFile(c.journalPath(), raw); err != nil {
				t.Fatal(err)
			}
			if written > 0 {
				if err = atomicFile(pr.Path, []byte(pr.Content)); err != nil {
					t.Fatal(err)
				}
			}
			if written > 1 {
				if err = atomicFile(pr.companions[0].Path, pr.companions[0].Content); err != nil {
					t.Fatal(err)
				}
			}
			if err = c.restoreAgent("codex"); err != nil {
				t.Fatal(err)
			}
			for _, path := range pr.Paths {
				if _, err = os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("incomplete recovery", path, err)
				}
			}
		})
	}
}

func TestClaudeCLIReplacementKeepsOriginalBackup(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, _, _ := c.agentPaths("claude-cli")
	original := []byte(`{"env":{"CUSTOM":"keep","ANTHROPIC_AUTH_TOKEN":"old"},"permissions":{"allow":[]}}`)
	if err := atomicFile(path, original); err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"first-deployment", "next-deployment"} {
		pr, err := c.previewWithAPIKey("claude-cli", model, "nn_selected_key_1234567890", "friendly-model")
		if err != nil {
			t.Fatal(err)
		}
		if err = c.apply(pr.Digest, pr.Content); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.restoreAgent("claude-cli"); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != string(original) {
		t.Fatal("original CLI settings lost")
	}
}

func TestCodexAuthChangeAfterPreviewWritesNothing(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	pr, err := c.previewWithAPIKey("codex", "deployment-id", "nn_selected_key_1234567890")
	if err != nil {
		t.Fatal(err)
	}
	if err = atomicFile(pr.Paths[1], []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err == nil {
		t.Fatal("changed auth accepted")
	}
	if _, err = os.Stat(c.configPath()); !os.IsNotExist(err) {
		t.Fatal("config written despite auth conflict")
	}
	if _, err = os.Stat(c.journalPath()); !os.IsNotExist(err) {
		t.Fatal("journal written despite auth conflict")
	}
}

func TestNativeJSONExpansionRejectedBeforeWrite(t *testing.T) {
	for _, agent := range []string{"codex", "claude-desktop"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			if runtime.GOOS == "linux" {
				t.Setenv("XDG_CONFIG_HOME", filepath.Join(c.home, ".config"))
			}
			path := filepath.Join(c.home, ".codex", "auth.json")
			if agent == "claude-desktop" {
				paths, _ := c.desktopPaths()
				path = paths[0]
			}
			// Compact input fits the read limit; indentation pushes the generated file over it.
			original := []byte(`{"unknown":[` + strings.Repeat(`0,`, maxFrame/3) + `0]}`)
			if len(original) > maxFrame {
				t.Fatal("invalid fixture")
			}
			if err := atomicFile(path, original); err != nil {
				t.Fatal(err)
			}
			alias := "friendly-model"
			if agent == "claude-desktop" {
				alias = "claude-sonnet-newnoa"
			}
			if _, err := c.previewWithAPIKey(agent, "deployment-id", "nn_selected_key_1234567890", alias); err == nil {
				t.Fatal("accepted unreadable expanded output")
			}
			raw, _ := os.ReadFile(path)
			if string(raw) != string(original) {
				t.Fatal("rejected preview wrote input")
			}
			if c.pending != nil {
				t.Fatal("rejected preview retained installable token")
			}
		})
	}
}

func TestCodexAuthConflictPreventsPartialRestore(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	pr, err := c.previewWithAPIKey("codex", "deployment-id", "nn_selected_key_1234567890")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	authPath := filepath.Join(c.home, ".codex", "auth.json")
	if err = atomicFile(authPath, []byte(`{"OPENAI_API_KEY":"external"}`)); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("codex"); err == nil {
		t.Fatal("overwrote external auth")
	}
	if _, err = os.Stat(c.configPath()); err != nil {
		t.Fatal("partially restored config")
	}
}

func TestCodexReasoningRejectsUnsupportedValue(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	for _, effort := range []string{"invented", "high\nmodel=evil"} {
		raw, _ := json.Marshal(map[string]string{"agent": "codex", "model": "deployment-id", "apiKey": "nn_selected_key_1234567890", "reasoningEffort": effort})
		if _, err := c.handle("agent.preview", raw); err == nil {
			t.Fatal("accepted", effort)
		}
	}
}

func TestClaudeDesktopNativeFilesAndRecovery(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(c.home, ".config"))
	pr, err := c.previewWithAPIKey("claude-desktop", "deployment-id", "nn_selected_key_1234567890", "claude-sonnet-newnoa")
	if err != nil {
		t.Fatal(err)
	}
	if len(pr.Paths) != 4 {
		t.Fatalf("expected four Desktop files: %#v", pr.Paths)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	for _, path := range pr.Paths {
		if _, err = os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
	profile, _ := os.ReadFile(pr.Paths[2])
	if !strings.Contains(string(profile), `"inferenceGatewayAuthScheme": "bearer"`) || !strings.Contains(string(profile), "claude-sonnet-newnoa") || strings.Contains(string(profile), "nn_selected_key_1234567890") || !strings.Contains(string(profile), managedAgentKey("nn_selected_key_1234567890")) {
		t.Fatal("wrong Desktop profile", string(profile))
	}
	pr, err = c.previewWithAPIKey("claude-desktop", "deployment-next", "nn_next_key_1234567890", "claude-sonnet-newnoa")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("claude-desktop"); err != nil {
		t.Fatal(err)
	}
	for _, path := range pr.Paths {
		if _, err = os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("new Desktop file retained", path, err)
		}
	}
}

func TestCodexReasoningMaxAndUltra(t *testing.T) {
	for _, effort := range []string{"max", "ultra"} {
		c := newCore(t.TempDir(), "http://127.0.0.1:8091")
		raw, _ := json.Marshal(map[string]string{"agent": "codex", "model": "deployment-id", "apiKey": "nn_selected_key_1234567890", "reasoningEffort": effort})
		if _, err := c.handle("agent.preview", raw); err != nil {
			t.Fatal(err)
		}
	}
}
