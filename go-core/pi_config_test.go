package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPiRoundTrip(t *testing.T) {
	t.Setenv("PI_CODING_AGENT_DIR", "")
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	paths, journalPath := c.piPaths()
	original := [][]byte{[]byte(`{"unknown":9007199254740993,"providers":{"keep":{"apiKey":"untouched"}}}`), []byte(`{"defaultProvider":"old","defaultModel":"old-model","unknown":9007199254740993}`)}
	for i, path := range paths {
		if err := atomicFile(path, original[i]); err != nil {
			t.Fatal(err)
		}
	}
	pr, err := c.preview("pi-agent", "deployment-id")
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]json.RawMessage
	if err = json.Unmarshal([]byte(pr.Content), &content); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(content["models.json"], []byte("9007199254740993")) || !bytes.Contains(content["settings.json"], []byte("9007199254740993")) {
		t.Fatal("unknown numbers changed")
	}
	var settings struct {
		DefaultProvider, DefaultModel string
		EnabledModels                 []string
	}
	json.Unmarshal(content["settings.json"], &settings)
	if settings.DefaultProvider != "newnoa" || settings.DefaultModel != "deployment-id" || len(settings.EnabledModels) != 1 || settings.EnabledModels[0] != "newnoa/deployment-id" {
		t.Fatal("incorrect defaults")
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	state, err := c.detectAgent("pi-agent")
	if err != nil || state["configurationState"] != "applied" || state["supportedProtocol"] != "chat/completions" {
		t.Fatal(state, err)
	}
	info, err := os.Stat(journalPath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("journal permissions", err)
	}
	if err = c.restore(); err != nil {
		t.Fatal(err)
	}
	for i, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(raw, original[i]) {
			t.Fatal("restore changed original", err)
		}
	}
}

func TestPiInterruptedRestoreAndUnsafePaths(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	paths, journalPath := c.piPaths()
	original := []byte(`{"preserved":true}`)
	for _, path := range paths {
		if err := atomicFile(path, original); err != nil {
			t.Fatal(err)
		}
	}
	pr, err := c.preview("pi-agent", "model")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	if err = atomicFile(paths[0], original); err != nil {
		t.Fatal(err)
	}
	state, err := c.detectAgent("pi-agent")
	if err != nil || state["configurationState"] != "recovery_required" {
		t.Fatal(state, err)
	}
	if err = c.restore(); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(raw, original) {
			t.Fatal("interrupted restore failed", err)
		}
	}
	if err = os.Remove(paths[1]); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(paths[0], paths[1]); err != nil {
		t.Fatal(err)
	}
	if _, err = c.preview("pi-agent", "model"); err == nil {
		t.Fatal("symlink accepted")
	}
	if err = os.Remove(paths[1]); err != nil {
		t.Fatal(err)
	}
	if err = atomicFile(journalPath, []byte(`{"stage":"prepared","files":[]}`)); err != nil {
		t.Fatal(err)
	}
	if c.restorePi() == nil {
		t.Fatal("malformed journal accepted")
	}
}

func TestPiPreparedRecovery(t *testing.T) {
	for _, written := range []int{0, 1, 2} {
		t.Run(string(rune('0'+written)), func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			pr, err := c.preview("pi-agent", "model")
			if err != nil {
				t.Fatal(err)
			}
			paths, journalPath := c.piPaths()
			j := piJournal{Files: c.pending.piFiles, Stage: "prepared"}
			data, _ := json.Marshal(j)
			if err = atomicFile(journalPath, data); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < written; i++ {
				if err = atomicFile(paths[i], c.pending.piFiles[i].Content); err != nil {
					t.Fatal(err)
				}
			}
			state, err := c.detectAgent(pr.Agent)
			if err != nil || state["configurationState"] != "recovery_required" {
				t.Fatal(state, err)
			}
			if err = newCore(c.home, c.gateway).restore(); err != nil {
				t.Fatal(err)
			}
			for _, path := range paths {
				if _, err = os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("new file not removed", path)
				}
			}
		})
	}
}

func TestPiConflictsAndRefusals(t *testing.T) {
	t.Setenv("PI_CODING_AGENT_DIR", "")
	for _, index := range []int{0, 1} {
		c := newCore(t.TempDir(), "http://127.0.0.1:8091")
		paths, journalPath := c.piPaths()
		pr, err := c.preview("pi-agent", "model")
		if err != nil {
			t.Fatal(err)
		}
		atomicFile(paths[index], []byte(`{"external":true}`))
		if c.apply(pr.Digest, pr.Content) == nil {
			t.Fatal("external edit overwritten")
		}
		if _, err = os.Stat(journalPath); !os.IsNotExist(err) {
			t.Fatal("journal created before preflight")
		}
		os.Remove(paths[index])
		pr, err = c.preview("pi-agent", "model")
		if err != nil {
			t.Fatal(err)
		}
		if err = c.apply(pr.Digest, pr.Content); err != nil {
			t.Fatal(err)
		}
		before, _ := os.ReadFile(paths[1-index])
		atomicFile(paths[index], []byte(`{"external":true}`))
		if c.restoreAgent("pi-agent") == nil {
			t.Fatal("restore overwrote external edit")
		}
		after, _ := os.ReadFile(paths[1-index])
		if !bytes.Equal(before, after) {
			t.Fatal("restore changed other file before conflict check")
		}
		state, err := c.detectAgent("pi-agent")
		if err != nil || state["configurationState"] != "conflict" {
			t.Fatal(state, err)
		}
	}
	for _, raw := range []string{`null`, `[]`, `{"providers":null}`, `{"providers":{"newnoa":{}}}`} {
		c := newCore(t.TempDir(), "http://127.0.0.1:8091")
		paths, _ := c.piPaths()
		atomicFile(paths[0], []byte(raw))
		if _, err := c.preview("pi-agent", "model"); err == nil {
			t.Fatal("unsafe config accepted", raw)
		}
	}
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	pr, err := c.preview("pi-agent", "model")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PI_CODING_AGENT_DIR", "/custom")
	if c.apply(pr.Digest, pr.Content) == nil {
		t.Fatal("changed location ignored")
	}
	if _, err = c.preview("pi-agent", "model"); err == nil {
		t.Fatal("custom location accepted")
	}
	t.Setenv("PI_CODING_AGENT_DIR", "")
	atomicFile(filepath.Join(c.home, ".pi", "agent", "auth.json"), []byte(`{"newnoa":{"type":"api_key","key":"existing"}}`))
	if _, err = c.preview("pi-agent", "model"); err == nil {
		t.Fatal("existing authentication ignored")
	}
}
