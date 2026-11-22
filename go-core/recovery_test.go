package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestRecoveryContinuesAfterOtherAgentConflict(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	for _, agent := range []string{"codex", "claude-cli"} {
		pr, err := c.preview(agent, "deployment-id")
		if err != nil {
			t.Fatal(err)
		}
		if err = c.apply(pr.Digest, pr.Content); err != nil {
			t.Fatal(err)
		}
	}
	external := []byte("# external edit\n")
	if err := atomicFile(c.configPath(), external); err != nil {
		t.Fatal(err)
	}
	if c.restore() == nil {
		t.Fatal("Codex conflict not reported")
	}
	actual, err := os.ReadFile(c.configPath())
	if err != nil || !bytes.Equal(actual, external) {
		t.Fatal("external edit changed", err)
	}
	if _, err = os.Stat(c.journalPath()); err != nil {
		t.Fatal("conflict journal removed", err)
	}
	claudePath, claudeJournal, _ := c.agentPaths("claude-cli")
	for _, path := range []string{claudePath, claudeJournal} {
		if _, err = os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("Claude recovery skipped after Codex conflict", err)
		}
	}
}

func TestPreparedRecoveryChecksExistence(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli"} {
		for _, originalExists := range []bool{false, true} {
			for _, changed := range []bool{false, true} {
				t.Run(agent+"/"+map[bool]string{false: "missing", true: "empty"}[originalExists]+"/"+map[bool]string{false: "unchanged", true: "changed"}[changed], func(t *testing.T) {
					c := newCore(t.TempDir(), "http://127.0.0.1:8091")
					configPath, journalPath, err := c.agentPaths(agent)
					if err != nil {
						t.Fatal(err)
					}
					currentExists := originalExists != changed
					if currentExists {
						if err = atomicFile(configPath, nil); err != nil {
							t.Fatal(err)
						}
					}
					data, err := json.Marshal(journal{Original: nil, Existed: originalExists, Installed: hash([]byte("installed configuration")), Stage: "prepared"})
					if err != nil {
						t.Fatal(err)
					}
					if err = atomicFile(journalPath, data); err != nil {
						t.Fatal(err)
					}
					err = c.restoreAgent(agent)
					if changed {
						if err == nil {
							t.Fatal("existence change was accepted as unchanged")
						}
						if _, err = os.Stat(journalPath); err != nil {
							t.Fatal("conflict lost recovery evidence", err)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						if _, err = os.Stat(journalPath); !os.IsNotExist(err) {
							t.Fatal("unchanged preparation retained journal", err)
						}
					}
					_, err = os.Stat(configPath)
					if currentExists && err != nil || !currentExists && !os.IsNotExist(err) {
						t.Fatal("recovery changed file existence", err)
					}
				})
			}
		}
	}
}
