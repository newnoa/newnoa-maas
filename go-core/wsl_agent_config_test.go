package main

import (
	"encoding/json"
	"os"
	"testing"
)

func TestWSLStartupPreservesAppliedAndRecoversPrepared(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli"} {
		for _, interrupted := range []bool{false, true} {
			c := newCore(t.TempDir(), "https://server.example.test")
			pr, err := c.previewWithAPIKey(agent, "fixture-model", "nn_fixture_only_123456789")
			if err != nil {
				t.Fatal(err)
			}
			if err = c.apply(pr.Digest, pr.Content); err != nil {
				t.Fatal(err)
			}
			_, journalPath, err := c.agentPaths(agent)
			if err != nil {
				t.Fatal(err)
			}
			if interrupted {
				j, err := c.readProviderJournal(agent)
				if err != nil || j == nil {
					t.Fatalf("open journal: %v", err)
				}
				j.Stage = "prepared"
				raw, _ := json.Marshal(j)
				if err = atomicFile(journalPath, raw); err != nil {
					t.Fatal(err)
				}
			}
			restarted := newCore(c.home, c.gateway)
			if err := restarted.restoreOnStartup(true); err != nil {
				t.Fatal(err)
			}
			_, err = os.Stat(journalPath)
			if (interrupted && !os.IsNotExist(err)) || (!interrupted && err != nil) {
				t.Fatalf("%s interrupted=%t journal=%v", agent, interrupted, err)
			}
			if !interrupted {
				if err := restarted.restoreAgent(agent); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
					t.Fatal("explicit restore left the journal")
				}
			}
		}
	}
}
