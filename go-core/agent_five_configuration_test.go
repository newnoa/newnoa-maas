package main

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
)

func TestFiveAgentManagedModelAdditionAndRestore(t *testing.T) {
	for _, agent := range []string{"pi-agent", "openclaw"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			var paths []string
			if agent == "pi-agent" {
				paths, _ = c.piPaths()
			} else {
				p, _, _ := c.agentPaths(agent)
				paths = []string{p}
			}
			original := []byte("{\"unrelated\":9007199254740993}\n")
			for _, p := range paths {
				if err := atomicFile(p, original); err != nil {
					t.Fatal(err)
				}
			}
			for _, model := range []string{"model-one", "model-two", "model-two"} {
				p, err := c.previewWithAPIKey(agent, model, c.localKey)
				if err != nil {
					t.Fatal("managed model addition", err)
				}
				if err = c.apply(p.Digest, p.Content); err != nil {
					t.Fatal(err)
				}
			}
			raw, _ := os.ReadFile(paths[0])
			var config map[string]any
			if err := json.Unmarshal(raw, &config); err != nil {
				t.Fatal(err)
			}
			providers := config["providers"]
			if agent == "openclaw" {
				providers = config["models"].(map[string]any)["providers"]
			}
			models := providers.(map[string]any)["newnoa"].(map[string]any)["models"].([]any)
			if len(models) != 2 {
				t.Fatalf("models=%d, want two unique managed models", len(models))
			}
			reloaded := newCore(c.home, c.gateway)
			for _, m := range []string{"model-one", "model-two"} {
				if !reloaded.modelAliases[agent+"\n"+m].Keys[hash([]byte(c.localKey))] {
					t.Fatal("route missing after reload", m)
				}
			}
			if err := c.restoreAgent(agent); err != nil {
				t.Fatal(err)
			}
			for _, p := range paths {
				b, _ := os.ReadFile(p)
				if string(b) != string(original) {
					t.Fatal("initial backup lost")
				}
			}
		})
	}
}

func TestFiveAgentKeyReplacementAndConflict(t *testing.T) {
	for _, agent := range []string{"pi-agent", "openclaw"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			for i, model := range []string{"model-one", "model-two"} {
				key := "nn_first_api_key_123456789012345"
				if i == 1 {
					key = "nn_second_api_key_123456789012345"
				}
				p, err := c.previewWithAPIKey(agent, model, key)
				if err != nil {
					t.Fatal(err)
				}
				if err = c.apply(p.Digest, p.Content); err != nil {
					t.Fatal(err)
				}
			}
			reloaded := newCore(c.home, c.gateway)
			if _, ok := reloaded.modelAliases[agent+"\nmodel-one"]; ok {
				t.Fatal("stale key/model route retained")
			}
			var paths []string
			if agent == "pi-agent" {
				paths, _ = c.piPaths()
			} else {
				p, _, _ := c.agentPaths(agent)
				paths = []string{p}
			}
			raw, _ := os.ReadFile(paths[0])
			if string(raw) == "" {
				t.Fatal("missing config")
			}
			if err := atomicFile(paths[0], append(raw, ' ')); err != nil {
				t.Fatal(err)
			}
			if _, err := c.preview(agent, "third"); err == nil {
				t.Fatal("external edit accepted")
			}
			if err := c.restoreAgent(agent); err == nil {
				t.Fatal("external edit overwritten")
			}
		})
	}
}

func TestFiveAgentPiInterruptedUpdateRestoresFirstBackup(t *testing.T) {
	for _, written := range []int{0, 1, 2} {
		t.Run(string(rune('0'+written)), func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			paths, jpath := c.piPaths()
			original := []byte("{\"keep\":true}")
			for _, p := range paths {
				if err := atomicFile(p, original); err != nil {
					t.Fatal(err)
				}
			}
			p, err := c.preview("pi-agent", "first")
			if err != nil {
				t.Fatal(err)
			}
			if err = c.apply(p.Digest, p.Content); err != nil {
				t.Fatal(err)
			}
			previous, err := c.readPiJournal()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.preview("pi-agent", "second"); err != nil {
				t.Fatal(err)
			}
			j := piJournal{Files: append([]piFile(nil), c.pending.piFiles...), Stage: "prepared"}
			for i := range j.Files {
				j.Files[i].Original = previous.Files[i].Original
				j.Files[i].Existed = previous.Files[i].Existed
				j.Files[i].PreviousInstalled = previous.Files[i].Installed
			}
			data, _ := json.Marshal(j)
			if err = atomicFile(jpath, data); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < written; i++ {
				if err = atomicFile(paths[i], c.pending.piFiles[i].Content); err != nil {
					t.Fatal(err)
				}
			}
			status, err := c.detectPi()
			if err != nil || status["configurationState"] != "recovery_required" {
				t.Fatal("interrupted update status", status, err)
			}
			if err = c.restorePi(); err != nil {
				t.Fatal(err)
			}
			for _, p := range paths {
				actual, _ := os.ReadFile(p)
				if string(actual) != string(original) {
					t.Fatal("first backup lost")
				}
			}
		})
	}
}

func TestFiveAgentRPCKeyReplacementDropsOldRoutes(t *testing.T) {
	for _, agent := range []string{"pi-agent", "openclaw"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			c.server = &http.Server{}
			for i, model := range []string{"first", "second"} {
				key := "nn_first_agent_key_123456789012345"
				if i == 1 {
					key = "nn_second_agent_key_123456789012345"
				}
				b, _ := json.Marshal(map[string]string{"agent": agent, "model": model, "apiKey": key})
				v, err := c.handle("agent.preview", b)
				if err != nil {
					t.Fatal(err)
				}
				b, _ = json.Marshal(map[string]string{"digest": v.(preview).Digest})
				if _, err = c.handle("agent.apply", b); err != nil {
					t.Fatal(err)
				}
			}
			if _, ok := c.modelAliases[agent+"\nfirst"]; ok {
				t.Fatal("old credential route retained in live Core")
			}
		})
	}
}

func TestFiveAgentRPCKeyReplacementReusesAlias(t *testing.T) {
	for _, agent := range []string{"pi-agent", "openclaw"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			c.server = &http.Server{}
			first, second := "nn_first_agent_key_123456789012345", "nn_second_agent_key_123456789012345"
			apply := func(model, key string) error {
				b, _ := json.Marshal(map[string]string{"agent": agent, "model": model, "alias": "shared", "apiKey": key})
				v, err := c.handle("agent.preview", b)
				if err != nil {
					return err
				}
				b, _ = json.Marshal(map[string]string{"digest": v.(preview).Digest})
				_, err = c.handle("agent.apply", b)
				return err
			}
			if err := apply("model-one", first); err != nil {
				t.Fatal(err)
			}
			if err := apply("model-two", first); err == nil {
				t.Fatal("same-key alias remap accepted")
			}
			if err := apply("model-two", second); err != nil {
				t.Fatal("key replacement with same alias", err)
			}
			route := c.modelAliases[agent+"\nshared"]
			if route.Model != "model-two" || route.Keys[hash([]byte(first))] || !route.Keys[hash([]byte(second))] {
				t.Fatal("incorrect replacement route")
			}
		})
	}
}
