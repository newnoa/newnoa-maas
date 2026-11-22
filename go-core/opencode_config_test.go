package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestOpenCodeConfigRoundTrip(t *testing.T) {
	for _, name := range []string{"OPENCODE_CONFIG", "OPENCODE_CONFIG_CONTENT", "OPENCODE_CONFIG_DIR", "XDG_CONFIG_HOME"} {
		t.Setenv(name, "")
	}
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, journalPath, _ := c.agentPaths("opencode")
	original := []byte(`{"unknown":9007199254740993,"provider":{"other":{"name":"keep"}},"model":"other/old"}`)
	if err := atomicFile(path, original); err != nil {
		t.Fatal(err)
	}
	pr, err := c.preview("opencode", "deployment-id")
	if err != nil {
		t.Fatal(err)
	}
	var settings struct {
		Unknown    json.Number
		Model      string
		SmallModel string `json:"small_model"`
		Provider   map[string]json.RawMessage
	}
	if err = json.Unmarshal([]byte(pr.Content), &settings); err != nil {
		t.Fatal(err)
	}
	if settings.Unknown.String() != "9007199254740993" || settings.Model != "newnoa/deployment-id" || settings.SmallModel != settings.Model || settings.Provider["other"] == nil {
		t.Fatal("settings lost")
	}
	var provider struct {
		NPM     string
		Options map[string]string
		Models  map[string]any
	}
	if err = json.Unmarshal(settings.Provider["newnoa"], &provider); err != nil {
		t.Fatal(err)
	}
	if provider.NPM != "@ai-sdk/openai-compatible" || provider.Options["baseURL"] != "http://127.0.0.1:18222/v1" || provider.Options["apiKey"] != c.localKey || provider.Models["deployment-id"] == nil {
		t.Fatal("wrong provider")
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	state, err := c.detectAgent("opencode")
	if err != nil || state["configurationState"] != "applied" || state["supportedProtocol"] != "chat/completions" {
		t.Fatal(state, err)
	}
	if err = atomicFile(path, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if c.restoreAgent("opencode") == nil {
		t.Fatal("external edit overwritten")
	}
	if err = atomicFile(path, []byte(pr.Content)); err != nil {
		t.Fatal(err)
	}
	if err = c.restore(); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(actual, original) {
		t.Fatal("original not restored", err)
	}
	if _, err = os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatal("journal retained", err)
	}
}

func TestOpenCodeConfigRefusals(t *testing.T) {
	for _, name := range []string{"OPENCODE_CONFIG", "OPENCODE_CONFIG_CONTENT", "OPENCODE_CONFIG_DIR", "XDG_CONFIG_HOME"} {
		t.Setenv(name, "")
	}
	for _, raw := range []string{"null", "[]", `{"provider":null}`, `{"provider":[]}`, `{"provider":{"newnoa":{}}}`, `{"providers":{"newnoa":{}}}`} {
		c := newCore(t.TempDir(), "http://127.0.0.1:8091")
		path, _, _ := c.agentPaths("opencode")
		if err := atomicFile(path, []byte(raw)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.preview("opencode", "model"); err == nil {
			t.Fatal("unsafe settings accepted", raw)
		}
	}
	for _, name := range []string{"OPENCODE_CONFIG", "OPENCODE_CONFIG_CONTENT", "OPENCODE_CONFIG_DIR", "XDG_CONFIG_HOME"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "custom")
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			if _, err := c.preview("opencode", "model"); err == nil {
				t.Fatal("custom path ignored")
			}
		})
	}
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, _, _ := c.agentPaths("opencode")
	if err := atomicFile(path+"c", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.preview("opencode", "model"); err == nil {
		t.Fatal("JSONC shadow ignored")
	}
}

func TestOpenCodeApplyRechecksLocation(t *testing.T) {
	for _, name := range []string{"jsonc", "OPENCODE_CONFIG", "OPENCODE_CONFIG_CONTENT", "OPENCODE_CONFIG_DIR", "XDG_CONFIG_HOME"} {
		t.Run(name, func(t *testing.T) {
			for _, key := range []string{"OPENCODE_CONFIG", "OPENCODE_CONFIG_CONTENT", "OPENCODE_CONFIG_DIR", "XDG_CONFIG_HOME"} {
				t.Setenv(key, "")
			}
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			path, journalPath, _ := c.agentPaths("opencode")
			pr, err := c.preview("opencode", "model")
			if err != nil {
				t.Fatal(err)
			}
			if name == "jsonc" {
				if err := atomicFile(path+"c", []byte("{}")); err != nil {
					t.Fatal(err)
				}
			} else {
				t.Setenv(name, "custom")
			}
			if err := c.apply(pr.Digest, pr.Content); err == nil {
				t.Fatal("configuration location changed after preview but apply succeeded")
			}
			for _, target := range []string{path, journalPath} {
				if _, err := os.Lstat(target); !os.IsNotExist(err) {
					t.Fatalf("refused apply wrote %s: %v", target, err)
				}
			}
		})
	}
}

func TestOpenCodeAddsKeysAndModelsWithoutChangingDefault(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, journalPath, _ := c.agentPaths("opencode")
	keys := []string{"nn_first_fixture_key_1234567", "nn_second_fixture_key_123456"}
	for _, row := range []struct {
		model, alias string
		key          string
	}{{"m-one", "first", keys[0]}, {"m-two", "second", keys[1]}, {"m-three", "third", keys[1]}} {
		pr, err := c.previewWithAPIKey("opencode", row.model, row.key, row.alias)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.apply(pr.Digest, pr.Content); err != nil {
			t.Fatal(err)
		}
	}
	var settings struct {
		Model    string
		Provider map[string]struct {
			Options struct {
				APIKey string `json:"apiKey"`
			}
			Models map[string]json.RawMessage
		}
	}
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &settings) != nil || settings.Model != "newnoa/first" || len(settings.Provider) != 2 {
		t.Fatal("configured default or providers changed", err)
	}
	counts := map[string]int{}
	for _, provider := range settings.Provider {
		counts[provider.Options.APIKey] += len(provider.Models)
	}
	if counts[managedAgentKey(keys[0])] != 1 || counts[managedAgentKey(keys[1])] != 2 {
		t.Fatal("models were not grouped by their own selected key")
	}
	raw, err := os.ReadFile(journalPath)
	var j journal
	if err != nil || json.Unmarshal(raw, &j) != nil || j.Model != "m-one" || j.APIKeyHash != hash([]byte(keys[0])) || len(journalConnections(j)) != 3 {
		t.Fatal("journal lost original default or connections")
	}
	reloaded := newCore(c.home, c.gateway)
	if got := reloaded.modelAliases["opencode\nthird"]; got.Model != "m-three" || !got.Keys[hash([]byte(keys[1]))] || got.Keys[hash([]byte(keys[0]))] {
		t.Fatal("restarted proxy lost the added key-bound route")
	}
}

func TestOpenCodeManagedProviderLifecycle(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, _, _ := c.agentPaths("opencode")
	original := []byte(`{"provider":{"manual":{"name":"Unmanaged"}},"model":"manual/original"}`)
	if err := atomicFile(path, original); err != nil {
		t.Fatal(err)
	}
	const first = "nn_opencode_lifecycle_first_123456"
	const second = "nn_opencode_lifecycle_second_123456"
	const owner = "11111111-1111-4111-8111-111111111111"
	const firstID = "22222222-2222-4222-8222-222222222222"
	const secondID = "33333333-3333-4333-8333-333333333333"
	apply := func(keyID, secret, action string, models ...providerModel) {
		t.Helper()
		p, err := c.previewManagedProvider("opencode", owner, keyID, secret, action, models)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.apply(p.Digest, p.Content); err != nil {
			t.Fatal(err)
		}
	}
	checkNative := func(alias string, counts ...int) {
		t.Helper()
		binary := os.Getenv("NEWNOA_TEST_OPENCODE")
		if binary == "" {
			return
		}
		cmd := exec.Command(binary, "debug", "config")
		cmd.Dir = c.home
		cmd.Env = append(os.Environ(), "HOME="+c.home, "XDG_CONFIG_HOME="+filepath.Join(c.home, ".config"), "XDG_DATA_HOME="+filepath.Join(c.home, ".local", "share"), "XDG_CACHE_HOME="+filepath.Join(c.home, ".cache"), "XDG_STATE_HOME="+filepath.Join(c.home, ".local", "state"), "OPENCODE_DISABLE_AUTOUPDATE=true")
		raw, err := cmd.Output()
		if err != nil {
			t.Fatal("native OpenCode config read failed", err)
		}
		var config struct {
			Model    string
			Provider map[string]struct{ Models map[string]json.RawMessage }
		}
		if json.Unmarshal(raw, &config) != nil || !strings.HasSuffix(config.Model, "/"+alias) {
			t.Fatal("native OpenCode default mismatch")
		}
		var found []int
		for id, p := range config.Provider {
			if strings.HasPrefix(id, "newnoa-") {
				found = append(found, len(p.Models))
			}
		}
		slices.Sort(found)
		if !slices.Equal(found, counts) {
			t.Fatal("native OpenCode provider model counts differ", found)
		}
	}
	apply(firstID, first, "add", providerModel{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1", "one"}, providerModel{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2", "two"})
	apply(secondID, second, "add", providerModel{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3", "three"})
	checkNative("one", 1, 2)
	state, err := c.detectAgent("opencode")
	if err != nil || state["model"] != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1" {
		t.Fatal("append changed default", err)
	}
	apply(secondID, second, "default", providerModel{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3", ""})
	checkNative("three", 1, 2)
	selection, entries, err := c.remoteManagedSelection("opencode")
	if err != nil || selection.Secret != managedAgentKey(second) || selection.Alias != "three" || selection.KeyID != secondID || len(entries) != 1 || entries[0].APIKeyHash != hash([]byte(second)) {
		t.Fatal("remote launch did not isolate the default provider", err)
	}
	state, _ = c.detectAgent("opencode")
	if state["model"] != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3" {
		t.Fatal("explicit default was ignored")
	}
	if _, err := c.previewManagedProvider("opencode", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4", firstID, "", "remove", nil); err == nil {
		t.Fatal("another account can remove provider")
	}
	if _, err := c.previewManagedProvider("opencode", owner, secondID, "", "remove", nil); err == nil {
		t.Fatal("removing a default silently selects another route")
	}
	apply(firstID, "", "remove-model", providerModel{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2", ""})
	state, _ = c.detectAgent("opencode")
	if slices.Contains(state["configuredModels"].([]string), "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2") {
		t.Fatal("removed model remains available")
	}
	apply(firstID, "", "remove")
	checkNative("three", 1)
	c = newCore(c.home, c.gateway)
	if err := c.restore(); err != nil {
		t.Fatal(err)
	}
	if _, found := c.modelAliases["opencode\none"]; found {
		t.Fatal("removed alias survived restart")
	}
	if r := c.modelAliases["opencode\nthree"]; r.Model != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3" || !r.Keys[hash([]byte(second))] {
		t.Fatal("unrelated connection changed")
	}
	apply(secondID, "", "remove")
	if result, err := os.ReadFile(path); err != nil || !bytes.Equal(result, original) {
		t.Fatal("last removal failed exact restore", err)
	}
}
