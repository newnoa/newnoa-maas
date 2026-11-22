package main

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

func TestOpenClawRoundTrip(t *testing.T) {
	for _, key := range []string{"OPENCLAW_HOME", "OPENCLAW_STATE_DIR", "OPENCLAW_CONFIG_PATH", "OPENCLAW_PROFILE"} {
		t.Setenv(key, "")
	}
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, journal, _ := c.agentPaths("openclaw")
	original := []byte(`{"unknown":9007199254740993,"models":{"providers":{"keep":{"value":9007199254740993}}},"agents":{"defaults":{"model":{"primary":"old/model","fallbacks":["other/model"],"extra":9007199254740993},"workspace":"preserved"}}}`)
	if err := atomicFile(path, original); err != nil {
		t.Fatal(err)
	}
	pr, err := c.preview("openclaw", "deployment-id")
	if err != nil {
		t.Fatal(err)
	}
	var value struct {
		Unknown json.Number
		Models  struct {
			Providers map[string]struct {
				BaseURL string `json:"baseUrl"`
				APIKey  string `json:"apiKey"`
				API     string
			}
		}
		Agents struct {
			Defaults struct {
				Workspace string
				Model     map[string]json.RawMessage
			}
		}
	}
	if err = json.Unmarshal([]byte(pr.Content), &value); err != nil {
		t.Fatal(err)
	}
	provider := value.Models.Providers["newnoa"]
	if value.Unknown.String() != "9007199254740993" || provider.APIKey != c.localKey || provider.API != "openai-completions" || provider.BaseURL != "http://127.0.0.1:18222/v1" || value.Agents.Defaults.Workspace != "preserved" || string(value.Agents.Defaults.Model["primary"]) != `"newnoa/deployment-id"` || value.Agents.Defaults.Model["fallbacks"] != nil || string(value.Agents.Defaults.Model["extra"]) != "9007199254740993" {
		t.Fatal("incorrect configuration")
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	if _, err = c.preview("openclaw", "second"); err != nil {
		t.Fatal("managed addition rejected", err)
	}
	state, err := c.detectAgent("openclaw")
	if err != nil || state["configurationState"] != "applied" || state["supportedProtocol"] != "chat/completions" {
		t.Fatal(state, err)
	}
	if err = atomicFile(path, []byte(`{"external":true}`)); err != nil {
		t.Fatal(err)
	}
	if c.restoreAgent("openclaw") == nil {
		t.Fatal("external edit overwritten")
	}
	if err = atomicFile(path, []byte(pr.Content)); err != nil {
		t.Fatal(err)
	}
	if err = c.restore(); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(original, actual) {
		t.Fatal("restore failed", err)
	}
	if _, err = os.Stat(journal); !os.IsNotExist(err) {
		t.Fatal("journal retained")
	}
}

func TestOpenClawRefusals(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `{models:{}}`, `{"models":null}`, `{"models":{"providers":{"newnoa":{}}}}`, `{"agents":{"defaults":[]}}`, `{"agents":{"defaults":{"model":42}}}`} {
		c := newCore(t.TempDir(), "http://127.0.0.1:8091")
		path, _, _ := c.agentPaths("openclaw")
		if err := atomicFile(path, []byte(raw)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.preview("openclaw", "model"); err == nil {
			t.Fatal("unsafe settings accepted", raw)
		}
	}
	for _, key := range []string{"OPENCLAW_HOME", "OPENCLAW_STATE_DIR", "OPENCLAW_CONFIG_PATH", "OPENCLAW_PROFILE"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "")
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			pr, err := c.preview("openclaw", "model")
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(key, "custom")
			if c.apply(pr.Digest, pr.Content) == nil {
				t.Fatal("changed path ignored")
			}
			if _, err = c.preview("openclaw", "model"); err == nil {
				t.Fatal("custom path accepted")
			}
		})
	}
}

func TestNativeProviderListPreservesDefaultAndRecovery(t *testing.T) {
	for _, agent := range []string{"openclaw", "pi-agent", "hermes-agent"} {
		t.Run(agent, func(t *testing.T) { testNativeProviderList(t, agent) })
	}
}

func testNativeProviderList(t *testing.T, agent string) {
	t.Helper()
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, _, _ := c.agentPaths(agent)
	original := []byte(`{"models":{"providers":{"manual":{"baseUrl":"https://example.invalid"}}},"agents":{"defaults":{"model":"manual/old","workspace":"keep"}}}`)
	if agent == "hermes-agent" {
		original = []byte("# unmanaged\nproviders:\n  manual:\n    base_url: https://example.invalid\nmodel:\n  provider: custom:manual\n  default: old\n")
	}
	var piSettings string
	const originalPiSettings = `{"defaultProvider":"manual","defaultModel":"old","enabledModels":["manual/keep"]}`
	if agent == "pi-agent" {
		paths, _ := c.piPaths()
		path, piSettings = paths[0], paths[1]
		original = []byte(`{"providers":{"manual":{"baseUrl":"https://example.invalid"}}}`)
		if err := atomicFile(piSettings, []byte(originalPiSettings)); err != nil {
			t.Fatal(err)
		}
	}
	if err := atomicFile(path, original); err != nil {
		t.Fatal(err)
	}
	const owner = "11111111-1111-4111-8111-111111111111"
	const keyA = "22222222-2222-4222-8222-222222222222"
	const keyB = "33333333-3333-4333-8333-333333333333"
	const secretA = "nn_openclaw_fixture_key_a_123"
	const secretB = "nn_openclaw_fixture_key_b_123"
	a := providerModel{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1", "one"}
	b := providerModel{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2", "two"}
	d := providerModel{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3", "three"}
	apply := func(key, secret, action string, models ...providerModel) {
		t.Helper()
		p, err := c.previewManagedProvider(agent, owner, key, secret, action, models)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.apply(p.Digest, p.Content); err != nil {
			t.Fatal(err)
		}
	}
	check := func(alias, key, secret string) {
		t.Helper()
		selected, entries, err := c.remoteManagedSelection(agent)
		if err != nil || selected.Alias != alias || selected.KeyID != key || selected.Secret != managedAgentKey(secret) || selected.Owner != owner || len(entries) != 1 {
			t.Fatal("remote default not tied to expected account, key, and model", err)
		}
	}
	apply(keyA, secretA, "add", a, b)
	apply(keyB, secretB, "add", d)
	check("one", keyA, secretA)
	apply(keyB, secretB, "default", d)
	check("three", keyB, secretB)
	if _, err := c.previewManagedProvider(agent, owner, keyB, "", "remove", nil); err == nil {
		t.Fatal("removed active default")
	}
	apply(keyA, "", "remove-model", b)
	if piSettings != "" {
		var settings struct{ EnabledModels []string }
		raw, err := os.ReadFile(piSettings)
		if err != nil || json.Unmarshal(raw, &settings) != nil || len(settings.EnabledModels) != 3 || settings.EnabledModels[0] != "manual/keep" {
			t.Fatal("Pi did not preserve external enabled models or added a stale entry", err)
		}
	}
	c = newCore(c.home, c.gateway)
	if err := c.restore(); err != nil {
		t.Fatal(err)
	}
	check("three", keyB, secretB)
	if r := c.modelAliases[agent+"\none"]; r.Model != a.ID || !r.Keys[hash([]byte(secretA))] || r.Keys[hash([]byte(secretB))] {
		t.Fatal("key A routes are not isolated")
	}
	if _, ok := c.modelAliases[agent+"\ntwo"]; ok {
		t.Fatal("removed alias survived restart")
	}
	apply(keyA, "", "remove")
	apply(keyB, "", "remove")
	if raw, err := os.ReadFile(path); err != nil || !bytes.Equal(raw, original) {
		t.Fatal("original bytes not restored", err)
	}
	if piSettings != "" {
		if raw, err := os.ReadFile(piSettings); err != nil || string(raw) != originalPiSettings {
			t.Fatal("Pi settings were not restored", err)
		}
	}
}

func TestProviderCatalogAdoptsSingleLegacyDefault(t *testing.T) {
	for _, agent := range []string{"openclaw", "pi-agent", "hermes-agent"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			const owner = "11111111-1111-4111-8111-111111111111"
			const key = "22222222-2222-4222-8222-222222222222"
			const secret = "nn_catalog_migration_original_secret"
			a := providerModel{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1", "old-model"}
			b := providerModel{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2", "new-model"}
			legacy, err := c.previewWithAPIKey(agent, a.ID, secret, a.Alias)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.apply(legacy.Digest, legacy.Content); err != nil {
				t.Fatal(err)
			}
			if _, err := c.previewManagedProvider(agent, owner, key, "nn_catalog_migration_wrong_secret", "add", []providerModel{b}); err == nil {
				t.Fatal("mixed legacy credential accepted")
			}
			migration, err := c.previewManagedProvider(agent, owner, key, secret, "add", []providerModel{b})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.apply(migration.Digest, migration.Content); err != nil {
				t.Fatal(err)
			}
			selection, _, err := c.remoteManagedSelection(agent)
			if err != nil || selection.Model != a.ID || selection.KeyID != key || selection.Owner != owner {
				t.Fatal("old default changed during catalog migration", err)
			}
			j, err := c.readProviderJournal(agent)
			if err != nil || len(j.ProviderCatalog.Providers) != 1 || len(j.ProviderCatalog.Providers[0].Models) != 2 {
				t.Fatal("legacy model missing after migration", err)
			}
		})
	}
}
