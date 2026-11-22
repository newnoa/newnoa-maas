package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func hanaFixture(t *testing.T) (*core, []string, string, []byte) {
	t.Helper()
	t.Setenv("HANA_HOME", "")
	c := newCore(t.TempDir(), "http://gateway.invalid")
	paths, journalPath := c.hanaPaths()
	original := []byte(`{"catalogVersion":2,"providers":{"existing":{"api_key":"preserve-secret"}},"capabilities":{"custom":{"enabled":true}},"meta":{"keep":true}}`)
	if err := os.MkdirAll(filepath.Dir(paths[0]), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths[0], original, 0600); err != nil {
		t.Fatal(err)
	}
	return c, paths, journalPath, original
}

func TestHanaConfigurationLifecycle(t *testing.T) {
	c, paths, journalPath, original := hanaFixture(t)
	pr, err := c.previewWithAPIKey("hana-agent", "deployment-id", "sk-newnoa-fixture-secret", "friendly/model")
	if err != nil {
		t.Fatal(err)
	}
	if len(pr.Paths) != 3 {
		t.Fatalf("paths: %#v", pr.Paths)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	if err = c.canChangeProxyPort(18888); err == nil {
		t.Fatal("allowed changing a managed proxy address")
	}
	if err = c.canChangeProxyPort(18222); err != nil {
		t.Fatal(err)
	}
	var catalog map[string]json.RawMessage
	raw, _ := os.ReadFile(paths[0])
	if err = json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(catalog["providers"]), managedAgentKey("sk-newnoa-fixture-secret")) || !strings.Contains(string(catalog["providers"]), "preserve-secret") {
		t.Fatalf("provider merge: %s", raw)
	}
	plugin, _ := os.ReadFile(paths[2])
	for _, expected := range []string{`"id": "newnoa"`, `"defaultApi": "openai-completions"`, `"defaultBaseUrl": "http://127.0.0.1:18222/v1"`, `"id": "friendly/model"`} {
		if !strings.Contains(string(plugin), expected) {
			t.Fatalf("plugin lacks %s: %s", expected, plugin)
		}
	}
	if strings.Contains(string(plugin), "sk-newnoa") {
		t.Fatal("credentials leaked into plugin")
	}
	for _, file := range append(paths, journalPath) {
		info, err := os.Stat(file)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("permissions: %s %v", file, err)
		}
	}
	status, err := c.detectAgent("hana-agent")
	if err != nil || status["configurationState"] != "applied" || status["model"] != "deployment-id" {
		t.Fatalf("status: %v %v", status, err)
	}
	encoded, _ := json.Marshal(status)
	if strings.Contains(string(encoded), "secret") {
		t.Fatal("credentials leaked into status")
	}
	if _, err = c.previewWithAPIKey("hana-agent", "other", "sk-newnoa-fixture-secret"); err == nil {
		t.Fatal("allowed replacing managed files without restoring")
	}
	restarted := newCore(c.home, "http://gateway.invalid")
	if route := restarted.modelAliases["hana-agent\nfriendly/model"]; route.Model != "deployment-id" || !route.Keys[hash([]byte("sk-newnoa-fixture-secret"))] {
		t.Fatalf("alias restart: %#v", route)
	}
	if err = c.restoreAgent("hana-agent"); err != nil {
		t.Fatal(err)
	}
	restored, _ := os.ReadFile(paths[0])
	if string(restored) != string(original) {
		t.Fatalf("original lost: %s", restored)
	}
	for _, file := range []string{paths[1], paths[2], journalPath} {
		if _, err = os.Stat(file); !os.IsNotExist(err) {
			t.Fatalf("managed file remains: %s %v", file, err)
		}
	}
	if _, err = c.previewWithAPIKey("hana-agent", "other", "sk-newnoa-fixture-secret"); err != nil {
		t.Fatal(err)
	}
}

func TestHanaRestorePreflightsEveryFile(t *testing.T) {
	c, paths, journalPath, _ := hanaFixture(t)
	pr, err := c.previewWithAPIKey("hana-agent", "deployment-id", "sk-newnoa-fixture-secret")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	installed, _ := os.ReadFile(paths[0])
	if err = os.WriteFile(paths[2], []byte(`{"outside":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("hana-agent"); err == nil {
		t.Fatal("overwrote external changes")
	}
	after, _ := os.ReadFile(paths[0])
	if string(after) != string(installed) {
		t.Fatal("partial restore despite conflict")
	}
	if _, err = os.Stat(journalPath); err != nil {
		t.Fatal("lost recovery journal")
	}
	status, err := c.detectAgent("hana-agent")
	if err != nil || status["configurationState"] != "conflict" {
		t.Fatalf("status: %v %v", status, err)
	}
}

func TestHanaRejectsUnsafeOrUnsupportedConfiguration(t *testing.T) {
	for _, kind := range []string{"custom-home", "missing", "version", "existing-provider", "existing-plugin", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			c, paths, _, _ := hanaFixture(t)
			switch kind {
			case "custom-home":
				t.Setenv("HANA_HOME", filepath.Join(c.home, "other"))
			case "missing":
				os.Remove(paths[0])
			case "version":
				os.WriteFile(paths[0], []byte(`{"catalogVersion":1,"providers":{}}`), 0600)
			case "existing-provider":
				os.WriteFile(paths[0], []byte(`{"catalogVersion":2,"providers":{"newnoa":{}}}`), 0600)
			case "existing-plugin":
				os.MkdirAll(filepath.Dir(paths[1]), 0700)
				os.WriteFile(paths[1], []byte(`{}`), 0600)
			case "symlink":
				os.Remove(paths[0])
				os.Symlink(filepath.Join(c.home, "outside"), paths[0])
			}
			if _, err := c.previewWithAPIKey("hana-agent", "deployment-id", "sk-newnoa-fixture-secret"); err == nil {
				t.Fatal("accepted unsafe/unsupported state")
			}
		})
	}
}

func TestHanaPreviewDetectsChangesAndInterruptedRestore(t *testing.T) {
	c, paths, journalPath, original := hanaFixture(t)
	pr, err := c.previewWithAPIKey("hana-agent", "deployment-id", "sk-newnoa-fixture-secret")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(paths[0], append(original, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err == nil {
		t.Fatal("accepted changed source")
	}
	if err = os.WriteFile(paths[0], original, 0600); err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(journalPath)
	var j hanaJournal
	if err = json.Unmarshal(data, &j); err != nil {
		t.Fatal(err)
	}
	j.Stage = "prepared"
	data, _ = json.Marshal(j)
	if err = atomicFile(journalPath, data); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(paths[2]); err != nil {
		t.Fatal(err)
	}
	status, err := c.detectAgent("hana-agent")
	if err != nil || status["configurationState"] != "recovery_required" {
		t.Fatalf("status: %v %v", status, err)
	}
	if err = c.restoreAgent("hana-agent"); err != nil {
		t.Fatal(err)
	}
	restored, _ := os.ReadFile(paths[0])
	if string(restored) != string(original) {
		t.Fatal("original lost")
	}
}

func TestHanaPreviewDoesNotReturnCredentials(t *testing.T) {
	c, _, _, _ := hanaFixture(t)
	value, err := c.handle("agent.preview", json.RawMessage(`{"agent":"hana-agent","model":"deployment-id","apiKey":"sk-newnoa-fixture-secret"}`))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "sk-newnoa") || strings.Contains(string(raw), "preserve-secret") {
		t.Fatal("preview exposed credentials")
	}
}

func TestHanaPreviewRejectsOversizedResult(t *testing.T) {
	c, paths, _, _ := hanaFixture(t)
	data, _ := json.Marshal(map[string]any{"catalogVersion": 2, "providers": map[string]any{}, "padding": strings.Repeat("x", maxFrame-90)})
	if len(data) > maxFrame {
		t.Fatal("oversized test input")
	}
	if err := os.WriteFile(paths[0], data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.previewWithAPIKey("hana-agent", "deployment-id", "sk-newnoa-fixture-secret"); err == nil {
		t.Fatal("accepted oversized generated configuration")
	}
}

func TestHanaManagedProfiles(t *testing.T) {
	c, paths, journalPath, original := hanaFixture(t)
	const owner, firstID, secondID = "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003"
	const first, second = "nn_test_hana_first_123456", "nn_test_hana_second_123456"
	change := func(id, secret, action string, models ...providerModel) {
		t.Helper()
		pr, err := c.previewManagedProvider("hana-agent", owner, id, secret, action, models)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.handle("agent.apply", json.RawMessage(`{"digest":"`+pr.Digest+`"}`)); err != nil {
			t.Fatal(err)
		}
	}
	verify := func(want map[string][]string) {
		t.Helper()
		type profile struct {
			URL    string
			API    string
			Models []string
		}
		got := map[string]profile{}
		// Optional real openHanako checkout. Both readers run entirely against the fixture home.
		if upstream := os.Getenv("NEWNOA_TEST_HANA_SOURCE"); upstream != "" {
			cmd := exec.Command("node", "--input-type=module", "-e", `
				import { LocalProviderPluginStore } from './core/local-provider-plugin-store.ts';
				import { ProviderCatalogStore } from './core/provider-catalog.ts';
				const plugins = new LocalProviderPluginStore(process.env.HANA_HOME).readAll();
				const providers = new ProviderCatalogStore(process.env.HANA_HOME).getProviders();
				const rows = Object.fromEntries(plugins.map(p => [providers[p.id].api_key,
					{URL:p.defaultBaseUrl, API:p.defaultApi, Models:p.models.map(m=>m.id)}]));
				console.log(JSON.stringify(rows));`)
			cmd.Dir = upstream
			cmd.Env = append(os.Environ(), "HOME="+c.home, "HANA_HOME="+filepath.Join(c.home, ".hanako"))
			raw, err := cmd.Output()
			if err != nil || json.Unmarshal(raw, &got) != nil {
				t.Fatalf("openHanako configuration readers: %v", err)
			}
		} else {
			raw, err := os.ReadFile(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			_, providers, err := hanaCatalog(raw)
			if err != nil {
				t.Fatal(err)
			}
			for native, entry := range providers {
				if !strings.HasPrefix(native, "newnoa") {
					continue
				}
				files, _ := c.hanaPaths(native)
				plugin, err := os.ReadFile(files[2])
				if err != nil {
					t.Fatal(err)
				}
				var config struct {
					DefaultBaseURL, DefaultAPI string
					Models                     []struct{ ID string }
				}
				if json.Unmarshal(plugin, &config) != nil {
					t.Fatal("invalid plugin")
				}
				p := profile{URL: config.DefaultBaseURL, API: config.DefaultAPI}
				for _, m := range config.Models {
					p.Models = append(p.Models, m.ID)
				}
				got[providerConfigSecret("hana-agent", entry)] = p
			}
		}
		if len(got) != len(want) {
			t.Fatalf("number of visible profiles %d, want %d", len(got), len(want))
		}
		for key, aliases := range want {
			p := got[managedAgentKey(key)]
			if p.URL != c.proxyBaseURL()+"/v1" || p.API != "openai-completions" || !slices.Equal(p.Models, aliases) {
				t.Fatal("profile credentials, URL, API or models differ")
			}
		}
	}
	change(firstID, first, "add", providerModel{"00000000-0000-4000-8000-000000000004", "group/one"}, providerModel{"00000000-0000-4000-8000-000000000005", "two"})
	change(secondID, second, "add", providerModel{"00000000-0000-4000-8000-000000000006", "three"})
	verify(map[string][]string{first: {"group/one", "two"}, second: {"three"}})
	if _, err := c.previewManagedProvider("hana-agent", "00000000-0000-4000-8000-000000000007", firstID, "", "remove", nil); err == nil {
		t.Fatal("removed another account's profile")
	}
	c = newCore(c.home, c.gateway)
	if err := c.restore(); err != nil {
		t.Fatal(err)
	}
	if r := c.modelAliases["hana-agent\ngroup/one"]; r.Model != "00000000-0000-4000-8000-000000000004" || !r.Keys[hash([]byte(first))] {
		t.Fatal("profile route lost on restart")
	}
	change(firstID, "", "remove-model", providerModel{ID: "00000000-0000-4000-8000-000000000004"})
	verify(map[string][]string{first: {"two"}, second: {"three"}})
	change(firstID, "", "remove")
	verify(map[string][]string{second: {"three"}})
	if _, ok := c.modelAliases["hana-agent\ntwo"]; ok {
		t.Fatal("removed route retained")
	}
	change(secondID, "", "remove")
	verify(map[string][]string{})
	raw, err := os.ReadFile(paths[0])
	if err != nil || string(raw) != string(original) {
		t.Fatal("original catalog not restored")
	}
	if _, err = os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatal("journal remains")
	}
}

func TestHanaAdoptsOriginalKeyAndDetectsPluginConflicts(t *testing.T) {
	c, paths, _, _ := hanaFixture(t)
	const secret = "nn_test_hana_legacy_123456"
	pr, err := c.previewWithAPIKey("hana-agent", "existing", secret, "old/alias")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	models := []providerModel{{"00000000-0000-4000-8000-000000000008", "new"}}
	if _, err = c.previewManagedProvider("hana-agent", "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", "nn_other_key_1234567", "add", models); err == nil {
		t.Fatal("adopted unrelated key")
	}
	pr, err = c.previewManagedProvider("hana-agent", "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", secret, "add", models)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	j, err := c.readHanaJournal()
	if err != nil || len(j.ProviderCatalog.Providers) != 1 || len(j.ProviderCatalog.Providers[0].Models) != 2 {
		t.Fatal("legacy models lost")
	}
	pr, err = c.previewManagedProvider("hana-agent", "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000003", "nn_other_key_1234567", "add", models)
	if err != nil {
		t.Fatal(err)
	}
	if err = atomicFile(paths[2], []byte(`{"outside":true}`)); err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err == nil {
		t.Fatal("overwrote external edit")
	}
	if err = c.restoreAgent("hana-agent"); err == nil {
		t.Fatal("restored over external edit")
	}
}
