package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyManagedCredentialMigratesAndRestoresPriorConfig(t *testing.T) {
	home := t.TempDir()
	c := newCore(home, "http://127.0.0.1:8091")
	defer c.shutdown()
	original := []byte("{\"unrelated\":true}\n")
	cfg, jp, _ := c.agentPaths("claude-cli")
	if err := os.MkdirAll(filepath.Dir(cfg), 0700); err != nil {
		t.Fatal(err)
	}
	if err := atomicFile(cfg, original); err != nil {
		t.Fatal(err)
	}
	parent := "sk-legacy_fixture_key_012345678901234"
	p, err := c.previewWithAPIKey("claude-cli", "some-model", parent)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	// Construct the previously deployed format, including its recorded digest.
	current, _ := os.ReadFile(cfg)
	legacy := bytes.ReplaceAll(current, []byte(managedAgentKey(parent)), []byte(parent))
	data, _ := os.ReadFile(jp)
	var j journal
	if err = json.Unmarshal(data, &j); err != nil {
		t.Fatal(err)
	}
	j.Installed = hash(legacy)
	data, _ = json.Marshal(j)
	if err = atomicFile(cfg, legacy); err != nil {
		t.Fatal(err)
	}
	if err = atomicFile(jp, data); err != nil {
		t.Fatal(err)
	}
	if err = c.migrateAgentFileKeys("claude-cli"); err != nil {
		t.Fatal(err)
	}
	if err = c.migrateAgentFileKeys("claude-cli"); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(cfg)
	if !bytes.Equal(got, current) || bytes.Contains(got, []byte(parent)) {
		t.Fatal("legacy key not converted")
	}
	if err = c.restoreAgent("claude-cli"); err != nil {
		t.Fatal(err)
	}
	got, _ = os.ReadFile(cfg)
	if !bytes.Equal(got, original) {
		t.Fatal("unrelated settings were not restored")
	}
}

func TestNativeKeyMigrationRejectsInvalidStateWithoutChangingFiles(t *testing.T) {
	const key = "sk-unmigrated_fixture_012345678901"
	original := []byte(`{"unrelated":true}`)
	legacy := []byte(`{"api_key":"` + key + `"}`)
	clean := redactKnownKeys(legacy, map[string]bool{hash([]byte(key)): true})
	for _, tc := range []struct {
		name, want string
		config     []byte
		change     func(*journal)
		marker     func(*keyFileMigration)
	}{
		{name: "pending change", want: "pending native configuration", change: func(j *journal) { j.Stage = "prepared" }},
		{name: "unexpected service journal", want: "Invalid managed service journal"},
		{name: "backup contains selected key", want: "rotate the original credential", change: func(j *journal) { j.Original = legacy }},
		{name: "manual edit with live key", want: "changed outside NEWNOA", config: append(bytes.Clone(legacy), '\n')},
		{name: "manual edit without live key", config: original},
		{name: "invalid marker", want: "Invalid native credential migration state", marker: func(m *keyFileMigration) { m.Files = nil }},
		{name: "changed journal", want: "journal changed during migration", marker: func(m *keyFileMigration) { m.Journal = "other journal" }},
		{name: "changed replacement digest", want: "migration digest changed", marker: func(m *keyFileMigration) { m.Files[hash(legacy)] = "wrong digest" }},
		{name: "changed final digest", want: "journal digest mismatch", marker: func(m *keyFileMigration) { m.Final = "wrong journal" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &core{home: t.TempDir()}
			cfg, journalPath, err := c.agentPaths("claude-cli")
			if err != nil {
				t.Fatal(err)
			}
			meta := journal{Stage: "applied", APIKeyHash: hash([]byte(key)), Original: original, Installed: hash(legacy)}
			if tc.change != nil {
				tc.change(&meta)
			}
			journalBytes, err := json.Marshal(meta)
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "unexpected service journal" {
				journalBytes = append(journalBytes[:len(journalBytes)-1], []byte(`,"Service":true}`)...)
			}
			config := tc.config
			if config == nil {
				config = legacy
			}
			for path, contents := range map[string][]byte{cfg: config, journalPath: journalBytes} {
				if err := atomicFile(path, contents); err != nil {
					t.Fatal(err)
				}
			}
			if tc.marker != nil {
				marker := keyFileMigration{Files: map[string]string{hash(legacy): hash(clean)}, Journal: hash(journalBytes)}
				tc.marker(&marker)
				markerBytes, err := json.Marshal(marker)
				if err != nil {
					t.Fatal(err)
				}
				if err := atomicFile(journalPath+".key-reference-migration", markerBytes); err != nil {
					t.Fatal(err)
				}
			}
			err = c.migrateAgentFileKeys("claude-cli")
			if (tc.want == "" && err != nil) || (tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want))) {
				t.Fatalf("migration error = %v, expected %q", err, tc.want)
			}
			for path, want := range map[string][]byte{cfg: config, journalPath: journalBytes} {
				if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("migration changed %s: %v", filepath.Base(path), err)
				}
			}
		})
	}
}

func TestNativeKeyMigrationRemovesMarkerAfterCommittedJournal(t *testing.T) {
	c := &core{home: t.TempDir()}
	_, path, err := c.agentPaths("claude-cli")
	if err != nil {
		t.Fatal(err)
	}
	current := []byte(`{"Stage":"applied","Installed":"new-digest"}`)
	if err := atomicFile(path, current); err != nil {
		t.Fatal(err)
	}
	marker := path + ".key-reference-migration"
	body, err := json.Marshal(keyFileMigration{Files: map[string]string{"old-digest": "new-digest"}, Journal: "old-journal", Final: hash(current)})
	if err != nil {
		t.Fatal(err)
	}
	if err := atomicFile(marker, body); err != nil {
		t.Fatal(err)
	}
	if err := c.migrateAgentFileKeys("claude-cli"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("committed migration marker remains: %v", err)
	}
}

func TestNativeKeyMigrationRequiresSafeAndCompleteJournal(t *testing.T) {
	for _, tc := range []struct {
		name, agent, raw, target, want string
	}{
		{"invalid agent", "not-managed", "", "", "unsupported"},
		{"directory journal", "claude-cli", "", "journal", "non-regular"},
		{"corrupt journal", "claude-cli", "{", "journal", "unexpected end"},
		{"unsafe marker", "claude-cli", `{"Stage":"applied"}`, "marker", "non-regular"},
		{"missing config", "claude-cli", `{"Stage":"applied"}`, "", ""},
		{"unsafe config", "claude-cli", `{"Stage":"applied"}`, "config", "non-regular"},
		{"malformed Hana ID", "hana-agent", `{"Stage":"applied","IDs":["../foreign"]}`, "", "Invalid managed Hana provider identifier"},
		{"incomplete Pi journal", "pi-agent", `{"Stage":"applied"}`, "", "Invalid managed native configuration file count"},
		{"incomplete Hana journal", "hana-agent", `{"Stage":"applied","IDs":["newnoa"]}`, "", "Invalid managed native configuration file count"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &core{home: t.TempDir()}
			config, journalPath, _ := c.agentPaths(tc.agent)
			if tc.raw != "" {
				if err := atomicFile(journalPath, []byte(tc.raw)); err != nil {
					t.Fatal(err)
				}
			}
			invalid := map[string]string{"journal": journalPath, "config": config, "marker": journalPath + ".key-reference-migration"}[tc.target]
			if invalid != "" && tc.name != "corrupt journal" {
				if err := os.MkdirAll(invalid, 0700); err != nil {
					t.Fatal(err)
				}
			}
			err := c.migrateAgentFileKeys(tc.agent)
			if tc.want == "" {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("expected missing configuration error, got %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}

func TestNativeFileMigrationRecoversAfterReplacement(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	defer c.shutdown()
	cfg, jp, _ := c.agentPaths("claude-cli")
	parent := "sk-fixture_interrupted_migration_0123456789"
	original := []byte("{}")
	if err := os.MkdirAll(filepath.Dir(cfg), 0700); err != nil {
		t.Fatal(err)
	}
	if err := atomicFile(cfg, original); err != nil {
		t.Fatal(err)
	}
	p, err := c.previewWithAPIKey("claude-cli", "test", parent)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	clean, _ := os.ReadFile(cfg)
	legacy := bytes.ReplaceAll(clean, []byte(managedAgentKey(parent)), []byte(parent))
	installedJournal, _ := os.ReadFile(jp)
	oldJournal := bytes.ReplaceAll(installedJournal, []byte(hash(clean)), []byte(hash(legacy)))
	if err = atomicFile(jp, oldJournal); err != nil {
		t.Fatal(err)
	}
	marker, _ := json.Marshal(keyFileMigration{Files: map[string]string{hash(legacy): hash(clean)}, Journal: hash(oldJournal), Final: hash(installedJournal)})
	if err = atomicFile(jp+".key-reference-migration", marker); err != nil {
		t.Fatal(err)
	}
	if err = c.migrateAgentFileKeys("claude-cli"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(jp)
	if err != nil || !bytes.Equal(got, installedJournal) {
		t.Fatal("journal did not converge")
	}
	if _, err = os.Stat(jp + ".key-reference-migration"); !os.IsNotExist(err) {
		t.Fatal("migration marker remained")
	}
}
