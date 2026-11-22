package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strings"
)

// The sidecar contains digests only. A crash between any two renames can be
// resumed without keeping an extra disk copy of the original account keys.
type keyFileMigration struct {
	Files   map[string]string `json:"files"` // installed digest -> sanitized digest
	Journal string            `json:"journal"`
	Final   string            `json:"final"`
}

// Only literal whole tokens belonging to the managed configuration are migrated.
// Matching a complete token prevents recognizing the public prefix inside a reference.
var nativeConfigToken = regexp.MustCompile(`[a-zA-Z0-9_-]+`)

func redactKnownKeys(raw []byte, known map[string]bool) []byte {
	return nativeConfigToken.ReplaceAllFunc(raw, func(key []byte) []byte {
		value := string(key)
		if (!strings.HasPrefix(value, "sk-") && !strings.HasPrefix(value, "nn_")) || !known[hash(key)] {
			return key
		}
		return []byte(managedAgentKey(value))
	})
}
func (c *core) migrateAgentFileKeys(agent string) error {
	cfg, jp, err := c.agentPaths(agent)
	if err != nil {
		return err
	}
	if err = c.safeConfigFile(jp); err != nil {
		return err
	}
	raw, err := os.ReadFile(jp)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var meta struct {
		Stage       string
		Service     bool
		IDs         []string
		APIKeyHash  string
		KeyHashes   map[string]string
		Connections []agentConnection
		Original    []byte
		Installed   string
		Companions  []managedFile
		Files       []piFile
	}
	if err = json.Unmarshal(raw, &meta); err != nil {
		return err
	}
	if meta.Stage != "applied" {
		return errors.New("Finish the pending native configuration change before using remote access")
	}
	if meta.Service {
		if agent != "hana-agent" {
			return errors.New("Invalid managed service journal")
		}
		return c.migrateHanaServiceKeys()
	}
	known := map[string]bool{meta.APIKeyHash: true}
	for _, v := range meta.KeyHashes {
		known[v] = true
	}
	for _, v := range meta.Connections {
		known[v.APIKeyHash] = true
	}
	var paths, installed []string
	originals := [][]byte{meta.Original}
	if agent == "pi-agent" || agent == "hana-agent" {
		if agent == "pi-agent" {
			paths, _ = c.piPaths()
		} else {
			for _, id := range meta.IDs {
				if id != "newnoa" && !hanaNativeID.MatchString(id) {
					return errors.New("Invalid managed Hana provider identifier")
				}
			}
			paths, _ = c.hanaPaths(meta.IDs...)
		}
		if len(paths) != len(meta.Files) {
			return errors.New("Invalid managed native configuration file count")
		}
		for _, f := range meta.Files {
			installed = append(installed, f.Installed)
			originals = append(originals, f.Original)
		}
	} else {
		if err = c.validateCompanionPaths(agent, meta.Companions); err != nil {
			return err
		}
		paths, installed = []string{cfg}, []string{meta.Installed}
		for _, f := range meta.Companions {
			paths = append(paths, f.Path)
			installed = append(installed, f.Installed)
			originals = append(originals, f.Original)
		}
	}
	for _, original := range originals {
		if !bytes.Equal(original, redactKnownKeys(original, known)) {
			return errors.New("A pre-existing native configuration backup contains a selected account key; rotate the original credential before remote access")
		}
	}
	marker := jp + ".key-reference-migration"
	if err = c.safeConfigFile(marker); err != nil {
		return err
	}
	var state keyFileMigration
	saved, err := os.ReadFile(marker)
	resuming := err == nil
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if resuming {
		if json.Unmarshal(saved, &state) != nil || state.Files == nil {
			return errors.New("Invalid native credential migration state")
		}
	} else {
		state = keyFileMigration{Files: map[string]string{}, Journal: hash(raw)}
	}
	if resuming && state.Journal != hash(raw) {
		if state.Final == "" || state.Final != hash(raw) {
			return errors.New("Native configuration journal changed during migration")
		}
		for _, digest := range installed {
			if _, old := state.Files[digest]; old {
				return errors.New("Managed native journal changed during credential migration")
			}
		}
		// The new journal was committed just before a crash.
		return os.Remove(marker)
	}
	var next = make([][]byte, len(paths))
	for i, path := range paths {
		if err = c.safeConfigFile(path); err != nil {
			return err
		}
		current, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		observed := hash(current)
		if observed != installed[i] {
			if resuming && state.Files[installed[i]] == observed {
				continue
			}
			// Preserve unrelated user edits. Refuse remote access if any managed secret remains.
			if !resuming && bytes.Equal(current, redactKnownKeys(current, known)) {
				continue
			}
			return errors.New("Native configuration changed outside NEWNOA; review its credentials before remote access")
		}
		clean := redactKnownKeys(current, known)
		if bytes.Equal(current, clean) {
			continue
		}
		next[i] = clean
		digest := hash(clean)
		if resuming && state.Files[observed] != digest {
			return errors.New("Native credential migration digest changed")
		}
		state.Files[observed] = digest
	}
	if len(state.Files) == 0 {
		return nil
	}
	updated := raw
	for old, newDigest := range state.Files {
		updated = bytes.ReplaceAll(updated, []byte(`"`+old+`"`), []byte(`"`+newDigest+`"`))
	}
	if !resuming {
		state.Final = hash(updated)
		encoded, e := json.Marshal(state)
		if e != nil {
			return e
		}
		if e = atomicFile(marker, encoded); e != nil {
			return e
		}
	} else if state.Final != hash(updated) {
		return errors.New("Native migration journal digest mismatch")
	}
	for i, path := range paths {
		if next[i] != nil {
			if err = atomicFile(path, next[i]); err != nil {
				return err
			}
		}
	}
	if err = atomicFile(jp, updated); err != nil {
		return err
	}
	return os.Remove(marker)
}

func (c *core) migrateAgentKeys() error {
	for _, agent := range []string{"codex", "claude-cli", "pi-agent", "openclaw", "hermes-agent", "opencode", "hana-agent", "claude-desktop"} {
		if err := c.migrateAgentFileKeys(agent); err != nil {
			return errors.New(agent + ": " + err.Error())
		}
	}
	return nil
}
