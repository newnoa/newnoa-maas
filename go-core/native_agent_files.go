package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type managedFile struct {
	Path              string `json:"path"`
	Original          []byte `json:"original"`
	Existed           bool   `json:"existed"`
	Installed         string `json:"installed"`
	PreviousInstalled string `json:"previousInstalled,omitempty"`
	Content           []byte `json:"-"`
}

func (c *core) validateCompanionPaths(agent string, files []managedFile) error {
	if len(files) == 0 {
		return nil
	} // Legacy one-file journals remain recoverable.
	expected := []string{}
	switch agent {
	case "", "codex":
		expected = []string{filepath.Join(c.home, ".codex", "auth.json")}
		if len(files) == 2 {
			expected = append(expected, filepath.Join(c.home, ".codex", "newnoa-models.json"))
		}
	case "claude-desktop":
		paths, err := c.desktopPaths()
		if err != nil {
			return err
		}
		expected = []string{paths[0], paths[1], paths[3]}
	default:
		return errors.New("invalid recovery journal")
	}
	if len(files) != len(expected) {
		return errors.New("invalid recovery journal")
	}
	for i, file := range files {
		if file.Path != expected[i] {
			return errors.New("invalid recovery journal")
		}
	}
	return nil
}

func (c *core) readManagedFile(path string) (managedFile, error) {
	if err := c.safeConfigFile(path); err != nil {
		return managedFile{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return managedFile{}, err
	}
	return managedFile{Path: path, Original: raw, Existed: err == nil}, nil
}

func (c *core) checkManagedFiles(files []managedFile, recovery bool) error {
	if len(files) > 3 {
		return errors.New("invalid recovery journal")
	}
	for _, file := range files {
		current, err := c.readManagedFile(file.Path)
		if err != nil {
			return err
		}
		limit := maxFrame
		if file.Path == filepath.Join(c.home, ".codex", "newnoa-models.json") {
			limit = codexCatalogMaxBytes
		}
		if len(file.Installed) != 64 || len(file.Original) > limit {
			return errors.New("invalid recovery journal")
		}
		original := current.Existed == file.Existed && hash(current.Original) == hash(file.Original)
		if original {
			continue
		}
		if recovery && current.Existed && (hash(current.Original) == file.Installed || (file.PreviousInstalled != "" && hash(current.Original) == file.PreviousInstalled)) {
			continue
		}
		return errors.New("configuration changed outside NEWNOA; preview again or review recovery")
	}
	return nil
}

func (c *core) checkInstalledManagedFiles(files []managedFile) error {
	for _, file := range files {
		current, err := c.readManagedFile(file.Path)
		if err != nil {
			return err
		}
		if !current.Existed || hash(current.Original) != file.Installed {
			return errors.New("configuration modified outside NEWNOA; recovery requires manual review")
		}
	}
	return nil
}

func (c *core) restoreManagedFiles(files []managedFile) error {
	for _, file := range files {
		current, err := c.readManagedFile(file.Path)
		if err != nil {
			return err
		}
		if current.Existed == file.Existed && hash(current.Original) == hash(file.Original) {
			continue
		}
		if file.Existed {
			err = atomicFile(file.Path, file.Original)
		} else {
			err = os.Remove(file.Path)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *core) prepareCodexAuth(apiKey string) (managedFile, error) {
	file, err := c.readManagedFile(filepath.Join(c.home, ".codex", "auth.json"))
	if err != nil {
		return file, err
	}
	auth := map[string]json.RawMessage{}
	if file.Existed && (json.Unmarshal(file.Original, &auth) != nil || auth == nil) {
		return file, errors.New("existing Codex auth must be a JSON object; no changes made")
	}
	// Explicit API-key mode prevents a saved OAuth/Bedrock login from taking precedence.
	auth["OPENAI_API_KEY"], _ = json.Marshal(apiKey)
	auth["auth_mode"] = json.RawMessage(`"apikey"`)
	file.Content, err = json.MarshalIndent(auth, "", "  ")
	file.Content = append(file.Content, '\n')
	if len(file.Content) > maxFrame {
		return file, errors.New("configuration too large")
	}
	file.Installed = hash(file.Content)
	return file, err
}

func validateAgentReasoning(agent, effort string) error {
	if effort == "" {
		return nil
	}
	if agent != "codex" {
		return errors.New("reasoning effort configuration is supported only for Codex")
	}
	switch effort {
	case "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra":
		return nil
	}
	return errors.New("invalid Codex reasoning effort")
}

func (c *core) withCodexReasoning(pr preview, effort string) (preview, error) {
	// Preserve the rest of the TOML verbatim, including comments and project settings.
	out := []string{}
	root := true
	assignment := regexp.MustCompile(`^\s*model_reasoning_effort\s*=\s*"([^"\\]|\\.)*"\s*(#.*)?$`)
	for _, line := range strings.Split(pr.Content, "\n") {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "[") {
			root = false
		}
		if root && regexp.MustCompile(`^model_reasoning_effort\s*=`).MatchString(trim) {
			if !assignment.MatchString(line) {
				c.pending = nil
				return preview{}, errors.New("complex root model settings require manual editing")
			}
			continue
		}
		out = append(out, line)
	}
	pr.Content = "model_reasoning_effort = " + `"` + effort + `"` + "\n" + strings.Join(out, "\n")
	if len(pr.Content) > maxFrame {
		c.pending = nil
		return preview{}, errors.New("configuration too large")
	}
	c.pending = &pr
	return pr, nil
}
