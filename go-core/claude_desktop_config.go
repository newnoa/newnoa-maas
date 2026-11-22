package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

const desktopProfileID = "6e65776e-6f61-4000-8000-000000000001"

func desktopPathsFor(home, platform string) ([]string, error) {
	var root string
	switch platform {
	case "linux":
		root = filepath.Join(home, ".config")
	case "darwin":
		root = filepath.Join(home, "Library", "Application Support")
	case "windows":
		root = filepath.Join(home, "AppData", "Local")
	default:
		return nil, errors.New("Claude Desktop configuration is unsupported on this platform")
	}
	library := filepath.Join(root, "Claude-3p", "configLibrary")
	return []string{filepath.Join(root, "Claude", "claude_desktop_config.json"), filepath.Join(root, "Claude-3p", "claude_desktop_config.json"), filepath.Join(library, desktopProfileID+".json"), filepath.Join(library, "_meta.json")}, nil
}

func (c *core) desktopPaths() ([]string, error) { return desktopPathsFor(c.home, runtime.GOOS) }

func (c *core) checkDesktopLocation() error {
	// Keep recovery anchored to the same default paths when another process changes its environment.
	if runtime.GOOS == "linux" {
		if custom := os.Getenv("XDG_CONFIG_HOME"); custom != "" && filepath.IsAbs(custom) && filepath.Clean(custom) != filepath.Join(c.home, ".config") {
			return errors.New("custom Claude Desktop configuration paths require manual configuration")
		}
	}
	if runtime.GOOS == "windows" {
		if custom := os.Getenv("LOCALAPPDATA"); custom != "" && filepath.Clean(custom) != filepath.Join(c.home, "AppData", "Local") {
			return errors.New("custom Claude Desktop configuration paths require manual configuration")
		}
	}
	return nil
}

func jsonConfig(file managedFile) (map[string]json.RawMessage, error) {
	doc := map[string]json.RawMessage{}
	if file.Existed && (json.Unmarshal(file.Original, &doc) != nil || doc == nil) {
		return nil, errors.New("existing Claude Desktop configuration must be a JSON object; no changes made")
	}
	return doc, nil
}

func (c *core) previewClaudeDesktop(model, apiKey, alias string) (preview, error) {
	if err := c.checkDesktopLocation(); err != nil {
		return preview{}, err
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`).MatchString(model) {
		return preview{}, errors.New("invalid model identifier")
	}
	if !regexp.MustCompile(`^(anthropic/)?claude-(sonnet|opus|haiku)-[a-zA-Z0-9_.-]+$`).MatchString(alias) {
		return preview{}, errors.New("Claude Desktop model mapping must use a claude-sonnet-, claude-opus-, or claude-haiku- model name")
	}
	paths, err := c.desktopPaths()
	if err != nil {
		return preview{}, err
	}
	files := make([]managedFile, len(paths))
	docs := make([]map[string]json.RawMessage, len(paths))
	for i, path := range paths {
		files[i], err = c.readManagedFile(path)
		if err != nil {
			return preview{}, err
		}
		docs[i], err = jsonConfig(files[i])
		if err != nil {
			return preview{}, err
		}
	}
	if _, err = c.managedConnectionJournal("claude-desktop", files[2].Original, files[2].Existed); err != nil {
		return preview{}, err
	}
	for i := 0; i < 2; i++ {
		docs[i]["deploymentMode"] = json.RawMessage(`"3p"`)
	}
	for key, value := range map[string]any{"inferenceProvider": "gateway", "inferenceGatewayBaseUrl": c.proxyBaseURL(), "inferenceGatewayAuthScheme": "bearer", "inferenceGatewayApiKey": apiKey, "disableDeploymentModeChooser": true, "inferenceModels": []string{alias}} {
		docs[2][key], _ = json.Marshal(value)
	}
	entries := []map[string]json.RawMessage{}
	if raw, ok := docs[3]["entries"]; ok {
		if json.Unmarshal(raw, &entries) != nil {
			return preview{}, errors.New("Claude Desktop profile entries must be an array")
		}
	}
	found := false
	for _, entry := range entries {
		if entry == nil {
			return preview{}, errors.New("invalid Claude Desktop profile entry")
		}
		if string(entry["id"]) == `"`+desktopProfileID+`"` {
			entry["name"] = json.RawMessage(`"NEWNOA"`)
			found = true
		}
	}
	if !found {
		entries = append(entries, map[string]json.RawMessage{"id": json.RawMessage(`"` + desktopProfileID + `"`), "name": json.RawMessage(`"NEWNOA"`)})
	}
	docs[3]["entries"], _ = json.Marshal(entries)
	docs[3]["appliedId"], _ = json.Marshal(desktopProfileID)
	for i := range files {
		files[i].Content, err = json.MarshalIndent(docs[i], "", "  ")
		if err != nil {
			return preview{}, err
		}
		files[i].Content = append(files[i].Content, '\n')
		if len(files[i].Content) > maxFrame {
			return preview{}, errors.New("configuration too large")
		}
		files[i].Installed = hash(files[i].Content)
	}
	pr := preview{Model: model, Alias: alias, Agent: "claude-desktop", Digest: randomToken(), SourceDigest: hash(files[2].Original), Content: string(files[2].Content), Path: paths[2], Paths: paths, Existed: files[2].Existed, apiKey: apiKey, companions: []managedFile{files[0], files[1], files[3]}}
	if len(pr.Content)+len(strings.Join(paths, "")) > maxFrame {
		return preview{}, errors.New("configuration too large")
	}
	c.pending = &pr
	return pr, nil
}
