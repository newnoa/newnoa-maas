package main

import (
	"encoding/json"
	"errors"
	"os"
)

func (c *core) detectAgent(agent string) (map[string]any, error) {
	if agent == "hana-agent" {
		result, err := c.detectHana()
		if result != nil {
			result["operation"], result["capabilities"] = agentOperation(agent)
		}
		return result, err
	}
	var locationErr error
	switch agent {
	case "", "codex":
		locationErr = c.checkCodexLocation()
	case "opencode":
		locationErr = c.checkOpenCodeLocation()
	case "openclaw":
		locationErr = c.checkOpenClawLocation()
	case "hermes-agent":
		locationErr = c.checkHermesLocation()
	case "claude-desktop":
		locationErr = c.checkDesktopLocation()
	}
	if locationErr != nil {
		return nil, locationErr
	}
	if agent == "claude-cli" {
		if err := c.checkClaudeLocation(); err != nil {
			return nil, err
		}
	}
	if agent == "pi-agent" {
		result, err := c.detectPi()
		if result != nil {
			result["operation"], result["capabilities"] = agentOperation(agent)
		}
		return result, err
	}
	path, journalPath, err := c.agentPaths(agent)
	if err != nil {
		return nil, err
	}
	for _, file := range []string{path, journalPath} {
		if err = c.safeConfigFile(file); err != nil {
			return nil, err
		}
	}
	raw, err := os.ReadFile(path)
	exists := err == nil
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if agent == "" {
		agent = "codex"
	}
	protocol := "responses"
	if agent == "opencode" || agent == "openclaw" || agent == "hermes-agent" {
		protocol = "chat/completions"
	}
	if agent == "claude-cli" || agent == "claude-desktop" {
		protocol = "messages"
	}
	result := map[string]any{"agent": agent, "exists": exists, "path": path, "supportedProtocol": protocol, "experimental": true, "configurationState": "not_applied", "proxyRunning": c.server != nil}
	result["operation"], result["capabilities"] = agentOperation(agent)
	data, err := os.ReadFile(journalPath)
	if os.IsNotExist(err) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	var j journal
	if json.Unmarshal(data, &j) != nil || (j.Stage != "prepared" && j.Stage != "applied") || len(j.Installed) != 64 {
		return nil, errors.New("invalid recovery journal")
	}
	if err := c.validateCompanionPaths(agent, j.Companions); err != nil {
		return nil, err
	}
	result["configurationState"] = "conflict"
	if exists && hash(raw) == j.Installed {
		if err := c.checkInstalledManagedFiles(j.Companions); err != nil {
			return result, nil
		}
		result["configurationState"] = "applied"
		if j.Configuration != nil {
			result["configuration"] = j.Configuration
		}
		if j.Stage == "prepared" {
			result["configurationState"] = "recovery_required"
		} else if j.Model != "" {
			result["model"] = j.Model
			models := []string{}
			seen := map[string]bool{}
			for _, entry := range journalConnections(j) {
				if !seen[entry.Model] {
					models = append(models, entry.Model)
					seen[entry.Model] = true
				}
			}
			result["configuredModels"] = models
		}
	} else if j.Stage == "prepared" && ((exists == j.Existed && hash(raw) == hash(j.Original)) || (exists && j.PreviousInstalled != "" && hash(raw) == j.PreviousInstalled)) {
		result["configurationState"] = "recovery_required"
	}
	return result, nil
}
