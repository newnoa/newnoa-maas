package main

import (
	"encoding/json"
	"errors"
	"os"
)

type agentCapabilities struct {
	ProviderList bool `json:"providerList,omitempty"`
	Replace      bool `json:"replace"`
	Add          bool `json:"add"`
	MultipleKeys bool `json:"multipleKeys"`
	Restore      bool `json:"restore"`
}

func agentOperation(agent string) (string, agentCapabilities) {
	switch agent {
	case "", "codex", "claude-cli", "claude-desktop":
		return "replace", agentCapabilities{Replace: true, Restore: true}
	case "opencode", "openclaw", "pi-agent", "hermes-agent", "hana-agent":
		return "add", agentCapabilities{Add: true, MultipleKeys: true, Restore: true, ProviderList: true}
	default:
		return "configure", agentCapabilities{Restore: true}
	}
}

func providerListAgent(agent string) bool {
	_, capabilities := agentOperation(agent)
	return capabilities.ProviderList
}

type agentConnection struct {
	Model      string `json:"model"`
	Alias      string `json:"alias"`
	APIKeyHash string `json:"apiKeyHash"`
}

func journalConnections(j journal) []agentConnection {
	if j.Configuration != nil {
		return j.Configuration.connections(j.APIKeyHash)
	}
	if len(j.Connections) > 0 {
		return j.Connections
	}
	if j.Model != "" && j.Alias != "" && len(j.APIKeyHash) == 64 {
		return []agentConnection{{j.Model, j.Alias, j.APIKeyHash}}
	}
	return nil
}

// Only a fully applied, unchanged managed file can extend its first recovery snapshot.
func (c *core) managedConnectionJournal(agent string, raw []byte, existed bool) (*journal, error) {
	_, path, err := c.agentPaths(agent)
	if err != nil {
		return nil, err
	}
	if err = c.safeConfigFile(path); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if agent != "codex" && agent != "hermes-agent" && agent != "claude-cli" && agent != "claude-desktop" && agent != "openclaw" && agent != "opencode" {
		return nil, errors.New("restore the previous change first")
	}
	var j journal
	if json.Unmarshal(data, &j) != nil || j.Stage != "applied" || len(j.Installed) != 64 {
		return nil, errors.New("restore the previous change first")
	}
	if !existed || hash(raw) != j.Installed {
		return nil, errors.New("configuration modified outside NEWNOA; recovery requires manual review")
	}
	if err := c.validateCompanionPaths(agent, j.Companions); err != nil {
		return nil, err
	}
	if err := c.checkInstalledManagedFiles(j.Companions); err != nil {
		return nil, err
	}
	if j.ProxyPort != c.proxyPortValue() {
		return nil, errors.New("restore all managed agent configurations before changing the proxy port")
	}
	return &j, nil
}

func managedModelEntries(previous json.RawMessage, alias string) (json.RawMessage, error) {
	var models []map[string]json.RawMessage
	if len(previous) > 0 && json.Unmarshal(previous, &models) != nil {
		return nil, errors.New("managed models must be an array")
	}
	entry := map[string]json.RawMessage{}
	entry["id"], _ = json.Marshal(alias)
	entry["name"], _ = json.Marshal(alias)
	entry["input"] = json.RawMessage(`["text"]`)
	for i, m := range models {
		var id string
		if json.Unmarshal(m["id"], &id) != nil || id == "" {
			return nil, errors.New("invalid managed model")
		}
		if id == alias {
			models[i] = entry
			return json.Marshal(models)
		}
	}
	models = append(models, entry)
	if len(models) > 64 {
		return nil, errors.New("managed catalog must contain at most 64 models")
	}
	return json.Marshal(models)
}
