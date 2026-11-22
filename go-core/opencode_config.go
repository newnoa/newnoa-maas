package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
)

func (c *core) checkOpenCodeLocation() error {
	if os.Getenv("OPENCODE_CONFIG") != "" || os.Getenv("OPENCODE_CONFIG_CONTENT") != "" || os.Getenv("OPENCODE_CONFIG_DIR") != "" || os.Getenv("XDG_CONFIG_HOME") != "" && filepath.Clean(os.Getenv("XDG_CONFIG_HOME")) != filepath.Join(c.home, ".config") {
		return errors.New("custom OpenCode configuration paths require manual configuration")
	}
	path, _, _ := c.agentPaths("opencode")
	if err := c.safeConfigFile(path); err != nil {
		return err
	}
	if _, err := os.Lstat(path + "c"); err == nil {
		return errors.New("existing OpenCode JSONC requires manual configuration")
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (c *core) previewOpenCode(model, apiKey string, aliases ...string) (preview, error) {
	alias := model
	if len(aliases) > 0 && aliases[0] != "" {
		alias = aliases[0]
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`).MatchString(model) {
		return preview{}, errors.New("invalid model identifier")
	}
	if err := c.checkOpenCodeLocation(); err != nil {
		return preview{}, err
	}
	path, _, _ := c.agentPaths("opencode")
	raw, err := os.ReadFile(path)
	existed := err == nil
	if err != nil && !os.IsNotExist(err) {
		return preview{}, err
	}
	managed, err := c.managedConnectionJournal("opencode", raw, existed)
	if err != nil {
		return preview{}, err
	}
	if managed != nil && managed.ProviderCatalog != nil {
		return preview{}, errors.New("Use the managed provider list to change this configuration")
	}
	settings, providers, err := openCodeProviders(raw, existed)
	if err != nil {
		return preview{}, err
	}
	keyHash := managedAgentKeyHash(apiKey)
	providerID := "newnoa"
	known := false
	if managed != nil {
		original := map[string]json.RawMessage{}
		if managed.Existed && json.Unmarshal(managed.Original, &original) != nil {
			return preview{}, errors.New("invalid OpenCode recovery snapshot")
		}
		var originalProviders map[string]json.RawMessage
		if value, ok := original["provider"]; ok && json.Unmarshal(value, &originalProviders) != nil {
			return preview{}, errors.New("invalid OpenCode recovery providers")
		}
		for id, value := range providers {
			if originalProviders[id] != nil {
				continue
			}
			var old struct{ Options map[string]string }
			if json.Unmarshal(value, &old) == nil && managedAgentKeyHash(old.Options["apiKey"]) == keyHash {
				if known {
					return preview{}, errors.New("duplicate managed OpenCode providers")
				}
				providerID, known = id, true
			}
		}
		if !known {
			providerID = "newnoa-" + randomToken()[:16]
		}
	}
	if value, exists := settings["providers"]; exists {
		var native map[string]json.RawMessage
		if json.Unmarshal(value, &native) != nil || native == nil || native[providerID] != nil {
			return preview{}, errors.New("existing native OpenCode provider settings conflict with the managed provider")
		}
	}
	models := map[string]json.RawMessage{}
	if value, exists := providers[providerID]; exists {
		if !known {
			return preview{}, errors.New("existing OpenCode provider conflicts with the selected key")
		}
		var old struct{ Models map[string]json.RawMessage }
		if json.Unmarshal(value, &old) != nil || old.Models == nil {
			return preview{}, errors.New("invalid managed OpenCode provider")
		}
		models = old.Models
	}
	models[alias], _ = json.Marshal(map[string]string{"name": alias})
	providers[providerID], err = json.Marshal(map[string]any{
		"npm": "@ai-sdk/openai-compatible", "name": "NEWNOA",
		"options": map[string]string{"baseURL": c.proxyBaseURL() + "/v1", "apiKey": apiKey},
		"models":  models,
	})
	if err != nil {
		return preview{}, err
	}
	settings["provider"], err = json.Marshal(providers)
	if err != nil {
		return preview{}, err
	}
	if managed == nil {
		settings["model"], _ = json.Marshal(providerID + "/" + alias)
		settings["small_model"] = settings["model"]
	}
	content, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return preview{}, err
	}
	pr := preview{Model: model, Alias: alias, Agent: "opencode", Digest: randomToken(), SourceDigest: hash(raw), Content: string(content) + "\n", Path: path, Existed: existed, apiKey: apiKey}
	c.pending = &pr
	return pr, nil
}

func openCodeProviders(raw []byte, existed bool) (map[string]json.RawMessage, map[string]json.RawMessage, error) {
	settings := map[string]json.RawMessage{}
	if existed && (json.Unmarshal(raw, &settings) != nil || settings == nil) {
		return nil, nil, errors.New("existing settings must be a JSON object; no changes made")
	}
	providers := map[string]json.RawMessage{}
	// OpenCode otherwise adds this on first read, invalidating the recovery digest.
	if _, ok := settings["$schema"]; !ok {
		settings["$schema"] = json.RawMessage(`"https://opencode.ai/config.json"`)
	}
	if value, ok := settings["provider"]; ok {
		if json.Unmarshal(value, &providers) != nil || providers == nil {
			return nil, nil, errors.New("existing provider must be a JSON object; no changes made")
		}
	}
	return settings, providers, nil
}
