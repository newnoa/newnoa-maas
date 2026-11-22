package main

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
)

func (c *core) checkOpenClawLocation() error {
	for _, key := range []string{"OPENCLAW_HOME", "OPENCLAW_STATE_DIR", "OPENCLAW_CONFIG_PATH", "OPENCLAW_PROFILE"} {
		if os.Getenv(key) != "" {
			return errors.New("custom OpenClaw paths require manual configuration")
		}
	}
	path, _, _ := c.agentPaths("openclaw")
	return c.safeConfigFile(path)
}

func (c *core) previewOpenClaw(model, apiKey string, aliases ...string) (preview, error) {
	alias := model
	if len(aliases) > 0 && aliases[0] != "" {
		alias = aliases[0]
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`).MatchString(model) {
		return preview{}, errors.New("invalid model identifier")
	}
	if err := c.checkOpenClawLocation(); err != nil {
		return preview{}, err
	}
	path, _, _ := c.agentPaths("openclaw")
	raw, err := os.ReadFile(path)
	existed := err == nil
	if err != nil && !os.IsNotExist(err) {
		return preview{}, err
	}
	settings, models, providers, err := openClawProviderSettings(raw, existed)
	if err != nil {
		return preview{}, err
	}
	managed, err := c.managedConnectionJournal("openclaw", raw, existed)
	if err != nil {
		return preview{}, err
	}
	if managed != nil && managed.ProviderCatalog != nil {
		return preview{}, errors.New("Use the managed provider list to change this configuration")
	}
	var previousModels json.RawMessage
	if old, ok := providers["newnoa"]; ok {
		if managed == nil {
			return preview{}, errors.New("existing newnoa provider must be restored or removed before applying")
		}
		if managed.APIKeyHash == managedAgentKeyHash(apiKey) {
			var provider map[string]json.RawMessage
			if json.Unmarshal(old, &provider) != nil {
				return preview{}, errors.New("invalid managed provider")
			}
			previousModels = provider["models"]
		}
	}
	entries, err := managedModelEntries(previousModels, alias)
	if err != nil {
		return preview{}, err
	}
	providers["newnoa"], _ = json.Marshal(map[string]any{"baseUrl": c.proxyBaseURL() + "/v1", "apiKey": apiKey, "api": "openai-completions", "models": entries})
	models["providers"], _ = json.Marshal(providers)
	settings["models"], _ = json.Marshal(models)
	if err := setOpenClawDefault(settings, "newnoa", alias); err != nil {
		return preview{}, err
	}
	content, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return preview{}, err
	}
	pr := preview{Model: model, Alias: alias, Agent: "openclaw", Digest: randomToken(), SourceDigest: hash(raw), Content: string(content) + "\n", Path: path, Existed: existed, apiKey: apiKey}
	c.pending = &pr
	return pr, nil
}

func openClawObject(parent map[string]json.RawMessage, key string) (map[string]json.RawMessage, error) {
	value := map[string]json.RawMessage{}
	if raw, ok := parent[key]; ok && (json.Unmarshal(raw, &value) != nil || value == nil) {
		return nil, errors.New("OpenClaw configuration section must be a JSON object")
	}
	return value, nil
}

func openClawProviderSettings(raw []byte, existed bool) (settings, models, providers map[string]json.RawMessage, err error) {
	settings = map[string]json.RawMessage{}
	if existed && (json.Unmarshal(raw, &settings) != nil || settings == nil) {
		return nil, nil, nil, errors.New("OpenClaw settings require a strict JSON object; JSON5 needs manual configuration")
	}
	models, err = openClawObject(settings, "models")
	if err != nil {
		return nil, nil, nil, err
	}
	providers, err = openClawObject(models, "providers")
	if err != nil {
		return nil, nil, nil, err
	}
	return settings, models, providers, nil
}

func setOpenClawDefault(settings map[string]json.RawMessage, native, alias string) error {
	agents, err := openClawObject(settings, "agents")
	if err != nil {
		return err
	}
	defaults, err := openClawObject(agents, "defaults")
	if err != nil {
		return err
	}
	// Replace fallback routing together with primary so selection cannot silently use another provider.
	selected := map[string]json.RawMessage{}
	if previous, ok := defaults["model"]; ok {
		var oldName *string
		if json.Unmarshal(previous, &oldName) != nil || oldName == nil {
			if json.Unmarshal(previous, &selected) != nil || selected == nil {
				return errors.New("OpenClaw default model must be a string or object")
			}
		}
	}
	selected["primary"], _ = json.Marshal(native + "/" + alias)
	delete(selected, "fallbacks")
	defaults["model"], _ = json.Marshal(selected)
	agents["defaults"], _ = json.Marshal(defaults)
	settings["agents"], _ = json.Marshal(agents)
	return nil
}
