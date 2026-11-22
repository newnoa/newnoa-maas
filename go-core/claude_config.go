package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
)

func (c *core) agentPaths(agent string) (string, string, error) {
	switch agent {
	case "", "codex":
		return c.configPath(), c.journalPath(), nil
	case "claude-cli":
		return filepath.Join(c.home, ".claude", "settings.json"), filepath.Join(c.home, ".newnoa", "claude-cli-journal.json"), nil
	case "claude-desktop":
		paths, err := c.desktopPaths()
		if err != nil {
			return "", "", err
		}
		return paths[2], filepath.Join(c.home, ".newnoa", "claude-desktop-journal.json"), nil
	case "hana-agent":
		paths, journal := c.hanaPaths()
		return paths[0], journal, nil
	case "pi-agent":
		paths, journal := c.piPaths()
		return paths[0], journal, nil
	case "opencode":
		return filepath.Join(c.home, ".config", "opencode", "opencode.json"), filepath.Join(c.home, ".newnoa", "opencode-journal.json"), nil
	case "openclaw":
		return filepath.Join(c.home, ".openclaw", "openclaw.json"), filepath.Join(c.home, ".newnoa", "openclaw-journal.json"), nil
	case "hermes-agent":
		return filepath.Join(c.home, ".hermes", "config.yaml"), filepath.Join(c.home, ".newnoa", "hermes-agent-journal.json"), nil
	default:
		return "", "", errors.New("unsupported local agent")
	}
}

func (c *core) checkClaudeLocation() error {
	if dir := os.Getenv("CLAUDE_CONFIG_DIR"); dir != "" && filepath.Clean(dir) != filepath.Join(c.home, ".claude") {
		return errors.New("custom Claude CLI configuration paths require manual configuration")
	}
	return nil
}

func (c *core) previewClaude(model, apiKey string, aliases ...string) (preview, error) {
	alias := model
	if len(aliases) > 0 && aliases[0] != "" {
		alias = aliases[0]
	}
	if err := c.checkClaudeLocation(); err != nil {
		return preview{}, err
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`).MatchString(model) {
		return preview{}, errors.New("invalid model identifier")
	}
	path, _, _ := c.agentPaths("claude-cli")
	if err := c.safeConfigFile(path); err != nil {
		return preview{}, err
	}
	raw, err := os.ReadFile(path)
	existed := err == nil
	if err != nil && !os.IsNotExist(err) {
		return preview{}, err
	}
	if _, err := c.managedConnectionJournal("claude-cli", raw, existed); err != nil {
		return preview{}, err
	}
	settings := map[string]json.RawMessage{}
	if existed && (json.Unmarshal(raw, &settings) != nil || settings == nil) {
		return preview{}, errors.New("existing settings must be a JSON object; no changes made")
	}
	env := map[string]json.RawMessage{}
	if value, ok := settings["env"]; ok {
		if json.Unmarshal(value, &env) != nil || env == nil {
			return preview{}, errors.New("existing env must be a JSON object; no changes made")
		}
	}
	// Both credential styles can produce ambiguous authentication at the local proxy.
	delete(env, "ANTHROPIC_AUTH_TOKEN")
	for key, value := range map[string]string{
		"ANTHROPIC_BASE_URL":             c.proxyBaseURL(),
		"ANTHROPIC_API_KEY":              apiKey,
		"ANTHROPIC_MODEL":                alias,
		"ANTHROPIC_DEFAULT_OPUS_MODEL":   alias,
		"ANTHROPIC_DEFAULT_SONNET_MODEL": alias,
		"ANTHROPIC_DEFAULT_HAIKU_MODEL":  alias,
		"ANTHROPIC_SMALL_FAST_MODEL":     alias,
	} {
		env[key], _ = json.Marshal(value)
	}
	settings["env"], err = json.Marshal(env)
	if err != nil {
		return preview{}, err
	}
	content, err := json.MarshalIndent(settings, "", "  ")
	if err != nil {
		return preview{}, err
	}
	pr := preview{Model: model, Alias: alias, Agent: "claude-cli", Digest: randomToken(), SourceDigest: hash(raw), Content: string(content) + "\n", Path: path, Existed: existed, apiKey: apiKey}
	c.pending = &pr
	return pr, nil
}
