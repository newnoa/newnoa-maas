package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

func (c *core) checkHermesLocation() error {
	for _, key := range []string{"HERMES_HOME", "HERMES_PROFILE", "HERMES_CONFIG", "HERMES_CONFIG_PATH"} {
		if os.Getenv(key) != "" {
			return errors.New("custom Hermes paths require manual configuration")
		}
	}
	path, _, _ := c.agentPaths("hermes-agent")
	return c.safeConfigFile(path)
}

func hermesMapping(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return errors.New("Hermes configuration sections must be YAML mappings")
	}
	seen := map[string]bool{}
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || seen[key.Value] {
			return errors.New("Hermes YAML keys must be unique strings")
		}
		seen[key.Value] = true
	}
	return nil
}

func hermesSection(node *yaml.Node, name string) (*yaml.Node, error) {
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == name {
			return node.Content[i+1], hermesMapping(node.Content[i+1])
		}
	}
	child := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: name}, child)
	return child, nil
}

func hermesSet(node *yaml.Node, name, value string, remove bool) {
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == name {
			if remove {
				node.Content = append(node.Content[:i], node.Content[i+2:]...)
			} else {
				node.Content[i+1] = &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
			}
			return
		}
	}
	if !remove {
		node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: name}, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value})
	}
}

func hermesProviderConflict(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	return name == "newnoa" || name == "custom:newnoa" || name == "custom:custom:newnoa"
}

func hermesProviderEntryConflict(entry *yaml.Node) bool {
	if entry.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i < len(entry.Content); i += 2 {
		if (entry.Content[i].Value == "name" || entry.Content[i].Value == "provider_key") && hermesProviderConflict(entry.Content[i+1].Value) {
			return true
		}
	}
	return false
}

func (c *core) previewHermes(model, apiKey string, aliases ...string) (preview, error) {
	alias := model
	if len(aliases) > 0 && aliases[0] != "" {
		alias = aliases[0]
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`).MatchString(model) {
		return preview{}, errors.New("invalid model identifier")
	}
	if err := c.checkHermesLocation(); err != nil {
		return preview{}, err
	}
	path, _, _ := c.agentPaths("hermes-agent")
	raw, err := os.ReadFile(path)
	existed := err == nil
	if err != nil && !os.IsNotExist(err) {
		return preview{}, err
	}
	managed, err := c.managedConnectionJournal("hermes-agent", raw, existed)
	if err != nil {
		return preview{}, err
	}
	if managed != nil && managed.ProviderCatalog != nil {
		return preview{}, errors.New("Use the managed provider list to change this configuration")
	}
	providerID := "newnoa-" + hash([]byte(apiKey + "\n" + model + "\n" + alias))[:24]
	document, err := readHermesDocument(raw, existed)
	if err != nil {
		return preview{}, err
	}
	root := document.Content[0]
	providers, err := hermesSection(root, "providers")
	if err != nil {
		return preview{}, err
	}
	for i := 0; i < len(providers.Content); i += 2 {
		if managed == nil && (hermesProviderConflict(providers.Content[i].Value) || hermesProviderEntryConflict(providers.Content[i+1])) {
			return preview{}, errors.New("existing newnoa provider must be restored or removed before applying")
		}
		if providers.Content[i].Value == providerID && managed == nil {
			return preview{}, errors.New("existing newnoa provider requires manual configuration")
		}
	}
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value != "custom_providers" {
			continue
		}
		legacy := root.Content[i+1]
		if legacy.Kind != yaml.SequenceNode {
			return preview{}, errors.New("legacy Hermes providers require a YAML sequence")
		}
		for _, entry := range legacy.Content {
			if hermesProviderEntryConflict(entry) {
				return preview{}, errors.New("existing legacy newnoa provider requires manual configuration")
			}
		}
	}
	provider, err := hermesSection(providers, providerID)
	if err != nil {
		return preview{}, err
	}
	for _, entry := range [][2]string{{"base_url", c.proxyBaseURL() + "/v1"}, {"api_key", apiKey}, {"api_mode", "chat_completions"}, {"default_model", alias}} {
		hermesSet(provider, entry[0], entry[1], false)
	}
	extra, _ := hermesSection(provider, "extra_body")
	hermesSet(extra, "max_tokens", "", true)
	extra.Content = append(extra.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "max_tokens"}, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: "4096"})
	if err := setHermesDefault(root, providerID, alias); err != nil {
		return preview{}, err
	}
	content, err := yaml.Marshal(document)
	if err != nil {
		return preview{}, err
	}
	if len(content) > maxFrame/3 {
		return preview{}, errors.New("Hermes configuration too large for preview")
	}
	pr := preview{Model: model, Alias: alias, Agent: "hermes-agent", Digest: randomToken(), SourceDigest: hash(raw), Path: path, Content: string(content), Existed: existed, apiKey: apiKey}
	c.pending = &pr
	return pr, nil
}

func readHermesDocument(raw []byte, existed bool) (*yaml.Node, error) {
	document := yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{{Kind: yaml.MappingNode, Tag: "!!map"}}}
	var err error
	if existed {
		decoder := yaml.NewDecoder(bytes.NewReader(raw))
		if err = decoder.Decode(&document); err != nil || len(document.Content) != 1 {
			return nil, errors.New("Hermes requires a YAML mapping document")
		}
		var extra yaml.Node
		if decoder.Decode(&extra) != io.EOF {
			return nil, errors.New("multiple Hermes YAML documents are unsupported")
		}
	}
	root := document.Content[0]
	// Alias and merge semantics differ across YAML loaders; refuse rather than change inherited routing.
	var validate func(*yaml.Node) error
	validate = func(node *yaml.Node) error {
		if node.Kind == yaml.AliasNode || node.Anchor != "" {
			return errors.New("Hermes YAML anchors require manual configuration")
		}
		if node.Kind == yaml.MappingNode {
			if err := hermesMapping(node); err != nil {
				return err
			}
		}
		for _, child := range node.Content {
			if err := validate(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err = hermesMapping(root); err != nil {
		return nil, err
	}
	if err = validate(root); err != nil {
		return nil, err
	}
	return &document, nil
}

func setHermesDefault(root *yaml.Node, providerID, alias string) error {
	selected, err := hermesSection(root, "model")
	if err != nil {
		return err
	}
	for _, key := range []string{"api_key", "api", "key_env", "api_key_env", "base_url", "api_mode", "openai_runtime"} {
		hermesSet(selected, key, "", true)
	}
	hermesSet(selected, "provider", "custom:"+providerID, false)
	hermesSet(selected, "default", alias, false)
	for _, key := range []string{"fallback_model", "smart_model_routing"} {
		hermesSet(root, key, "", true)
	}
	return nil
}
