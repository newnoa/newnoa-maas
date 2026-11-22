package main

import (
	"encoding/json"
	"errors"
	"strings"

	"gopkg.in/yaml.v3"
)

func hermesProviderSettings(raw []byte, exists bool) (doc, providers *yaml.Node, rows map[string]json.RawMessage, defaultID string, err error) {
	doc, err = readHermesDocument(raw, exists)
	if err != nil {
		return
	}
	root := doc.Content[0]
	providers, err = hermesSection(root, "providers")
	if err != nil {
		return
	}
	rows = map[string]json.RawMessage{}
	for i := 0; i < len(providers.Content); i += 2 {
		// Only the tool-owned prefix may enter catalog mutations; all other nodes stay verbatim structures.
		name := providers.Content[i].Value
		if !strings.HasPrefix(name, "newnoa-") && !hermesProviderConflict(name) {
			continue
		}
		var value map[string]any
		if err = providers.Content[i+1].Decode(&value); err != nil {
			return
		}
		rows[name], err = json.Marshal(value)
		if err != nil {
			return
		}
	}
	for i := 0; i < len(root.Content); i += 2 {
		switch root.Content[i].Value {
		case "custom_providers":
			legacy := root.Content[i+1]
			if legacy.Kind != yaml.SequenceNode {
				return nil, nil, nil, "", errors.New("legacy Hermes providers require a YAML sequence")
			}
			for _, entry := range legacy.Content {
				if hermesProviderEntryConflict(entry) {
					return nil, nil, nil, "", errors.New("existing legacy NEWNOA provider requires manual configuration")
				}
			}
			continue
		case "model":
		default:
			continue
		}
		var model struct{ Provider string }
		if root.Content[i+1].Kind == yaml.MappingNode && root.Content[i+1].Decode(&model) == nil {
			defaultID = strings.TrimPrefix(model.Provider, "custom:")
		}
	}
	return
}

func finishHermesProviderCatalog(doc, providers *yaml.Node, rows map[string]json.RawMessage, previous *journal, catalog *providerCatalog, alias string) ([]byte, error) {
	if previous != nil && previous.ProviderCatalog != nil {
		for _, row := range previous.ProviderCatalog.Providers {
			if _, exists := rows[row.Native]; !exists {
				hermesSet(providers, row.Native, "", true)
			}
		}
	}
	for _, row := range catalog.Providers {
		var settings map[string]json.RawMessage
		if json.Unmarshal(rows[row.Native], &settings) != nil || settings == nil {
			return nil, errors.New("Missing Hermes provider")
		}
		if row.KeyID == catalog.DefaultKey {
			// Hermes requires the selected custom provider to advertise this same default.
			settings["default_model"], _ = json.Marshal(alias)
		}
		data, err := json.Marshal(settings)
		if err != nil {
			return nil, err
		}
		var node yaml.Node
		if err = yaml.Unmarshal(data, &node); err != nil || len(node.Content) != 1 {
			return nil, errors.New("Invalid Hermes provider mapping")
		}
		node.Content[0].Style = 0
		hermesSet(providers, row.Native, "", false)
		for i := 0; i < len(providers.Content); i += 2 {
			if providers.Content[i].Value == row.Native {
				providers.Content[i+1] = node.Content[0]
				break
			}
		}
	}
	return yaml.Marshal(doc)
}
