package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
)

var errResponsesTools = errors.New("This upstream cannot preserve the requested Responses tools or history")

type responsesToolName struct {
	namespace, name string
	custom          bool
}

// Normalize a request copy. The original remains the authority and history payload.
func normalizeResponsesFunctions(body map[string]any) (map[string]any, map[string]responsesToolName, error) {
	input, _ := body["input"].([]any)
	top, _ := body["tools"].([]any)
	if body["tools"] != nil && top == nil {
		return nil, nil, errResponsesTools
	}
	tools := make([]any, 0, len(top))
	items := make([]any, 0, len(input))
	aliases := make(map[string]responsesToolName)
	owners := make(map[responsesToolName]string)
	seen := make(map[string]map[string]any)
	providerTools := make([]any, 0)
	// Namespace text is repeated for each child on the upstream wire.
	remainingNamespaceBytes := 1 << 20
	changed := false
	add := func(v any, namespace, description string) error {
		tool, ok := v.(map[string]any)
		if !ok {
			return errResponsesTools
		}
		custom := tool["type"] == "custom"
		if tool["type"] != "function" && !custom {
			if namespace != "" || tool["type"] != "web_search" {
				return errResponsesTools
			}
			providerTools = append(providerTools, v)
			return nil
		}
		name, _ := tool["name"].(string)
		if name == "" || len(name) > 256 || len(seen) >= 1024 {
			return errResponsesTools
		}
		flat := name
		if namespace != "" || custom {
			id := responsesToolName{namespace, name, custom}
			digest := sha256.Sum256([]byte(fmt.Sprintf("%d:%s%s", len(namespace), namespace, name)))
			flat = fmt.Sprintf("newnoa_%x", digest[:20])
			if old, exists := aliases[flat]; exists && old != id {
				return errResponsesTools
			}
			aliases[flat], owners[id] = id, flat
			tool = maps.Clone(tool)
			tool["name"] = flat
			if custom {
				format, ok := tool["format"].(map[string]any)
				if !ok || !responsesFreeformSupported(namespace, name, format) {
					return errResponsesTools
				}
				for field := range tool {
					if field != "type" && field != "name" && field != "description" && field != "format" {
						return errResponsesTools
					}
				}
				encodedFormat, err := json.Marshal(format)
				if err != nil {
					return errResponsesTools
				}
				desc, _ := tool["description"].(string)
				tool["description"] = desc + "\nPass the exact freeform input as `input`. Tool format: " + string(encodedFormat)
				tool["type"], tool["strict"] = "function", true
				tool["parameters"] = map[string]any{"type": "object", "properties": map[string]any{"input": map[string]string{"type": "string"}}, "required": []string{"input"}, "additionalProperties": false}
				delete(tool, "format")
				changed = true
			}
			if description != "" {
				if len(description)+1 > remainingNamespaceBytes {
					return errResponsesTools
				}
				remainingNamespaceBytes -= len(description) + 1
				memberDescription, _ := tool["description"].(string)
				tool["description"] = description + "\n" + memberDescription
			}
		}
		if old := seen[flat]; old != nil {
			if !reflect.DeepEqual(old, tool) {
				return errResponsesTools
			}
			return nil
		}
		seen[flat] = tool
		tools = append(tools, tool)
		return nil
	}
	declare := func(v any) error {
		tool, ok := v.(map[string]any)
		if !ok || tool["type"] != "namespace" {
			return add(v, "", "")
		}
		name, _ := tool["name"].(string)
		description, _ := tool["description"].(string)
		children, _ := tool["tools"].([]any)
		if name == "" || len(name) > 256 || len(children) == 0 {
			return errResponsesTools
		}
		for k := range tool {
			if k != "type" && k != "name" && k != "description" && k != "tools" {
				return errResponsesTools
			}
		}
		for _, child := range children {
			if err := add(child, name, description); err != nil {
				return err
			}
		}
		changed = true
		return nil
	}
	for _, v := range top {
		if err := declare(v); err != nil {
			return nil, nil, err
		}
	}
	plainNames := make(map[string]bool)
	for _, v := range top {
		tool, _ := v.(map[string]any)
		if tool["type"] != "namespace" && tool["type"] != "custom" {
			if name, ok := tool["name"].(string); ok {
				plainNames[name] = true
			}
		}
	}
	for _, v := range input {
		item, _ := v.(map[string]any)
		if item["type"] != "additional_tools" {
			items = append(items, v)
			continue
		}
		declared, ok := item["tools"].([]any)
		if item["role"] != "developer" || !ok {
			return nil, nil, errResponsesTools
		}
		for k := range item {
			if k != "type" && k != "role" && k != "id" && k != "tools" {
				return nil, nil, errResponsesTools
			}
		}
		for _, tool := range declared {
			if err := declare(tool); err != nil {
				return nil, nil, err
			}
			d, _ := tool.(map[string]any)
			if d["type"] == "function" {
				name, _ := d["name"].(string)
				plainNames[name] = true
			}
		}
		changed = true
	}
	for flat := range aliases {
		if plainNames[flat] {
			return nil, nil, errResponsesTools
		}
	}
	rewrite := func(item map[string]any) (map[string]any, error) {
		namespace, _ := item["namespace"].(string)
		custom := item["type"] == "custom_tool_call" || item["type"] == "custom"
		if namespace == "" && !custom {
			return item, nil
		}
		name, _ := item["name"].(string)
		flat, ok := owners[responsesToolName{namespace, name, custom}]
		if !ok {
			return nil, errResponsesTools
		}
		item = maps.Clone(item)
		item["name"] = flat
		delete(item, "namespace")
		if custom {
			if item["type"] == "custom_tool_call" {
				input, ok := item["input"].(string)
				if !ok {
					return nil, errResponsesTools
				}
				encoded, _ := json.Marshal(map[string]string{"input": input})
				item["arguments"] = string(encoded)
				delete(item, "input")
				item["type"] = "function_call"
			} else {
				item["type"] = "function"
			}
		}
		changed = true
		return item, nil
	}
	for i, v := range items {
		item, _ := v.(map[string]any)
		switch item["type"] {
		case "function_call", "custom_tool_call":
			call, err := rewrite(item)
			if err != nil {
				return nil, nil, err
			}
			items[i] = call
		case "custom_tool_call_output":
			out := maps.Clone(item)
			out["type"] = "function_call_output"
			items[i], changed = out, true
		case "tool_search_call", "tool_search_output", "compaction", "context_compaction", "configuration_update":
			return nil, nil, errResponsesTools
		}
	}
	if choice, ok := body["tool_choice"].(map[string]any); ok && (choice["namespace"] != nil || choice["type"] == "custom") {
		if choice["type"] != "function" && choice["type"] != "custom" {
			return nil, nil, errResponsesTools
		}
		out, err := rewrite(choice)
		if err != nil {
			return nil, nil, err
		}
		body = maps.Clone(body)
		body["tool_choice"] = out
	}
	if len(providerTools) > 0 {
		// llama.cpp ignores an optional provider-side web search declaration; do not silently disable an explicit request to use it.
		if body["tool_choice"] != nil && body["tool_choice"] != "auto" && body["tool_choice"] != "none" {
			return nil, nil, errResponsesTools
		}
		if len(tools) == 0 && body["tool_choice"] != "none" {
			return nil, nil, errResponsesTools
		}
		tools = append(tools, providerTools...)
	}
	if changed {
		body = maps.Clone(body)
		if input != nil {
			body["input"] = items
		}
		body["tools"] = tools
	}
	return body, aliases, nil
}

func restoreResponsesFunctions(body map[string]any, aliases map[string]responsesToolName) (bool, error) {
	changed := false
	fix := func(v any, partial bool) error {
		item, _ := v.(map[string]any)
		name, _ := item["name"].(string)
		if id, ok := aliases[name]; ok && item["type"] == "function_call" {
			if item["namespace"] != nil {
				return errResponsesTools
			}
			if id.custom {
				input := ""
				if !partial {
					arguments, _ := item["arguments"].(string)
					var values map[string]any
					if json.Unmarshal([]byte(arguments), &values) != nil || len(values) != 1 {
						return errResponsesTools
					}
					var exists bool
					if input, exists = values["input"].(string); !exists {
						return errResponsesTools
					}
				}
				item["input"], item["type"] = input, "custom_tool_call"
				delete(item, "arguments")
			}
			item["name"] = id.name
			if id.namespace != "" {
				item["namespace"] = id.namespace
			}
			changed = true
		}
		return nil
	}
	if err := fix(body["item"], body["type"] == "response.output_item.added"); err != nil {
		return false, err
	}
	for _, document := range []map[string]any{body, responseMap(body["response"])} {
		output, _ := document["output"].([]any)
		for _, item := range output {
			if err := fix(item, false); err != nil {
				return false, err
			}
		}
	}
	return changed, nil
}

func responseMap(v any) map[string]any { m, _ := v.(map[string]any); return m }

func responsesFreeformSupported(namespace, name string, format map[string]any) bool {
	if format["type"] == "text" {
		return len(format) == 1
	}
	definition, _ := format["definition"].(string)
	if format["type"] != "grammar" || format["syntax"] != "lark" || len(format) != 3 {
		return false
	}
	// Keep native validation and approval on the client; only wrap known wire grammars.
	switch fmt.Sprintf("%x", sha256.Sum256([]byte(definition))) {
	case "d6367f4826ed608c424b0a308f3d6163527df63c22513d089b91863552f8bfeb", "2c1d2069867f720f53f5c8e08100287d56c0b4818e2921067de5c6614fb7ca5e":
		return name == "apply_patch"
	case "8eead048e20069c17fcd0b6fe5ee9ed29364868723f8e08889de146a2b4b7a8e":
		return namespace == "functions" && name == "exec"
	default:
		return false
	}
}
