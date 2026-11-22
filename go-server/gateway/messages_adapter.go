package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
)

// Anthropic-only features must fail explicitly rather than change the requested semantics.
func adaptMessagesRequest(body map[string]any, path string) (map[string]any, error) {
	bad := errors.New("Unsupported Messages feature for configured OpenAI protocol")
	out := map[string]any{}
	for k, v := range body {
		switch k {
		case "model", "stream", "temperature", "top_p":
			out[k] = v
		case "max_tokens", "messages", "system", "tools", "tool_choice", "metadata":
		case "stop_sequences":
			if path == "/v1/responses" {
				return nil, bad
			}
			out["stop"] = v
		case "thinking", "output_config":
		default:
			return nil, fmt.Errorf("Unsupported Messages field %s for configured OpenAI protocol", k)
		}
	}
	if path != "/v1/chat/completions" && path != "/v1/responses" {
		return nil, bad
	}
	effort := ""
	disabled := false
	if value, exists := body["thinking"]; exists {
		thinking, ok := value.(map[string]any)
		if !ok {
			return nil, bad
		}
		for key := range thinking {
			if key != "type" && key != "budget_tokens" && key != "display" {
				return nil, bad
			}
		}
		if display, exists := thinking["display"]; exists && display != "omitted" && display != "summarized" {
			return nil, bad
		}
		switch thinking["type"] {
		case "disabled":
			disabled = true
		case "adaptive":
			effort = "medium"
		case "enabled":
			budget, ok := thinking["budget_tokens"].(float64)
			if !ok || budget < 1024 || budget > 2000000 || math.Trunc(budget) != budget {
				return nil, bad
			}
			if limit, ok := body["max_tokens"].(float64); ok && budget >= limit {
				return nil, bad
			}
			effort = "medium"
		default:
			return nil, bad
		}
		if thinking["type"] != "enabled" {
			if _, exists := thinking["budget_tokens"]; exists {
				return nil, bad
			}
		}
	}
	if value, exists := body["output_config"]; exists {
		config, ok := value.(map[string]any)
		if !ok || disabled {
			return nil, bad
		}
		for key := range config {
			if key != "effort" {
				return nil, bad
			}
		}
		if value, exists := config["effort"]; exists {
			switch value {
			case "low", "medium", "high", "xhigh", "max":
				effort = value.(string)
			default:
				return nil, bad
			}
		}
	}
	if effort != "" {
		if path == "/v1/responses" {
			out["reasoning"] = map[string]any{"effort": effort}
		} else {
			out["reasoning_effort"] = effort
		}
	}
	chat := []any{}
	textBlocks := func(v any) (string, error) {
		if s, ok := v.(string); ok {
			return s, nil
		}
		list, ok := v.([]any)
		if !ok {
			return "", bad
		}
		parts := []string{}
		for _, x := range list {
			b, ok := x.(map[string]any)
			if !ok || b["type"] != "text" {
				return "", bad
			}
			s, ok := b["text"].(string)
			if !ok {
				return "", bad
			}
			parts = append(parts, s)
		}
		return strings.Join(parts, "\n"), nil
	}
	if system, exists := body["system"]; exists {
		s, err := textBlocks(system)
		if err != nil {
			return nil, err
		}
		chat = append(chat, map[string]any{"role": "system", "content": s})
	}
	messages, ok := body["messages"].([]any)
	if !ok {
		return nil, bad
	}
	for _, value := range messages {
		m, ok := value.(map[string]any)
		if !ok {
			return nil, bad
		}
		role, _ := m["role"].(string)
		if role != "user" && role != "assistant" {
			return nil, bad
		}
		if s, ok := m["content"].(string); ok {
			chat = append(chat, map[string]any{"role": role, "content": s})
			continue
		}
		blocks, ok := m["content"].([]any)
		if !ok {
			return nil, bad
		}
		content := []any{}
		calls := []any{}
		results := []any{}
		for _, v := range blocks {
			b, ok := v.(map[string]any)
			if !ok {
				return nil, bad
			}
			switch b["type"] {
			case "text":
				s, ok := b["text"].(string)
				if !ok {
					return nil, bad
				}
				content = append(content, map[string]any{"type": "text", "text": s})
			case "image":
				if role != "user" {
					return nil, bad
				}
				source, ok := b["source"].(map[string]any)
				if !ok {
					return nil, bad
				}
				u := ""
				switch source["type"] {
				case "url":
					u, _ = source["url"].(string)
				case "base64":
					media, _ := source["media_type"].(string)
					data, _ := source["data"].(string)
					if !strings.HasPrefix(media, "image/") || data == "" {
						return nil, bad
					}
					u = "data:" + media + ";base64," + data
				default:
					return nil, bad
				}
				if u == "" {
					return nil, bad
				}
				content = append(content, map[string]any{"type": "image_url", "image_url": map[string]any{"url": u}})
			case "tool_use":
				id, _ := b["id"].(string)
				name, _ := b["name"].(string)
				input, ok := b["input"].(map[string]any)
				if role != "assistant" || id == "" || name == "" || !ok {
					return nil, bad
				}
				args, _ := json.Marshal(input)
				calls = append(calls, map[string]any{"id": id, "type": "function", "function": map[string]any{"name": name, "arguments": string(args)}})
			case "tool_result":
				id, _ := b["tool_use_id"].(string)
				if role != "user" || id == "" {
					return nil, bad
				}
				s := ""
				if value, exists := b["content"]; exists {
					var err error
					s, err = textBlocks(value)
					if err != nil {
						return nil, err
					}
				}
				if failed, _ := b["is_error"].(bool); failed {
					s = "Tool error: " + s
				}
				results = append(results, map[string]any{"role": "tool", "tool_call_id": id, "content": s})
			default:
				return nil, bad
			}
		}
		chat = append(chat, results...)
		if len(content) > 0 || len(calls) > 0 {
			item := map[string]any{"role": role, "content": content}
			if len(calls) > 0 {
				item["tool_calls"] = calls
			}
			chat = append(chat, item)
		}
	}
	tools := []any{}
	if value, exists := body["tools"]; exists {
		list, ok := value.([]any)
		if !ok {
			return nil, bad
		}
		for _, v := range list {
			tool, ok := v.(map[string]any)
			if !ok {
				return nil, bad
			}
			name, _ := tool["name"].(string)
			schema, ok := tool["input_schema"].(map[string]any)
			if name == "" || !ok {
				return nil, bad
			}
			for key := range tool {
				if key != "name" && key != "description" && key != "input_schema" && key != "cache_control" {
					return nil, bad
				}
			}
			fn := map[string]any{"name": name, "parameters": schema, "strict": false}
			if d, exists := tool["description"]; exists {
				fn["description"] = d
			}
			tools = append(tools, map[string]any{"type": "function", "function": fn})
		}
	}
	if value, exists := body["tool_choice"]; exists {
		choice, ok := value.(map[string]any)
		if !ok {
			return nil, bad
		}
		switch choice["type"] {
		case "auto":
			out["tool_choice"] = "auto"
		case "any":
			out["tool_choice"] = "required"
		case "none":
			out["tool_choice"] = "none"
		case "tool":
			name, ok := choice["name"].(string)
			if !ok || name == "" {
				return nil, bad
			}
			out["tool_choice"] = map[string]any{"type": "function", "function": map[string]any{"name": name}}
		default:
			return nil, bad
		}
		if disabled, exists := choice["disable_parallel_tool_use"]; exists {
			v, ok := disabled.(bool)
			if !ok {
				return nil, bad
			}
			out["parallel_tool_calls"] = !v
		}
	}
	if path == "/v1/chat/completions" {
		out["messages"] = chat
		out["max_completion_tokens"] = body["max_tokens"]
		if len(tools) > 0 {
			out["tools"] = tools
		}
		if out["stream"] == true {
			out["stream_options"] = map[string]any{"include_usage": true}
		}
		return out, nil
	}
	input := []any{}
	for _, value := range chat {
		m := value.(map[string]any)
		if m["role"] == "tool" {
			input = append(input, map[string]any{"type": "function_call_output", "call_id": m["tool_call_id"], "output": m["content"]})
			continue
		}
		parts := []any{}
		if s, ok := m["content"].(string); ok {
			parts = append(parts, map[string]any{"type": "input_text", "text": s})
		} else {
			for _, v := range m["content"].([]any) {
				b := v.(map[string]any)
				if b["type"] == "text" {
					kind := "input_text"
					if m["role"] == "assistant" {
						kind = "output_text"
					}
					parts = append(parts, map[string]any{"type": kind, "text": b["text"]})
				} else {
					parts = append(parts, map[string]any{"type": "input_image", "image_url": b["image_url"].(map[string]any)["url"]})
				}
			}
		}
		if len(parts) > 0 {
			input = append(input, map[string]any{"role": m["role"], "content": parts})
		}
		if calls, ok := m["tool_calls"].([]any); ok {
			for _, v := range calls {
				c := v.(map[string]any)
				fn := c["function"].(map[string]any)
				input = append(input, map[string]any{"type": "function_call", "call_id": c["id"], "name": fn["name"], "arguments": fn["arguments"]})
			}
		}
	}
	out["input"] = input
	out["max_output_tokens"] = body["max_tokens"]
	if len(tools) > 0 {
		flat := []any{}
		for _, v := range tools {
			fn := v.(map[string]any)["function"].(map[string]any)
			fn["type"] = "function"
			flat = append(flat, fn)
		}
		out["tools"] = flat
	}
	if choice, ok := out["tool_choice"].(map[string]any); ok {
		out["tool_choice"] = map[string]any{"type": "function", "name": choice["function"].(map[string]any)["name"]}
	}
	return out, nil
}

func anthropicUsage(ev *evidence) (map[string]any, error) {
	if ev.input == nil || ev.output == nil {
		return nil, errors.New("Missing upstream usage")
	}
	input := *ev.input
	usage := map[string]any{"input_tokens": input, "output_tokens": *ev.output}
	if ev.cached != nil {
		input -= *ev.cached
		usage["cache_read_input_tokens"] = *ev.cached
	}
	if ev.write != nil {
		input -= *ev.write
		usage["cache_creation_input_tokens"] = *ev.write
	}
	if input < 0 {
		return nil, errors.New("Invalid upstream cache usage")
	}
	usage["input_tokens"] = input
	return usage, nil
}
func adaptedStop(reason string) (string, error) {
	switch reason {
	case "stop", "completed", "end_turn":
		return "end_turn", nil
	case "length", "max_output_tokens":
		return "max_tokens", nil
	case "tool_calls", "tool_use":
		return "tool_use", nil
	case "content_filter":
		return "refusal", nil
	}
	return "", errors.New("Unsupported upstream finish reason")
}
func adaptMessagesResponse(data []byte, path, model string, ev *evidence) ([]byte, error) {
	var source struct {
		ID         string `json:"id"`
		Status     string `json:"status"`
		Incomplete struct {
			Reason string `json:"reason"`
		} `json:"incomplete_details"`
		Choices []struct {
			Message struct {
				Content   *string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
			Finish string `json:"finish_reason"`
		} `json:"choices"`
		Output []struct {
			Type      string `json:"type"`
			CallID    string `json:"call_id"`
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
			Content   []struct {
				Type    string `json:"type"`
				Text    string `json:"text"`
				Refusal string `json:"refusal"`
			} `json:"content"`
		} `json:"output"`
	}
	bad := errors.New("Invalid OpenAI completion")
	if json.Unmarshal(data, &source) != nil || source.ID == "" {
		return nil, bad
	}
	content := []any{}
	reason := "completed"
	addTool := func(id, name, args string) error {
		var input map[string]any
		if id == "" || name == "" || json.Unmarshal([]byte(args), &input) != nil || input == nil {
			return bad
		}
		content = append(content, map[string]any{"type": "tool_use", "id": id, "name": name, "input": input})
		reason = "tool_use"
		return nil
	}
	if path == "/v1/chat/completions" {
		if len(source.Choices) != 1 {
			return nil, bad
		}
		choice := source.Choices[0]
		reason = choice.Finish
		if choice.Message.Content != nil && *choice.Message.Content != "" {
			content = append(content, map[string]any{"type": "text", "text": *choice.Message.Content})
		}
		for _, call := range choice.Message.ToolCalls {
			if err := addTool(call.ID, call.Function.Name, call.Function.Arguments); err != nil {
				return nil, err
			}
		}
	} else {
		if source.Status != "completed" {
			if source.Status != "incomplete" || source.Incomplete.Reason != "max_output_tokens" {
				return nil, bad
			}
			reason = "max_output_tokens"
		}
		for _, item := range source.Output {
			switch item.Type {
			case "message":
				for _, part := range item.Content {
					switch part.Type {
					case "output_text":
						content = append(content, map[string]any{"type": "text", "text": part.Text})
					case "refusal":
						content = append(content, map[string]any{"type": "text", "text": part.Refusal})
					default:
						return nil, bad
					}
				}
			case "function_call":
				if err := addTool(item.CallID, item.Name, item.Arguments); err != nil {
					return nil, err
				}
			case "reasoning":
			default:
				return nil, bad
			}
		}
	}
	stop, err := adaptedStop(reason)
	if err != nil {
		return nil, err
	}
	usage, err := anthropicUsage(ev)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"id": source.ID, "type": "message", "role": "assistant", "model": model, "content": content, "stop_reason": stop, "stop_sequence": nil, "usage": usage})
}

// The bridge emits each delta as it arrives; source evidence remains independent of client delivery.
type messagesBridge struct {
	path, model    string
	pending, event []byte
	started        bool
	blocks         int
	toolBytes      int
	textIndex      *int
	tools          map[int]*bridgeTool
	reason         string
}
type bridgeTool struct {
	index                   int
	id, name, args, allArgs string
	started                 bool
}

func messagesSSE(kind string, value map[string]any) []byte {
	value["type"] = kind
	data, _ := json.Marshal(value)
	return []byte("event: " + kind + "\ndata: " + string(data) + "\n\n")
}
func (b *messagesBridge) feed(data []byte) ([]byte, error) {
	b.pending = append(b.pending, data...)
	var out []byte
	for {
		i := bytes.IndexByte(b.pending, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimSuffix(string(b.pending[:i]), "\r")
		b.pending = b.pending[i+1:]
		if line == "" {
			if len(b.event) > 0 {
				value, err := b.convert(strings.TrimSuffix(string(b.event), "\n"))
				b.event = nil
				if err != nil {
					return out, err
				}
				out = append(out, value...)
			}
		} else if strings.HasPrefix(line, "data:") {
			b.event = append(b.event, []byte(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))...)
			b.event = append(b.event, '\n')
		}
	}
	if len(b.pending)+len(b.event) > 8*maxBody {
		return out, errors.New("Oversized upstream event")
	}
	return out, nil
}
func (b *messagesBridge) convert(raw string) ([]byte, error) {
	if raw == "[DONE]" {
		return nil, nil
	}
	var event struct {
		Type        string          `json:"type"`
		ID          string          `json:"id"`
		Delta       json.RawMessage `json:"delta"`
		OutputIndex int             `json:"output_index"`
		Item        struct {
			Type   string `json:"type"`
			CallID string `json:"call_id"`
			Name   string `json:"name"`
		} `json:"item"`
		Response struct {
			ID         string `json:"id"`
			Incomplete struct {
				Reason string `json:"reason"`
			} `json:"incomplete_details"`
		} `json:"response"`
		Choices []struct {
			Index int `json:"index"`
			Delta struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					Index    int    `json:"index"`
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"delta"`
			Finish *string `json:"finish_reason"`
		} `json:"choices"`
	}
	if json.Unmarshal([]byte(raw), &event) != nil {
		return nil, errors.New("Invalid upstream event")
	}
	out := []byte{}
	id := event.ID
	if id == "" {
		id = event.Response.ID
	}
	if !b.started && id != "" {
		b.started = true
		out = append(out, messagesSSE("message_start", map[string]any{"message": map[string]any{"id": id, "type": "message", "role": "assistant", "model": b.model, "content": []any{}, "stop_reason": nil, "stop_sequence": nil, "usage": map[string]any{"input_tokens": 0, "output_tokens": 0}}})...)
	}
	text := func(s string) {
		if s == "" {
			return
		}
		if b.textIndex == nil {
			i := b.blocks
			b.blocks++
			b.textIndex = &i
			out = append(out, messagesSSE("content_block_start", map[string]any{"index": i, "content_block": map[string]any{"type": "text", "text": ""}})...)
		}
		out = append(out, messagesSSE("content_block_delta", map[string]any{"index": *b.textIndex, "delta": map[string]any{"type": "text_delta", "text": s}})...)
	}
	tool := func(key int, id, name, args string) error {
		if key < 0 || key >= 10000 || b.blocks >= 10000 {
			return errors.New("Too many upstream tool blocks")
		}
		b.toolBytes += len(id) + len(name) + len(args)
		if b.toolBytes > 8*maxBody {
			return errors.New("Oversized upstream tool arguments")
		}
		if b.tools == nil {
			b.tools = map[int]*bridgeTool{}
		}
		t := b.tools[key]
		if t == nil {
			t = &bridgeTool{index: -1}
			b.tools[key] = t
		}
		if t.started && (id != "" && id != t.id || name != "" && name != t.name) {
			return errors.New("Upstream changed an active tool identity")
		}
		if !t.started {
			t.id += id
			t.name += name
		}
		t.args += args
		t.allArgs += args
		if !t.started && t.id != "" && t.name != "" && (b.path == "/v1/responses" || t.args != "") {
			t.started = true
			t.index = b.blocks
			b.blocks++
			out = append(out, messagesSSE("content_block_start", map[string]any{"index": t.index, "content_block": map[string]any{"type": "tool_use", "id": t.id, "name": t.name, "input": map[string]any{}}})...)
		}
		if t.started && t.args != "" {
			out = append(out, messagesSSE("content_block_delta", map[string]any{"index": t.index, "delta": map[string]any{"type": "input_json_delta", "partial_json": t.args}})...)
			t.args = ""
		}
		b.reason = "tool_use"
		return nil
	}
	if b.path == "/v1/chat/completions" {
		for _, choice := range event.Choices {
			if choice.Index != 0 {
				return nil, errors.New("Multiple upstream choices unsupported")
			}
			text(choice.Delta.Content)
			for _, call := range choice.Delta.ToolCalls {
				if err := tool(call.Index, call.ID, call.Function.Name, call.Function.Arguments); err != nil {
					return nil, err
				}
			}
			if choice.Finish != nil {
				b.reason = *choice.Finish
			}
		}
	} else {
		switch event.Type {
		case "response.output_text.delta", "response.refusal.delta":
			var s string
			if json.Unmarshal(event.Delta, &s) != nil {
				return nil, errors.New("Invalid text delta")
			}
			text(s)
		case "response.output_item.added":
			if event.Item.Type == "function_call" {
				if err := tool(event.OutputIndex, event.Item.CallID, event.Item.Name, ""); err != nil {
					return nil, err
				}
			}
		case "response.function_call_arguments.delta":
			var s string
			if json.Unmarshal(event.Delta, &s) != nil {
				return nil, errors.New("Invalid tool delta")
			}
			if err := tool(event.OutputIndex, "", "", s); err != nil {
				return nil, err
			}
		case "response.incomplete":
			if event.Response.Incomplete.Reason != "max_output_tokens" {
				return nil, errors.New("Incomplete upstream response")
			}
			b.reason = "max_output_tokens"
		case "response.completed":
			if b.reason == "" {
				b.reason = "completed"
			}
		}
	}
	return out, nil
}
func (b *messagesBridge) finish(ev *evidence) ([]byte, error) {
	if !b.started || !ev.complete() {
		return nil, errors.New("Incomplete upstream stream")
	}
	for _, t := range b.tools {
		var input map[string]any
		if !t.started || t.args != "" || json.Unmarshal([]byte(t.allArgs), &input) != nil || input == nil {
			return nil, errors.New("Incomplete upstream tool")
		}
	}
	stop, err := adaptedStop(b.reason)
	if err != nil {
		return nil, err
	}
	usage, err := anthropicUsage(ev)
	if err != nil {
		return nil, err
	}
	var out []byte
	for i := 0; i < b.blocks; i++ {
		out = append(out, messagesSSE("content_block_stop", map[string]any{"index": i})...)
	}
	out = append(out, messagesSSE("message_delta", map[string]any{"delta": map[string]any{"stop_reason": stop, "stop_sequence": nil}, "usage": usage})...)
	return append(out, messagesSSE("message_stop", map[string]any{})...), nil
}
