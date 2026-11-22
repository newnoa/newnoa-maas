package main

import (
	"bytes"
	"encoding/json"
)

func reservationHints(raw []byte, path string) map[string]any {
	hints := map[string]any{}
	if path == "/v1/audio/transcriptions" || imagePath(path) || path == "/v1/systemone" {
		return hints
	}
	var body map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&body) != nil {
		return hints
	}
	field := "max_tokens"
	if path == "/v1/responses" {
		field = "max_output_tokens"
	} else if _, present := body["max_completion_tokens"]; present {
		field = "max_completion_tokens"
	}
	for _, alias := range []string{"max_tokens", "max_completion_tokens", "max_output_tokens"} {
		if _, present := body[alias]; present && alias != field {
			// Conflicting protocol caps are forwarded unchanged but cannot bound a hold.
			field = ""
			break
		}
	}
	choices := int64(1)
	if value, present := body["n"]; present {
		choices = 0
		if number, ok := value.(json.Number); ok {
			choices, _ = number.Int64()
		}
	}
	if number, ok := body[field].(json.Number); ok && field != "" && choices == 1 {
		if limit, err := number.Int64(); err == nil && limit > 0 && limit <= 2_000_000 {
			hints["reserveOutputTokens"] = limit
		}
	}
	if knownTextInput(body, path) {
		// Original serialized bytes include text, tools and metadata; never trust caller estimates.
		hints["reserveInputUnits"] = int64(len(raw))
	}
	return hints
}

func knownTextInput(body map[string]any, path string) bool {
	for key := range body {
		switch key {
		case "model", "messages", "input", "system", "instructions", "tools", "functions", "tool_choice", "function_call", "parallel_tool_calls", "max_tokens", "max_completion_tokens", "max_output_tokens", "n", "stream", "stream_options", "temperature", "top_p", "top_k", "min_p", "seed", "stop", "stop_sequences", "frequency_penalty", "presence_penalty", "repetition_penalty", "logit_bias", "logprobs", "top_logprobs", "response_format", "text", "reasoning", "reasoning_effort", "thinking", "metadata", "user", "store", "service_tier", "safety_identifier", "prompt_cache_key", "reserveInputUnits", "reserveOutputTokens", "encoding_format", "dimensions", "query", "documents", "top_n", "return_documents":
		default:
			return false
		}
	}
	if tools, present := body["tools"]; present && tools != nil {
		rows, ok := tools.([]any)
		if !ok {
			return false
		}
		for _, row := range rows {
			tool, ok := row.(map[string]any)
			if !ok || tool["type"] != "function" {
				return false
			}
		}
	}
	for _, key := range []string{"system", "instructions"} {
		if value := body[key]; value != nil && !serializedText(value) {
			return false
		}
	}
	switch path {
	case "/v1/chat/completions", "/v1/messages":
		messages, ok := body["messages"].([]any)
		if !ok || len(messages) == 0 {
			return false
		}
		for _, value := range messages {
			message, ok := value.(map[string]any)
			if !ok || !serializedText(message["content"]) {
				return false
			}
			for key := range message {
				switch key {
				case "role", "content", "name", "tool_call_id", "tool_calls", "function_call", "refusal":
				default:
					return false
				}
			}
		}
		return true
	case "/v1/responses":
		return serializedText(body["input"])
	case "/v1/embeddings":
		return serializedText(body["input"])
	case "/v1/rerank":
		return serializedText(body["query"]) && serializedText(body["documents"])
	}
	return false
}

func serializedText(value any) bool {
	if _, ok := value.(string); ok {
		return true
	}
	blocks, ok := value.([]any)
	if !ok {
		return false
	}
	for _, value := range blocks {
		if _, ok := value.(string); ok {
			continue
		}
		block, ok := value.(map[string]any)
		if !ok || block["type"] != "text" && block["type"] != "input_text" && block["type"] != "output_text" {
			return false
		}
		if _, ok := block["text"].(string); !ok {
			return false
		}
		for key := range block {
			if key != "type" && key != "text" && key != "cache_control" {
				return false
			}
		}
	}
	return true
}
