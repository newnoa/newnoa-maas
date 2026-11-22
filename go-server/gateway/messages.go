package main

import (
	"encoding/json"
	"net/http"
)

type messagesWriter struct{ http.ResponseWriter }

func (w *messagesWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type messagesUsage struct {
	Input    *int64 `json:"input_tokens"`
	Output   *int64 `json:"output_tokens"`
	Creation *int64 `json:"cache_creation_input_tokens"`
	Read     *int64 `json:"cache_read_input_tokens"`
}

func messageStop(reason string) bool {
	switch reason {
	case "end_turn", "max_tokens", "stop_sequence", "tool_use", "pause_turn", "refusal", "model_context_window_exceeded":
		return true
	}
	return false
}
func (e *evidence) messagesUsage(value *messagesUsage, initial bool) bool {
	if value == nil || value.Output == nil || initial && value.Input == nil {
		return false
	}
	for _, n := range []*int64{value.Input, value.Output, value.Creation, value.Read} {
		if n != nil && (*n < 0 || *n > 1000000000) {
			return false
		}
	}
	next := e.nativeUsage
	if initial {
		next = messagesUsage{}
	}
	if value.Input != nil {
		next.Input = value.Input
	}
	if value.Output != nil {
		next.Output = value.Output
	}
	if value.Creation != nil {
		next.Creation = value.Creation
	}
	if value.Read != nil {
		next.Read = value.Read
	}
	if next.Input == nil || next.Output == nil || !initial && e.output != nil && *next.Output < *e.output {
		return false
	}
	total := *next.Input
	if next.Creation != nil {
		total += *next.Creation
	}
	if next.Read != nil {
		total += *next.Read
	}
	if total > 1000000000 {
		return false
	}
	e.nativeUsage = next
	e.input = &total
	e.output = next.Output
	e.cached = next.Read
	e.write = next.Creation
	return true
}
func (e *evidence) messagesResponse(data []byte) bool {
	var message struct {
		ID      string            `json:"id"`
		Type    string            `json:"type"`
		Role    string            `json:"role"`
		Content []json.RawMessage `json:"content"`
		Stop    string            `json:"stop_reason"`
		Usage   *messagesUsage    `json:"usage"`
	}
	return json.Unmarshal(data, &message) == nil && message.ID != "" && message.Type == "message" && message.Role == "assistant" && message.Content != nil && messageStop(message.Stop) && e.messagesUsage(message.Usage, true)
}
func (e *evidence) messagesEvent(data string) {
	var event struct {
		Type    string `json:"type"`
		Index   *int   `json:"index"`
		Message struct {
			ID    string         `json:"id"`
			Type  string         `json:"type"`
			Role  string         `json:"role"`
			Usage *messagesUsage `json:"usage"`
		} `json:"message"`
		Delta struct {
			Stop string `json:"stop_reason"`
		} `json:"delta"`
		Usage *messagesUsage `json:"usage"`
	}
	valid := json.Unmarshal([]byte(data), &event) == nil && !e.done
	if valid {
		switch event.Type {
		case "ping":
		case "message_start":
			valid = !e.messageStarted && event.Message.ID != "" && event.Message.Type == "message" && event.Message.Role == "assistant" && e.messagesUsage(event.Message.Usage, true)
			e.messageStarted = valid
		case "message_delta":
			valid = e.messageStarted && !e.messageEnded && len(e.nativeBlocks) == 0 && e.messagesUsage(event.Usage, false)
			if event.Delta.Stop != "" {
				valid = valid && messageStop(event.Delta.Stop)
				e.messageEnded = valid
			}
		case "message_stop":
			valid = e.messageStarted && e.messageEnded && len(e.nativeBlocks) == 0 && e.input != nil && e.output != nil
			e.done = valid
		case "content_block_start", "content_block_delta", "content_block_stop":
			valid = e.messageStarted && !e.messageEnded && event.Index != nil && *event.Index >= 0
			if valid {
				index := *event.Index
				if event.Type == "content_block_start" {
					valid = index == e.nativeBlockCount && index < 10000
					if valid {
						if e.nativeBlocks == nil {
							e.nativeBlocks = map[int]bool{}
						}
						e.nativeBlocks[index] = true
						e.nativeBlockCount++
					}
				} else {
					valid = e.nativeBlocks[index]
					if valid && event.Type == "content_block_stop" {
						delete(e.nativeBlocks, index)
					}
				}
			}
		default:
			valid = false
		}
	}
	if !valid {
		e.invalid = true
		e.done = false
	}
}
