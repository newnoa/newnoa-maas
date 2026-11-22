package main

import (
	"context"
	"encoding/json"
	"strings"
)

const historySummaryVersion = "dialogue-v1"

func historySummaryText(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	parts, _ := value.([]any)
	var text []string
	for _, part := range parts {
		block, _ := part.(map[string]any)
		switch historyText(block["type"]) {
		case "", "text", "input_text", "output_text":
			if s := historyText(block["text"]); strings.TrimSpace(s) != "" {
				text = append(text, s)
			}
		}
	}
	return strings.Join(text, "\n")
}

func historySummaryMessage(value any, role string) string {
	rows, _ := value.([]any)
	for i := len(rows) - 1; i >= 0; i-- {
		row, _ := rows[i].(map[string]any)
		if row["role"] == role {
			if text := historySummaryText(row["content"]); strings.TrimSpace(text) != "" {
				return text
			}
		}
	}
	return ""
}

func historySummaryAnswer(body map[string]any) string {
	if text := historySummaryMessage(body["output"], "assistant"); text != "" {
		return text
	}
	if choices, ok := body["choices"].([]any); ok && len(choices) > 0 {
		choice, _ := choices[0].(map[string]any)
		for _, key := range []string{"message", "delta"} {
			message, _ := choice[key].(map[string]any)
			if text := historySummaryText(message["content"]); text != "" {
				return text
			}
		}
		return historyText(choice["text"])
	}
	if body["role"] == "assistant" {
		return historySummaryText(body["content"])
	}
	return ""
}

func historySummaryResponse(raw string) string {
	var body map[string]any
	if json.Unmarshal([]byte(raw), &body) == nil {
		return historySummaryAnswer(body)
	}
	var deltas strings.Builder
	var complete string
	for _, line := range strings.Split(raw, "\n") {
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var event map[string]any
		if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &event) != nil {
			continue
		}
		switch event["type"] {
		case "response.output_text.delta":
			deltas.WriteString(historyText(event["delta"]))
		case "content_block_delta":
			delta, _ := event["delta"].(map[string]any)
			if delta["type"] == "text_delta" {
				deltas.WriteString(historyText(delta["text"]))
			}
		case "content_block_start":
			block, _ := event["content_block"].(map[string]any)
			if block["type"] == "text" {
				deltas.WriteString(historyText(block["text"]))
			}
		case "response.completed", "response.done":
			response, _ := event["response"].(map[string]any)
			complete = historySummaryAnswer(response)
		default:
			deltas.WriteString(historySummaryAnswer(event))
		}
	}
	if complete != "" {
		return complete
	}
	return deltas.String()
}

func historyContentSummary(request, response string) string {
	var body map[string]any
	var question, assistant string
	if json.Unmarshal([]byte(request), &body) == nil {
		for _, key := range []string{"messages", "input"} {
			if question = historySummaryMessage(body[key], "user"); question != "" {
				break
			}
		}
		if question == "" {
			question = historyText(body["input"])
		}
		if question == "" {
			question = historyText(body["prompt"])
		}
		for _, key := range []string{"messages", "input"} {
			if assistant = historySummaryMessage(body[key], "assistant"); assistant != "" {
				break
			}
		}
	} else if trimmed := strings.TrimSpace(request); !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[") {
		question = request
	}
	if strings.TrimSpace(question) == "" {
		question = historySummaryResponse(response)
		if strings.TrimSpace(question) == "" {
			question = assistant
		}
	}
	runes := []rune(strings.Join(strings.Fields(question), " "))
	return string(runes[:min(len(runes), 120)])
}

// Old databases retain their original bodies; refresh only the visible page, without returning bodies to the UI.
func refreshHistorySummaries(ctx context.Context, store historyRepository, viewer string, q historyQuery, items []map[string]any) error {
	if q.IncludeContent {
		return nil
	}
	for _, value := range items {
		if value["contentSummaryVersion"] == historySummaryVersion {
			continue
		}
		if q.TestModelType != "" {
			// Legacy previews may contain raw request JSON. Detail is read only on selection.
			delete(value, "contentSummary")
			continue
		}
		body, err := store.Get(ctx, viewer, q.Scope, historyText(value["id"]))
		if err != nil {
			return err
		}
		value["contentSummary"] = historyContentSummary(historyText(body["request"]), historyText(body["response"]))
		value["contentSummaryVersion"] = historySummaryVersion
	}
	return nil
}
