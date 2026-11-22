package main

import (
	"errors"
	"net/http"
	"strings"
)

// Keep this wire policy identical in gateway, owner and desktop core.
func messagesBeta(h http.Header) (string, error) {
	values := h.Values("Anthropic-Beta")
	if len(values) == 0 {
		return "", nil
	}
	if len(values) != 1 || len(values[0]) > 512 {
		return "", errors.New("invalid Anthropic beta headers")
	}
	seen := map[string]bool{}
	for _, item := range strings.Split(values[0], ",") {
		item = strings.TrimSpace(item)
		switch item {
		case "claude-code-20250219", "interleaved-thinking-2025-05-14", "context-management-2025-06-27", "prompt-caching-scope-2026-01-05", "mid-conversation-system-2026-04-07", "effort-2025-11-24",
			"per-turn-control-2026-07-01", "mid-conversation-tool-changes-2026-07-01", "thinking-token-count-2026-05-13", "thinking-display-updates-2026-08-18":
		default:
			return "", errors.New("unsupported Anthropic beta")
		}
		if seen[item] {
			return "", errors.New("duplicate Anthropic beta")
		}
		seen[item] = true
	}
	// Preserve order: the exact forwarded header participates in the permit digest.
	return values[0], nil
}

func messagesBinding(beta string) string {
	if beta == "" {
		return ""
	}
	return "anthropic-beta:" + beta + "\n"
}
