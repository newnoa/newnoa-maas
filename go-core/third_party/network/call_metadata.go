package network

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"time"
)

// CallMetadata describes the caller, never credentials or forwarded address claims.
func CallMetadata(r *http.Request, body map[string]any) map[string]any {
	source := KnownCallerSource(r.Header.Get("X-Newnoa-Agent"))
	if source == "" {
		source = KnownCallerSource(r.UserAgent())
	}
	if source == "" {
		source = "direct_chat"
	}
	result := map[string]any{"callerSource": source}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip != nil {
		result["clientIp"] = ip.String()
	}
	effort, _ := body["reasoning_effort"].(string)
	if reasoning, ok := body["reasoning"].(map[string]any); ok {
		effort, _ = reasoning["effort"].(string)
	}
	switch effort {
	case "none", "minimal", "low", "medium", "high", "xhigh", "max":
		result["reasoningEffort"] = effort
	}
	return result
}

// KnownCallerSource can also recover explicit identifiers already stored in old records.
func KnownCallerSource(agent string) string {
	lower := strings.ToLower(agent)
	switch {
	case strings.EqualFold(agent, "NEWNOA Model Test"):
		return "model_test"
	case strings.EqualFold(agent, "NEWNOA Direct Chat"):
		return "direct_chat"
	case strings.Contains(lower, "codex"):
		return "codex"
	case strings.Contains(lower, "claude-code") || strings.Contains(lower, "claude_code"):
		return "claude"
	case strings.Contains(lower, "hermes"):
		return "hermes"
	case strings.Contains(lower, "openclaw"):
		return "openclaw"
	case strings.Contains(lower, "opencode"):
		return "opencode"
	}
	return ""
}

type CallTiming struct {
	Started time.Time
	First   *time.Time
	pending []byte
	event   []byte
}

// Observe waits for actual output; SSE role, heartbeat and usage events do not count.
func (t *CallTiming) Observe(data []byte, stream bool, now time.Time) {
	if t.First != nil {
		return
	}
	if !stream {
		t.pending = append(t.pending, data...)
		if len(t.pending) > 8<<20 {
			t.pending = nil
			return
		}
		if HasCallOutput(t.pending) {
			t.First = &now
			t.pending = nil
		}
		return
	}
	t.pending = append(t.pending, data...)
	for {
		end := bytes.IndexByte(t.pending, '\n')
		if end < 0 {
			break
		}
		line := bytes.TrimSpace(t.pending[:end])
		t.pending = t.pending[end+1:]
		if len(line) == 0 {
			if HasCallOutput(t.event) {
				t.First = &now
				t.pending = nil
				t.event = nil
				return
			}
			t.event = nil
		} else if bytes.HasPrefix(line, []byte("data:")) {
			t.event = append(t.event, bytes.TrimSpace(line[5:])...)
			t.event = append(t.event, '\n')
		}
	}
	// A malformed or oversized event must not turn this diagnostic into unbounded storage.
	if len(t.pending)+len(t.event) > 8<<20 {
		t.pending = nil
		t.event = nil
	}
}

func (t *CallTiming) Finish(result map[string]any, output *int64, now time.Time) {
	elapsed := now.Sub(t.Started)
	if elapsed < 0 {
		return
	}
	result["durationMs"] = elapsed.Milliseconds()
	if t.First != nil {
		result["firstTokenMs"] = t.First.Sub(t.Started).Milliseconds()
	}
	if output != nil && *output >= 0 && elapsed > 0 {
		result["outputTps"] = float64(*output) / elapsed.Seconds()
	}
}

func HasCallOutput(data []byte) bool {
	var value map[string]any
	if json.Unmarshal(data, &value) != nil {
		return false
	}
	nonempty := func(v any) bool { s, ok := v.(string); return ok && s != "" }
	if nonempty(value["text"]) {
		return true
	}
	if typ, _ := value["type"].(string); strings.HasSuffix(typ, ".delta") && nonempty(value["delta"]) {
		return true
	}
	if delta, ok := value["delta"].(map[string]any); ok {
		for _, key := range []string{"text", "thinking", "partial_json"} {
			if nonempty(delta[key]) {
				return true
			}
		}
	}
	if block, ok := value["content_block"].(map[string]any); ok && nonempty(block["text"]) {
		return true
	}
	if choices, ok := value["choices"].([]any); ok {
		for _, choice := range choices {
			row, _ := choice.(map[string]any)
			if nonempty(row["text"]) {
				return true
			}
			for _, key := range []string{"delta", "message"} {
				if part, ok := row[key].(map[string]any); ok {
					for _, field := range []string{"content", "reasoning_content", "reasoning"} {
						if nonempty(part[field]) {
							return true
						}
					}
					if calls, ok := part["tool_calls"].([]any); ok && len(calls) > 0 {
						return true
					}
				}
			}
		}
	}
	for _, key := range []string{"output", "content"} {
		if blocks, ok := value[key].([]any); ok && len(blocks) > 0 {
			return true
		}
	}
	return false
}
