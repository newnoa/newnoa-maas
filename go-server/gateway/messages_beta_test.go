package main

import (
	"net/http"
	"strings"
	"testing"
)

func TestMessagesBetaWirePolicy(t *testing.T) {
	valid := "claude-code-20250219,interleaved-thinking-2025-05-14,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,effort-2025-11-24"
	h := http.Header{}
	if v, e := messagesBeta(h); e != nil || v != "" {
		t.Fatal(v, e)
	}
	h.Set("Anthropic-Beta", valid)
	if v, e := messagesBeta(h); e != nil || v != valid {
		t.Fatal(v, e)
	}
	if messagesBinding(valid) != "anthropic-beta:"+valid+"\n" || messagesBinding("") != "" {
		t.Fatal("binding changed")
	}
	for _, v := range []string{"unknown", "", valid + ",", "effort-2025-11-24,effort-2025-11-24", strings.Repeat("a", 513)} {
		h.Set("Anthropic-Beta", v)
		if _, e := messagesBeta(h); e == nil {
			t.Fatal("accepted invalid beta", v)
		}
	}
	h.Set("Anthropic-Beta", valid)
	h.Add("Anthropic-Beta", valid)
	if _, e := messagesBeta(h); e == nil {
		t.Fatal("multiple header fields accepted")
	}
}

// Captured from the single network-isolated Claude Code 2.1.325 process.
// Later requests remove optional beta tokens; they are not identical-header retries.
var claude2325Betas = []string{
	"interleaved-thinking-2025-05-14,context-management-2025-06-27,claude-code-20250219,per-turn-control-2026-07-01,mid-conversation-tool-changes-2026-07-01,thinking-token-count-2026-05-13,effort-2025-11-24,thinking-display-updates-2026-08-18",
	"interleaved-thinking-2025-05-14,context-management-2025-06-27,claude-code-20250219,per-turn-control-2026-07-01,mid-conversation-tool-changes-2026-07-01,effort-2025-11-24,thinking-display-updates-2026-08-18",
	"interleaved-thinking-2025-05-14,context-management-2025-06-27,claude-code-20250219,per-turn-control-2026-07-01,mid-conversation-tool-changes-2026-07-01,effort-2025-11-24",
}

func TestMessagesBetaClaude2325CapturedHeaders(t *testing.T) {
	values := append([]string{}, claude2325Betas...)
	values = append(values, "per-turn-control-2026-07-01", "mid-conversation-tool-changes-2026-07-01", "thinking-token-count-2026-05-13", "thinking-display-updates-2026-08-18")
	values = append(values, " thinking-display-updates-2026-08-18 , per-turn-control-2026-07-01 ")
	values = append(values, "per-turn-control-2026-07-01"+strings.Repeat(" ", 512-len("per-turn-control-2026-07-01")))
	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			h := http.Header{"Anthropic-Beta": {value}}
			got, err := messagesBeta(h)
			if err != nil || got != value {
				t.Fatalf("captured/explicit beta was rejected or reordered: got=%q err=%v", got, err)
			}
		})
	}
	for _, value := range []string{
		"per-turn-control-2026-07-01,per-turn-control-2026-07-01",
		"per-turn-control-2026-07-01, per-turn-control-2026-07-01",
		"mid-conversation-tool-changes-2026-07-01,unknown-2026-11-17",
		"thinking-display-updates-2026-08-18,",
		"per-turn-control-2026-07-01" + strings.Repeat(" ", 513-len("per-turn-control-2026-07-01")),
	} {
		if _, err := messagesBeta(http.Header{"Anthropic-Beta": {value}}); err == nil {
			t.Fatalf("accepted invalid beta %q", value)
		}
	}
	if _, err := messagesBeta(http.Header{"Anthropic-Beta": {"per-turn-control-2026-07-01", "thinking-token-count-2026-05-13"}}); err == nil {
		t.Fatal("multiple new beta header fields accepted")
	}
}
