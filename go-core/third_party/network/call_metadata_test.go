package network

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestCallMetadataUsesObservedPeerAndBoundedRequestFields(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/responses", nil)
	r.RemoteAddr = "[2001:db8::1]:1234"
	r.Header.Set("User-Agent", "codex_cli_rs/1.0")
	r.Header.Set("X-Forwarded-For", "198.51.100.99")
	m := CallMetadata(r, map[string]any{"reasoning": map[string]any{"effort": "high"}})
	if m["callerSource"] != "codex" || m["reasoningEffort"] != "high" || m["clientIp"] != "2001:db8::1" {
		t.Fatal(m)
	}
	r.Header.Set("X-Newnoa-Agent", "NEWNOA Model Test")
	if CallMetadata(r, nil)["callerSource"] != "model_test" {
		t.Fatal("explicit model-test source lost")
	}
	if _, ok := CallMetadata(r, map[string]any{"reasoning_effort": "secret arbitrary text"})["reasoningEffort"]; ok {
		t.Fatal("unbounded effort accepted")
	}
}

func TestCallTimingIgnoresSSEHeadersAndTracksSplitFirstOutput(t *testing.T) {
	start := time.Unix(100, 0)
	timing := CallTiming{Started: start}
	timing.Observe([]byte("data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"), true, start.Add(time.Second))
	if timing.First != nil {
		t.Fatal("role event counted as first output")
	}
	timing.Observe([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi"), true, start.Add(2*time.Second))
	timing.Observe([]byte("\"}}]}\r\n\r\n"), true, start.Add(3*time.Second))
	result := map[string]any{}
	output := int64(20)
	timing.Finish(result, &output, start.Add(4*time.Second))
	if result["firstTokenMs"] != int64(3000) || result["durationMs"] != int64(4000) || result["outputTps"] != float64(5) {
		t.Fatal(result)
	}
}

func TestCallTimingKeepsMissingOutputUnknown(t *testing.T) {
	start := time.Unix(100, 0)
	timing := CallTiming{Started: start}
	timing.Observe([]byte("data: {\"type\":\"response.created\"}\n\n"), true, start.Add(time.Second))
	result := map[string]any{}
	timing.Finish(result, nil, start.Add(2*time.Second))
	if _, ok := result["firstTokenMs"]; ok {
		t.Fatal(result)
	}
	if _, ok := result["outputTps"]; ok {
		t.Fatal(result)
	}
	for _, raw := range []string{`{"type":"response.output_text.delta","delta":"hello"}`, `{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"hmm"}}`, `{"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{}"}}]}}]}`} {
		if !HasCallOutput([]byte(raw)) {
			t.Fatal("missed actual output", raw)
		}
	}
}

func TestCallTimingNonStreamContentCanSpanWrites(t *testing.T) {
	start := time.Unix(100, 0)
	timing := CallTiming{Started: start}
	timing.Observe([]byte(`{"choices":[{"message":{"content":"he`), false, start.Add(time.Second))
	if timing.First != nil {
		t.Fatal("partial JSON counted as output")
	}
	timing.Observe([]byte(`llo"}}]}`), false, start.Add(2*time.Second))
	if timing.First == nil || !timing.First.Equal(start.Add(2*time.Second)) {
		t.Fatal("split JSON output not captured")
	}
}

func TestCallTimingHandlesMultilineDataEvent(t *testing.T) {
	start := time.Unix(100, 0)
	timing := CallTiming{Started: start}
	timing.Observe([]byte("data: {\"choices\":\n"), true, start.Add(time.Second))
	timing.Observe([]byte("data: [{\"delta\":{\"content\":\"hello\"}}]}\n\n"), true, start.Add(2*time.Second))
	if timing.First == nil {
		t.Fatal("multiline data event lost first token")
	}
}
