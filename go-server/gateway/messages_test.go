package main

import (
	"strings"
	"testing"
)

const nativeStart = `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"usage":{"input_tokens":2,"cache_creation_input_tokens":3,"cache_read_input_tokens":5,"output_tokens":0}}}`
const nativeDelta = `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}`

func nativeSSE(events ...string) string {
	var out string
	for _, event := range events {
		out += "data: " + event + "\n\n"
	}
	return out
}
func TestNativeMessagesStreamEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		complete   bool
	}{
		{"complete", nativeSSE(nativeStart, nativeDelta, `{"type":"message_stop"}`), true},
		{"truncated", nativeSSE(nativeStart, nativeDelta), false},
		{"wrong-terminal", nativeSSE(nativeStart, nativeDelta, "[DONE]"), false},
		{"stop-without-start", nativeSSE(`{"type":"message_stop"}`), false},
		{"stop-without-usage", nativeSSE(nativeStart, `{"type":"message_stop"}`), false},
		{"error", nativeSSE(nativeStart, `{"type":"error","error":{"message":"private"}}`), false},
		{"negative", nativeSSE(nativeStart, strings.Replace(nativeDelta, `"output_tokens":7`, `"output_tokens":-1`, 1), `{"type":"message_stop"}`), false},
		{"after-stop", nativeSSE(nativeStart, nativeDelta, `{"type":"message_stop"}`, nativeDelta), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := evidence{anthropic: true}
			for _, b := range []byte(tc.data) {
				e.feed([]byte{b})
			}
			if e.complete() != tc.complete {
				t.Fatalf("complete=%v invalid=%v", e.complete(), e.invalid)
			}
			if tc.complete && (*e.input != 10 || *e.output != 7) {
				t.Fatal("cache totals or cumulative output wrong")
			}
		})
	}
}
func TestNativeMessagesResponseValidation(t *testing.T) {
	good := `{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"cache_read_input_tokens":5,"output_tokens":7}}`
	e := evidence{anthropic: true}
	if !e.messagesResponse([]byte(good)) || *e.input != 7 || *e.output != 7 {
		t.Fatal("valid native response rejected")
	}
	for _, bad := range []string{`{}`, strings.Replace(good, `"end_turn"`, `null`, 1), strings.Replace(good, `"input_tokens":2`, `"input_tokens":-1`, 1), strings.Replace(good, `"output_tokens":7`, `"output_tokens":0.5`, 1), strings.Replace(good, `"role":"assistant"`, `"role":"user"`, 1)} {
		if (&evidence{anthropic: true}).messagesResponse([]byte(bad)) {
			t.Fatal("invalid native response accepted", bad)
		}
	}
}
func TestNativeUsageDeltasAreCumulativeAndRetainOmittedCacheFields(t *testing.T) {
	e := evidence{anthropic: true}
	e.feed([]byte(nativeSSE(nativeStart,
		`{"type":"message_delta","delta":{},"usage":{"output_tokens":3}}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"input_tokens":4,"output_tokens":7}}`,
		`{"type":"message_stop"}`)))
	if !e.complete() || *e.input != 12 || *e.output != 7 {
		t.Fatal("delta totals were added or cache fields lost", e.input, e.output)
	}
}

func TestNativeContentBlockLifecycle(t *testing.T) {
	start := `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool_1","name":"Read","input":{}}}`
	delta := `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}`
	stop := `{"type":"content_block_stop","index":0}`
	for _, tc := range []struct {
		name   string
		blocks []string
		valid  bool
	}{
		{"tool", []string{start, delta, stop}, true},
		{"open-block", []string{start, delta}, false},
		{"delta-without-start", []string{delta}, false},
		{"stop-without-start", []string{stop}, false},
		{"duplicate-start", []string{start, start, stop}, false},
		{"duplicate-stop", []string{start, stop, stop}, false},
		{"reuse-index", []string{start, stop, start, stop}, false},
		{"wrong-index", []string{start, strings.Replace(delta, `"index":0`, `"index":1`, 1), stop}, false},
		{"missing-index", []string{strings.Replace(start, `"index":0,`, "", 1), stop}, false},
		{"negative-index", []string{strings.Replace(start, `"index":0`, `"index":-1`, 1), stop}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := evidence{anthropic: true}
			events := append([]string{nativeStart}, tc.blocks...)
			events = append(events, nativeDelta, `{"type":"message_stop"}`)
			e.feed([]byte(nativeSSE(events...)))
			if e.complete() != tc.valid {
				t.Fatalf("complete=%v want=%v", e.complete(), tc.valid)
			}
		})
	}
}
