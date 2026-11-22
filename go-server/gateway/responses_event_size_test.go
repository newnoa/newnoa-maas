package main

import (
	"strings"
	"testing"
)

func TestResponsesLargeCompletionEvidence(t *testing.T) {
	const limit = 8 * maxBody
	for _, tc := range []struct {
		name, payload string
		chunk         int
		complete      bool
	}{
		{"tool schemas", `{"type":"response.completed","response":{"status":"completed","tools":[{"description":"` + strings.Repeat("x", 128*1024) + `"}],"usage":{"input_tokens":2,"output_tokens":3}}}`, 32 * 1024, true},
		{"large output", `{"type":"response.completed","response":{"status":"completed","output":[{"text":"` + strings.Repeat("x", 256*1024) + `"}],"usage":{"input_tokens":2,"output_tokens":3}}}`, 4093, true},
		{"oversized event", `{"type":"response.completed","response":{"status":"completed","output":"` + strings.Repeat("x", limit) + `"}}`, 32 * 1024, false},
		{"malformed large event", `{"type":"response.completed","response":{"status":"completed","output":"` + strings.Repeat("x", 128*1024), 32 * 1024, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := evidence{responses: true}
			wire := "event: response.completed\ndata: " + tc.payload + "\n\n"
			for start := 0; start < len(wire); start += tc.chunk {
				e.feed([]byte(wire[start:min(start+tc.chunk, len(wire))]))
			}
			if e.complete() != tc.complete {
				t.Fatalf("complete=%v invalid=%v, want complete=%v", e.complete(), e.invalid, tc.complete)
			}
			if tc.complete && (e.input == nil || *e.input != 2 || e.output == nil || *e.output != 3) {
				t.Fatal("large event lost usage")
			}
			if !tc.complete && (!e.invalid || len(e.pending)+len(e.event) != 0) {
				t.Fatal("invalid event retained")
			}
		})
	}
}
