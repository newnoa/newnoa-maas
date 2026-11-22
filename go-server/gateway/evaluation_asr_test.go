package main

import "testing"

func TestEvaluationTranscriptionRequiresKnownUsage(t *testing.T) {
	for _, body := range []string{
		`{"text":""}`, `{"text":null,"usage":{"prompt_tokens":1,"completion_tokens":0,"total_tokens":1}}`,
		`{"text":"","usage":{"prompt_tokens":1,"completion_tokens":0}}`,
		`{"text":"","usage":{"prompt_tokens":1,"completion_tokens":0,"total_tokens":2}}`,
		`{"text":"","usage":{"prompt_tokens":true,"completion_tokens":0,"total_tokens":1}}`,
		`{"text":"","usage":{"prompt_tokens":65,"completion_tokens":0,"total_tokens":65}}`,
	} {
		if _, _, ok := evaluationTranscriptionUsage([]byte(body), 64); ok {
			t.Fatalf("invalid ASR evidence accepted: %s", body)
		}
	}
	for _, text := range []string{`""`, `"hello"`} {
		input, output, ok := evaluationTranscriptionUsage([]byte(`{"text":`+text+`,"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`), 64)
		if !ok || input != 3 || output != 1 {
			t.Fatal("valid ASR protocol rejected")
		}
	}
}
