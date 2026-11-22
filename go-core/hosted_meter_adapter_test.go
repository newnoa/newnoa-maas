package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestHostedMeterUnsupportedJSONPreservesActualUsageWithoutInventingCache(t *testing.T) {
	for _, protocol := range []string{"/v1/embeddings", "/v1/rerank", "/v1/systemone", imageGenerationPath} {
		t.Run(protocol, func(t *testing.T) {
			m, err := newHostedMeter("fixture", protocol, []byte(`{"model":"fixture"}`))
			if err != nil {
				t.Fatal(err)
			}
			m.response(false, "application/json")
			body := []byte(`{"data":[],"results":[],"usage":{"input_tokens":12,"output_tokens":3}}`)
			ready, err := m.feed(body)
			if err != nil || len(ready) != 0 {
				t.Fatal("unsupported JSON released before completeness", err)
			}
			ready, err = m.finish()
			if err != nil || string(ready) != string(body) {
				t.Fatal("existing JSON protocol rejected/changed", err)
			}
			s := m.snapshot()
			if !s.ResponseStarted || !s.TerminalUsage || s.EstimatedInputTokens != nil || s.EstimatedOutputTokens != nil || s.Reported == nil || s.Reported.InputTokens == nil || *s.Reported.InputTokens != 12 || s.Reported.OutputTokens == nil || *s.Reported.OutputTokens != 3 || s.Reported.CachedInputTokens != nil || s.Reported.CacheWriteTokens != nil {
				t.Fatalf("fabricated media/cache estimates: %+v", s)
			}
		})
	}
}

func TestHostedMeterPlainTranscriptionStaysUnavailableWithoutZero(t *testing.T) {
	m, err := newHostedMeter("fixture", "/v1/audio/transcriptions", []byte("[TEST] binary multipart not a JSON prompt"))
	if err != nil {
		t.Fatal(err)
	}
	m.response(false, "text/plain")
	body := []byte("[TEST] transcript")
	ready, err := m.feed(body)
	if err != nil || string(ready) != string(body) {
		t.Fatal("text ASR regression", err)
	}
	if _, err = m.finish(); err != nil {
		t.Fatal(err)
	}
	s := m.snapshot()
	if !s.ResponseStarted || s.EstimatedInputTokens != nil || s.EstimatedOutputTokens != nil || s.Reported != nil || s.TerminalUsage {
		t.Fatalf("unsupported media billed as text/zero: %+v", s)
	}
	raw, _ := json.Marshal(s)
	if strings.Contains(string(raw), "transcript") || strings.Contains(string(raw), "multipart") {
		t.Fatal("snapshot contains plaintext")
	}
}
