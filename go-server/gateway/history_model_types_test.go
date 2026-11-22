package main

import (
	"encoding/json"
	"testing"
)

func TestHistoryPreservesEveryAuthorizedModelType(t *testing.T) {
	for _, kind := range []string{"llm", "decision", "embedding", "reranking", "asr", "recommendation", "predictive", "world", "other"} {
		if got := historyModelType(kind); got != kind {
			t.Errorf("authorized model type %q became %q", kind, got)
		}
	}
	for _, kind := range []string{"", "invented", "llm\nforged"} {
		if got := historyModelType(kind); got != "" {
			t.Errorf("untrusted model type %q accepted", kind)
		}
	}
}

func TestSanitizedNativeAttachmentsExplicitlyMarkMissingContent(t *testing.T) {
	_, captured, _, err := historyPayload([]byte(`{"messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"cHJpdmF0ZQ==","format":"wav"}},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"cHJpdmF0ZQ=="}}]}]}`), "application/json")
	if err != nil {
		t.Fatal(err)
	}
	var saved map[string]any
	if err := json.Unmarshal(captured, &saved); err != nil {
		t.Fatal(err)
	}
	for _, value := range saved["messages"].([]any)[0].(map[string]any)["content"].([]any) {
		if value.(map[string]any)["contentSaved"] != false {
			t.Fatalf("sanitized attachment falsely appears saved: %s", captured)
		}
	}
}
