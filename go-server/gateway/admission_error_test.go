package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdmissionErrorsKeepSafeSpecificCause(t *testing.T) {
	for _, tc := range []struct{ code, want string }{
		{"ACCOUNT_CONCURRENCY_LIMIT", "Account inference concurrency limit reached"},
		{"API_KEY_CONCURRENCY_LIMIT", "NEWNOA API Key inference concurrency limit reached"},
		{"NODE_CONCURRENCY_LIMIT", "Go Server node inference concurrency limit reached"},
		{"MODEL_CONCURRENCY_LIMIT", "Model inference concurrency limit reached"},
		{"UPSTREAM_KEY_CONCURRENCY_LIMIT", "Upstream API Key concurrency limit reached"},
		{"POOL_ALLOCATION_LIMIT", "Credential pool allocation threshold reached"},
		{"POOL_QUEUE_FULL", "Credential pool waiting queue is full"},
		{"POOL_KEY_UNAVAILABLE", "No available upstream API Key for this caller"},
		{"ADMISSION_BUSY", "Concurrent admission in progress"},
		{"untrusted", "Too Many Requests"},
	} {
		for _, native := range []bool{false, true} {
			raw, _ := json.Marshal(map[string]any{"error": map[string]any{"code": tc.code, "message": "private-api-key-secret"}})
			w := httptest.NewRecorder()
			if native {
				controlFailure(&messagesWriter{w}, &controlError{status: 429, body: raw, retryAfter: "1"})
			} else {
				controlFailure(w, &controlError{status: 429, body: raw, retryAfter: "1"})
			}
			if w.Code != 429 || !strings.Contains(w.Body.String(), tc.want) || strings.Contains(w.Body.String(), "private-api-key-secret") || w.Header().Get("Retry-After") != "1" {
				t.Fatalf("code=%s native=%v status=%d body=%s", tc.code, native, w.Code, w.Body.String())
			}
		}
	}
	for _, body := range []string{`not-json`, `{"error":{"code":429}}`, `{"error":{"code":null}}`} {
		w := httptest.NewRecorder()
		controlFailure(w, &controlError{status: 429, body: []byte(body)})
		if !strings.Contains(w.Body.String(), "Too Many Requests") {
			t.Fatal(w.Body.String())
		}
	}
}
