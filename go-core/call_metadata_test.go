package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLocalHistoryCarriesCallMetadataAndTiming(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	historyPolicyFixture(t, h, "local")
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","reasoning_effort":"high"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "codex_cli_rs/1.0")
	req.RemoteAddr = "[2001:db8::1]:1234"
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	})
	h.capture(next, "account", "key").ServeHTTP(httptest.NewRecorder(), req)
	rows, err := h.list("account", "", 10)
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	value, err := h.get("account", rows[0]["id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []map[string]any{value, rows[0]} {
		if row["callerSource"] != "codex" || row["reasoningEffort"] != "high" || row["clientIp"] != "2001:db8::1" {
			t.Fatal(row)
		}
		for _, key := range []string{"firstTokenMs", "durationMs", "outputTps"} {
			if row[key] == nil {
				t.Fatalf("missing %s: %v", key, row)
			}
		}
	}
	payload := historyCapturePayload(value)
	if payload["callerSource"] != "codex" || payload["clientIp"] != "2001:db8::1" {
		t.Fatal("sync payload lost metadata", payload)
	}
}
