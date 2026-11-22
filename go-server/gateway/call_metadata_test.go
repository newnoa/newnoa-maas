package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInferencePersistsCallerReasoningIPAndPerformance(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()
	var reserved, finished map[string]any
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			io.WriteString(w, `{"model":"fixture"}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewDecoder(r.Body).Decode(&reserved)
			json.NewEncoder(w).Encode(map[string]any{"requestId": "request-test", "permit": "permit", "endpoint": upstream.URL})
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			json.NewDecoder(r.Body).Decode(&finished)
			io.WriteString(w, `{}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	defer api.Close()
	g := newGateway(api.URL, "secret-internal", "fixture", []string{"127.0.0.1"})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hi"}],"stream":true,"reasoning_effort":"high"}`))
	req.Header.Set("Authorization", "Bearer ses_fixture")
	req.Header.Set("X-Api-Key", "nn_fixture")
	req.Header.Set("User-Agent", "codex_cli_rs/1.0")
	req.RemoteAddr = "[2001:db8::1]:1234"
	res := httptest.NewRecorder()
	g.ServeHTTP(res, req)
	if res.Code != 200 || reserved["callerSource"] != "codex" || reserved["reasoningEffort"] != "high" || reserved["clientIp"] != "2001:db8::1" {
		t.Fatalf("metadata missing: %d %v", res.Code, reserved)
	}
	for _, key := range []string{"durationMs", "firstTokenMs", "outputTps"} {
		if _, ok := finished[key]; !ok {
			t.Fatalf("%s missing: %v", key, finished)
		}
	}
	if finished["status"] != "succeeded" {
		t.Fatal(finished)
	}
}
