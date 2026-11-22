package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestPrivateHostedRequestAuthorizesMetersAndCapturesOnServer(t *testing.T) {
	for _, tc := range []struct {
		name     string
		headers  []string
		metadata any
	}{{"normal", nil, nil}, {"duplicate", []string{"a", "b"}, nil}, {"oversized", []string{strings.Repeat("x", 201)}, nil}, {"conflicting", []string{"a"}, "b"}, {"non-string", nil, 42}} {
		t.Run(tc.name, func(t *testing.T) { privateHostedCapture(t, tc.headers, tc.metadata) })
	}
}
func privateHostedCapture(t *testing.T, conversationHeaders []string, conversationMetadata any) {
	requestID, keyID, storage := uuid.NewString(), uuid.NewString(), uuid.NewString()
	upstreamCalls, consumes, captures, finalized := 0, 0, 0, 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls++
		if r.Header.Get("Authorization") != "Bearer upstream-only" {
			t.Error("upstream received caller identity")
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["model"] != "actual-upstream-model" {
			t.Error(body)
		}
		io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"hosted answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":3}}`)
	}))
	defer upstream.Close()
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if strings.HasPrefix(r.URL.Path, "/api/v1/internal/") {
			if r.Header.Get("Authorization") != "Bearer internal-only" {
				t.Error("internal authentication lost")
			}
		} else if r.Header.Get("Authorization") != "Bearer caller-only" {
			t.Error("history credential changed")
		}
		switch r.URL.Path {
		case "/api/v1/internal/authorize":
			if body["secret"] != "caller-only" {
				t.Error("caller not authorized")
			}
			json.NewEncoder(w).Encode(map[string]any{"model": "actual-upstream-model", "history": map[string]any{"enabled": true, "keyId": keyID, "capturePublisher": true}})
		case "/api/v1/internal/dispatches":
			json.NewEncoder(w).Encode(map[string]any{"requestId": requestID, "endpoint": upstream.URL, "apiKey": "upstream-only", "permit": "fixture-permit"})
		case "/api/v1/internal/dispatches/" + requestID + "/consume":
			consumes++
			io.WriteString(w, `{}`)
		case "/api/v1/internal/requests/" + requestID + "/transport":
			io.WriteString(w, `{}`)
		case "/api/v1/internal/requests/" + requestID + "/finalize":
			finalized++
			body = meterFixtureSelectedEvidence(r, body)
			if body["status"] != "succeeded" || body["inputTokens"] != float64(12) || body["outputTokens"] != float64(3) {
				t.Errorf("usage lost: %#v", body)
			}
			io.WriteString(w, `{}`)
		case "/api/v1/client/history/policy":
			json.NewEncoder(w).Encode(map[string]any{"id": storage, "mode": "cloud", "capturePublisher": true})
		case "/api/v1/client/history":
			body = historyTestRecord(t, body)
			captures++
			if body["id"] != requestID || body["requestId"] != requestID || !strings.Contains(body["response"].(string), "hosted answer") || !strings.Contains(body["request"].(string), "private-hosted") {
				t.Error("cloud payload missing original request or response")
			}
			io.WriteString(w, `{"saved":true}`)
		default:
			t.Errorf("unexpected %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal-only", "fixture", []string{"127.0.0.1"})
	requestBody := map[string]any{"model": "private-hosted", "messages": []any{map[string]any{"role": "user", "content": "hello"}}, "max_tokens": 8}
	if conversationMetadata != nil {
		requestBody["metadata"] = map[string]any{"conversation_id": conversationMetadata}
	}
	raw, _ := json.Marshal(requestBody)
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw)))
	for _, value := range conversationHeaders {
		r.Header.Add("X-Newnoa-Conversation-Id", value)
	}
	r.Header.Set("X-Api-Key", "caller-only")
	r.Header.Set("Authorization", "Bearer ses_client_login")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 200 || upstreamCalls != 1 || consumes != 1 || finalized != 1 || captures != 1 || w.Header().Get("X-Newnoa-Request") != requestID {
		t.Fatalf("status=%d upstream=%d consume=%d finalized=%d cloud=%d body=%s", w.Code, upstreamCalls, consumes, finalized, captures, w.Body.String())
	}
}
