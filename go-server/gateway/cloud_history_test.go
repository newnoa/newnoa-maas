package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestServerCloudHistoryUsesCallerPolicyAndBoundedResponse(t *testing.T) {
	for _, mode := range []string{"cloud", "local", "off"} {
		t.Run(mode, func(t *testing.T) {
			key, storage, requestID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			var saved map[string]any
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer caller-key" {
					t.Error("cloud history used an upstream or internal credential")
				}
				switch r.URL.Path {
				case "/api/v1/client/history/policy":
					json.NewEncoder(w).Encode(map[string]any{"mode": mode, "id": storage})
				case "/api/v1/client/history":
					json.NewDecoder(r.Body).Decode(&saved)
					saved = historyTestRecord(t, saved)
					json.NewEncoder(w).Encode(map[string]any{"saved": true})
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal-token", "server", nil)
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(""))
			r.Header.Set("X-Newnoa-Conversation-Id", "conversation")
			r.Header.Set("X-Newnoa-Agent", "Codex")
			response := httptest.NewRecorder()
			writer, finish := g.captureCloudHistory(response, r, "caller-key", key, requestID, []byte(`{"model":"private-online","messages":[{"role":"user","content":"question"}]}`))
			writer.Header().Set("Content-Type", "text/event-stream")
			writer.Write([]byte("data: [DONE]\n\n"))
			if err := http.NewResponseController(writer).Flush(); err != nil {
				t.Fatal(err)
			}
			finish()
			if response.Body.String() != "data: [DONE]\n\n" {
				t.Fatal("capture changed the downstream stream")
			}
			if mode != "cloud" {
				if saved != nil {
					t.Fatal("non-cloud conversation was uploaded")
				}
				return
			}
			if saved["id"] != requestID || saved["requestId"] != requestID || saved["storageId"] != storage || saved["keyId"] != key || saved["agent"] != "Codex" || saved["response"] != response.Body.String() {
				t.Fatalf("incorrect cloud capture: %#v", saved)
			}
		})
	}
}

func TestServerCloudHistoryCannotFollowRedirectForAudioPolicy(t *testing.T) {
	calls := 0
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Redirect(w, r, "https://example.invalid", 302)
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "server", nil)
	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	w, finish := g.captureCloudHistory(httptest.NewRecorder(), r, "caller", uuid.NewString(), uuid.NewString(), []byte(`{"model":"m","messages":[{"content":[{"type":"input_audio","input_audio":{"data":"omitted"}}]}]}`))
	w.WriteHeader(200)
	finish()
	if calls != 1 {
		t.Fatal("audio policy was not checked")
	}
	if err := g.historyRemote(context.Background(), "/policy", "caller", map[string]any{}, nil); err == nil {
		t.Fatal("accepted API redirect")
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}

func TestCloudHistoryWriterBoundsCopyAndPreservesNativeMessages(t *testing.T) {
	response := httptest.NewRecorder()
	w := &cloudHistoryWriter{ResponseWriter: response}
	data := strings.Repeat("x", cloudHistoryLimit+1)
	w.Write([]byte(data))
	if !w.truncated || w.body.Len() != cloudHistoryLimit || response.Body.Len() != len(data) {
		t.Fatal("history limit changed the response")
	}
}

func TestOnlyCoreManagedEasyTierSkipsServerCloudCapture(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	if coreCapturesHistory(r, "easytier-direct") {
		t.Fatal("direct SDK lost cloud history")
	}
	r.Header.Set("X-Newnoa-History-Capture", "core")
	if coreCapturesHistory(r, "easytier-direct") {
		t.Fatal("untrusted Core header suppressed Server capture")
	}
	if coreCapturesHistory(r, "peer-tunnel") || coreCapturesHistory(r, "direct") {
		t.Fatal("FRP or direct stopped Server history")
	}
}
