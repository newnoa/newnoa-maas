//go:build noa_pibolt

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNoaEmbeddedPiTurn(t *testing.T) {
	const key = "11111111-1111-4111-8111-111111111111"
	const model = "22222222-2222-4222-8222-222222222222"
	const secret = "nn_fixture_only_not_a_user_secret"
	var scope atomic.Value
	scope.Store("client")
	var executions atomic.Int32
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer owner-session" || r.Header.Get("X-Api-Key") != secret {
			t.Error("incorrect destination or binding")
			w.WriteHeader(403)
			return
		}
		var body struct {
			Template struct {
				Effort string `json:"reasoning_effort"`
			} `json:"chat_template_kwargs"`
			Model    string
			Messages []map[string]any
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Model != model || len(body.Messages) < 2 {
			t.Error("missing skill prompt, messages or deployment")
			w.WriteHeader(400)
			return
		}
		if body.Template.Effort != "low" {
			t.Error("model chat template did not receive low reasoning effort")
		}
		system, _ := body.Messages[0]["content"].(string)
		if !strings.Contains(system, "判断服务边界") {
			t.Error("skill missing from model system prompt")
		}
		content := fmt.Sprintf(`{"scope":%q,"relation":"new"}`, scope.Load().(string))
		if strings.Contains(system, "## 返回宿主决策") {
			executions.Add(1)
			submitted, _ := json.Marshal(body.Messages[1:])
			if strings.Contains(string(submitted), "OLD_CONTEXT_SENTINEL") {
				t.Error("new topic received old conversation")
			}
			content = `{"action":"reply","text":"NOA_FIXTURE_OK"}`
		}
		w.Header().Set("Content-Type", "text/event-stream")
		delta, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"role": "assistant", "content": content}}}})
		fmt.Fprintf(w, "data: %s\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", delta)
	}))
	defer gateway.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer owner-session" {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/api/v1/me/model-tests/keys":
			json.NewEncoder(w).Encode([]any{map[string]any{"id": key, "systemKind": "charity", "models": []any{map[string]any{"id": model, "model": noaModel, "modelType": "llm", "online": true}}}})
		case "/api/v1/me/api-keys/" + key + "/copy-secret":
			json.NewEncoder(w).Encode(map[string]string{"secret": secret})
		default:
			t.Error("unexpected API call", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), gateway.URL)
	c.control = api.URL
	c.gatewaySelector = nil
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	binding, err := c.resolveNoaBinding(ctx, "owner-session")
	if err != nil {
		t.Fatal(err)
	}
	answer, err := c.runNoa(ctx, "owner-session", binding, []noaMessage{{Role: "user", Content: "OLD_CONTEXT_SENTINEL"}, {Role: "assistant", Content: "Previous reply"}, {Role: "user", Content: "Hello"}})
	if err != nil || !strings.Contains(answer, "NOA_FIXTURE_OK") {
		t.Fatalf("embedded turn failed: %v; response length %d", err, len(answer))
	}
	for _, denied := range []string{"outside", "internal"} {
		scope.Store(denied)
		answer, err = c.runNoa(ctx, "owner-session", binding, []noaMessage{{Role: "user", Content: "Hello"}})
		if err != nil || !strings.Contains(answer, `"action":"decline"`) {
			t.Errorf("expected refusal for %s: %v", denied, err)
		}
	}
	if executions.Load() != 1 {
		t.Error("out-of-scope decision reached execution stage")
	}
	if _, err := c.resolveNoaBinding(ctx, "other-session"); err == nil {
		t.Fatal("foreign session accepted")
	}
}
