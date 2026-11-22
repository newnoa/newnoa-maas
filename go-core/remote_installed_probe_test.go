package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Exercises the installed native CLI using a synthetic model and isolated home.
func TestRemoteClaudeInstalledProbe(t *testing.T) {
	if os.Getenv("NEWNOA_TEST_INSTALLED_CLAUDE") != "1" {
		t.Skip("requires installed supported Claude Code")
	}
	const reference = "nref_claude_native_fixture_0123456789"
	var answered atomic.Bool
	var resumed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/messages" || (r.Header.Get("X-Api-Key") != reference && r.Header.Get("Authorization") != "Bearer "+reference && r.Header.Get("X-Api-Key") != nativeTurnSecret && r.Header.Get("Authorization") != "Bearer "+nativeTurnSecret) {
			http.Error(w, "unrecognized fixture request", 403)
			return
		}
		var body struct {
			Model    string
			Messages json.RawMessage
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil || body.Model != "fixture-model" || !strings.Contains(string(body.Messages), "NEWNOA_REMOTE_OK") {
			http.Error(w, "incorrect fixture request", 400)
			return
		}
		if strings.Contains(string(body.Messages), "RESUME_MARK") && strings.Contains(string(body.Messages), "FIRST_MARK") {
			resumed.Store(true)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range []string{
			`{"type":"message_start","message":{"id":"msg_fixture","type":"message","role":"assistant","model":"fixture-model","content":[],"stop_reason":null,"usage":{"input_tokens":4,"output_tokens":0}}}`,
			`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"NEWNOA_REMOTE_OK"}}`,
			`{"type":"content_block_stop","index":0}`,
			`{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":3}}`,
			`{"type":"message_stop"}`,
		} {
			var header struct{ Type string }
			_ = json.Unmarshal([]byte(event), &header)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", header.Type, event)
		}
		answered.Store(true)
	}))
	defer server.Close()
	home := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, remoteSiteLaunch{Home: home, Secret: reference, BaseURL: server.URL, Alias: "fixture-model"})
	info, err := probeRemoteProvider(ctx, "claude-cli", home)
	if err != nil || !answered.Load() || !info.Capabilities["resume"] {
		t.Fatalf("native Claude probe: responded=%t, error=%v", answered.Load(), err)
	}
	verifyNativeTurns(t, ctx, "claude-cli", home, &resumed)
}

// This native validation is opt-in on test hosts with a pinned Codex binary.
func TestRemoteCodexInstalledProbe(t *testing.T) {
	if os.Getenv("NEWNOA_TEST_REMOTE_CODEX") != "1" {
		t.Skip("requires an installed, supported Codex CLI")
	}
	const reference = "nref_codex_native_fixture_0123456789"
	var answered atomic.Bool
	var resumed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" || (r.Header.Get("Authorization") != "Bearer "+reference && r.Header.Get("Authorization") != "Bearer "+nativeTurnSecret) {
			http.Error(w, "unrecognized fixture request", 403)
			return
		}
		var body struct {
			Model string
			Input json.RawMessage
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil || body.Model != "fixture-model" || !strings.Contains(string(body.Input), "NEWNOA_REMOTE_OK") {
			http.Error(w, "incorrect fixture request", 400)
			return
		}
		if strings.Contains(string(body.Input), "RESUME_MARK") && strings.Contains(string(body.Input), "FIRST_MARK") {
			resumed.Store(true)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, event := range []string{
			`{"type":"response.created","response":{"id":"resp_fixture","status":"in_progress","output":[]}}`,
			`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","role":"assistant","id":"msg_fixture","content":[]}}`,
			`{"type":"response.output_text.delta","item_id":"msg_fixture","output_index":0,"content_index":0,"delta":"NEWNOA_REMOTE_OK"}`,
			`{"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","id":"msg_fixture","content":[{"type":"output_text","text":"NEWNOA_REMOTE_OK"}]}}`,
			`{"type":"response.completed","response":{"id":"resp_fixture","status":"completed","output":[],"usage":{"input_tokens":4,"output_tokens":3,"total_tokens":7}}}`,
		} {
			var header struct{ Type string }
			if err := json.Unmarshal([]byte(event), &header); err != nil {
				t.Error(err)
				return
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", header.Type, event)
		}
		answered.Store(true)
	}))
	defer server.Close()
	home := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, remoteSiteLaunch{Home: home, Secret: reference, BaseURL: server.URL, Alias: "fixture-model"})
	info, err := probeRemoteProvider(ctx, "codex", home)
	if err != nil || !answered.Load() || !info.Capabilities["resume"] {
		t.Fatalf("native Codex probe: responded=%t, error=%v", answered.Load(), err)
	}
	verifyNativeTurns(t, ctx, "codex", home, &resumed)
}

const nativeTurnSecret = "rla_native_turn_fixture_0123456789"

func verifyNativeTurns(t *testing.T, ctx context.Context, agent, home string, resumed *atomic.Bool) {
	t.Helper()
	site := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch)
	site.Secret = nativeTurnSecret
	ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, site)
	ctx = context.WithValue(ctx, remoteProviderStateDirKey{}, home)
	ctx = context.WithValue(ctx, remoteProviderSessionSlotKey{}, "native-turn")
	cwd := filepath.Join(home, "project")
	if err := os.Mkdir(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	var nativeID string
	for _, mark := range []string{"FIRST_MARK", "RESUME_MARK"} {
		events := make(chan remoteProviderEvent, 128)
		p, err := startRemoteProvider(ctx, agent, cwd, nativeID, func(e remoteProviderEvent) { events <- e })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		id := p.(*remoteNativeProvider).nativeID
		if id == "" || nativeID != "" && nativeID != id {
			t.Fatal("native session identity changed")
		}
		nativeID = id
		operation := "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
		if mark == "RESUME_MARK" {
			operation = "aaaaaaaa-bbbb-4ccc-8ddd-ffffffffffff"
		}
		if err := p.StartTurn(ctx, remoteProviderInput{Text: mark + " Reply exactly NEWNOA_REMOTE_OK.", OperationID: operation}); err != nil {
			t.Fatal(err)
		}
		var output strings.Builder
		accepted := false
	receive:
		for {
			select {
			case e := <-events:
				switch e.Type {
				case "output":
					output.WriteString(e.Text)
				case "dispatch":
					if e.OperationID == operation && e.Delivery == "providerAccepted" {
						accepted = true
					}
				case "state":
					if e.State == "idle" && accepted {
						break receive
					}
				case "error", "exited":
					t.Fatalf("%s native turn ended before confirmation: %s", mark, e.Type)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		if output.String() != "NEWNOA_REMOTE_OK" {
			t.Fatal("missing expected native response")
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !resumed.Load() {
		t.Fatal("resumed context missing prior turn")
	}
}
