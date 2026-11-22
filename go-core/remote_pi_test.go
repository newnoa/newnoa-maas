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

func TestRemotePiManagedSelectionChecksBothFilesAndAccount(t *testing.T) {
	c, _, api := remoteAuthorizationFixture(t)
	t.Setenv("PI_CODING_AGENT_DIR", "")
	pr, err := c.previewWithAPIKey("pi-agent", "22222222-2222-4222-8222-222222222222", "nn_site_authorized_fixture_123456789")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	s, err := c.remoteSiteSelection(context.Background(), "rpc-session", "pi-agent")
	if err != nil || s.Model != "22222222-2222-4222-8222-222222222222" || s.KeyID == "" {
		t.Fatalf("invalid managed selection: %v", err)
	}
	api.mu.Lock()
	api.siteChange = "revoked"
	api.mu.Unlock()
	if _, err = c.remoteSiteSelection(context.Background(), "rpc-session", "pi-agent"); err == nil || err.Error() != remoteSiteKeyUnavailable {
		t.Fatalf("revoked key: %v", err)
	}
	if err := os.WriteFile(filepath.Join(c.home, ".pi", "agent", "settings.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = c.remoteSiteSelection(context.Background(), "rpc-session", "pi-agent"); err == nil || err.Error() != remoteSiteConfigurationChanged {
		t.Fatalf("edited settings: %v", err)
	}
}

func TestRemotePiProtocolAuthorizationAndTurns(t *testing.T) {
	remoteProtocolExecutable(t, "pi", "ok")
	c, m, _ := remoteAuthorizationFixture(t)
	t.Setenv("PI_CODING_AGENT_DIR", "")
	pr, err := c.previewWithAPIKey("pi-agent", "22222222-2222-4222-8222-222222222222", "nn_site_authorized_fixture_123456789")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	if _, err = callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "pi-agent"}); err != nil {
		t.Fatal(err)
	}
	a := waitRemoteAuthorization(t, m, "pi-agent")
	if !a.Authorized || a.Capabilities["modelChange"] || a.Capabilities["approvals"] {
		t.Fatalf("unexpected authorization result: %+v", a)
	}
	create := remoteEnvelope("pi-create", "session.create", "", map[string]any{"agent": "pi-agent", "cwd": m.home})
	create.ExpectedHostEpoch = m.epoch
	if _, err = callRemoteRPC(t, c, "remote.command", map[string]any{"command": create}); err != nil {
		t.Fatal(err)
	}
	receipt := waitRemote(t, m, create.OperationID)
	if receipt.Status != "providerAccepted" {
		t.Fatalf("create: %+v", receipt)
	}
	send := remoteEnvelope("pi-send", "message.send", receipt.SessionID, map[string]any{"text": "second turn"})
	send.ExpectedHostEpoch = m.epoch
	if _, err = callRemoteRPC(t, c, "remote.command", map[string]any{"command": send}); err != nil {
		t.Fatal(err)
	}
	if r := waitRemote(t, m, send.OperationID); r.Status != "providerAccepted" {
		t.Fatalf("send: %+v", r)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		s := m.sessions[receipt.SessionID]
		ok := s.State == "idle" && s.NativeID != "" && len(s.Messages) == 2 && s.Messages[1].Text == "NEWNOA_REMOTE_OK"
		m.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("missing native stream output or settled state")
}

// Run only with the installed, version-pinned Pi CLI. The receiver is a
// synthetic streaming model on loopback, not a live account or a health check.
func TestRemotePiInstalledLifecycle(t *testing.T) {
	if os.Getenv("NEWNOA_TEST_INSTALLED_PI") != "1" {
		t.Skip("requires installed Pi 1.5.2")
	}
	testRemoteInstalledLifecycle(t, "pi-agent")
}

func TestRemoteOpenClawInstalledLifecycle(t *testing.T) {
	if os.Getenv("NEWNOA_TEST_INSTALLED_OPENCLAW") != "1" {
		t.Skip("requires installed OpenClaw 2026.11.4")
	}
	testRemoteInstalledLifecycle(t, "openclaw")
}

func testRemoteInstalledLifecycle(t *testing.T, agent string) {
	t.Helper()
	var requests atomic.Int32
	var first, stop, resumed atomic.Bool
	var restoredContext atomic.Bool
	slow := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"object":"list","data":[{"id":"pi-bound-model","object":"model"}]}`)
			return
		}
		if agent == "hermes-agent" && r.URL.Path != "/v1/chat/completions" {
			// The native client probes alternate metadata protocols. As with Core,
			// the fixture exposes no such route; it must never serve inference there.
			http.NotFound(w, r)
			return
		}
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer rla_pi_native_fixture_only_0123456789" {
			t.Errorf("%s model path/credential mismatch: path=%s authPresent=%t authMatches=%t", agent, r.URL.Path, r.Header.Get("Authorization") != "", r.Header.Get("Authorization") == "Bearer rla_pi_native_fixture_only_0123456789")
			w.WriteHeader(400)
			return
		}
		var req struct {
			Model    string            `json:"model"`
			Stream   bool              `json:"stream"`
			Tools    []json.RawMessage `json:"tools"`
			Messages []struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
			} `json:"messages"`
		}
		if json.NewDecoder(r.Body).Decode(&req) != nil || req.Model != "pi-bound-model" || len(req.Messages) == 0 {
			t.Errorf("%s request did not bind the configured model", agent)
			w.WriteHeader(400)
			return
		}
		if agent == "hermes-agent" && len(req.Tools) != 0 {
			t.Errorf("Hermes exported %d tools without a verified permission flow", len(req.Tools))
		}
		ordinal := requests.Add(1)
		n := int32(0)
		if agent == "openclaw" {
			// Fixed native bridge fixture: no auxiliary requests; each of the
			// three separate user submissions has exactly one outbound request.
			n = ordinal
		} else {
			for i := len(req.Messages) - 1; i >= 0; i-- {
				row := req.Messages[i]
				if row.Role != "user" {
					continue
				}
				var plain string
				_ = json.Unmarshal(row.Content, &plain)
				var parts []struct {
					Text string `json:"text"`
				}
				if plain == "" && json.Unmarshal(row.Content, &parts) == nil {
					for _, p := range parts {
						plain += p.Text
					}
				}
				for j, marker := range []string{"NEWNOA_LIFECYCLE_FIRST", "NEWNOA_LIFECYCLE_STOP", "NEWNOA_LIFECYCLE_RESUMED"} {
					// Title/summarization jobs may quote the same user turn in their own prompt.
					if strings.TrimSpace(plain) == marker || (req.Stream && strings.Contains(plain, marker)) {
						n = int32(j + 1)
						break
					}
				}
				break
			}
		}
		if n == 1 {
			first.Store(true)
		}
		if n == 2 {
			stop.Store(true)
		}
		if n == 3 {
			resumed.Store(true)
			for _, row := range req.Messages {
				if row.Role == "assistant" && strings.Contains(string(row.Content), "Pi fixture reply") {
					restoredContext.Store(true)
				}
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher := w.(http.Flusher)
		if n == 2 {
			flusher.Flush()
			select {
			case slow <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
				select {
				case stopped <- struct{}{}:
				default:
				}
			case <-time.After(25 * time.Second):
				t.Error("native abort did not cancel model transport")
			}
			return
		}

		if !req.Stream && agent == "hermes-agent" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"id":"fixture","object":"chat.completion","created":1,"model":"pi-bound-model","choices":[{"index":0,"message":{"role":"assistant","content":"Pi fixture reply"},"finish_reason":"stop"}]}`)
			return
		}

		fmt.Fprint(w, "data: {\"id\":\"fixture\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"pi-bound-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Pi fixture reply\"},\"finish_reason\":null}]}\n\n")
		fmt.Fprint(w, "data: {\"id\":\"fixture\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"pi-bound-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
		flusher.Flush()
	}))
	defer server.Close()
	home := t.TempDir()
	cwd := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, remoteProviderStateDirKey{}, home)
	ctx = context.WithValue(ctx, remoteProviderSessionSlotKey{}, "fixture-session")
	ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, remoteSiteLaunch{Home: home, Alias: "pi-bound-model", BaseURL: server.URL, Secret: "rla_pi_native_fixture_only_0123456789", ContextLength: 131072})
	events := make(chan remoteProviderEvent, 64)
	start := func(id string) remoteProvider {
		p, err := startRemoteProvider(ctx, agent, cwd, id, func(e remoteProviderEvent) { events <- e })
		if err != nil {
			t.Fatalf("%s startup (resume=%t): %v", agent, id != "", err)
		}
		t.Cleanup(func() { _ = p.Close() })
		return p
	}
	wait := func(kind, state string) remoteProviderEvent {
		t.Helper()
		for {
			select {
			case <-ctx.Done():
				t.Fatalf("missing Pi event %s %s", kind, state)
			case e := <-events:
				if e.Type == "error" || e.Type == "exited" {
					t.Fatalf("Pi failed: %+v", e)
				}
				if e.Type == kind && (state == "" || e.State == state) {
					return e
				}
			}
		}
	}
	p := start("")
	id := wait("session", "").NativeID
	if err := p.StartTurn(ctx, remoteProviderInput{Text: "NEWNOA_LIFECYCLE_FIRST", OperationID: "first"}); err != nil {
		t.Fatal(err)
	}
	if e := wait("output", ""); e.Text != "Pi fixture reply" {
		t.Fatalf("unexpected output: %+v", e)
	}
	wait("state", "idle")
	if err := p.StartTurn(ctx, remoteProviderInput{Text: "NEWNOA_LIFECYCLE_STOP", OperationID: "stop"}); err != nil && !(remoteACPAgent(remoteProviderBinary(agent)) && isRemoteDispatchUnknown(err)) {
		t.Fatal(err)
	}
	select {
	case <-slow:
	case <-ctx.Done():
		t.Fatalf("%s second turn did not reach fixture: requests=%d first=%t", agent, requests.Load(), first.Load())
	}
	if err := p.Stop(ctx); err != nil {
		t.Fatalf("native stop: %v", err)
	}
	if agent != "hermes-agent" {
		wait("state", "stopped")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("model transport did not close with native processes")
	}

	p = start(id)
	if resumed := wait("session", ""); resumed.NativeID != id {
		t.Fatalf("did not resume same session: %+v", resumed)
	}
	if err := p.StartTurn(ctx, remoteProviderInput{Text: "NEWNOA_LIFECYCLE_RESUMED", OperationID: "resumed"}); err != nil {
		t.Fatalf("send after native resume: %v", err)
	}
	wait("output", "")
	wait("state", "idle")
	if !restoredContext.Load() {
		t.Fatalf("resumed %s turn had no correlated earlier context", agent)
	}
	if !stop.Load() || !first.Load() || !resumed.Load() || (agent == "openclaw" && requests.Load() != 3) {
		t.Fatalf("missing correlated turns: first=%t stop=%t resumed=%t total=%d", first.Load(), stop.Load(), resumed.Load(), requests.Load())
	}
}

func TestRemotePiReplyRequiresCommandAndID(t *testing.T) {
	p := &remoteNativeProvider{events: make(chan remoteProviderEvent, 4), piWaiters: map[string]remotePiWaiter{"1": {"prompt", make(chan remoteNativeMessage, 1)}}, dispatches: map[string]remoteNativeDispatch{"1": {operationID: "send", method: "prompt", expires: time.Now().Add(time.Minute)}}}
	consume := func(raw string) {
		var m remoteNativeMessage
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatal(err)
		}
		m.Raw = json.RawMessage(raw)
		p.piMessage(m)
	}
	consume(`{"type":"response","id":"1","command":"get_state","success":true}`)
	consume(`{"type":"response","id":"2","command":"prompt","success":true,"data":{"disposition":"started"}}`)
	if len(p.piWaiters["1"].reply) != 0 || len(p.events) != 0 {
		t.Fatal("unrelated reply acknowledged send")
	}
	consume(`{"type":"response","id":"1","command":"prompt","success":true,"data":{"disposition":"started"}}`)
	if len(p.piWaiters["1"].reply) != 1 || len(p.events) != 1 {
		t.Fatal("correlated reply did not acknowledge send")
	}
	e := <-p.events
	if e.OperationID != "send" || e.Delivery != "providerAccepted" {
		t.Fatalf("unexpected event %+v", e)
	}
}
