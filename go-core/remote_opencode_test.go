package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRemoteOpenCodeUsesOwnedState(t *testing.T) {
	if !remoteACPAgent("opencode") {
		t.Fatal("OpenCode is not recognized as an ACP executor")
	}
	if _, err := startRemoteOpenCode(context.Background(), t.TempDir(), "", nil, "unused", false); err == nil {
		t.Fatal("launch without managed authorization succeeded")
	}
}

func TestRemoteOpenCodeManagerSupportMatchesPlatform(t *testing.T) {
	m, err := newRemoteManager(t.TempDir(), remoteRPCOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer m.shutdown()
	if got := m.executors["opencode"].Supported; got != remoteOpenCodePlatform() {
		t.Fatalf("OpenCode manager support = %v, want %v", got, remoteOpenCodePlatform())
	}
}

func TestRemoteOpenCodeNative(t *testing.T) {
	binary := os.Getenv("NEWNOA_TEST_OPENCODE")
	if binary == "" {
		t.Skip("set NEWNOA_TEST_OPENCODE to the fixed 1.19.6 executable")
	}
	selected, err := remoteExecutorCommand("opencode")
	if err != nil {
		t.Fatal("put the requested OpenCode executable on PATH", err)
	}
	a, err := os.Stat(binary)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(selected.path)
	if err != nil || !os.SameFile(a, b) {
		t.Fatal("PATH OpenCode must match NEWNOA_TEST_OPENCODE", err)
	}
	const probeSecret = "nref_opencode_acp_probe_fixture_123456789"
	const secret = "rla_opencode_acp_fixture_only_0123456789"
	var hold atomic.Bool
	cancelled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" || (r.Header.Get("Authorization") != "Bearer "+secret && r.Header.Get("Authorization") != "Bearer "+probeSecret) {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		var in struct {
			Model    string
			Messages []struct {
				Role    string
				Content json.RawMessage
			}
		}
		if json.NewDecoder(r.Body).Decode(&in) != nil || in.Model != "fixture-alias" {
			http.Error(w, "unexpected model", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if hold.Load() {
			_, _ = w.Write([]byte("data: {\"id\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n"))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			cancelled <- struct{}{}
			return
		}
		reply := "ACP_OK"
		for _, m := range in.Messages {
			if m.Role == "user" && strings.Contains(string(m.Content), "Reply with exactly NEWNOA_REMOTE_OK") {
				reply = "NEWNOA_REMOTE_OK"
			}
		}
		_, _ = w.Write([]byte("data: {\"id\":\"fixture\",\"object\":\"chat.completion.chunk\",\"model\":\"fixture-alias\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"" + reply + "\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"fixture\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	home := t.TempDir()
	cwd := t.TempDir()
	if err := os.WriteFile(filepath.Join(cwd, "opencode.json"), []byte(`{"model":"unmanaged/project"}`), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, remoteSiteLaunch{Home: home, Secret: secret, BaseURL: server.URL, Alias: "fixture-alias"})
	ctx = context.WithValue(ctx, remoteProviderStateDirKey{}, home)
	ctx = context.WithValue(ctx, remoteProviderSessionSlotKey{}, "fixture-remote-slot")
	probeDir, err := os.MkdirTemp(home, "probe-")
	if err != nil {
		t.Fatal(err)
	}
	probeSite := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch)
	probeSite.Secret = probeSecret
	info, err := probeRemoteProvider(context.WithValue(ctx, remoteSiteLaunchKey{}, probeSite), "opencode", probeDir)
	if err != nil || info.Version != "1.19.6" || !info.Capabilities["resume"] {
		t.Fatalf("native authorization probe failed: %v", err)
	}
	var nativeID string
	for i := 0; i < 2; i++ {
		events := make(chan remoteProviderEvent, 32)
		p, err := startRemoteProvider(ctx, "opencode", cwd, nativeID, func(e remoteProviderEvent) { events <- e })
		if err != nil {
			t.Fatal(err)
		}
		actual := p.(*remoteNativeProvider).nativeID
		if actual == "" || nativeID != "" && actual != nativeID {
			p.Close()
			t.Fatal("session not recovered with same ID")
		}
		nativeID = actual
		err = p.StartTurn(ctx, remoteProviderInput{Text: "Reply ACP_OK", OperationID: "fixture-send"})
		if err != nil {
			p.Close()
			t.Fatal(err)
		}
		var output strings.Builder
		var accepted int
	loop:
		for {
			select {
			case e := <-events:
				if e.Type == "dispatch" && e.OperationID == "fixture-send" && e.Delivery == "providerAccepted" {
					accepted++
				}
				if e.Type == "output" {
					output.WriteString(e.Text)
				}
				if e.State == "idle" && e.Type == "state" || e.Type == "error" {
					break loop
				}
			case <-ctx.Done():
				break loop
			}
		}
		if i == 1 {
			hold.Store(true)
			if err = p.StartTurn(ctx, remoteProviderInput{Text: "Run until stopped", OperationID: "fixture-stop"}); err != nil {
				p.Close()
				t.Fatal(err)
			}
			if err = p.Stop(ctx); err != nil {
				p.Close()
				t.Fatal(err)
			}
			select {
			case <-cancelled:
			case <-ctx.Done():
				p.Close()
				t.Fatal("stop did not cancel the inference request")
			}
		}
		if err := p.Close(); err != nil || output.String() != "ACP_OK" || accepted != 1 {
			t.Fatalf("turn %d did not produce exactly one receipt and reply: receipts=%d reply=%q err=%v", i+1, accepted, output.String(), err)
		}
	}
}

func TestRemoteOpenCodeRechecksOwnerAndDefaultKey(t *testing.T) {
	t.Setenv("OPENCODE_CONFIG", "")
	t.Setenv("OPENCODE_CONFIG_CONTENT", "")
	t.Setenv("OPENCODE_CONFIG_DIR", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	c, _, _, api := remoteRPCFixture(t)
	const secret = "nn_site_authorized_fixture_123456789"
	const keyID = "11111111-1111-4111-8111-111111111111"
	models := []providerModel{{"22222222-2222-4222-8222-222222222222", "fixture"}}
	p, err := c.previewManagedProvider("opencode", remoteRPCOwner, keyID, secret, "add", models)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if selected, err := c.remoteSiteSelection(ctx, "rpc-session", "opencode"); err != nil || selected.KeyID != keyID {
		t.Fatal("current account and exact default key were rejected", err)
	}
	api.mu.Lock()
	api.owner = remoteRPCOtherOwner
	api.mu.Unlock()
	if _, err := c.remoteSiteSelection(ctx, "rpc-session", "opencode"); err == nil {
		t.Fatal("different account recovered an owned provider")
	}
	api.mu.Lock()
	api.owner = remoteRPCOwner
	api.siteChange = "revoked"
	api.mu.Unlock()
	if _, err := c.remoteSiteSelection(ctx, "rpc-session", "opencode"); err == nil {
		t.Fatal("revoked default key retained authorization")
	}
}
