package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func remoteModelFixture(t *testing.T, expires string) (*core, *remoteManager, *remoteSession, *remoteTestProvider, remotePayload, <-chan struct{}, *int) {
	t.Helper()
	const owner = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	const key = "11111111-1111-4111-8111-111111111111"
	const model = "22222222-2222-4222-8222-222222222222"
	const secret = "nn_selected_authorized_key_123456789"
	authorized := make(chan struct{})
	var once sync.Once
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/me":
			json.NewEncoder(w).Encode(map[string]string{"id": owner})
		case "/api/v1/me/remote-hosts":
			json.NewEncoder(w).Encode([]any{})
		case "/api/v1/me/api-keys":
			json.NewEncoder(w).Encode([]any{map[string]any{"id": key, "prefix": "nn_selec", "expires_at": expires, "paused": false, "deploymentIds": []string{model}}})
		case "/api/v1/me/api-keys/" + key + "/copy-secret":
			json.NewEncoder(w).Encode(map[string]string{"secret": secret})
			once.Do(func() { close(authorized) })
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(api.Close)
	c := newCore(t.TempDir(), api.URL)
	c.control = api.URL
	c.server = &http.Server{}
	t.Setenv("CODEX_HOME", "")
	pr, e := c.previewWithAPIKey("codex", model, secret, model)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.apply(pr.Digest, pr.Content); e != nil {
		t.Fatal(e)
	}
	m, err := newRemoteManager(c.home, owner)
	if err != nil {
		t.Fatal(err)
	}
	m.session = "account-session"
	target := &remoteSession{ID: "session", Agent: "codex", Cwd: c.home, NativeID: "native-session"}
	provider := &remoteTestProvider{}
	m.sessions[target.ID] = target
	m.providers[target.ID] = provider
	m.executors["codex"] = &remoteExecutor{Agent: "codex", Authorized: true}
	starts := new(int)
	m.start = func(context.Context, string, string, string, func(remoteProviderEvent)) (remoteProvider, error) {
		*starts++
		return &remoteTestProvider{}, nil
	}
	t.Cleanup(func() { m.shutdown() })
	return c, m, target, provider, remotePayload{KeyID: key, ModelID: model}, authorized, starts
}
func TestRemoteModelRejectsAuthorityExpiredWhileWaitingForConfigLock(t *testing.T) {
	c, m, target, provider, p, authorized, starts := remoteModelFixture(t, "")
	c.remoteConfigMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.changeRemoteModel(ctx, m, target, p) }()
	select {
	case <-authorized:
	case <-time.After(3 * time.Second):
		c.remoteConfigMu.Unlock()
		cancel()
		t.Fatal("authorization did not finish")
	}
	cancel()
	c.remoteConfigMu.Unlock()
	if err := <-done; err == nil {
		t.Fatal("expired job changed native model after waiting for lock")
	}
	provider.mu.Lock()
	closes := provider.stops
	provider.mu.Unlock()
	if closes != 0 || *starts != 0 {
		t.Fatal("expired operation closed or started a native process")
	}
}
func TestRemoteModelRejectsAPIExpiresAt(t *testing.T) {
	c, m, target, provider, p, _, starts := remoteModelFixture(t, "2000-01-01T00:00:00Z")
	if err := c.changeRemoteModel(context.Background(), m, target, p); err == nil {
		t.Fatal("API-shaped expired owned key changed native configuration")
	}
	provider.mu.Lock()
	closes := provider.stops
	provider.mu.Unlock()
	if closes != 0 || *starts != 0 {
		t.Fatal("expired key performed model side effects")
	}
}

type remoteModelCancelOnClose struct {
	*remoteTestProvider
	cancel context.CancelFunc
}

func (p *remoteModelCancelOnClose) Close() error {
	err := p.remoteTestProvider.Close()
	p.cancel()
	return err
}

func TestRemoteModelRevalidatesJobGrantAfterConfigLock(t *testing.T) {
	c, m, target, provider, p, authorized, starts := remoteModelFixture(t, "")
	c.remoteConfigMu.Lock()
	var revoked atomic.Bool
	ctx := context.WithValue(context.Background(), remoteCommandAuthorityKey{}, func(context.Context) error {
		if revoked.Load() {
			return errors.New("original job grant revoked")
		}
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- c.changeRemoteModel(ctx, m, target, p) }()
	select {
	case <-authorized:
	case <-time.After(3 * time.Second):
		c.remoteConfigMu.Unlock()
		t.Fatal("authorization did not finish")
	}
	revoked.Store(true)
	c.remoteConfigMu.Unlock()
	if err := <-done; err == nil {
		t.Fatal("revoked job grant performed a model change")
	}
	provider.mu.Lock()
	closes := provider.stops
	provider.mu.Unlock()
	if closes != 0 || *starts != 0 {
		t.Fatal("revoked grant performed native side effects")
	}
}
func TestRemoteModelStopsBeforeApplyWhenCloseExpiresAuthority(t *testing.T) {
	c, m, target, provider, p, _, starts := remoteModelFixture(t, "")
	before, err := os.ReadFile(c.configPath())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.providers[target.ID] = &remoteModelCancelOnClose{remoteTestProvider: provider, cancel: cancel}
	if err := c.changeRemoteModel(ctx, m, target, p); err == nil {
		t.Fatal("authority expired between close and configuration apply")
	}
	raw, err := os.ReadFile(c.configPath())
	if err != nil || string(raw) != string(before) {
		t.Fatal("original site configuration changed after authority expired")
	}
	if *starts != 0 {
		t.Fatal("expired operation started replacement native provider")
	}
}
