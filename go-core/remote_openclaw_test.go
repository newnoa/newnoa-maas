package main

import (
	"context"
	"runtime"
	"testing"
)

func TestRemoteOpenClawUsesManagedDefaultAndAccountGrant(t *testing.T) {
	c, _, api := remoteAuthorizationFixture(t)
	for _, env := range []string{"OPENCLAW_HOME", "OPENCLAW_STATE_DIR", "OPENCLAW_CONFIG_PATH", "OPENCLAW_PROFILE"} {
		t.Setenv(env, "")
	}
	p, err := c.previewWithAPIKey("openclaw", "22222222-2222-4222-8222-222222222222", "nn_site_authorized_fixture_123456789", "claw-default")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	site, err := c.remoteSiteSelection(context.Background(), "rpc-session", "openclaw")
	if err != nil || site.Alias != "claw-default" || site.Model != "22222222-2222-4222-8222-222222222222" || site.KeyID == "" {
		t.Fatalf("OpenClaw managed selection failed: %v", err)
	}
	api.mu.Lock()
	api.siteChange = "expired"
	api.mu.Unlock()
	if _, err := c.remoteSiteSelection(context.Background(), "rpc-session", "openclaw"); err == nil || err.Error() != remoteSiteKeyUnavailable {
		t.Fatalf("OpenClaw expired grant must reject: %v", err)
	}
	path, _, _ := c.agentPaths("openclaw")
	if err := atomicFile(path, []byte(`{"agents":{"defaults":{"model":{"primary":"other/model"}}}}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.remoteSiteSelection(context.Background(), "rpc-session", "openclaw"); err == nil || err.Error() != remoteSiteConfigurationChanged {
		t.Fatalf("OpenClaw changed config must reject: %v", err)
	}
}

func TestRemoteOpenClawManagerSupportMatchesPlatform(t *testing.T) {
	want := runtime.GOOS == "linux" || runtime.GOOS == "windows" || runtime.GOOS == "darwin"
	m, err := newRemoteManager(t.TempDir(), remoteRPCOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer m.shutdown()
	if got := m.executors["openclaw"].Supported; got != want {
		t.Fatalf("OpenClaw manager support = %t, want %t", got, want)
	}
}
