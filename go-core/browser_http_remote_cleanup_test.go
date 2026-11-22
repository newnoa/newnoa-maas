package main

import (
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBrowserLogoutPropagatesNativeCleanupFailure(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	_ = createRemote(t, m, cwd)
	p.mu.Lock()
	p.closeErr = errors.New("owned native child remains")
	p.mu.Unlock()
	t.Cleanup(func() { p.mu.Lock(); p.closeErr = nil; p.mu.Unlock() })
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	c.remote = m
	c.proxySession = "old-session"
	a := &browserAccount{core: c, ownerID: m.owner, tokenHash: sha256.Sum256([]byte("old-session")), active: true, claimed: true}
	if err := a.logout("old-session", ""); err == nil {
		t.Fatal("logout claimed success despite native cleanup failure")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ownerID != m.owner || !a.claimed || !a.cleanupPending {
		t.Fatal("failed cleanup released the original account claim")
	}
}

func TestBrowserClaimPropagatesNativeCleanupFailure(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	_ = createRemote(t, m, cwd)
	p.mu.Lock()
	p.closeErr = errors.New("owned native child remains")
	p.mu.Unlock()
	t.Cleanup(func() { p.mu.Lock(); p.closeErr = nil; p.mu.Unlock() })
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"id":"bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee"}`))
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	c.remote = m
	c.proxySession = "old-session"
	a := &browserAccount{core: c, ownerID: m.owner, tokenHash: sha256.Sum256([]byte("old-session")), active: true, claimed: true}
	if err := a.authenticate(httptest.NewRequest("GET", "http://localhost/", nil), "new-session", api.URL); err == nil {
		t.Fatal("new account claimed runtime despite native cleanup failure")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ownerID != m.owner || !a.cleanupPending {
		t.Fatal("failed native cleanup reassigned account ownership")
	}
}
