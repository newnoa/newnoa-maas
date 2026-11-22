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
	"testing"
	"time"
)

func TestManagedNativeConfigUsesLoopbackReference(t *testing.T) {
	const parent = "sk-test_agent_native_credential_0123456789"
	const id = "a4813426-9de7-4984-b22f-a707ccfda138"
	const model = "bc217a72-c309-4c7e-8fc7-f4c636caa36b"
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-session" {
			t.Error("wrong account session")
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/api/v1/me/api-keys":
			json.NewEncoder(w).Encode([]any{map[string]any{"id": id, "prefix": parent[:8], "deploymentIds": []string{model}}})
		case "/api/v1/me/api-keys/" + id + "/copy-secret":
			json.NewEncoder(w).Encode(map[string]string{"secret": parent})
		default:
			t.Error("unexpected authority request", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	for _, agent := range []string{"codex", "claude-cli", "pi-agent", "openclaw", "hermes-agent", "opencode"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			defer c.shutdown()
			c.control = api.URL
			p, err := c.previewWithAPIKey(agent, model, parent)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.apply(p.Digest, p.Content); err != nil {
				t.Fatal(err)
			}
			files := p.Paths
			if len(files) == 0 {
				files = []string{p.Path}
			}
			for _, f := range files {
				b, e := os.ReadFile(f)
				if e != nil {
					t.Fatal(e)
				}
				if strings.Contains(string(b), parent) {
					t.Fatalf("raw parent persisted in %s", f)
				}
			}
			site, err := c.remoteSiteSelection(context.Background(), "fixture-session", agent)
			if agent == "opencode" && err != nil {
				got, _, e := c.remoteManagedSelection("opencode")
				if e == nil || got.Secret == parent {
					t.Fatal("legacy OpenCode configuration was unexpectedly adopted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if site.Secret == parent || !strings.HasPrefix(site.Secret, "nref_") || site.KeyID != id {
				t.Fatal("launch did not use a bound local reference")
			}
		})
	}
}

func TestRemoteReferenceRedaction(t *testing.T) {
	for _, key := range []string{managedAgentKey("sk-native_fixture_key_0123456789"), "rla_01234567890123456789012345678901", "ra_01234567890123456789012345678901"} {
		if remoteRedact("local token "+key) != "local token [redacted]" {
			t.Error("remote text exposed a credential")
		}
	}
}

func TestNativeReferenceAuthorizationRejectsInactiveOrChangedAccount(t *testing.T) {
	const secret = "nn_reference_fixture_current_0123456789"
	const keyID = "11111111-1111-4111-8111-111111111111"
	const loopback = "127.0.0.1:8080"
	ref := managedAgentKey(secret)
	index := managedAgentKeyHash(ref)
	for _, tc := range []struct{ name, peer, want string }{
		{"non-loopback", "192.0.2.5:8080", "requires a loopback connection"},
		{"signed out", loopback, "Sign in"},
		{"inactive route", loopback, "no longer active"},
		{"list rejected", loopback, "HTTP 403"},
		{"unusable keys", loopback, "credential is unavailable"},
		{"copy rejected", loopback, "HTTP 403"},
		{"cache bounds", loopback, ""},
		{"session changed", loopback, "Account session changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &core{
				proxySession: "fixture-account",
				modelAliases: map[string]agentModelRoute{"managed\nalias": {Keys: map[string]bool{index: true}}},
			}
			switch tc.name {
			case "signed out":
				c.proxySession = ""
			case "inactive route":
				delete(c.modelAliases, "managed\nalias")
			case "cache bounds":
				c.nativeReferenceCache = make(map[string]string)
				for i := range 64 {
					c.nativeReferenceCache[fmt.Sprint(i)] = "obsolete-fixture"
				}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer fixture-account" {
					t.Error("request used the wrong session")
				}
				switch r.Method + " " + r.URL.Path {
				case "GET /api/v1/me/api-keys":
					if tc.name == "list rejected" {
						http.Error(w, "fixture", http.StatusForbidden)
						return
					}
					_ = json.NewEncoder(w).Encode([]map[string]any{
						{"id": "invalid", "prefix": secret[:8]},
						{"id": keyID, "paused": tc.name == "unusable keys", "prefix": secret[:8]},
					})
				case "POST /api/v1/me/api-keys/" + keyID + "/copy-secret":
					if tc.name == "copy rejected" {
						http.Error(w, "fixture", http.StatusForbidden)
						return
					}
					if tc.name == "session changed" {
						c.proxySessionMu.Lock()
						c.proxySession = "other-fixture-account"
						c.proxySessionMu.Unlock()
					}
					_ = json.NewEncoder(w).Encode(map[string]string{"secret": secret})
				default:
					t.Errorf("unexpected authority request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			c.control = server.URL
			got, err := c.localReferenceAuthorization(context.Background(), tc.peer, ref)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) || got != "" {
					t.Fatalf("expected %q without credentials, got %v", tc.want, err)
				}
				if c.nativeReferenceCache[ref] != "" {
					t.Fatal("rejected authorization cached a key")
				}
				return
			}
			if err != nil || got != secret || len(c.nativeReferenceCache) != 1 {
				t.Fatalf("credential was not resolved with a bounded cache: %v", err)
			}
			server.Close()
			got, err = c.localReferenceAuthorization(context.Background(), tc.peer, ref)
			if err != nil || got != secret {
				t.Fatalf("active managed route did not use the cached account key: %v", err)
			}
		})
	}
}

func TestInstalledRemoteLocalCredentialLaunch(t *testing.T) {
	if os.Getenv("NEWNOA_VERIFY_NATIVE_CREDENTIAL_LAUNCH") != "1" {
		t.Skip("requires installed compatible Codex and Claude executors")
	}
	for _, agent := range []string{"codex", "claude-cli"} {
		t.Run(agent, func(t *testing.T) {
			home := t.TempDir()
			cwd := t.TempDir()
			const local = "rla_native_test_credential_012345678901234567890123456789"
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{"data": []any{}})
			}))
			defer upstream.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			ctx = context.WithValue(ctx, remoteProviderStateDirKey{}, home)
			ctx = context.WithValue(ctx, remoteProviderSessionSlotKey{}, "native-session")
			ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, remoteSiteLaunch{Secret: local, Alias: "fixture-model", Model: "fixture-model", Home: home, BaseURL: upstream.URL})
			proc, err := startRemoteProvider(ctx, agent, cwd, "", func(remoteProviderEvent) {})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := proc.Close(); err != nil {
					t.Error(err)
				}
			}()
			native := proc.(*remoteNativeProvider)
			env := strings.Join(native.command.Environ(), "\n")
			if !strings.Contains(env, "NEWNOA_REMOTE_API_KEY="+local) || !strings.Contains(env, "HOME="+filepath.Join(home, "remote-executors", "native-session", remoteProviderBinary(agent))) {
				t.Fatal("native credential or home not bound to session")
			}
		})
	}
}

func TestLiveJavaRemoteCredentialContract(t *testing.T) {
	base := os.Getenv("NEWNOA_CORE_LIVE_API_BASE")
	if base == "" {
		t.Skip("requires isolated Java API and PostgreSQL fixture")
	}
	get := func(name string) string {
		t.Helper()
		v := os.Getenv("NEWNOA_CORE_LIVE_" + name)
		if v == "" {
			t.Fatalf("fixture missing %s", name)
		}
		return v
	}
	sessionID := get("AGENT_SESSION")
	m := &remoteManager{enabled: true, hostID: get("HOST"), epoch: get("EPOCH"), secret: get("HOST_SECRET"), session: get("SESSION"), sessions: map[string]*remoteSession{sessionID: {ID: sessionID}}, siteCredentials: map[string]*remoteAgentCredential{}}
	c := &core{remote: m, control: base}
	site, err := c.mintRemoteAgentCredential(context.Background(), m, sessionID, "codex", remoteSiteLaunch{KeyID: get("KEY"), Model: get("DEPLOYMENT"), Alias: "fixture-model"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := site.Revoke(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	first, matched, err := c.remoteAgentAuthorization(context.Background(), site.Secret)
	if err != nil || !matched || !strings.HasPrefix(first, "ra_") || !strings.HasPrefix(site.Secret, "rla_") || first == site.Secret {
		t.Fatal("minted credential does not resolve", err)
	}
	record := m.siteCredentials[hash([]byte(site.Secret))]
	record.refreshMu.Lock()
	record.expiresAt = time.Now().Add(3 * time.Minute)
	record.refreshMu.Unlock()
	rotated, matched, err := c.remoteAgentAuthorization(context.Background(), site.Secret)
	if err != nil || !matched || first == rotated {
		t.Fatal("rotated credential is unavailable", err)
	}
	if err = site.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err = c.remoteAgentAuthorization(context.Background(), site.Secret); err == nil {
		t.Fatal("closed session reference remains usable")
	}
}

func TestPiResumesExistingHostHistoryWithIsolatedCredentialHome(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "host")
	home := filepath.Join(state, "remote-executors", "session", "pi")
	id := "20202020-2222-4222-8222-222222222222"
	prior := filepath.Join(state, "pi-sessions", "previous_"+id+".jsonl")
	if err := os.MkdirAll(filepath.Dir(prior), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(prior, []byte("{\"type\":\"session\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), remoteProviderStateDirKey{}, state)
	args, _, resumed, clean, err := prepareRemotePi(ctx, remoteSiteLaunch{Home: home, Secret: "rla_test-session-key", BaseURL: "http://127.0.0.1:18222", Alias: "fixture"}, id, false)
	if err != nil {
		t.Fatal(err)
	}
	defer clean()
	if resumed != id || !strings.Contains(strings.Join(args, "\n"), "--session\n"+prior+"\n") {
		t.Fatal("existing Pi transcript was not selected")
	}
}

func TestRemoteCredentialCloseRetriesUnconfirmedRevocation(t *testing.T) {
	attempts := 0
	p := &remoteNativeProvider{revokeCredential: func(ctx context.Context) error {
		attempts++
		if attempts == 1 {
			return context.DeadlineExceeded
		}
		return nil
	}}
	if err := p.revokeRemoteCredential(); err == nil {
		t.Fatal("missing unconfirmed revocation error")
	}
	if err := p.revokeRemoteCredential(); err != nil {
		t.Fatal("retry did not recover", err)
	}
	if err := p.revokeRemoteCredential(); err != nil || attempts != 2 {
		t.Fatal("confirmed revocation repeated", attempts, err)
	}
}
