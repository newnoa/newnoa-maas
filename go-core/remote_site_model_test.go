package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRemoteAgentCredentialBindsLaunchAndRevokes(t *testing.T) {
	var minted, revoked bool
	hostID := "11111111-1111-4111-8111-111111111111"
	sessionID := "22222222-2222-4222-8222-222222222222"
	credentialID := "33333333-3333-4333-8333-333333333333"
	keyID := "44444444-4444-4444-8444-444444444444"
	deploymentID := "55555555-5555-4555-8555-555555555555"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer account-session" ||
			r.Header.Get("X-NewNoa-Host-Secret") != "host-secret" ||
			r.Header.Get("X-NewNoa-Host-Epoch") != "7" {
			t.Errorf("missing account or host authorization headers")
		}
		switch r.Method + " " + r.URL.Path {
		case "POST /api/v1/me/remote-hosts/" + hostID + "/sessions/" + sessionID + "/credentials":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["provider"] != "codex" || body["deploymentId"] != deploymentID ||
				body["parentKeyId"] != keyID || body["hostEpoch"] != "7" {
				t.Errorf("wrong credential binding: %#v", body)
			}
			minted = true
			_ = json.NewEncoder(w).Encode(map[string]any{"credentialId": credentialID, "credential": "ra_12345678901234567890123456789012", "expiresAt": time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339), "revision": 4})
		case "DELETE /api/v1/me/remote-hosts/" + hostID + "/sessions/" + sessionID + "/credentials/" + credentialID:
			revoked = true
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := &core{control: server.URL, modelAliases: map[string]agentModelRoute{}}
	m := &remoteManager{session: "account-session", hostID: hostID, epoch: "7", secret: "host-secret", siteCredentials: map[string]*remoteAgentCredential{}}
	launch, err := c.mintRemoteAgentCredential(context.Background(), m, sessionID, "codex", remoteSiteLaunch{Model: deploymentID, KeyID: keyID, Secret: "parent-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if !minted || !strings.HasPrefix(launch.Secret, "rla_") || launch.Secret == "parent-secret" ||
		launch.CredentialID != credentialID || launch.Revision != 4 {
		t.Fatalf("unexpected launch credential: %+v", launch)
	}
	if err = launch.Revoke(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !revoked {
		t.Fatal("credential was not revoked")
	}
}

func TestRemoteAgentLeaseRejectsUnusableAuthorityResponses(t *testing.T) {
	const host = "11111111-1111-4111-8111-111111111111"
	const session = "22222222-2222-4222-8222-222222222222"
	const id = "33333333-3333-4333-8333-333333333333"
	const secret = "ra_12345678901234567890123456789012"
	for _, tc := range []struct {
		name, id, secret string
		lifetime         time.Duration
		revision, status int
	}{
		{"denied", id, secret, time.Minute, 4, http.StatusForbidden},
		{"expired", id, secret, -time.Minute, 4, http.StatusOK},
		{"too long", id, secret, 2 * time.Hour, 4, http.StatusOK},
		{"wrong revision", id, secret, time.Minute, 5, http.StatusOK},
		{"invalid credential id", "unbound", secret, time.Minute, 4, http.StatusOK},
		{"invalid credential", id, "local-only", time.Minute, 4, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/api/v1"+agentCredentialPath(host, session) {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(issuedAgentLease{ID: tc.id, Secret: tc.secret, Revision: int64(tc.revision), ExpiresAt: time.Now().Add(tc.lifetime).UTC().Format(time.RFC3339)})
			}))
			defer server.Close()
			c := &core{control: server.URL}
			m := &remoteManager{session: "fixture-account", hostID: host, epoch: "7"}
			if _, expiry, err := c.issueAgentLease(context.Background(), m, m.session, host, m.epoch, session, id, id, "codex", 4); err == nil || !expiry.IsZero() {
				t.Fatal("unusable authority returned a valid lease", err)
			}
		})
	}
}

func TestRemoteAgentCredentialUsesRegistryAndCleansUpLateLease(t *testing.T) {
	const host = "11111111-1111-4111-8111-111111111111"
	const session = "22222222-2222-4222-8222-222222222222"
	const id = "33333333-3333-4333-8333-333333333333"
	const upstream = "ra_12345678901234567890123456789012"
	const local = "rla_fixture_012345678901234567890"
	localHash := hash([]byte(local))
	c := &core{}
	ctx := context.Background()
	if got, matched, err := c.remoteAgentAuthorization(ctx, "normal-key"); got != "" || matched || err != nil {
		t.Fatal("ordinary key was treated as a remote lease")
	}
	if _, matched, err := c.remoteAgentAuthorization(ctx, local); !matched || err == nil {
		t.Fatal("remote token was accepted without a manager")
	}
	m := &remoteManager{hostID: host, enabled: true, sessions: map[string]*remoteSession{session: {ID: session}}}
	c.remote = m
	site := remoteSiteLaunch{KeyID: id, Model: id, Alias: "fixture-model"}
	if _, err := c.mintRemoteAgentCredential(ctx, m, session, "unverified", site); err == nil {
		t.Fatal("unsupported agent admitted")
	}
	if _, err := c.mintRemoteAgentCredential(ctx, m, session, "codex", site); err == nil {
		t.Fatal("missing account session admitted")
	}
	record := &remoteAgentCredential{sessionID: session, upstreamToken: upstream, expiresAt: time.Now().Add(time.Hour), accountSession: "fixture-account", localHash: localHash, alias: site.Alias, credentialID: id}
	m.siteCredentials = map[string]*remoteAgentCredential{localHash: record}
	if _, matched, err := c.remoteAgentAuthorization(ctx, local); !matched || err == nil {
		t.Fatal("credential accepted without its account")
	}
	m.session, m.epoch = record.accountSession, "7"
	if got, matched, err := c.remoteAgentAuthorization(ctx, local); !matched || err != nil || got != upstream {
		t.Fatal("current credential not returned", err)
	}
	m.siteCredentials = nil
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.Method+" "+r.URL.Path)
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_ = json.NewEncoder(w).Encode(issuedAgentLease{ID: id, Secret: upstream, Revision: 4, ExpiresAt: time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339)})
	}))
	defer server.Close()
	c.control = server.URL
	if _, err := c.mintRemoteAgentCredential(ctx, m, session, "codex", site); err == nil {
		t.Fatal("late authority response was accepted without an active registry")
	}
	path := "/api/v1" + agentCredentialPath(host, session)
	if strings.Join(requests, "\n") != "POST "+path+"\nDELETE "+path+"/"+id {
		t.Fatal("late authority response did not revoke its issued key")
	}
}

func TestRemoteAgentRotationWaitsForRetiredLeaseRevocation(t *testing.T) {
	const host = "11111111-1111-4111-8111-111111111111"
	const session = "22222222-2222-4222-8222-222222222222"
	const oldID = "33333333-3333-4333-8333-333333333333"
	const newID = "44444444-4444-4444-8444-444444444444"
	const upstream = "ra_12345678901234567890123456789012"
	const local = "rla_fixture_012345678901234567890"
	localHash := hash([]byte(local))
	ctx := context.Background()
	var minted, deleted int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /api/v1" + agentCredentialPath(host, session):
			minted++
			if minted == 1 {
				http.Error(w, "fixture", http.StatusForbidden)
				return
			}
			_ = json.NewEncoder(w).Encode(issuedAgentLease{ID: newID, Secret: upstream, Revision: 4, ExpiresAt: time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339)})
		case "DELETE /api/v1" + agentCredentialPath(host, session) + "/" + oldID:
			deleted++
			if deleted < 3 {
				http.Error(w, "fixture", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected authority request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	record := &remoteAgentCredential{sessionID: session, credentialID: oldID, expiresAt: time.Now().Add(time.Minute), accountSession: "fixture-account", revision: 4}
	m := &remoteManager{hostID: host, epoch: "7", session: record.accountSession, enabled: true, sessions: map[string]*remoteSession{session: {ID: session}}, siteCredentials: map[string]*remoteAgentCredential{localHash: record}}
	c := &core{control: server.URL, remote: m}
	for _, status := range []string{"403", "503", "503"} {
		if got, matched, err := c.remoteAgentAuthorization(ctx, local); !matched || got != "" || err == nil || !strings.Contains(err.Error(), status) {
			t.Fatalf("authority failure %s admitted a key: %v", status, err)
		}
	}
	if got, matched, err := c.remoteAgentAuthorization(ctx, local); !matched || got != upstream || err != nil {
		t.Fatal("credential did not recover after confirmed revocation", err)
	}
	if minted != 2 || deleted != 3 || record.retiredID != "" || record.credentialID != newID {
		t.Fatalf("credential rotation or retry counts incorrect: %d mints, %d revocations", minted, deleted)
	}
}

func TestRemoteAgentCredentialRotatesBeforeExpiryAndRejectsOtherTokens(t *testing.T) {
	hostID := "11111111-1111-4111-8111-111111111111"
	sessionID := "22222222-2222-4222-8222-222222222222"
	keyID := "44444444-4444-4444-8444-444444444444"
	deploymentID := "55555555-5555-4555-8555-555555555555"
	var oldCredentialRevoked bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer account-session" {
			t.Errorf("unexpected account auth")
		}
		if r.Method == http.MethodPost {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["revision"] != float64(4) || body["parentKeyId"] != keyID {
				t.Errorf("rotation changed credential binding: %#v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"credentialId": "66666666-6666-4666-8666-666666666666", "credential": "ra_abcdefghijklmnopqrstuvwxyz123456", "expiresAt": time.Now().Add(45 * time.Minute).UTC().Format(time.RFC3339), "revision": 4})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/33333333-3333-4333-8333-333333333333") {
			oldCredentialRevoked = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		t.Errorf("unexpected credential request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	m := &remoteManager{
		hostID: hostID, epoch: "7", secret: "host-secret", session: "account-session", enabled: true,
		sessions: map[string]*remoteSession{sessionID: {ID: sessionID}}, providers: map[string]remoteProvider{sessionID: &remoteTestProvider{}},
		siteCredentials: map[string]*remoteAgentCredential{},
	}
	localToken := "rla_local-session-token-1234567890"
	localHash := hash([]byte(localToken))
	record := &remoteAgentCredential{sessionID: sessionID, agent: "codex", keyID: keyID, model: deploymentID, alias: "model-alias", localHash: localHash, credentialID: "33333333-3333-4333-8333-333333333333", upstreamToken: "ra_old", accountSession: "account-session", expiresAt: time.Now().Add(4 * time.Minute), revision: 4}
	m.siteCredentials[record.localHash] = record
	c := &core{control: server.URL, remote: m, modelAliases: map[string]agentModelRoute{"codex\nmodel-alias": {Model: deploymentID, Keys: map[string]bool{record.localHash: true}}}}
	got, matched, err := c.remoteAgentAuthorization(context.Background(), localToken)
	if !matched || err != nil || got != "ra_abcdefghijklmnopqrstuvwxyz123456" || !oldCredentialRevoked {
		t.Fatalf("credential rotation failed: token=%q matched=%t err=%v revoked=%t", got, matched, err, oldCredentialRevoked)
	}
	if _, matched, err = c.remoteAgentAuthorization(context.Background(), "rla_unregistered-token-1234567890"); !matched || err == nil {
		t.Fatal("unregistered local credential was admitted")
	}
}

func TestRemoteCredentialFreeHomeAndEnvironment(t *testing.T) {
	home := filepath.Join(t.TempDir(), "remote-session")
	env, err := remoteCredentialFreeEnvironment(home)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range env {
		if strings.Contains(entry, "parent-secret") || strings.HasPrefix(entry, "OPENAI_API_KEY=") || strings.HasPrefix(entry, "ANTHROPIC_API_KEY=") {
			t.Fatalf("credential leaked to base environment: %s", entry)
		}
	}
	if _, err = os.ReadDir(home); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(home)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("session home permissions = %v, err=%v", info.Mode().Perm(), err)
	}
	merged := mergeRemoteEnvironment(env, []string{"NEWNOA_REMOTE_API_KEY=rla_session-token-1234567890", "HOME=" + home})
	values := map[string]string{}
	for _, entry := range merged {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	if values["HOME"] != home || values["NEWNOA_REMOTE_API_KEY"] != "rla_session-token-1234567890" || values["OPENAI_API_KEY"] != "" {
		t.Fatal("unsafe isolated child environment")
	}
}

func TestRemoteCredentialHomeRejectsRelativeAndNonDirectoryPaths(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []string{"relative-session", path} {
		if env, err := remoteCredentialFreeEnvironment(candidate); err == nil || len(env) != 0 {
			t.Fatalf("unsafe home %q yielded an executor environment: %v", candidate, err)
		}
	}
}

func TestRemoteSiteAuthorizationRejectsMissingManagedConfiguration(t *testing.T) {
	c, _, _, _ := remoteRPCFixture(t)
	_, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "codex"})
	if err == nil {
		t.Fatal("remote authorization admitted an executor without site key/model configuration")
	}
}

func TestRemoteSiteSnapshotBlocksMissingConfiguration(t *testing.T) {
	c, _, _, _ := remoteRPCFixture(t)
	out, err := callRemoteRPC(t, c, "remote.snapshot", nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(out)
	var snapshot struct {
		Executors []struct {
			Agent              string `json:"agent"`
			SiteConfigured     bool   `json:"siteConfigured"`
			ConfigurationError string `json:"configurationError"`
		} `json:"executors"`
	}
	if err = json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	for _, e := range snapshot.Executors {
		if e.Agent == "codex" && (e.SiteConfigured || e.ConfigurationError == "") {
			t.Fatalf("missing config not explained: %+v", e)
		}
	}
}

func TestRemoteSiteAuthorizationRejectsRevokedKeyAndChangedConfig(t *testing.T) {
	for _, change := range []string{"revoked", "expired", "models", "config"} {
		t.Run(change, func(t *testing.T) {
			remoteProtocolExecutable(t, "codex", "ok")
			c, m, a := remoteAuthorizationFixture(t)
			a.mu.Lock()
			a.siteChange = change
			a.mu.Unlock()
			if change == "config" {
				if err := os.WriteFile(c.configPath(), []byte("model_provider = \"external\"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "codex"})
			if err == nil {
				t.Fatal("invalid site configuration admitted")
			}
			m.mu.Lock()
			authorized := m.executors["codex"].Authorized
			m.mu.Unlock()
			if authorized {
				t.Fatal("invalid site configuration retained access")
			}
		})
	}
}

func TestRemoteSiteSnapshotValidatesAccountKeyAndDefaultWithoutProbe(t *testing.T) {
	c, _, a := remoteAuthorizationFixture(t)
	out, err := callRemoteRPC(t, c, "remote.snapshot", nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(out)
	var snapshot struct {
		Executors []struct {
			Agent           string `json:"agent"`
			SiteConfigured  bool   `json:"siteConfigured"`
			ConfiguredModel string `json:"configuredModel"`
			ConfiguredKeyID string `json:"configuredKeyId"`
		} `json:"executors"`
	}
	_ = json.Unmarshal(raw, &snapshot)
	for _, e := range snapshot.Executors {
		if e.Agent == "codex" && (!e.SiteConfigured || e.ConfiguredModel != "22222222-2222-4222-8222-222222222222" || e.ConfiguredKeyID != "11111111-1111-4111-8111-111111111111") {
			t.Fatalf("site readiness: %+v", e)
		}
	}
	if strings.Contains(string(raw), "nn_site_authorized_fixture") {
		t.Fatal("site secret leaked in snapshot")
	}
	a.mu.Lock()
	a.siteChange = "revoked"
	a.mu.Unlock()
	out, err = callRemoteRPC(t, c, "remote.snapshot", nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(out)
	_ = json.Unmarshal(raw, &snapshot)
	for _, e := range snapshot.Executors {
		if e.SiteConfigured {
			t.Fatal("revoked key still ready")
		}
	}
}

func TestRemoteSiteRejectsRevocationBeforeSendSideEffects(t *testing.T) {
	remoteProtocolExecutable(t, "codex", "ok")
	c, m, a := remoteAuthorizationFixture(t)
	if _, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "codex"}); err != nil {
		t.Fatal(err)
	}
	if !waitRemoteAuthorization(t, m, "codex").Authorized {
		t.Fatal("fixture authorization failed")
	}
	create := remoteEnvelope("site-create", "session.create", "", map[string]any{"agent": "codex", "cwd": m.home})
	create.ExpectedHostEpoch = m.epoch
	if _, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": create}); err != nil {
		t.Fatal(err)
	}
	receipt := waitRemote(t, m, create.OperationID)
	if receipt.Status != "providerAccepted" {
		t.Fatalf("create: %+v", receipt)
	}
	a.mu.Lock()
	a.siteChange = "revoked"
	a.mu.Unlock()
	send := remoteEnvelope("revoked-send", "message.send", receipt.SessionID, map[string]any{"text": "do not send"})
	send.ExpectedHostEpoch = m.epoch
	if _, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": send}); err != nil {
		t.Fatal(err)
	}
	if r := waitRemote(t, m, send.OperationID); r.Status != "rejected" {
		t.Fatalf("revoked key send: %+v", r)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sessions[receipt.SessionID].Messages) != 0 {
		t.Fatal("rejected send wrote transcript")
	}
}

func TestRemoteSitePinsCodexNativeModelProviderAndTurn(t *testing.T) {
	binary := remoteProtocolExecutable(t, "codex", "ok")
	log := filepath.Join(t.TempDir(), "wire.jsonl")
	t.Setenv("NEWNOA_REMOTE_PROTOCOL_LOG", log)
	selection := remoteSiteLaunch{Model: "site-model", Effort: "low", Alias: "site-model", Secret: "rla_fixture_local_token_1234567890", BaseURL: "http://127.0.0.1:18222", Home: t.TempDir()}
	ctx := context.WithValue(context.Background(), remoteSiteLaunchKey{}, selection)
	ctx = context.WithValue(ctx, remoteProviderStateDirKey{}, t.TempDir())
	ctx = context.WithValue(ctx, remoteProviderSessionSlotKey{}, "codex-session")
	baseEnv, err := remoteCredentialFreeEnvironment(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	baseEnv = append(baseEnv, "NEWNOA_REMOTE_PROTOCOL_LOG="+log)
	ctx = context.WithValue(ctx, remoteExecutorEnvironmentKey{}, baseEnv)
	provider, err := startRemoteProviderCommand(ctx, "codex", t.TempDir(), "", nil, binary, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	childEnv := strings.Join(provider.(*remoteNativeProvider).command.Environ(), "\n")
	if !strings.Contains(childEnv, "NEWNOA_REMOTE_PROTOCOL_LOG="+log) ||
		!strings.Contains(childEnv, "NEWNOA_REMOTE_API_KEY=rla_fixture_local_token_1234567890") ||
		strings.Contains(childEnv, "nn_site_fixture") {
		t.Fatal("executor did not receive the expected isolated environment")
	}
	if err = provider.StartTurn(ctx, remoteProviderInput{Text: "fixture"}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"modelProvider":"newnoa_remote"`) || strings.Count(string(raw), `"model":"site-model"`) < 2 || !strings.Contains(string(raw), `"effort":"low"`) {
		t.Fatalf("site model/provider not pinned: %s", raw)
	}
}

func TestRemoteSitePinsClaudeArgsAndClearsForeignCredentials(t *testing.T) {
	binary := remoteProtocolExecutable(t, "claude-cli", "ok")
	selection := remoteSiteLaunch{Model: "site-model", Alias: "site-model", Secret: "nn_site_fixture_123456789", BaseURL: "http://127.0.0.1:18222", Home: t.TempDir()}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), remoteSiteLaunchKey{}, selection), 5*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, remoteProviderStateDirKey{}, t.TempDir())
	ctx = context.WithValue(ctx, remoteProviderSessionSlotKey{}, "claude-session")
	baseEnv, err := remoteCredentialFreeEnvironment(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx = context.WithValue(ctx, remoteExecutorEnvironmentKey{}, baseEnv)
	provider, err := startRemoteProviderCommand(ctx, "claude", t.TempDir(), "", nil, binary, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	native := provider.(*remoteNativeProvider)
	args := strings.Join(native.command.Args, " ")
	if !strings.Contains(args, "--model site-model --setting-sources user --effort low") {
		t.Fatalf("foreign project settings not excluded: %s", args)
	}
	env := strings.Join(native.command.Environ(), "\n")
	for _, want := range []string{"ANTHROPIC_API_KEY=" + selection.Secret, "ANTHROPIC_AUTH_TOKEN=\n", "CLAUDE_CODE_USE_BEDROCK=0", "ANTHROPIC_BASE_URL=" + selection.BaseURL, "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS=1", "CLAUDE_CODE_MAX_OUTPUT_TOKENS=4096", "API_TIMEOUT_MS=2160000", "CLAUDE_STREAM_FIRST_BYTE_TIMEOUT_MS=2160000"} {
		if !strings.Contains(env, want) {
			t.Fatalf("missing pinned env %q", want)
		}
	}
}

func TestRemoteSiteRejectsLegacyNativeSessionUntilAdoption(t *testing.T) {
	c, m, _ := remoteAuthorizationFixture(t)
	m.mu.Lock()
	target := &remoteSession{ID: "legacy", Agent: "codex", State: "idle", Source: "external", Capabilities: map[string]bool{"send": true}, Messages: []remoteMessage{}}
	m.sessions[target.ID] = target
	m.providers[target.ID] = &remoteTestProvider{}
	m.mu.Unlock()
	command := remoteEnvelope("legacy-send", "message.send", target.ID, map[string]any{"text": "not allowed"})
	command.ExpectedHostEpoch = m.epoch
	if _, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": command}); err != nil {
		t.Fatal(err)
	}
	receipt := waitRemote(t, m, command.OperationID)
	if receipt.Status != "rejected" || !strings.Contains(receipt.Error, "site model") {
		t.Fatalf("legacy session bypassed adoption: %+v", receipt)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(target.Messages) != 0 {
		t.Fatal("legacy send modified transcript")
	}
}

func TestRemoteSiteSnapshotDetectsExternalRemovalWithoutRewriting(t *testing.T) {
	for _, kind := range []string{"config", "credentials"} {
		t.Run(kind, func(t *testing.T) {
			remoteProtocolExecutable(t, "codex", "ok")
			c, m, _ := remoteAuthorizationFixture(t)
			if _, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "codex"}); err != nil {
				t.Fatal(err)
			}
			if !waitRemoteAuthorization(t, m, "codex").Authorized {
				t.Fatal("authorization failed")
			}
			path := c.configPath()
			if kind == "credentials" {
				path = filepath.Join(c.home, ".codex", "auth.json")
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			out, err := callRemoteRPC(t, c, "remote.snapshot", nil)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(out)
			var snap struct {
				Executors []remoteExecutor `json:"executors"`
			}
			if err = json.Unmarshal(data, &snap); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, e := range snap.Executors {
				if e.Agent == "codex" {
					found = true
					if e.SiteConfigured || e.Authorized || e.ConfigurationError == "" {
						t.Fatalf("removed site configuration retained readiness: %+v", e)
					}
				}
			}
			if !found {
				t.Fatal("no codex projection")
			}
			if _, err = os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("readiness rewrote externally removed file")
			}
		})
	}
}

func TestRemoteSiteReauthorizationCannotRelabelOldAttachedProvider(t *testing.T) {
	remoteProtocolExecutable(t, "codex", "ok")
	c, m, _ := remoteAuthorizationFixture(t)
	if _, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "codex"}); err != nil {
		t.Fatal(err)
	}
	if !waitRemoteAuthorization(t, m, "codex").Authorized {
		t.Fatal("authorization failed")
	}
	command := remoteEnvelope("old-provider-create", "session.create", "", map[string]any{"agent": "codex", "cwd": m.home})
	command.ExpectedHostEpoch = m.epoch
	if _, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": command}); err != nil {
		t.Fatal(err)
	}
	r := waitRemote(t, m, command.OperationID)
	if r.Status != "providerAccepted" {
		t.Fatal(r)
	}
	m.mu.Lock()
	m.sessions[r.SessionID].SiteIdentity = "previous-site-configuration"
	m.mu.Unlock()
	snapshot := m.snapshot()
	raw, _ := json.Marshal(snapshot)
	var projection struct {
		Sessions []struct{ ID, Source string } `json:"sessions"`
	}
	_ = json.Unmarshal(raw, &projection)
	if len(projection.Sessions) != 1 || projection.Sessions[0].Source != "unknown" {
		t.Fatalf("old provider was shown as adopted: %s", raw)
	}
	send := remoteEnvelope("old-provider-send", "message.send", r.SessionID, map[string]any{"text": "do not send"})
	send.ExpectedHostEpoch = m.epoch
	if _, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": send}); err != nil {
		t.Fatal(err)
	}
	if receipt := waitRemote(t, m, send.OperationID); receipt.Status != "rejected" {
		t.Fatalf("old provider accepted send: %+v", receipt)
	}
}

func TestRemoteModelChangeDoesNotRecreateExternallyRemovedSiteConfiguration(t *testing.T) {
	for _, kind := range []string{"config", "journal", "all"} {
		t.Run(kind, func(t *testing.T) {
			c, m, target, old, p, _, starts := remoteModelFixture(t, "")
			var err error
			path := c.configPath()
			if kind == "journal" {
				path = c.journalPath()
			}
			if err = os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if kind == "all" {
				if err = os.Remove(c.journalPath()); err != nil {
					t.Fatal(err)
				}
			}
			if err = c.changeRemoteModel(context.Background(), m, target, p); err == nil {
				t.Fatal("stale model change recreated removed site configuration")
			}
			if _, err = os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("externally removed site file was rewritten")
			}
			old.mu.Lock()
			stops := old.stops
			old.mu.Unlock()
			if stops != 0 || *starts != 0 {
				t.Fatal("invalid site configuration interrupted native process")
			}
		})
	}
}

func TestRemoteSiteClaudeDoesNotInheritProviderOrAuthTransportOverrides(t *testing.T) {
	binary := remoteProtocolExecutable(t, "claude-cli", "ok")
	poisoned := []string{"CLAUDE_CODE_USE_ANTHROPIC_AWS", "CLAUDE_CODE_USE_ANTHROPIC_GOOGLE_CLOUD", "CLAUDE_CODE_USE_MANTLE", "CLAUDE_CODE_USE_GATEWAY", "CLAUDE_CODE_GATEWAY_TOKEN", "CLAUDE_CODE_GATEWAY_TOKEN_FILE_DESCRIPTOR", "CLAUDE_CODE_OAUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR", "CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR", "CLAUDE_CODE_GB_BASE_URL", "CLAUDE_CODE_HOST_GATEWAY_LINEAGE", "CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST", "CLAUDE_CODE_HOST_CREDS_FILE", "CLAUDE_CODE_HOST_AUTH_ENV_VAR", "CLAUDE_BG_AUTH_SNAPSHOT_PATH", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_IDENTITY_TOKEN", "ANTHROPIC_IDENTITY_TOKEN_FILE", "ANTHROPIC_CUSTOM_HEADERS", "ANTHROPIC_UNIX_SOCKET", "CLAUDE_CODE_REMOTE_SETTINGS_PATH", "CLAUDE_CODE_API_BASE_URL", "CLAUDE_CODE_ENABLE_PROXY_AUTH_HELPER", "CCR_AGENT_PROXY_TOKEN_FILE_DESCRIPTOR"}
	for _, key := range poisoned {
		t.Setenv(key, "poisoned-foreign-provider")
	}
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:18225")
	selection := remoteSiteLaunch{Model: "site-model", Alias: "site-model", Secret: "nn_site_fixture_123456789", BaseURL: "http://127.0.0.1:18222", Home: t.TempDir()}
	ctx := context.WithValue(context.Background(), remoteSiteLaunchKey{}, selection)
	ctx = context.WithValue(ctx, remoteProviderStateDirKey{}, t.TempDir())
	ctx = context.WithValue(ctx, remoteProviderSessionSlotKey{}, "session-id")
	childHome := filepath.Join(t.TempDir(), "child-home")
	childEnv, err := remoteCredentialFreeEnvironment(childHome)
	if err != nil {
		t.Fatal(err)
	}
	ctx = context.WithValue(ctx, remoteExecutorEnvironmentKey{}, childEnv)
	provider, err := startRemoteProviderCommand(ctx, "claude", t.TempDir(), "", nil, binary, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	child := map[string]string{}
	for _, entry := range provider.(*remoteNativeProvider).command.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		child[key] = value
	}
	for _, key := range poisoned {
		if child[key] == "poisoned-foreign-provider" {
			t.Errorf("foreign provider/auth selector inherited: %s", key)
		}
		if os.Getenv(key) != "poisoned-foreign-provider" {
			t.Errorf("parent environment modified: %s", key)
		}
	}
	if child["HTTPS_PROXY"] != "http://127.0.0.1:18225" {
		t.Fatal("generic network proxy was changed")
	}
	if child["ANTHROPIC_API_KEY"] != selection.Secret || child["ANTHROPIC_BASE_URL"] != selection.BaseURL {
		t.Fatal("site transport was not bound")
	}
}

func TestRemoteSiteSessionPublishesConfiguredModelWithoutClaimingActualModel(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	m.checkSite = func(context.Context, string) (remoteSiteLaunch, error) {
		return remoteSiteLaunch{Model: "site-id", KeyID: "site-key-id", Alias: "site-model", Secret: "must-not-leak"}, nil
	}
	m.mintSiteCredential = func(_ context.Context, sessionID, agent string, site remoteSiteLaunch) (remoteSiteLaunch, error) {
		if sessionID == "" || agent != "codex" {
			return site, errors.New("incorrect credential binding")
		}
		site.Secret, site.CredentialID = "ra_12345678901234567890123456789012", "33333333-3333-4333-8333-333333333333"
		return site, nil
	}
	m.start = func(ctx context.Context, _, _, _ string, emit func(remoteProviderEvent)) (remoteProvider, error) {
		if site, ok := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch); !ok || site.Alias != "site-model" {
			return nil, errors.New("missing site launch model")
		}
		p.emit = emit
		emit(remoteProviderEvent{Type: "session", NativeID: "native-session"})
		return p, nil
	}
	id := createRemote(t, m, cwd)
	m.mu.Lock()
	raw, err := json.Marshal(m.sessions[id])
	m.mu.Unlock()
	if err != nil || !strings.Contains(string(raw), `"configuredModel":"site-model"`) ||
		!strings.Contains(string(raw), `"configuredModelId":"site-id"`) || !strings.Contains(string(raw), `"configuredKeyId":"site-key-id"`) ||
		!strings.Contains(string(raw), `"model":""`) || strings.Contains(string(raw), "must-not-leak") {
		t.Fatalf("unexpected session: %s (%v)", raw, err)
	}
}
