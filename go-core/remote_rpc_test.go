package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"golang.org/x/net/websocket"
	"time"
)

const remoteRPCOwner = "11111111-1111-4111-8111-111111111111"
const remoteRPCOtherOwner = "22222222-2222-4222-8222-222222222222"

type remoteRPCAPI struct {
	mu                                                  sync.Mutex
	owner                                               string
	meStatus, hostsStatus, registerStatus, deleteStatus int
	siteChange                                          string
	meCalls                                             int
	changeOwnerAt                                       int
	registered                                          map[string]any
	deleted                                             string
	pollStatus                                          int
}

func (a *remoteRPCAPI) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/v1/me/points/live" {
		serveRelayFixtureWS(w, r, "", false)
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer rpc-session" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch {
	case r.URL.Path == "/api/v1/me/points/live-ticket":
		if a.pollStatus != 0 {
			w.WriteHeader(a.pollStatus)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"ticket": relayFixtureTicket})
	case r.URL.Path == "/api/v1/me":
		a.meCalls++
		if a.meStatus != 0 {
			w.WriteHeader(a.meStatus)
			return
		}
		owner := a.owner
		if a.changeOwnerAt > 0 && a.meCalls >= a.changeOwnerAt {
			owner = remoteRPCOtherOwner
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"id": owner})
	case r.URL.Path == "/api/v1/deployments/catalog/22222222-2222-4222-8222-222222222222":
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "22222222-2222-4222-8222-222222222222", "contextLength": 65536, "detectedContextLength": 131072})
	case r.URL.Path == "/api/v1/me/api-keys":
		key := map[string]any{"id": "11111111-1111-4111-8111-111111111111", "prefix": "nn_site_", "deploymentIds": []string{"22222222-2222-4222-8222-222222222222"}}
		switch a.siteChange {
		case "revoked":
			key["revoked_at"] = "2026-01-01"
		case "expired":
			key["expires_at"] = "2000-01-01T00:00:00Z"
		case "models":
			key["deploymentIds"] = []string{}
		}
		_ = json.NewEncoder(w).Encode([]any{key})
	case r.URL.Path == "/api/v1/me/api-keys/11111111-1111-4111-8111-111111111111/copy-secret":
		_ = json.NewEncoder(w).Encode(map[string]string{"secret": "nn_site_authorized_fixture_123456789"})
	case r.URL.Path == "/api/v1/me/remote-hosts":
		if a.hostsStatus != 0 {
			w.WriteHeader(a.hostsStatus)
			return
		}
		_, _ = w.Write([]byte(`[]`))
	case r.URL.Path == "/api/v1/me/remote-hosts/register":
		if a.registerStatus != 0 {
			w.WriteHeader(a.registerStatus)
			return
		}
		if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&a.registered) != nil {
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case strings.Contains(r.URL.Path, "/credentials"):
		if r.Method == http.MethodPost {
			_ = json.NewEncoder(w).Encode(map[string]any{"credentialId": "33333333-3333-4333-8333-333333333333", "credential": "ra_12345678901234567890123456789012", "expiresAt": time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339), "revision": 4})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case strings.HasSuffix(r.URL.Path, "/poll"):
		if a.pollStatus != 0 {
			w.WriteHeader(a.pollStatus)
			return
		}
		_, _ = w.Write([]byte(`{"commands":[]}`))
	case strings.HasSuffix(r.URL.Path, "/publish"):
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodDelete:
		a.deleted = r.URL.Path
		if a.deleteStatus != 0 {
			w.WriteHeader(a.deleteStatus)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func remoteRPCFixture(t *testing.T) (*core, *remoteManager, *remoteTestProvider, *remoteRPCAPI) {
	t.Helper()
	t.Setenv("NEWNOA_RUNTIME_MODE", "")
	m, provider, home := remoteFixture(t)
	a := &remoteRPCAPI{owner: remoteRPCOwner}
	server := httptest.NewServer(http.HandlerFunc(a.serve))
	t.Cleanup(server.Close)
	c := &core{home: home, control: server.URL, remote: m}
	t.Cleanup(func() { _ = c.stopRemote() })
	return c, m, provider, a
}

func callRemoteRPC(t *testing.T, c *core, method string, p map[string]any) (any, error) {
	t.Helper()
	if p == nil {
		p = map[string]any{}
	}
	p["session"] = "rpc-session"
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return c.remoteRPC(method, raw)
}

func requireRemoteRPCError(t *testing.T, err error, part string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), part) {
		t.Fatalf("want error containing %q, got %v", part, err)
	}
}

func TestRemoteRPCRejectsRuntimeAndUntrustedParameters(t *testing.T) {
	c := &core{}
	for _, raw := range []string{`{"ownerId":"other"}`, `{"session":1}`, `{} {}`} {
		_, err := c.remoteRPC("remote.snapshot", json.RawMessage(raw))
		requireRemoteRPCError(t, err, "parameters")
	}
	if _, err := c.remoteRPC("remote.snapshot", nil); err == nil {
		t.Fatal("anonymous snapshot accepted")
	}
	for _, mode := range []string{"stateless", "docker"} {
		t.Run(mode, func(t *testing.T) {
			c.publicStateless = mode == "stateless"
			if mode == "docker" {
				t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
			}
			_, err := c.remoteRPC("remote.shutdown", nil)
			requireRemoteRPCError(t, err, "unavailable in this runtime")
		})
	}
}

func TestRemoteRPCRequiresAPIIdentityAndSurfacePolicy(t *testing.T) {
	for _, mode := range []string{"revoked", "me-denied", "invalid-owner", "surface-denied"} {
		t.Run(mode, func(t *testing.T) {
			c, m, _, a := remoteRPCFixture(t)
			switch mode {
			case "revoked":
				c.historyRevokedHash = sha256.Sum256([]byte("rpc-session"))
			case "me-denied":
				a.meStatus = 401
			case "invalid-owner":
				a.owner = "client-selected-owner"
			case "surface-denied":
				a.hostsStatus = 403
			}
			_, err := callRemoteRPC(t, c, "remote.snapshot", nil)
			if err == nil {
				t.Fatal("unauthorized RPC accepted")
			}
			if mode == "me-denied" || mode == "surface-denied" {
				var denied *remoteHTTPError
				if !errors.As(err, &denied) || denied.Status != map[string]int{"me-denied": 401, "surface-denied": 403}[mode] {
					t.Fatalf("API authority denial lost: %v", err)
				}
				requireRemoteRPCError(t, err, "HTTP")
			}
			if m.session != "" {
				t.Fatal("rejected identity bound to runtime")
			}
			if mode == "revoked" && a.meCalls != 0 {
				t.Fatal("known revoked session reached API")
			}
		})
	}
}

func TestRemoteRPCSnapshotReceiptsAndOSBinding(t *testing.T) {
	c, m, _, _ := remoteRPCFixture(t)
	for _, method := range []string{"remote.snapshot", "agent.remote.status"} {
		out, err := callRemoteRPC(t, c, method, nil)
		if err != nil {
			t.Fatal(err)
		}
		snapshot := out.(map[string]any)
		if snapshot["hostEpoch"] != m.epoch || snapshot["hostId"] != m.hostID {
			t.Fatal("snapshot has wrong host binding")
		}
	}
	m.mu.Lock()
	m.operations["terminal"] = &remoteReceipt{OperationID: "terminal", Status: "providerAccepted", created: time.Now()}
	m.mu.Unlock()
	out, err := callRemoteRPC(t, c, "remote.operation.get", map[string]any{"operationId": "terminal"})
	if err != nil || out.(remoteReceipt).Status != "providerAccepted" {
		t.Fatalf("receipt: %v %v", out, err)
	}
	_, err = callRemoteRPC(t, c, "remote.operation.get", map[string]any{"operationId": "missing"})
	requireRemoteRPCError(t, err, "not found")
	_, err = callRemoteRPC(t, c, "remote.unknown", nil)
	requireRemoteRPCError(t, err, "Unsupported")
	uid := m.uid
	m.uid = "a-different-os-user"
	defer func() { m.uid = uid }()
	_, err = callRemoteRPC(t, c, "remote.snapshot", nil)
	requireRemoteRPCError(t, err, "Operating system user changed")
}

func TestRemoteRPCCommandsBindEpochAndExecuteStop(t *testing.T) {
	c, m, provider, _ := remoteRPCFixture(t)
	create := remoteEnvelope("rpc-create", "session.create", "", map[string]any{"agent": "codex", "cwd": m.home})
	for _, epoch := range []string{"", "invalid", remoteRPCOtherOwner} {
		create.ExpectedHostEpoch = epoch
		_, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": create})
		requireRemoteRPCError(t, err, "epoch changed")
	}
	_, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": map[string]any{"protocolVersion": 2}})
	if err == nil {
		t.Fatal("invalid command accepted")
	}
	create.ExpectedHostEpoch = m.epoch
	out, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": create})
	if err != nil {
		t.Fatal(err)
	}
	sid := out.(remoteReceipt).SessionID
	if r := waitRemote(t, m, create.OperationID); r.Status != "providerAccepted" {
		t.Fatalf("create: %+v", r)
	}
	for _, typ := range []string{"message.send", "run.stop"} {
		payload := map[string]any{}
		if typ == "message.send" {
			payload["text"] = "offline request"
		}
		command := remoteEnvelope("rpc-"+typ, typ, sid, payload)
		command.ExpectedHostEpoch = m.epoch
		if _, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": command}); err != nil {
			t.Fatal(err)
		}
		if r := waitRemote(t, m, command.OperationID); r.Status != "providerAccepted" {
			t.Fatalf("%s: %+v", typ, r)
		}
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.sends != 1 || provider.stops != 1 {
		t.Fatalf("RPC did not reach owned provider once: sends=%d stops=%d", provider.sends, provider.stops)
	}
}

func TestRemoteRPCRechecksAccountBeforeNativeExecution(t *testing.T) {
	c, m, provider, a := remoteRPCFixture(t)
	a.changeOwnerAt = 2
	command := remoteEnvelope("rpc-rebound", "session.create", "", map[string]any{"agent": "codex", "cwd": m.home})
	command.ExpectedHostEpoch = m.epoch
	if _, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": command}); err != nil {
		t.Fatal(err)
	}
	r := waitRemote(t, m, command.OperationID)
	if r.Status != "rejected" || !strings.Contains(r.Error, "account changed") {
		t.Fatalf("account switched before execution: %+v", r)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.emit != nil || provider.sends != 0 {
		t.Fatal("changed account reached native executor")
	}
}

func TestRemoteRPCAccountRebindStopsPreviousOwnedProvider(t *testing.T) {
	for _, failCleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "stops-and-isolates", true: "cleanup-failure-blocks-rebind"}[failCleanup], func(t *testing.T) {
			c, m, p, a := remoteRPCFixture(t)
			sid := createRemote(t, m, m.home)
			if failCleanup {
				p.closeErr = errors.New("owned process cleanup failed")
			}
			a.owner = remoteRPCOtherOwner
			out, err := callRemoteRPC(t, c, "remote.snapshot", nil)
			if failCleanup {
				requireRemoteRPCError(t, err, "could not be stopped")
				if c.remote != m {
					t.Fatal("failed cleanup discarded previous owner")
				}
				p.mu.Lock()
				p.closeErr = nil
				p.mu.Unlock()
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.remote.owner != remoteRPCOtherOwner || c.remote.epoch == m.epoch || len(out.(map[string]any)["sessions"].([]any)) != 0 {
				t.Fatal("account inherited old host epoch or sessions")
			}
			m.mu.Lock()
			closed := m.closed
			_, retained := m.sessions[sid]
			m.mu.Unlock()
			p.mu.Lock()
			stops := p.stops
			p.mu.Unlock()
			if !closed || !retained || stops != 1 {
				t.Fatal("old runtime was not stopped while retaining its own journal")
			}
		})
	}
}

func TestRemoteRPCHostEnableRegistrationAndRevocation(t *testing.T) {
	for _, mode := range []string{"no-executor", "registration-denied", "enabled", "disable-denied", "disabled", "relay-revoked"} {
		t.Run(mode, func(t *testing.T) {
			c, m, _, a := remoteRPCFixture(t)
			t.Setenv("CODEX_HOME", "")
			pr, e := c.previewWithAPIKey("codex", "22222222-2222-4222-8222-222222222222", "nn_site_authorized_fixture_123456789")
			if e != nil {
				t.Fatal(e)
			}
			if e = c.apply(pr.Digest, pr.Content); e != nil {
				t.Fatal(e)
			}
			if mode == "no-executor" {
				m.executors["codex"].Authorized = false
			}
			if mode == "registration-denied" {
				a.registerStatus = 403
			}
			if mode == "disable-denied" {
				a.deleteStatus = 503
			}
			if mode == "relay-revoked" {
				a.pollStatus = 403
			}
			enable := mode != "disabled" && mode != "disable-denied"
			out, err := callRemoteRPC(t, c, "remote.host.enable", map[string]any{"enabled": enable})
			if mode == "no-executor" || mode == "registration-denied" || mode == "disable-denied" {
				if err == nil {
					t.Fatal("invalid host transition succeeded")
				}
				m.mu.Lock()
				enabled := m.enabled
				m.mu.Unlock()
				if enabled {
					t.Fatal("rejected host transition left remote access enabled")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if enable && err == nil {
				a.mu.Lock()
				body := a.registered
				a.mu.Unlock()
				if body["hostId"] != m.hostID || body["hostEpoch"] != m.epoch || body["hostSecret"] != m.secret || body["platform"] == "" {
					t.Fatal("registration lost host binding")
				}
				capabilities := body["capabilities"].([]any)
				if len(capabilities) != 1 || capabilities[0] != "codex" {
					t.Fatal("unapproved executor advertised")
				}
				if out.(map[string]any)["enabled"] != true {
					t.Fatal("registered host not enabled")
				}
				if mode == "enabled" {
					if _, err := callRemoteRPC(t, c, "remote.host.enable", map[string]any{"enabled": true}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if !enable {
				m.mu.Lock()
				closed, revoked := m.closed, m.revoked
				m.mu.Unlock()
				if !closed || revoked != (mode == "disabled") {
					t.Fatal("disable did not stop locally or misreported revocation")
				}
				a.mu.Lock()
				deleted := a.deleted
				a.mu.Unlock()
				if deleted != "/api/v1/me/remote-hosts/"+m.hostID {
					t.Fatal("wrong host revoked")
				}
			}
			if mode == "relay-revoked" {
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					m.mu.Lock()
					closed, revoked := m.closed, m.revoked
					m.mu.Unlock()
					if closed && revoked {
						return
					}
					time.Sleep(time.Millisecond)
				}
				t.Fatal("API revocation did not close local runtime")
			}
		})
	}
}

func TestRemoteRPCShutdownDoesNotRequireSessionAndReportsCleanupFailure(t *testing.T) {
	c, m, p, _ := remoteRPCFixture(t)
	createRemote(t, m, m.home)
	p.closeErr = errors.New("owned process still alive")
	out, err := c.remoteRPC("remote.shutdown", nil)
	if err == nil || out.(map[string]bool)["stopped"] || c.remote == nil {
		t.Fatal("cleanup failure hidden")
	}
	p.mu.Lock()
	p.closeErr = nil
	p.mu.Unlock()
	out, err = c.remoteRPC("remote.shutdown", nil)
	if err != nil || !out.(map[string]bool)["stopped"] || c.remote != nil {
		t.Fatal("successful retry did not release runtime")
	}
	if _, err = c.remoteRPC("remote.shutdown", nil); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteHTTPRejectsInvalidAuthorityAndResponses(t *testing.T) {
	c := &core{control: "not-a-url"}
	if err := c.remoteHTTP(context.Background(), "GET", "/me", "rpc-session", nil, nil, nil); err == nil {
		t.Fatal("invalid authority accepted")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`not JSON`)) }))
	defer server.Close()
	c.control = server.URL
	var out map[string]any
	requireRemoteRPCError(t, c.remoteHTTP(context.Background(), "GET", "/me", "rpc-session", nil, nil, &out), "Invalid remote authority response")
	if err := c.remoteHTTP(context.Background(), "POST", "/me", "rpc-session", nil, make(chan int), nil); err == nil {
		t.Fatal("unserializable request accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	requireRemoteRPCError(t, c.remoteHTTP(ctx, "GET", "/me", "rpc-session", nil, nil, nil), "unavailable")
}

func TestRemoteAuthorizationRetriesHostRegistrationWithoutAnotherProbe(t *testing.T) {
	c, m, _, a := remoteRPCFixture(t)
	t.Setenv("CODEX_HOME", "")
	preview, err := c.previewWithAPIKey("codex", remoteRPCOtherOwner, "nn_site_authorized_fixture_123456789")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(preview.Digest, preview.Content); err != nil {
		t.Fatal(err)
	}
	a.registerStatus = http.StatusForbidden
	if _, err = callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "codex"}); err == nil {
		t.Fatal("relay rejected registration but authorization reported success")
	}
	a.mu.Lock()
	a.registerStatus = 0
	a.mu.Unlock()
	if _, err = callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "codex"}); err != nil {
		t.Fatal(err)
	}
	if m.snapshot()["enabled"] != true {
		t.Fatal("authorized executor did not register the host")
	}
}

func TestRemoteStatusDoesNotReplaceHostingSession(t *testing.T) {
	c, m, _, api := remoteRPCFixture(t)
	polls := make(chan string, 8)
	sockets := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/live") {
			fixtureSocket(func(ws *websocket.Conn) {
				sockets <- ws
				var msg string
				for websocket.Message.Receive(ws, &msg) == nil {
					if websocket.Message.Send(ws, msg) != nil {
						return
					}
				}
			}).ServeHTTP(w, r)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/poll") {
			select {
			case polls <- r.Header.Get("Authorization"):
			default:
			}
		}
		if r.Header.Get("Authorization") == "Bearer observer" {
			if r.Method != "GET" || r.URL.Path == "/api/v1/me/api-keys" {
				w.WriteHeader(401)
				return
			}
			r.Header.Set("Authorization", "Bearer rpc-session")
		}
		api.serve(w, r)
	}))
	defer server.Close()
	c.control = server.URL
	pr, err := c.previewWithAPIKey("codex", "22222222-2222-4222-8222-222222222222", "nn_site_authorized_fixture_123456789")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	if _, err := callRemoteRPC(t, c, "remote.host.enable", map[string]any{"enabled": true}); err != nil {
		t.Fatal(err)
	}
	var ws *websocket.Conn
	select {
	case ws = <-sockets:
	case <-time.After(3 * time.Second):
		t.Fatal("missing host connection")
	}
	poll := func() {
		if err := websocket.JSON.Send(ws, map[string]string{"type": "remote.changed", "kind": "commands", "hostId": m.hostID}); err != nil {
			t.Fatal(err)
		}
		t.Helper()
		select {
		case token := <-polls:
			if token != "Bearer rpc-session" {
				t.Fatal("relay polled with an observer's credential")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("host stopped polling")
		}
	}
	poll()
	for _, method := range []string{"agent.remote.status", "remote.snapshot", "remote.operation.get"} {
		_, err := c.remoteRPC(method, json.RawMessage(`{"session":"observer","operationId":"missing"}`))
		if err != nil && method != "remote.operation.get" {
			t.Fatal(err)
		}
	}
	poll()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || !m.enabled || m.session != "rpc-session" || !m.executors["codex"].Authorized {
		t.Fatal("observer changed the registered host lifecycle")
	}
}
