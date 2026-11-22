package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func remoteAuthorizationFixture(t *testing.T) (*core, *remoteManager, *remoteRPCAPI) {
	t.Helper()
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CLAUDE_CONFIG_DIR", "")
	c, old, _, api := remoteRPCFixture(t)
	if err := old.shutdown(); err != nil {
		t.Fatal(err)
	}
	c.remote = nil
	c.server = &http.Server{}
	for _, agent := range []string{"codex", "claude-cli"} {
		pr, err := c.previewWithAPIKey(agent, "22222222-2222-4222-8222-222222222222", "nn_site_authorized_fixture_123456789")
		if err != nil {
			t.Fatal(err)
		}
		if err = c.apply(pr.Digest, pr.Content); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := callRemoteRPC(t, c, "remote.snapshot", nil); err != nil {
		t.Fatal(err)
	}
	return c, c.remote, api
}

func waitRemoteAuthorization(t *testing.T, m *remoteManager, agent string) remoteExecutor {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		state := *m.executors[agent]
		m.mu.Unlock()
		if state.State != "authorizing" {
			return state
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("executor authorization did not settle")
	return remoteExecutor{}
}

func TestRemoteAuthorizationRPCProbesOfflineCLIAndPinsIdentity(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli"} {
		t.Run(agent, func(t *testing.T) {
			remoteProtocolExecutable(t, agent, "ok")
			c, m, _ := remoteAuthorizationFixture(t)
			out, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": agent})
			if err != nil || out.(remoteExecutor).State != "authorizing" {
				t.Fatalf("authorization admission: %v %v", out, err)
			}
			e := waitRemoteAuthorization(t, m, agent)
			if !e.Authorized || e.State != "ready" || len(e.Identity) != 64 || e.Version == "" || !e.Capabilities["modelChange"] {
				t.Fatalf("offline conversation was not authorized: %+v", e)
			}
			if m.snapshot()["enabled"] != true {
				t.Fatal("successful explicit authorization did not register the host")
			}
			out, err = callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": agent})
			if err != nil || out.(remoteExecutor).Identity != e.Identity || out.(remoteExecutor).State != "ready" {
				t.Fatal("already-authorized executor started another probe")
			}
			command := remoteEnvelope("authorized-create", "session.create", "", map[string]any{"agent": agent, "cwd": m.home})
			command.ExpectedHostEpoch = m.epoch
			if _, err = callRemoteRPC(t, c, "remote.command", map[string]any{"command": command}); err != nil {
				t.Fatal(err)
			}
			if r := waitRemote(t, m, command.OperationID); r.Status != "providerAccepted" {
				t.Fatalf("authorized native session failed: %+v", r)
			}
		})
	}
}

func TestRemoteAuthorizationRPCRejectsChangedConfigOrCredentials(t *testing.T) {
	for _, change := range []string{"codex-config", "codex-auth", "claude-config", "claude-auth", "executable"} {
		t.Run(change, func(t *testing.T) {
			agent := "codex"
			if strings.HasPrefix(change, "claude") {
				agent = "claude-cli"
			}
			binary := remoteProtocolExecutable(t, agent, "ok")
			c, m, _ := remoteAuthorizationFixture(t)
			if _, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": agent}); err != nil {
				t.Fatal(err)
			}
			if e := waitRemoteAuthorization(t, m, agent); !e.Authorized {
				t.Fatalf("initial authorization: %+v", e)
			}
			config, _, err := c.agentPaths(agent)
			if err != nil {
				t.Fatal(err)
			}
			path := config
			switch change {
			case "codex-auth":
				path = filepath.Join(c.home, ".codex", "auth.json")
			case "claude-auth":
				path = filepath.Join(c.home, ".claude", ".credentials.json")
			case "executable":
				path = binary
			}
			if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, []byte("changed local identity"), 0700); err != nil {
				t.Fatal(err)
			}
			command := remoteEnvelope("changed-create", "session.create", "", map[string]any{"agent": agent, "cwd": m.home})
			command.ExpectedHostEpoch = m.epoch
			_, err = callRemoteRPC(t, c, "remote.command", map[string]any{"command": command})
			requireRemoteRPCError(t, err, "changed")
			m.mu.Lock()
			authorized, state, count := m.executors[agent].Authorized, m.executors[agent].State, len(m.sessions)
			m.mu.Unlock()
			if authorized || state != "failed" || count != 0 {
				t.Fatal("changed local identity retained authorization or admitted session")
			}
		})
	}
}

func TestRemoteAuthorizationRPCFailureNeverGrantsAccess(t *testing.T) {
	for _, mode := range []string{"missing-cli", "custom-codex-home", "custom-claude-home", "unsafe-config", "unsafe-credentials", "probe-failed", "account-changed"} {
		t.Run(mode, func(t *testing.T) {
			c, m, a := remoteAuthorizationFixture(t)
			agent := "codex"
			switch mode {
			case "missing-cli":
				t.Setenv("PATH", t.TempDir())
			case "custom-codex-home":
				t.Setenv("CODEX_HOME", t.TempDir())
			case "custom-claude-home":
				agent = "claude-cli"
				t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
			default:
				cliMode := "ok"
				if mode == "probe-failed" {
					cliMode = "version-failure"
				}
				remoteProtocolExecutable(t, agent, cliMode)
			}
			if mode == "unsafe-config" || mode == "unsafe-credentials" {
				path := c.configPath()
				if mode == "unsafe-credentials" {
					path = filepath.Join(c.home, ".codex", "auth.json")
				}
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				_ = os.Remove(path)
				if err := os.Symlink(filepath.Join(t.TempDir(), "foreign-config"), path); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "account-changed" {
				a.changeOwnerAt = 3
			}
			_, admissionErr := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": agent})
			if admissionErr != nil && mode != "unsafe-config" && mode != "unsafe-credentials" {
				t.Fatal(admissionErr)
			}
			e := waitRemoteAuthorization(t, m, agent)
			if e.Authorized || (admissionErr == nil && (e.State != "failed" || e.Error == "")) {
				t.Fatalf("failed probe granted access: %+v", e)
			}
			command := remoteEnvelope("failed-create", "session.create", "", map[string]any{"agent": agent, "cwd": m.home})
			command.ExpectedHostEpoch = m.epoch
			_, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": command})
			if err == nil {
				t.Fatal("failed authorization still admitted native work")
			}
			deadline := time.Now().Add(time.Second)
			for {
				probes, err := filepath.Glob(filepath.Join(filepath.Dir(m.path), "probe-*"))
				if err != nil {
					t.Fatal(err)
				}
				if len(probes) == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("probe directory leaked: %v", probes)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestRemoteAuthorizationRPCRejectsUnsupportedAndAvoidsDuplicateProbe(t *testing.T) {
	c, m, _ := remoteAuthorizationFixture(t)
	for _, agent := range []string{"unknown", "hana-agent"} {
		_, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": agent})
		requireRemoteRPCError(t, err, "verified remote adapter")
	}
	m.mu.Lock()
	m.executors["codex"].State = "authorizing"
	m.mu.Unlock()
	out, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "codex"})
	if err != nil || out.(remoteExecutor).State != "authorizing" {
		t.Fatal("pending authorization was not preserved")
	}
}

func TestRemoteCommandAuthorityHonorsCancellationAndRejection(t *testing.T) {
	if err := remoteCheckAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := errors.New("account revoked before side effect")
	ctx := context.WithValue(context.Background(), remoteCommandAuthorityKey{}, func(context.Context) error { return want })
	if !errors.Is(remoteCheckAuthority(ctx), want) {
		t.Fatal("execution authority rejection ignored")
	}
	called := false
	ctx = context.WithValue(context.Background(), remoteCommandAuthorityKey{}, func(context.Context) error { called = true; return nil })
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if !errors.Is(remoteCheckAuthority(ctx), context.Canceled) || called {
		t.Fatal("canceled operation revalidated or executed")
	}
}

func TestRemoteAuthorizationRetriesAddingAnExecutorToAnEnabledHost(t *testing.T) {
	remoteProtocolExecutable(t, "codex", "ok")
	c, m, api := remoteAuthorizationFixture(t)
	// A previously authorized executor has already started the shared host relay.
	m.mu.Lock()
	m.enabled = true
	m.mu.Unlock()
	api.mu.Lock()
	api.registerStatus = http.StatusTooManyRequests
	api.mu.Unlock()
	if _, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "codex"}); err != nil {
		t.Fatal(err)
	}
	e := waitRemoteAuthorization(t, m, "codex")
	if !e.Authorized || !strings.Contains(e.Error, "HTTP 429") {
		t.Fatalf("expected successful native probe and unsuccessful registration: %+v", e)
	}
	api.mu.Lock()
	api.registerStatus = 0
	api.mu.Unlock()
	out, err := callRemoteRPC(t, c, "agent.remote.authorize", map[string]any{"agent": "codex"})
	if err != nil {
		t.Fatal(err)
	}
	if got := out.(remoteExecutor); got.State != "ready" || got.Error != "" || got.Identity != e.Identity {
		t.Fatalf("registration retry did not confirm the same native authorization: %+v", got)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if got := api.registered["capabilities"]; !reflect.DeepEqual(got, []any{"codex"}) {
		t.Fatalf("executor capability was not published: %v", got)
	}
}

func TestRemoteAuthorizationBudgetAllowsSlowCodexPrefill(t *testing.T) {
	// The real Spark probe was still at 58% prefill when the 45s timer fired.
	if got := remoteProbeTimeout("codex"); got != 120*time.Second {
		t.Fatalf("Codex conversation has %s; want a bounded 120s", got)
	}
	if got := remoteAuthorizationTimeout("codex"); got != remoteProbeTimeout("codex")+15*time.Second {
		t.Fatalf("outer timeout %s truncates the probe or leaves no room for setup and revalidation", got)
	}
	for _, row := range []struct {
		agent       string
		probe, auth time.Duration
	}{
		{"claude-cli", 45 * time.Second, 60 * time.Second}, {"pi-agent", 45 * time.Second, 60 * time.Second},
		{"hermes-agent", 100 * time.Second, 110 * time.Second},
	} {
		if remoteProbeTimeout(row.agent) != row.probe || remoteAuthorizationTimeout(row.agent) != row.auth {
			t.Fatalf("unrelated %s authorization budget changed", row.agent)
		}
	}
}

func TestRemoteAuthorizationOpenClawSeparatesStartupAndTurnBudgets(t *testing.T) {
	if got := remoteProbeStartupTimeout("openclaw"); got != 70*time.Second {
		t.Fatalf("OpenClaw startup budget is %s; want 70s", got)
	}
	if got := remoteProbeTimeout("openclaw"); got != 120*time.Second {
		t.Fatalf("OpenClaw turn budget is %s; want 120s after setup", got)
	}
	if got := remoteAuthorizationTimeout("openclaw"); got != 205*time.Second {
		t.Fatalf("OpenClaw authorization total is %s; want 205s", got)
	}
}
