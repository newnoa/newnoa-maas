package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestRemoteHermesUsesManagedDefaultAndAccountGrant(t *testing.T) {
	c, _, api := remoteAuthorizationFixture(t)
	for _, env := range []string{"HERMES_HOME", "HERMES_PROFILE", "HERMES_CONFIG", "HERMES_CONFIG_PATH"} {
		t.Setenv(env, "")
	}
	p, err := c.previewWithAPIKey("hermes-agent", "22222222-2222-4222-8222-222222222222", "nn_site_authorized_fixture_123456789", "hermes-model-default")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	site, err := c.remoteSiteSelection(context.Background(), "rpc-session", "hermes-agent")
	if err != nil || site.Alias != "hermes-model-default" || site.KeyID == "" {
		t.Fatalf("Hermes managed selection failed: %v", err)
	}
	api.mu.Lock()
	api.siteChange = "models"
	api.mu.Unlock()
	if _, err = c.remoteSiteSelection(context.Background(), "rpc-session", "hermes-agent"); err == nil || err.Error() != remoteSiteKeyUnavailable {
		t.Fatalf("Hermes lost key grant must reject: %v", err)
	}
}

func TestRemoteHermesRequiresStrictNativeLoad(t *testing.T) {
	remoteProtocolExecutable(t, "hermes", "ok")
	binary, err := exec.LookPath("hermes")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"original", "missing"} {
		t.Run(id, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			p, err := startRemoteProviderCommand(ctx, "hermes", t.TempDir(), id, func(remoteProviderEvent) {}, binary, []string{"fixture-acp"}, nil)
			if id == "missing" {
				if err == nil {
					_ = p.Close()
					t.Fatal("native load returned null but Core accepted it")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = p.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRemoteHermesManagerSupportMatchesPlatform(t *testing.T) {
	want := runtime.GOOS == "linux" || runtime.GOOS == "darwin" || runtime.GOOS == "windows"
	m, err := newRemoteManager(t.TempDir(), remoteRPCOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer m.shutdown()
	if got := m.executors["hermes-agent"].Supported; got != want {
		t.Fatalf("Hermes manager support = %t, want %t", got, want)
	}
}

func TestRemoteHermesInstalledLifecycle(t *testing.T) {
	if os.Getenv("NEWNOA_TEST_INSTALLED_HERMES") != "1" {
		t.Skip("requires pinned Hermes with the ACP extra")
	}
	testRemoteInstalledLifecycle(t, "hermes-agent")
}

func TestRemoteHermesPinsAuthorizedModelWithoutDiscovery(t *testing.T) {
	if remoteHermesPinnedVersion(runtime.GOOS) == "" {
		t.Skip("Hermes launch is not supported on this platform")
	}
	home := t.TempDir()
	const alias = "authorized-model"
	ctx := context.WithValue(context.Background(), remoteSiteLaunchKey{}, remoteSiteLaunch{
		Home: home, Alias: alias, BaseURL: "http://127.0.0.1:18222", ContextLength: 131072,
	})
	// Inspect the generated profile without running an installed executor.
	_, err := startRemoteHermes(ctx, home, "", nil, filepath.Join(home, "missing-hermes"), true)
	if err == nil || err.Error() != "Hermes ACP dependencies are unavailable; install the pinned ACP extra" {
		t.Fatalf("expected missing executable, got %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(home, "hermes", "authorization", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Auxiliary struct {
			Title struct {
				Enabled *bool
			} `json:"title_generation"`
		}
		Providers map[string]struct {
			Discover *bool    `json:"discover_models"`
			Models   []string `json:"models"`
		} `json:"providers"`
	}
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	if config.Auxiliary.Title.Enabled == nil || *config.Auxiliary.Title.Enabled {
		t.Fatal("authorization probe must not generate a title using the model")
	}
	provider := config.Providers["newnoa-remote"]
	if len(config.Providers) != 1 || provider.Discover == nil || *provider.Discover || !reflect.DeepEqual(provider.Models, []string{alias}) {
		t.Fatal("isolated profile must list only the authorized model and not discover additional models")
	}
}

func TestRemoteHermesPinsExplicitCatalogContext(t *testing.T) {
	c, _, _ := remoteAuthorizationFixture(t)
	s, err := c.remoteHermesContext(context.Background(), "rpc-session", remoteSiteLaunch{Model: "22222222-2222-4222-8222-222222222222"})
	if err != nil || s.ContextLength != 131072 {
		t.Fatalf("expected detected model context, got %+v %v", s, err)
	}
	if _, err = c.remoteHermesContext(context.Background(), "rpc-session", remoteSiteLaunch{Model: "00000000-0000-0000-0000-000000000000"}); err == nil {
		t.Fatal("unknown capacity must not be invented")
	}
}

func TestRemoteHermesAttachUsesPrivateSingleUseTicket(t *testing.T) {
	doc := []byte(`{"state":"ready","url":"ws://127.0.0.1:1234/api/ws","protocol":"hermes-gateway-v1","ticket":"fixture-token-123456789","protocols":["hermes-gateway-v1","hermes-gateway-ticket.fixture-token-123456789"],"endpoint":{"profile_id":"/tmp/owned","supervisor":"external"}}`)
	target, protocols, err := parseHermesAttach(doc, "/tmp/owned")
	if err != nil || target.Host != "127.0.0.1:1234" || !reflect.DeepEqual(protocols, []string{"hermes-gateway-v1", "hermes-gateway-ticket.fixture-token-123456789"}) {
		t.Fatalf("valid ticket refused: %v", err)
	}
	if _, _, err := parseHermesAttach(doc, "/tmp/other"); err == nil {
		t.Fatal("unowned profile accepted")
	}
	if _, _, err := parseHermesAttach(bytes.ReplaceAll(doc, []byte("127.0.0.1"), []byte("192.0.2.2")), "/tmp/owned"); err == nil {
		t.Fatal("non-loopback target accepted")
	}
}

func TestRemoteHermesStopDetachesOnlyAfterOwnedProviderCloses(t *testing.T) {
	for _, succeeds := range []bool{true, false} {
		t.Run(strconv.FormatBool(succeeds), func(t *testing.T) {
			m, p, cwd := remoteFixture(t)
			id := createRemote(t, m, cwd)
			m.mu.Lock()
			m.sessions[id].Agent = "hermes-agent"
			m.sessions[id].State = "running"
			m.sessions[id].OwnedGateway = &remoteOwnedProcess{PID: 2, Identity: "fixture"}
			m.mu.Unlock()
			if !succeeds {
				p.closeErr = errors.New("cannot verify cleanup")
			}
			if _, err := m.command(remoteEnvelope("stop-hermes", "run.stop", id, map[string]any{})); err != nil {
				t.Fatal(err)
			}
			r := waitRemote(t, m, "stop-hermes")
			m.mu.Lock()
			defer m.mu.Unlock()
			s := m.sessions[id]
			if succeeds && (r.Status != "providerAccepted" || s.State != "stopped" || s.OwnedGateway != nil || m.providers[id] != nil) {
				t.Fatal("confirmed stop did not detach provider", r, s.State)
			}
			if !succeeds && (r.Status != "rejected" || s.State != "unknown" || s.OwnedGateway == nil || m.providers[id] == nil) {
				t.Fatal("unconfirmed stop dropped ownership", r, s.State)
			}
			p.closeErr = nil
		})
	}
}

func TestRemoteHermesVersionDisablesDetachedInstalls(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture")
	}
	t.Setenv("HERMES_DISABLE_LAZY_INSTALLS", "0")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", `test "$HERMES_DISABLE_LAZY_INSTALLS" = 1 && echo 'Hermes Agent v0.23.2'`)
	prepareVersionCommand(cmd)
	if _, err := remoteHermesVersionLine(cmd); err != nil {
		t.Fatalf("version probe permitted Hermes to install dependencies: %v", err)
	}
}

func TestRemoteHermesVersionDoesNotWaitForOptionalUpdate(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("shell fixture")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hermes"), []byte("#!/bin/sh\nprintf 'Hermes Agent v0.23.1 (fixture)\\n'\nexec /bin/sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	info, err := remoteProviderDetailsContext(ctx, "hermes-agent")
	if err != nil || info.Version != "0.23.1" {
		t.Fatalf("local version was held behind optional update check: %v", err)
	}
}

func TestRemoteHermesSeedsFreshProfileFromSiblingCatalog(t *testing.T) {
	root := t.TempDir()
	hermesDir := filepath.Join(root, "hermes")
	old := filepath.Join(hermesDir, "older")
	recent := filepath.Join(hermesDir, "recent")
	target := filepath.Join(hermesDir, "new")
	for _, dir := range []string{old, recent, target} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	const filename = "models_dev_cache.json"
	current := []byte(`{"openai":{"id":"openai","models":{}}}`)
	stamp := time.Now().Add(-time.Hour).Truncate(time.Second)
	for i, row := range []struct {
		dir  string
		body []byte
	}{{old, []byte(`{"other":{}}`)}, {recent, current}} {
		path := filepath.Join(row.dir, filename)
		if err := os.WriteFile(path, row.body, 0600); err != nil {
			t.Fatal(err)
		}
		at := stamp.Add(time.Duration(i-1) * time.Hour)
		if err := os.Chtimes(path, at, at); err != nil {
			t.Fatal(err)
		}
	}
	// Exercise the production startup path up to the runtime check.
	ctx := context.WithValue(context.Background(), remoteSiteLaunchKey{}, remoteSiteLaunch{
		Home: root, Alias: "authorized-model", BaseURL: "http://127.0.0.1:18222", ContextLength: 131072,
	})
	ctx = context.WithValue(ctx, remoteProviderStateDirKey{}, root)
	ctx = context.WithValue(ctx, remoteProviderSessionSlotKey{}, "new")
	if _, err := startRemoteHermes(ctx, root, "", nil, filepath.Join(root, "missing-hermes"), false); err == nil {
		t.Fatal("missing executable unexpectedly started")
	}
	path := filepath.Join(target, filename)
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, current) {
		t.Fatalf("fresh profile did not reuse its newest sibling catalog: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || !info.ModTime().Equal(stamp) {
		t.Fatalf("catalog freshness was changed: %v", err)
	}
	owned := []byte(`{"alreadyPresent":{}}`)
	if err := os.WriteFile(path, owned, 0600); err != nil {
		t.Fatal(err)
	}
	_, _ = startRemoteHermes(ctx, root, "", nil, filepath.Join(root, "missing-hermes"), false)
	got, err = os.ReadFile(path)
	if err != nil || !bytes.Equal(got, owned) {
		t.Fatalf("existing cache was overwritten: %v", err)
	}
}

func TestRemoteHermesPinnedVersionByHostPlatform(t *testing.T) {
	for platform, want := range map[string]string{"linux": "0.23.1", "windows": "0.23.2", "darwin": "0.23.2", "freebsd": ""} {
		if got := remoteHermesPinnedVersion(platform); got != want {
			t.Errorf("%s version = %q, want %q", platform, got, want)
		}
	}
}

func TestRemoteHermesVersionRejectsUnusableOutput(t *testing.T) {
	missing := exec.Command(filepath.Join(t.TempDir(), "missing-hermes"))
	if line, err := remoteHermesVersionLine(missing); err == nil || len(line) != 0 {
		t.Fatalf("missing executable returned %q, %v", line, err)
	}
	if runtime.GOOS == "windows" {
		return
	}
	for _, output := range []string{"Hermes without newline", string(bytes.Repeat([]byte("x"), 121)) + "\n"} {
		cmd := exec.Command("/bin/sh", "-c", `printf '%s' "$1"`, "fixture", output)
		if line, err := remoteHermesVersionLine(cmd); err == nil || len(line) != 0 {
			t.Fatalf("invalid version output was accepted: %q", line)
		}
	}
}

func TestRemoteHermesLeavesFreshProfileEmptyWithoutUsableCatalog(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"absent", ""},
		{"invalid-json", "{not-json}"},
		{"wrong-shape", "[1, 2, 3]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			target := filepath.Join(root, "hermes", "fresh")
			old := filepath.Join(root, "hermes", "old")
			for _, dir := range []string{old, target} {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			const name = "models_dev_cache.json"
			if tc.body != "" {
				if err := os.WriteFile(filepath.Join(old, name), []byte(tc.body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			seedRemoteHermesCatalog(root, target)
			if _, err := os.Stat(filepath.Join(target, name)); !os.IsNotExist(err) {
				t.Fatalf("unexpected destination cache state: %v", err)
			}
		})
	}
}

func TestRemoteHermesCatalogStaysWithinStateDirectory(t *testing.T) {
	root, other := t.TempDir(), t.TempDir()
	source, target := filepath.Join(other, "source"), filepath.Join(other, "target")
	for _, dir := range []string{source, target} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	const name = "models_dev_cache.json"
	if err := os.WriteFile(filepath.Join(source, name), []byte(`{"models":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	seedRemoteHermesCatalog(root, target)
	if _, err := os.Stat(filepath.Join(target, name)); !os.IsNotExist(err) {
		t.Fatalf("cache copied outside the account directory: %v", err)
	}
}
