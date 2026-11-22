package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Prevent Hermes from starting a detached default-plugin installer during bootstrap.
const remoteHermesNoLazyInstalls = "HERMES_DISABLE_LAZY_INSTALLS=1"

func remoteHermesPinnedVersion(platform string) string {
	switch platform {
	case "linux":
		return "0.23.1"
	case "darwin", "windows":
		return "0.23.2"
	default:
		return ""
	}
}

// Hermes emits its installed version before synchronously checking for updates.
// Read that line and promptly close the prepared process group.
func remoteHermesVersionLine(cmd *exec.Cmd) ([]byte, error) {
	cmd.Env = append(cmd.Environ(), "PYTHONUNBUFFERED=1", remoteHermesNoLazyInstalls)
	prepareVersionCommand(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	defer func() {
		cleanupVersionCommand(cmd)
		_ = cmd.Wait()
	}()
	line, err := bufio.NewReaderSize(io.LimitReader(stdout, 122), 122).ReadString('\n')
	if err != nil || len(strings.TrimSpace(line)) > 120 {
		return nil, errors.New("Hermes did not report a bounded version line")
	}
	return []byte(line), nil
}

// Hermes uses this public metadata cache for its ACP model picker even with
// provider discovery disabled. New per-session profiles otherwise block on a
// network download. Reuse only within the same account's profile directory.
func seedRemoteHermesCatalog(stateDir, dir string) {
	const filename = "models_dev_cache.json"
	const maxBytes = 12 << 20
	destination := filepath.Join(dir, filename)
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		return
	}
	guard := &core{home: stateDir}
	if guard.safeConfigFile(filepath.Join(dir, ".cache-path-check")) != nil {
		return
	}
	parent, err := os.Open(filepath.Dir(dir))
	if err != nil {
		return
	}
	entries, _ := parent.ReadDir(128)
	_ = parent.Close()
	var newest os.FileInfo
	var source string
	now := time.Now()
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == filepath.Base(dir) {
			continue
		}
		profile := filepath.Join(filepath.Dir(dir), entry.Name())
		if guard.safeConfigFile(filepath.Join(profile, ".cache-path-check")) != nil {
			continue
		}
		path := filepath.Join(profile, filename)
		info, err := os.Lstat(path)
		if err == nil && info.Mode().IsRegular() && info.Size() > 2 && info.Size() <= maxBytes &&
			!info.ModTime().After(now) && (newest == nil || info.ModTime().After(newest.ModTime())) {
			newest, source = info, path
		}
	}
	if newest == nil {
		return
	}
	file, err := os.Open(source)
	if err != nil {
		return
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(newest, opened) || !opened.Mode().IsRegular() {
		return
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	raw = bytes.TrimSpace(bytes.TrimPrefix(raw, []byte{0xef, 0xbb, 0xbf}))
	if err != nil || len(raw) <= 2 || len(raw) > maxBytes || raw[0] != '{' || !json.Valid(raw) {
		return
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		return
	}
	if atomicFile(destination, raw) == nil {
		// Preserve staleness so Hermes still refreshes an older catalog itself.
		_ = os.Chtimes(destination, newest.ModTime(), newest.ModTime())
	}
}

// Start the Gateway as our own foreground subprocess, then use the CLI's
// attach-only discovery. Passing a checked explicit URL to ACP disables its
// detached Gateway auto-start/fallback path.
func startRemoteHermes(ctx context.Context, cwd, nativeID string, emit func(remoteProviderEvent), binary string, probe bool) (remoteProvider, error) {
	site, ok := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch)
	if !ok {
		return nil, errors.New(remoteSiteConfigurationRequired)
	}
	if site.ContextLength < 64000 || site.ContextLength > 2000000 {
		return nil, errors.New("Hermes requires a configured context length of at least 64000 tokens")
	}
	if remoteHermesPinnedVersion(runtime.GOOS) == "" {
		return nil, errors.New("Hermes remote Gateway requires Linux, macOS or Windows")
	}
	stateDir, _ := ctx.Value(remoteProviderStateDirKey{}).(string)
	slot, _ := ctx.Value(remoteProviderSessionSlotKey{}).(string)
	if probe {
		stateDir, slot = cwd, "authorization"
	}
	if !filepath.IsAbs(stateDir) || slot == "" || slot == "." || slot == ".." || filepath.Base(slot) != slot {
		return nil, errors.New("Hermes native state directory is unavailable")
	}
	dir := filepath.Join(stateDir, "hermes", slot)
	configPath := filepath.Join(dir, "config.yaml")
	if (&core{home: site.Home}).safeConfigFile(configPath) != nil || os.MkdirAll(dir, 0700) != nil {
		return nil, errors.New("Hermes state directory is unsafe")
	}
	if !probe {
		seedRemoteHermesCatalog(stateDir, dir)
	}
	// Hermes returns a canonical profile_id, including Darwin's /private/var spelling.
	if runtime.GOOS == "darwin" {
		resolved, err := filepath.EvalSymlinks(dir)
		if err != nil {
			return nil, errors.New("Cannot resolve isolated Hermes profile")
		}
		dir = resolved
	}
	// JSON is a valid YAML document. No user hooks, external providers, profile,
	// projects' plugins, or MCP configuration are inherited by this runtime.
	settings := map[string]any{
		"model": map[string]any{"provider": "custom:newnoa-remote", "default": site.Alias, "context_length": site.ContextLength},
		"providers": map[string]any{"newnoa-remote": map[string]any{
			"base_url": site.BaseURL + "/v1", "key_env": "NEWNOA_REMOTE_API_KEY",
			"api_mode": "chat_completions", "default_model": site.Alias,
			// ACP only needs the model already authorized by Core; discovery can block session creation.
			"discover_models": false, "models": []string{site.Alias},
			"extra_body": map[string]int{"max_tokens": 4096},
		}},
		"agent": map[string]any{"reasoning_effort": "low"},
		// The foreground Gateway resolves CLI toolsets; the ACP fallback uses acp.
		"platform_toolsets": map[string][]string{"cli": {}, "acp": {}},
	}
	if probe {
		// The disposable authorization session does not need a generated title.
		settings["auxiliary"] = map[string]any{"title_generation": map[string]bool{"enabled": false}}
	}
	config, err := json.Marshal(settings)
	if err != nil || atomicFile(configPath, config) != nil {
		return nil, errors.New("Cannot write isolated Hermes configuration")
	}
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir}
	if runtime.GOOS == "windows" {
		env = openClawRemoteEnv(dir)
	}
	env = append(env, "HERMES_HOME="+dir, "NEWNOA_REMOTE_API_KEY="+site.Secret, "HERMES_ENABLE_PROJECT_PLUGINS=0", "PYTHONIOENCODING=utf-8", remoteHermesNoLazyInstalls)
	if base, ok := ctx.Value(remoteExecutorEnvironmentKey{}).([]string); ok {
		env = mergeRemoteEnvironment(base, env)
	}
	// Optional dependency path is operator-supplied, never part of client commands.
	if dep := os.Getenv("NEWNOA_HERMES_ACP_PYTHONPATH"); dep != "" {
		if !filepath.IsAbs(dep) {
			return nil, errors.New("Invalid Hermes ACP dependency directory")
		}
		env = append(env, "PYTHONPATH="+dep)
	}
	checkCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	check := exec.CommandContext(checkCtx, binary, "--profile", "default", "acp", "--check")
	check.Dir, check.Env = cwd, env
	err = runVersionCommand(check)
	cancel()
	if err != nil {
		return nil, errors.New("Hermes ACP dependencies are unavailable; install the pinned ACP extra")
	}
	gateway, err := launchRemoteGateway(ctx, binary, cwd, []string{"--profile", "default", "gateway", "run", "--quiet", "--no-supervise", "--external-supervisor"}, env)
	if err != nil {
		return nil, err
	}
	target, protocols, err := waitHermesAttach(ctx, gateway, binary, cwd, dir, env)
	if err != nil {
		return nil, errors.Join(err, gateway.Close())
	}
	endpoint, stop, err := hermesTicketBridge(target, protocols)
	if err != nil {
		return nil, errors.Join(err, gateway.Close())
	}
	closeGateway := func() error { stop(); return gateway.Close() }
	p, err := startRemoteProviderCommand(context.WithValue(ctx, remoteGatewayContextKey{}, closeGateway), "hermes", cwd, nativeID, emit, binary, []string{"--profile", "default", "acp"}, append(env, "HERMES_TUI_GATEWAY_URL="+endpoint), probe)
	if err != nil {
		return nil, errors.Join(err, closeGateway())
	}
	return p, nil
}

func waitHermesAttach(ctx context.Context, gateway *remoteGatewayProcess, binary, cwd, home string, env []string) (*url.URL, []string, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, nil, errors.New("Hermes Gateway readiness timed out")
		case <-gateway.done:
			return nil, nil, errors.New("Owned Hermes Gateway exited before readiness")
		default:
		}
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		read := exec.CommandContext(readCtx, binary, "--profile", "default", "gateway", "attach", "--json", "--timeout", "3")
		read.Dir, read.Env = cwd, env
		raw, err := versionCommandOutput(read)
		cancel()
		if err == nil {
			return parseHermesAttach(raw, home)
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-gateway.done:
			return nil, nil, errors.New("Owned Hermes Gateway exited before readiness")
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func parseHermesAttach(raw []byte, home string) (*url.URL, []string, error) {
	const protocol = "hermes-gateway-v1"
	var ticket struct {
		State, URL, Protocol, Ticket string
		Protocols                    []string
		Endpoint                     struct {
			ProfileID  string `json:"profile_id"`
			Supervisor string `json:"supervisor"`
		}
	}
	if len(raw) > 16384 || json.Unmarshal(raw, &ticket) != nil || ticket.State != "ready" || ticket.Endpoint.ProfileID != home || ticket.Endpoint.Supervisor != "external" {
		return nil, nil, errors.New("Hermes Gateway identity did not match its owned profile")
	}
	u, err := url.Parse(ticket.URL)
	if err != nil || u.Scheme != "ws" || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.Path != "/api/ws" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, nil, errors.New("Hermes Gateway is not a verified private loopback endpoint")
	}
	if ticket.Protocol != protocol || len(ticket.Ticket) < 16 || len(ticket.Ticket) > 1024 || len(ticket.Protocols) != 2 || ticket.Protocols[0] != protocol || ticket.Protocols[1] != "hermes-gateway-ticket."+ticket.Ticket || strings.ContainsAny(ticket.Ticket, " \t\r\n,") {
		return nil, nil, errors.New("Hermes Gateway did not return a valid ticket protocol")
	}
	return u, ticket.Protocols, nil
}

// ACP's explicit URL prevents a detached auto-start, but cannot pass ticket
// subprotocols. Forward exactly one upgrade to the verified Gateway, keeping
// the ticket out of argv, logs, and the browser.
func hermesTicketBridge(target *url.URL, protocols []string) (string, func(), error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return "", nil, errors.New("Cannot start private Hermes ticket bridge")
	}
	var lock sync.Mutex
	var conn net.Conn
	closed := false
	path := "/" + randomToken()
	host := listener.Addr().String()
	proxy := &httputil.ReverseProxy{
		Transport: &http.Transport{DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext, ResponseHeaderTimeout: 8 * time.Second, DisableKeepAlives: true},
		Rewrite: func(req *httputil.ProxyRequest) {
			req.Out.URL = &url.URL{Scheme: "http", Host: target.Host, Path: target.Path}
			req.Out.Host = target.Host
			req.Out.Header.Set("Sec-WebSocket-Protocol", strings.Join(protocols, ", "))
		},
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Sec-WebSocket-Protocol") != protocols[0] {
				return errors.New("Hermes Gateway refused the owned ticket")
			}
			resp.Header.Del("Sec-WebSocket-Protocol")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "Hermes Gateway attach rejected", http.StatusBadGateway)
		},
	}
	server := &http.Server{ReadHeaderTimeout: 3 * time.Second, MaxHeaderBytes: 8192,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return context.WithValue(ctx, hermesBridgeConnKey{}, c)
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Host != host || r.Method != http.MethodGet || r.URL.Path != path || r.URL.RawQuery != "" || r.Header.Get("Origin") != "" || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
				http.NotFound(w, r)
				return
			}
			c, _ := r.Context().Value(hermesBridgeConnKey{}).(net.Conn)
			if c == nil {
				http.NotFound(w, r)
				return
			}
			lock.Lock()
			if closed || conn != nil {
				lock.Unlock()
				http.NotFound(w, r)
				return
			}
			conn = c
			lock.Unlock()
			// Prevent further connects before forwarding the one admitted peer.
			_ = listener.Close()
			proxy.ServeHTTP(w, r)
		}),
	}
	go func() { _ = server.Serve(listener) }()
	stop := func() {
		lock.Lock()
		closed = true
		if conn != nil {
			_ = conn.Close()
		}
		lock.Unlock()
		_ = server.Close()
	}
	return "ws://" + host + path, stop, nil
}

type hermesBridgeConnKey struct{}

// Context is catalog metadata for the already-authorized deployment, not a
// model inference or a client-selected override. It prevents Hermes probing
// unrelated server protocols to guess an unknown context window.
func (c *core) remoteHermesContext(ctx context.Context, session string, site remoteSiteLaunch) (remoteSiteLaunch, error) {
	var detail struct {
		ID         string `json:"id"`
		Configured int64  `json:"contextLength"`
		Detected   int64  `json:"detectedContextLength"`
	}
	if !nodePattern.MatchString(site.Model) || c.remoteHTTP(ctx, "GET", "/deployments/catalog/"+site.Model, session, nil, nil, &detail) != nil || detail.ID != site.Model {
		return site, errors.New("Hermes model context metadata is unavailable")
	}
	site.ContextLength = detail.Detected
	if site.ContextLength == 0 {
		site.ContextLength = detail.Configured
	}
	if site.ContextLength < 64000 || site.ContextLength > 2000000 {
		return site, errors.New("Hermes requires a configured context length of at least 64000 tokens")
	}
	return site, nil
}
