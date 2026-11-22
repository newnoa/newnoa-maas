package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type remoteProviderInput struct {
	OperationID string
	Text        string
	Images      []remoteProviderImage
}
type remoteProviderImage struct{ URL string }
type remoteProviderReply struct {
	Instance  uint64
	RequestID string
	Decision  string
	Answers   map[string][]string
}
type remoteProviderApproval struct {
	Instance        uint64
	ID, Kind, Title string
	Options         []string
	Data            json.RawMessage
}
type remoteProviderEvent struct {
	probeFailureKind                          string
	ApprovalInstance                          uint64
	OperationID, Delivery                     string
	Type, NativeID, RunID, Text, State, Model string
	Approval                                  *remoteProviderApproval
	Error                                     string
}
type remoteProviderInfo struct {
	Agent, Version string
	Capabilities   map[string]bool
	command        remoteCommand
}
type remoteProvider interface {
	StartTurn(context.Context, remoteProviderInput) error
	Steer(context.Context, remoteProviderInput) error
	Stop(context.Context) error
	Respond(context.Context, remoteProviderReply) error
	Close() error
}

type remoteProviderLifetimeKey struct{}
type remoteProviderStateDirKey struct{}
type remoteExecutorEnvironmentKey struct{}

func mergeRemoteEnvironment(base, extra []string) []string {
	values := make(map[string]string, len(base)+len(extra))
	order := make([]string, 0, len(base)+len(extra))
	for _, entry := range append(append([]string(nil), base...), extra...) {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = entry
	}
	out := make([]string, 0, len(order))
	for _, key := range order {
		out = append(out, values[key])
	}
	return out
}

// Startup authority expires with the operation; the owned CLI lives with its host.
func remoteProviderContext(lifetime, operation context.Context) context.Context {
	return context.WithValue(operation, remoteProviderLifetimeKey{}, lifetime)
}

type remoteNativeProvider struct {
	agent             string
	siteModel         string
	siteEffort        string
	command           *exec.Cmd
	stdin             io.WriteCloser
	cancel            context.CancelFunc
	done              chan struct{}
	writeMu           sync.Mutex
	replyMu           sync.Mutex
	turnMu            sync.Mutex
	credentialMu      sync.Mutex
	revokeCredential  func(context.Context) error
	mu                sync.Mutex
	seq               uint64
	pending           map[string]chan remoteNativeMessage
	dispatches        map[string]remoteNativeDispatch
	approvalClaims    map[string]*remoteApprovalClaim
	approvalInstances map[string]uint64
	approvalSequence  uint64
	approvals         map[string]remoteNativeMessage
	nativeID, runID   string
	emit              func(remoteProviderEvent)
	events            chan remoteProviderEvent
	closed            bool
	closing           bool
	probe             bool
	completedRuns     map[string]bool
	piTerminalError   bool
	piWaiters         map[string]remotePiWaiter
	acp               *remoteACPState
	closeGateway      func() error
	gatewayCloseOnce  sync.Once
	gatewayCloseErr   error
	owned             remoteOwnedProcess
}
type remoteNativeMessage struct {
	ID        json.RawMessage `json:"id"`
	Method    string          `json:"method"`
	Params    json.RawMessage `json:"params"`
	Result    json.RawMessage `json:"result"`
	Error     json.RawMessage `json:"error"`
	Type      string          `json:"type"`
	RequestID string          `json:"request_id"`
	Request   json.RawMessage `json:"request"`
	Response  json.RawMessage `json:"response"`
	Raw       json.RawMessage `json:"-"`
}

func remoteProviderDetails(agent string) (remoteProviderInfo, error) {
	return remoteProviderDetailsContext(context.Background(), agent)
}

func remoteProviderBinary(agent string) string {
	switch agent {
	case "claude-cli":
		return "claude"
	case "pi-agent":
		return "pi"
	case "hermes-agent":
		return "hermes"
	default:
		return agent
	}
}

// The version probe and both native launch routes use the same resolved invocation.
type remoteCommand struct {
	path, entry string
}

func (c remoteCommand) args(args ...string) []string {
	if c.entry == "" {
		return args
	}
	return append([]string{c.entry}, args...)
}

func remoteExecutorCommand(agent string) (remoteCommand, error) {
	path, err := exec.LookPath(agent)
	if err != nil {
		return remoteCommand{}, err
	}
	if runtime.GOOS == "windows" && agent == "openclaw" {
		return windowsOpenClawCommand(path)
	}
	if runtime.GOOS == "windows" && agent == "opencode" {
		// npm's fixed layout contains a real exe; do not invoke the batch shim.
		if filepath.IsAbs(path) && strings.EqualFold(filepath.Base(path), "opencode.cmd") {
			dir := filepath.Dir(path)
			modules := filepath.Join(dir, "node_modules")
			if strings.EqualFold(filepath.Base(dir), ".bin") && strings.EqualFold(filepath.Base(filepath.Dir(dir)), "node_modules") {
				modules = filepath.Dir(dir)
			}
			path = filepath.Join(modules, "opencode-ai", "bin", "opencode.exe")
		}
		if !filepath.IsAbs(path) || !strings.EqualFold(filepath.Ext(path), ".exe") {
			return remoteCommand{}, errors.New("OpenCode requires a native Windows executable")
		}
		if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() {
			return remoteCommand{}, errors.New("OpenCode Windows executable is unavailable")
		}
	}
	return remoteCommand{path: path}, nil
}

func remoteProviderDetailsContext(parent context.Context, agent string) (remoteProviderInfo, error) {
	requestedAgent := agent
	agent = remoteProviderBinary(agent)
	if agent != "codex" && agent != "claude" && agent != "pi" && !remoteACPAgent(agent) {
		return remoteProviderInfo{}, fmt.Errorf("remote executor %q is unsupported", agent)
	}
	command, err := remoteExecutorCommand(agent)
	if err != nil {
		return remoteProviderInfo{}, fmt.Errorf("%s CLI is not installed or its executable cannot be resolved", agent)
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	c := exec.CommandContext(ctx, command.path, command.args("--version")...)
	if runtime.GOOS == "windows" && agent == "openclaw" {
		home, err := os.MkdirTemp("", "newnoa-openclaw-version-")
		if err != nil {
			return remoteProviderInfo{}, errors.New("OpenClaw version probe isolation is unavailable")
		}
		defer os.RemoveAll(home)
		c.Dir, c.Env = home, openClawRemoteEnv(home)
	}
	var out []byte
	if agent == "hermes" {
		out, err = remoteHermesVersionLine(c)
	} else {
		out, err = versionCommandOutput(c)
	}
	if err != nil {
		return remoteProviderInfo{}, fmt.Errorf("%s CLI version detection failed", agent)
	}
	version := strings.TrimSpace(string(out))
	if agent == "hermes" {
		version, _, _ = strings.Cut(version, "\n")
	}
	if len(version) > 120 {
		return remoteProviderInfo{}, errors.New("invalid CLI version response")
	}
	if agent == "hermes" {
		want := remoteHermesPinnedVersion(runtime.GOOS)
		prefix := "Hermes Agent v" + want
		if want == "" || !(version == prefix || strings.HasPrefix(version, prefix+" ")) {
			return remoteProviderInfo{}, errors.New("Hermes remote requires validated 0.23.1 on Linux or 0.23.2 on macOS and Windows")
		}
		return remoteProviderInfo{Agent: requestedAgent, command: command, Version: want, Capabilities: map[string]bool{"create": true, "resume": true, "send": true, "interrupt": true, "actualModel": true}}, nil
	}
	if agent == "openclaw" {
		if want := remoteOpenClawVersion(runtime.GOOS); want == "" || version != want {
			return remoteProviderInfo{}, errors.New("OpenClaw remote requires validated 2026.11.4 on Linux or 2026.11.5 on macOS and Windows")
		}
		return remoteProviderInfo{Agent: requestedAgent, command: command, Version: version, Capabilities: map[string]bool{"create": true, "resume": true, "send": true, "interrupt": true}}, nil
	}
	if agent == "opencode" {
		if !remoteOpenCodePlatform() || version != "1.19.6" {
			return remoteProviderInfo{}, errors.New("OpenCode remote requires validated 1.19.6 on Linux, macOS or Windows")
		}
		return remoteProviderInfo{Agent: requestedAgent, command: command, Version: version, Capabilities: map[string]bool{"create": true, "resume": true, "send": true, "interrupt": true}}, nil
	}
	if agent == "pi" {
		if version != "1.5.2" {
			return remoteProviderInfo{}, errors.New("Pi CLI version is not validated; expected 1.5.2")
		}
		return remoteProviderInfo{Agent: requestedAgent, command: command, Version: version, Capabilities: map[string]bool{"create": true, "resume": true, "send": true, "interrupt": true, "actualModel": true}}, nil
	}
	if !remoteCodexClaudeVersionAllowed(runtime.GOOS, agent, version) {
		expected := map[string]string{"codex": "0.169.0", "claude": "2.1.325"}[agent]
		if remoteCodexClaudeCurrentPlatform(runtime.GOOS) {
			expected += " or " + map[string]string{"codex": "0.170.0", "claude": "2.1.328"}[agent]
		}
		return remoteProviderInfo{}, fmt.Errorf("%s CLI version is not validated; expected %s", agent, expected)
	}
	return remoteProviderInfo{Agent: requestedAgent, command: command, Version: version, Capabilities: map[string]bool{"create": true, "resume": true, "send": true, "steer": agent == "codex", "interrupt": true, "approvals": true, "questions": true, "image": true, "images": true, "actualModel": agent == "claude", "modelChange": false}}, nil
}

func remoteCodexClaudeCurrentPlatform(platform string) bool {
	return platform == "darwin" || platform == "windows"
}

func remoteCodexClaudeVersionAllowed(platform, agent, version string) bool {
	switch agent {
	case "codex":
		return version == "codex-cli 0.169.0" || remoteCodexClaudeCurrentPlatform(platform) && version == "codex-cli 0.170.0"
	case "claude":
		return version == "2.1.325 (Claude Code)" || remoteCodexClaudeCurrentPlatform(platform) && version == "2.1.328 (Claude Code)"
	default:
		return false
	}
}

func startRemoteProvider(ctx context.Context, agent, cwd, nativeID string, emit func(remoteProviderEvent)) (remoteProvider, error) {
	if err := remoteCheckAuthority(ctx); err != nil {
		return nil, err
	}
	info, err := remoteProviderDetailsContext(ctx, agent)
	if err != nil {
		return nil, err
	}
	return launchRemoteProvider(ctx, remoteProviderBinary(agent), cwd, nativeID, emit, info.command, false)
}

func launchRemoteProvider(ctx context.Context, agent, cwd, nativeID string, emit func(remoteProviderEvent), command remoteCommand, probe bool) (remoteProvider, error) {
	if site, managed := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch); managed {
		local := strings.HasPrefix(site.Secret, "rla_")
		if probe {
			local = strings.HasPrefix(site.Secret, "nref_")
		}
		if !local {
			return nil, errors.New("Remote executors require an authorized local credential")
		}
	}
	binary := command.path
	if agent == "hermes" {
		return startRemoteHermes(ctx, cwd, nativeID, emit, binary, probe)
	}
	if agent == "opencode" {
		return startRemoteOpenCode(ctx, cwd, nativeID, emit, binary, probe)
	}
	if agent == "openclaw" {
		return startRemoteOpenClaw(ctx, cwd, nativeID, emit, command, probe)
	}
	return startRemoteProviderCommand(ctx, agent, cwd, nativeID, emit, binary, nil, nil, probe)
}

func startRemoteProviderCommand(ctx context.Context, agent, cwd, nativeID string, emit func(remoteProviderEvent), binary string, args, env []string, probe ...bool) (remoteProvider, error) {
	if err := remoteCheckAuthority(ctx); err != nil {
		return nil, err
	}
	isProbe := len(probe) > 0 && probe[0]
	site, siteManaged := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch)
	if agent != "codex" && agent != "claude" && agent != "pi" && !remoteACPAgent(agent) {
		return nil, errors.New("unsupported remote executor")
	}
	if !filepath.IsAbs(cwd) {
		return nil, errors.New("remote working directory must be absolute")
	}
	if siteManaged {
		stateDir, _ := ctx.Value(remoteProviderStateDirKey{}).(string)
		slot, _ := ctx.Value(remoteProviderSessionSlotKey{}).(string)
		if isProbe {
			stateDir = cwd
			slot = "authorization"
		}
		if !filepath.IsAbs(stateDir) || slot == "" || slot == "." || slot == ".." || filepath.Base(slot) != slot {
			return nil, errors.New("Remote native session isolation is unavailable")
		}
		home := filepath.Join(stateDir, "remote-executors", slot, agent)
		if err := (&core{home: stateDir}).safeConfigFile(filepath.Join(home, "config-check")); err != nil {
			return nil, err
		}
		if (agent == "codex" || agent == "claude") && nativeID != "" && !isProbe {
			// Older native sessions live in the user's original CLI home.
			// Their verified managed account keys have been migrated to loopback references.
			if _, err := os.Stat(home); os.IsNotExist(err) {
				home = site.Home
			}
		}
		if agent == "codex" && home != site.Home {
			// The native CLI requires an explicit CODEX_HOME to exist on first launch.
			dir := filepath.Join(home, ".codex")
			if (&core{home: stateDir}).safeConfigFile(filepath.Join(dir, "config.toml")) != nil || os.MkdirAll(dir, 0700) != nil {
				return nil, errors.New("Cannot initialize isolated Codex home")
			}
		}
		isolated, err := remoteCredentialFreeEnvironment(home)
		if err != nil {
			return nil, err
		}
		if extra, ok := ctx.Value(remoteExecutorEnvironmentKey{}).([]string); ok {
			isolated = mergeRemoteEnvironment(isolated, extra)
		}
		ctx = context.WithValue(ctx, remoteExecutorEnvironmentKey{}, isolated)
		site.Home = home
		ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, site)
	}
	if agent == "pi" && !siteManaged {
		return nil, errors.New(remoteSiteConfigurationRequired)
	}
	var piCleanup func()
	if agent == "pi" {
		var err error
		if isProbe {
			ctx = context.WithValue(ctx, remoteProviderStateDirKey{}, cwd)
		}
		args, env, nativeID, piCleanup, err = prepareRemotePi(ctx, site, nativeID, isProbe)
		if err != nil {
			return nil, err
		}
		defer func() {
			if piCleanup != nil {
				piCleanup()
			}
		}()
	}
	if args == nil {
		if remoteACPAgent(agent) {
			return nil, errors.New("ACP executor requires an owned Gateway launch")
		}
		if agent == "codex" {
			args = []string{"app-server", "--stdio"}
			if siteManaged {
				provider := `{name="NEWNOA Remote",base_url=` + strconv.Quote(site.BaseURL+"/v1") + `,wire_api="responses",env_key="NEWNOA_REMOTE_API_KEY",requires_openai_auth=false}`
				args = append(args, "-c", `model_provider="newnoa_remote"`, "-c", "model="+strconv.Quote(site.Alias), "-c", "model_providers.newnoa_remote="+provider)
			}
			if isProbe {
				args = append(args, "-c", "mcp_servers={}", "-c", "features.shell_tool=false", "-c", "features.code_mode=false", "-c", "web_search=\"disabled\"")
			}
		} else {
			args = []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--include-partial-messages", "--replay-user-messages", "--permission-prompt-tool", "stdio"}
			if isProbe {
				args = append(args, "--tools", "", "--strict-mcp-config", "--mcp-config", `{"mcpServers":{}}`, "--no-session-persistence", "--settings", `{"disableAllHooks":true}`)
			}
			if siteManaged {
				args = append(args, "--model", site.Alias, "--setting-sources", "user", "--effort", "low")
			}
			if nativeID != "" {
				if _, err := uuid.Parse(nativeID); err != nil {
					return nil, errors.New("invalid Claude native session ID")
				}
				args = append(args, "--resume", nativeID)
			} else {
				nativeID = uuid.NewString()
				args = append(args, "--session-id", nativeID)
			}
		}
	}
	lifetime := ctx
	if host, ok := ctx.Value(remoteProviderLifetimeKey{}).(context.Context); ok {
		lifetime = host
	}
	life, cancel := context.WithCancel(lifetime)
	cmd := exec.CommandContext(life, binary, args...)
	cmd.Dir = cwd
	base, isolatedEnvironment := ctx.Value(remoteExecutorEnvironmentKey{}).([]string)
	if siteManaged && !isProbe && !isolatedEnvironment {
		cancel()
		return nil, errors.New("Remote executor environment isolation is unavailable")
	}
	if siteManaged && agent != "pi" && !remoteACPAgent(agent) {
		// CLI 2.1.325 can choose non-Anthropic transport before consulting BASE_URL.
		for _, key := range []string{
			"CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY", "CLAUDE_CODE_USE_ANTHROPIC_AWS", "CLAUDE_CODE_USE_ANTHROPIC_GOOGLE_CLOUD", "CLAUDE_CODE_USE_MANTLE", "CLAUDE_CODE_USE_GATEWAY",
			"CLAUDE_CODE_GATEWAY_TOKEN", "CLAUDE_CODE_GATEWAY_TOKEN_FILE_DESCRIPTOR", "CLAUDE_CODE_OAUTH_TOKEN", "CLAUDE_CODE_OAUTH_REFRESH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR", "CLAUDE_CODE_SESSIONS_OAUTH_TOKEN", "CLAUDE_CODE_API_KEY_FILE_DESCRIPTOR", "CLAUDE_CODE_WEBSOCKET_AUTH_FILE_DESCRIPTOR", "CCR_AGENT_PROXY_TOKEN_FILE_DESCRIPTOR",
			"CLAUDE_CODE_GB_BASE_URL", "CLAUDE_CODE_API_BASE_URL", "CLAUDE_CODE_HOST_GATEWAY_LINEAGE", "CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST", "CLAUDE_CODE_HOST_CREDS_FILE", "CLAUDE_CODE_HOST_AUTH_ENV_VAR", "CLAUDE_CODE_REMOTE_SETTINGS_PATH", "CLAUDE_BG_AUTH_SNAPSHOT_PATH", "CLAUDE_CODE_ENABLE_PROXY_AUTH_HELPER", "CLAUDE_CODE_HFI_BEARER_TOKEN", "AGENT_PROXY_AUTH_TOKEN",
			"ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_IDENTITY_TOKEN", "ANTHROPIC_IDENTITY_TOKEN_FILE", "ANTHROPIC_CUSTOM_HEADERS", "ANTHROPIC_UNIX_SOCKET", "ANTHROPIC_AWS_API_KEY", "ANTHROPIC_FOUNDRY_API_KEY", "ANTHROPIC_FOUNDRY_AUTH_TOKEN",
		} {
			env = append(env, key+"=")
		}
		env = append(env, "NEWNOA_REMOTE_API_KEY="+site.Secret, "HOME="+site.Home, "USERPROFILE="+site.Home, "CODEX_HOME="+filepath.Join(site.Home, ".codex"), "CLAUDE_CONFIG_DIR="+filepath.Join(site.Home, ".claude"), "OPENAI_API_KEY="+site.Secret, "OPENAI_BASE_URL="+site.BaseURL+"/v1", "ANTHROPIC_API_KEY="+site.Secret, "ANTHROPIC_AUTH_TOKEN=", "ANTHROPIC_BASE_URL="+site.BaseURL, "ANTHROPIC_MODEL="+site.Alias, "CLAUDE_CODE_USE_BEDROCK=0", "CLAUDE_CODE_USE_VERTEX=0", "CLAUDE_CODE_USE_FOUNDRY=0")
		if agent == "claude" {
			// Match the native protocol supported by the managed proxy, rather
			// than stripping unknown beta fields after authority admission.
			// Keep native request and first-byte waits open through the managed inference window.
			timeoutMS := strconv.FormatInt(inferenceWrapperTimeout.Milliseconds(), 10)
			env = append(env, "API_TIMEOUT_MS="+timeoutMS, "CLAUDE_STREAM_FIRST_BYTE_TIMEOUT_MS="+timeoutMS)
			env = append(env, "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS=1", "CLAUDE_CODE_MAX_OUTPUT_TOKENS=4096", "DISABLE_NONESSENTIAL_MODEL_CALLS=1")
		}

	}
	prepareRemoteProviderCommand(cmd)
	cmd.WaitDelay = 2 * time.Second
	if isolatedEnvironment {
		cmd.Env = mergeRemoteEnvironment(base, env)
	} else if agent == "pi" || remoteACPAgent(agent) {
		cmd.Env = env
	} else if env != nil {
		cmd.Env = append(cmd.Environ(), env...)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, errors.New("cannot open native CLI input")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		stdin.Close()
		return nil, errors.New("cannot open native CLI output")
	}
	// Native stderr can include authentication material; never surface it in remote events.
	cmd.Stderr = io.Discard
	p := &remoteNativeProvider{agent: agent, command: cmd, stdin: stdin, cancel: cancel, done: make(chan struct{}), pending: map[string]chan remoteNativeMessage{}, approvals: map[string]remoteNativeMessage{}, nativeID: nativeID, emit: emit, events: make(chan remoteProviderEvent, 128), probe: isProbe, completedRuns: map[string]bool{}}
	if siteManaged {
		p.revokeCredential = site.Revoke
	}
	if err = remoteCheckAuthority(ctx); err != nil {
		cancel()
		stdin.Close()
		stdout.Close()
		return nil, err
	}
	p.closeGateway, _ = ctx.Value(remoteGatewayContextKey{}).(func() error)
	started := make(chan error, 1)
	readReady := make(chan struct{})
	go func() {
		// Linux parent-death signals follow the creating thread, which must outlive the CLI.
		if runtime.GOOS == "linux" {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
		}
		startErr := remoteCheckAuthority(ctx)
		cleanup := func() {}
		if startErr == nil {
			cleanup, startErr = startRemoteNativeProcess(cmd)
		}
		started <- startErr
		if startErr != nil {
			close(p.done)
			return
		}
		defer cleanup()
		<-readReady
		p.read(stdout)
		_ = waitVersionCommand(cmd)
		p.gatewayCloseOnce.Do(func() {
			if p.closeGateway != nil {
				p.gatewayCloseErr = p.closeGateway()
			}
		})
		if err := p.revokeRemoteCredential(); err != nil && p.emit != nil {
			p.event(remoteProviderEvent{Type: "error", Error: "Remote agent credential revocation could not be confirmed"})
		}
		p.mu.Lock()
		p.closed = true
		closing := p.closing
		p.mu.Unlock()
		if !closing {
			p.event(remoteProviderEvent{Type: "exited", State: "failed", Error: "native CLI exited unexpectedly"})
		}
		close(p.done)
	}()
	if err = <-started; err != nil {
		cancel()
		stdin.Close()
		stdout.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("cannot start native CLI")
	}
	if piCleanup != nil {
		clean := piCleanup
		piCleanup = nil
		go func() { <-p.done; clean() }()
	}
	identity, _ := remoteProcessIdentity(cmd.Process.Pid)
	p.owned = remoteOwnedProcess{PID: cmd.Process.Pid, Identity: identity, Executable: cmd.Path, Job: remoteProcessJob(cmd.Process.Pid)}
	if err = remoteRecordOwnedProcess(ctx, p.owned); err != nil {
		p.mu.Lock()
		p.closing = true
		p.mu.Unlock()
		cancel()
		stdin.Close()
		stdout.Close()
		close(readReady)
		select {
		case <-p.done:
			return nil, err
		case <-time.After(3 * time.Second):
			return nil, fmt.Errorf("native launch record failed and process cleanup timed out: %w", err)
		}
	}

	go func() {
		for {
			select {
			case e := <-p.events:
				if p.emit != nil {
					p.emit(e)
				}
			case <-p.done:
				for {
					select {
					case e := <-p.events:
						if p.emit != nil {
							p.emit(e)
						}
					default:
						return
					}
				}
			}
		}
	}()
	close(readReady)
	fail := func(err error) (remoteProvider, error) { p.Close(); return nil, err }
	if agent == "codex" {
		if _, err = p.rpc(ctx, "initialize", map[string]any{"clientInfo": map[string]any{"name": "newnoa_remote", "version": "1.0"}, "capabilities": map[string]any{"experimentalApi": true}}); err != nil {
			return fail(err)
		}
		if err = p.write(map[string]any{"method": "initialized"}); err != nil {
			return fail(err)
		}
		method := "thread/start"
		params := map[string]any{"cwd": cwd}
		if siteManaged {
			params["model"] = site.Alias
			params["modelProvider"] = "newnoa_remote"
			p.siteModel = site.Alias
			p.siteEffort = site.Effort
		}
		if isProbe {
			params["ephemeral"] = true
			// The real authorization turn only needs a reply, not the coding-session prompt.
			params["baseInstructions"] = "Reply exactly as requested without using tools."
			params["developerInstructions"] = ""
			params["approvalPolicy"] = "never"
		}
		if nativeID != "" {
			method = "thread/resume"
			params["threadId"] = nativeID
		}
		response, e := p.rpc(ctx, method, params)
		if e != nil {
			return fail(e)
		}
		var r struct {
			Thread struct {
				ID string `json:"id"`
			} `json:"thread"`
		}
		if json.Unmarshal(response, &r) != nil || r.Thread.ID == "" {
			return fail(errors.New("native CLI returned no session ID"))
		}
		if nativeID != "" && r.Thread.ID != nativeID {
			return fail(errors.New("native CLI resumed a different session"))
		}
		p.mu.Lock()
		p.nativeID = r.Thread.ID
		p.mu.Unlock()
	} else if remoteACPAgent(agent) {
		if err = p.initializeACP(ctx, cwd, nativeID); err != nil {
			return fail(err)
		}
	} else if agent == "pi" {
		p.siteModel = site.Alias
		if err = p.initializePi(ctx, nativeID); err != nil {
			return fail(err)
		}
	} else {
		if _, err = p.rpc(ctx, "initialize", map[string]any{"hooks": map[string]any{}}); err != nil {
			return fail(err)
		}
	}
	// Script launchers can exec their interpreter without changing process birth.
	if current, e := remoteProcessIdentity(cmd.Process.Pid); e == nil && identity != "" {
		before, after := strings.SplitN(identity, ":", 3), strings.SplitN(current, ":", 3)
		if len(before) == 3 && len(after) == 3 && before[0] == after[0] && before[1] == after[1] {
			p.owned.Identity = current
		} else {
			return fail(errors.New("native CLI process birth identity changed during startup"))
		}
	}
	if err = remoteCheckAuthority(ctx); err != nil {
		return fail(err)
	}
	p.event(remoteProviderEvent{Type: "session", State: "idle"})
	return p, nil
}

func (p *remoteNativeProvider) OwnedProcess() remoteOwnedProcess { return p.owned }

func (p *remoteNativeProvider) event(e remoteProviderEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.eventLocked(e)
}

func (p *remoteNativeProvider) eventLocked(e remoteProviderEvent) {
	if e.NativeID == "" {
		e.NativeID = p.nativeID
	}
	if e.RunID == "" {
		e.RunID = p.runID
	}
	select {
	case p.events <- e:
	default:
		p.cancel()
	}
}
func (p *remoteNativeProvider) write(v any) error {
	_, err := p.writeGuarded(v, nil)
	return err
}

// The guard commits ownership under the writer lock, immediately before write.
func (p *remoteNativeProvider) writeGuarded(v any, guard func() error) (bool, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return false, errors.New("cannot encode native request")
	}
	if len(data) > 8<<20 {
		return false, errors.New("native request exceeds size limit")
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	if guard != nil {
		if err := guard(); err != nil {
			return false, err
		}
	}
	finished := make(chan error, 1)
	go func() {
		raw := append(data, '\n')
		n, e := p.stdin.Write(raw)
		if e == nil && n != len(raw) {
			e = io.ErrShortWrite
		}
		finished <- e
	}()
	select {
	case err = <-finished:
		if err != nil {
			return true, errors.New("native CLI input closed")
		}
		return true, nil
	case <-p.done:
		return true, errors.New("native CLI closed")
	case <-time.After(5 * time.Second):
		p.cancel()
		return true, errors.New("native CLI input timed out")
	}
}
func (p *remoteNativeProvider) rpc(ctx context.Context, method string, params any) (json.RawMessage, error) {
	return p.rpcDispatch(ctx, method, params, "")
}

func (p *remoteNativeProvider) rpcDispatch(ctx context.Context, method string, params any, operationID string) (json.RawMessage, error) {
	if err := remoteCheckAuthority(ctx); err != nil {
		return nil, err
	}
	deadline := 15 * time.Second
	if remoteACPAgent(p.agent) {
		deadline = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	p.mu.Lock()
	p.seq++
	id := fmt.Sprint(p.seq)
	ch := make(chan remoteNativeMessage, 1)
	if p.agent == "pi" {
		if p.piWaiters == nil {
			p.piWaiters = map[string]remotePiWaiter{}
		}
		p.piWaiters[id] = remotePiWaiter{method, ch}
	} else {
		p.pending[id] = ch
	}
	if operationID != "" {
		if err := p.armDispatchLocked(id, operationID, method); err != nil {
			delete(p.pending, id)
			delete(p.piWaiters, id)
			p.mu.Unlock()
			return nil, err
		}
	}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.pending, id); delete(p.piWaiters, id); p.mu.Unlock() }()
	var req any = map[string]any{"id": id, "method": method, "params": params}
	if remoteACPAgent(p.agent) {
		req = map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(id), "method": method, "params": params}
	} else if p.agent == "pi" {
		request, ok := params.(map[string]any)
		if !ok {
			return nil, errors.New("invalid Pi request")
		}
		request["id"], request["type"] = id, method
		req = request
	} else if p.agent == "claude" {
		request := map[string]any{"subtype": method}
		b, _ := json.Marshal(params)
		json.Unmarshal(b, &request)
		req = map[string]any{"type": "control_request", "request_id": id, "request": request}
	}
	attempted, err := p.writeGuarded(req, func() error { return remoteCheckAuthority(ctx) })
	if err != nil {
		if !attempted {
			p.forgetDispatch(id)
		}
		return nil, remoteDispatchOutcome(err, operationID != "" && attempted)
	}
	select {
	case msg := <-ch:
		if len(msg.Error) > 0 && string(msg.Error) != "null" {
			return nil, errors.New("native CLI rejected " + method)
		}
		return msg.Result, nil
	case <-ctx.Done():
		return nil, remoteDispatchOutcome(fmt.Errorf("native %s: %w", method, ctx.Err()), operationID != "")
	case <-p.done:
		return nil, remoteDispatchOutcome(errors.New("native CLI closed before acknowledgment"), operationID != "")
	}
}
func remoteMessageID(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}
func (p *remoteNativeProvider) read(r io.Reader) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64<<10), 4<<20)
	for s.Scan() {
		var m remoteNativeMessage
		if json.Unmarshal(s.Bytes(), &m) != nil {
			p.event(remoteProviderEvent{Type: "error", State: "failed", Error: "invalid native CLI protocol", probeFailureKind: "protocol-invalid"})
			p.cancel()
			return
		}
		m.Raw = append(json.RawMessage(nil), s.Bytes()...)
		if p.agent == "codex" {
			p.codexMessage(m)
		} else if p.agent == "pi" {
			p.piMessage(m)
		} else if remoteACPAgent(p.agent) {
			p.acpMessage(m)
		} else {
			p.claudeMessage(m)
		}
	}
	if s.Err() != nil {
		kind := "output-failed"
		if errors.Is(s.Err(), bufio.ErrTooLong) {
			kind = "output-limit"
		}
		p.event(remoteProviderEvent{Type: "error", State: "failed", Error: "native CLI output exceeded limit or failed", probeFailureKind: kind})
		p.cancel()
	}
}

func remoteCodexInput(in remoteProviderInput) ([]map[string]any, error) {
	out := []map[string]any{}
	if in.Text != "" {
		out = append(out, map[string]any{"type": "text", "text": in.Text})
	}
	for _, im := range in.Images {
		if !strings.HasPrefix(im.URL, "data:image/") {
			return nil, errors.New("image must be an owned image data URL")
		}
		out = append(out, map[string]any{"type": "image", "url": im.URL})
	}
	if len(out) == 0 {
		return nil, errors.New("message is empty")
	}
	return out, nil
}
func remoteClaudeInput(in remoteProviderInput) ([]map[string]any, error) {
	out := []map[string]any{}
	if in.Text != "" {
		out = append(out, map[string]any{"type": "text", "text": in.Text})
	}
	for _, im := range in.Images {
		header, data, ok := strings.Cut(im.URL, ",")
		if !ok || !strings.HasPrefix(header, "data:image/") || !strings.HasSuffix(header, ";base64") {
			return nil, errors.New("image must be an owned base64 image data URL")
		}
		mime := strings.TrimSuffix(strings.TrimPrefix(header, "data:"), ";base64")
		if mime != "image/png" && mime != "image/jpeg" && mime != "image/gif" && mime != "image/webp" {
			return nil, errors.New("unsupported image media type")
		}
		if _, err := base64.StdEncoding.DecodeString(data); err != nil {
			return nil, errors.New("invalid image encoding")
		}
		out = append(out, map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": mime, "data": data}})
	}
	if len(out) == 0 {
		return nil, errors.New("message is empty")
	}
	return out, nil
}

func claudeAcknowledgmentContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// Replay can be delayed until model response processing.
	// Keep the operation's budget; callers without one still get a finite bound.
	if _, bounded := ctx.Deadline(); bounded {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, 45*time.Second)
}

func (p *remoteNativeProvider) StartTurn(ctx context.Context, in remoteProviderInput) error {
	p.turnMu.Lock()
	defer p.turnMu.Unlock()
	if err := remoteCheckAuthority(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	if p.runID != "" {
		p.mu.Unlock()
		return errors.New("native run is already active; use a supported steer operation")
	}
	nativeID := p.nativeID
	p.mu.Unlock()
	if p.agent == "pi" {
		return p.startPiTurn(ctx, in)
	}
	if remoteACPAgent(p.agent) {
		return p.startACPTurn(ctx, in)
	}
	if p.agent == "codex" {
		input, err := remoteCodexInput(in)
		if err != nil {
			return err
		}
		params := map[string]any{"threadId": nativeID, "input": input}
		if p.probe {
			params["effort"] = "low"
		} else if p.siteEffort != "" {
			params["effort"] = p.siteEffort
		}
		if p.siteModel != "" {
			params["model"] = p.siteModel
		}
		if in.OperationID != "" {
			params["clientUserMessageId"] = in.OperationID
		}
		r, err := p.rpcDispatch(ctx, "turn/start", params, in.OperationID)
		if err != nil {
			return err
		}
		var v struct {
			Turn struct {
				ID string `json:"id"`
			} `json:"turn"`
		}
		if json.Unmarshal(r, &v) != nil || v.Turn.ID == "" {
			return remoteDispatchOutcome(errors.New("native CLI returned no run ID"), in.OperationID != "")
		}
		p.mu.Lock()
		completed := p.completedRuns[v.Turn.ID]
		if !completed {
			p.runID = v.Turn.ID
			p.eventLocked(remoteProviderEvent{Type: "state", State: "running", RunID: v.Turn.ID})
		}
		p.mu.Unlock()
		return nil
	}
	input, err := remoteClaudeInput(in)
	if err != nil {
		return err
	}
	id := uuid.NewString()
	ch := make(chan remoteNativeMessage, 1)
	p.mu.Lock()
	p.pending[id] = ch
	if err := p.armDispatchLocked(id, in.OperationID, "user"); err != nil {
		delete(p.pending, id)
		p.mu.Unlock()
		return err
	}
	p.runID = id
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.pending, id); p.mu.Unlock() }()
	attempted, err := p.writeGuarded(map[string]any{"type": "user", "uuid": id, "session_id": nativeID, "message": map[string]any{"role": "user", "content": input}, "parent_tool_use_id": nil}, func() error { return remoteCheckAuthority(ctx) })
	if err != nil {
		if !attempted {
			p.forgetDispatch(id)
			p.mu.Lock()
			if p.runID == id {
				p.runID = ""
			}
			p.mu.Unlock()
		}
		return remoteDispatchOutcome(err, attempted)
	}
	ctx, cancel := claudeAcknowledgmentContext(ctx)
	defer cancel()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return remoteDispatchOutcome(fmt.Errorf("native message acknowledgment: %w", ctx.Err()), true)
	case <-p.done:
		return remoteDispatchOutcome(errors.New("native CLI closed before message acknowledgment"), true)
	}
}
func (p *remoteNativeProvider) Steer(ctx context.Context, in remoteProviderInput) error {
	if p.agent != "codex" {
		if p.agent == "claude" {
			return errors.New("Claude Code 2.1.325 does not expose a verified native steer operation; running input is unsupported")
		}
		return errors.New("This native executor does not support immediate input")
	}
	input, err := remoteCodexInput(in)
	if err != nil {
		return err
	}
	p.mu.Lock()
	nativeID, runID := p.nativeID, p.runID
	p.mu.Unlock()
	if runID == "" {
		return errors.New("no active native run")
	}
	params := map[string]any{"threadId": nativeID, "expectedTurnId": runID, "input": input}
	if in.OperationID != "" {
		params["clientUserMessageId"] = in.OperationID
	}
	_, err = p.rpcDispatch(ctx, "turn/steer", params, in.OperationID)
	return err
}
func (p *remoteNativeProvider) Stop(ctx context.Context) error {
	if remoteACPAgent(p.agent) {
		return p.stopACPTurn(ctx)
	}
	p.mu.Lock()
	nativeID, runID := p.nativeID, p.runID
	p.mu.Unlock()
	if runID == "" {
		return errors.New("no active native run")
	}
	method := "interrupt"
	if p.agent == "pi" {
		method = "abort"
	}
	params := map[string]any{}
	if p.agent == "codex" {
		method = "turn/interrupt"
		params = map[string]any{"threadId": nativeID, "turnId": runID}
	}
	_, err := p.rpc(ctx, method, params)
	return err
}
func (p *remoteNativeProvider) Close() error {
	p.mu.Lock()
	p.closing = true
	p.mu.Unlock()
	p.cancel()
	p.stdin.Close()
	select {
	case <-p.done:
		return errors.Join(p.verifyExitCleanup(), p.gatewayCloseErr, p.revokeRemoteCredential())
	case <-time.After(3 * time.Second):
		return errors.Join(errors.New("native CLI process cleanup timed out"), p.revokeRemoteCredential())
	}
}

func (p *remoteNativeProvider) revokeRemoteCredential() error {
	p.credentialMu.Lock()
	defer p.credentialMu.Unlock()
	if p.revokeCredential == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := p.revokeCredential(ctx); err != nil {
		return err
	}
	p.revokeCredential = nil
	return nil
}

func (p *remoteNativeProvider) codexMessage(m remoteNativeMessage) {
	id := remoteMessageID(m.ID)
	if m.Method == "" && id != "" {
		p.mu.Lock()
		p.codexDispatchReplyLocked(id, m)
		ch := p.pending[id]
		p.mu.Unlock()
		if ch != nil {
			select {
			case ch <- m:
			default:
			}
		}
		return
	}
	var data map[string]any
	if json.Unmarshal(m.Params, &data) != nil {
		return
	}
	threadID, _ := data["threadId"].(string)
	turnID, _ := data["turnId"].(string)
	if turn, ok := data["turn"].(map[string]any); ok {
		turnID, _ = turn["id"].(string)
	}
	p.mu.Lock()
	stale := threadID != "" && p.nativeID != "" && threadID != p.nativeID || turnID != "" && (p.completedRuns[turnID] || p.runID != "" && p.runID != turnID)
	p.mu.Unlock()
	if stale {
		return
	}
	if len(m.ID) > 0 {
		kind := m.Method
		options := []string{}
		switch kind {
		case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
			options = []string{"accept", "decline", "cancel"}
			if decisions, ok := data["availableDecisions"].([]any); ok {
				options = nil
				for _, v := range decisions {
					if s, ok := v.(string); ok && (s == "accept" || s == "decline" || s == "cancel") {
						options = append(options, s)
					}
				}
			}
		case "item/tool/requestUserInput":
			options = []string{"answer"}
		case "item/permissions/requestApproval":
			options = []string{"deny"}
		default:
			p.write(map[string]any{"id": m.ID, "error": map[string]any{"code": -32601, "message": "unsupported approval capability"}})
			p.event(remoteProviderEvent{Type: "error", Error: "unsupported native approval: " + kind})
			return
		}
		p.mu.Lock()
		p.approvals[id] = m
		delete(p.approvalClaims, id)
		instance := p.newApprovalInstanceLocked(id)
		p.mu.Unlock()
		safe := remoteApprovalData(kind, data)
		p.event(remoteProviderEvent{Type: "approval", State: "awaitingApproval", Approval: &remoteProviderApproval{ID: id, Instance: instance, Kind: kind, Title: kind, Options: options, Data: safe}})
		return
	}
	switch m.Method {
	case "thread/started":
		if thread, ok := data["thread"].(map[string]any); ok {
			if native, ok := thread["id"].(string); ok {
				p.mu.Lock()
				p.nativeID = native
				p.mu.Unlock()
			}
		}
	case "turn/started":
		if turn, ok := data["turn"].(map[string]any); ok {
			if run, ok := turn["id"].(string); ok {
				p.mu.Lock()
				p.runID = run
				p.mu.Unlock()
				p.event(remoteProviderEvent{Type: "state", State: "running", RunID: run})
			}
		}
	case "turn/completed":
		state := "idle"
		if turn, ok := data["turn"].(map[string]any); ok {
			status, _ := turn["status"].(string)
			switch status {
			case "interrupted":
				state = "stopped"
			case "failed":
				state = "failed"
				if p.probe && remoteCodexPaymentFailure(turn["error"]) {
					p.event(remoteProviderEvent{Type: "error", probeFailureKind: "payment-required"})
				}
			}
		}
		p.mu.Lock()
		p.eventLocked(remoteProviderEvent{Type: "state", State: state})
		if p.completedRuns == nil {
			p.completedRuns = map[string]bool{}
		}
		if len(p.completedRuns) >= 128 {
			p.completedRuns = map[string]bool{}
		}
		if turnID != "" {
			p.completedRuns[turnID] = true
		}
		p.runID = ""
		p.approvals = map[string]remoteNativeMessage{}
		p.approvalClaims = nil
		p.approvalInstances = nil
		p.mu.Unlock()
	case "item/agentMessage/delta":
		if delta, ok := data["delta"].(string); ok {
			p.event(remoteProviderEvent{Type: "output", Text: delta})
		}
	case "error":
		if data["willRetry"] == true {
			return
		}
		kind := "native-codex-error"
		if p.probe && remoteCodexPaymentFailure(data["error"]) {
			kind = "payment-required"
		}
		p.event(remoteProviderEvent{Type: "error", State: "failed", Error: "native Codex execution error", probeFailureKind: kind})
	case "item/started":
		if p.probe {
			if item, ok := data["item"].(map[string]any); ok {
				kind, _ := item["type"].(string)
				if kind != "agentMessage" && kind != "reasoning" && kind != "userMessage" {
					p.event(remoteProviderEvent{Type: "error", Error: "native authorization probe attempted a tool", probeFailureKind: "tool-attempt"})
					p.cancel()
				}
			}
		}
	}
}
func (p *remoteNativeProvider) claudeMessage(m remoteNativeMessage) {
	var data map[string]any
	if json.Unmarshal(m.Raw, &data) != nil {
		return
	}
	if parent, ok := data["parent_tool_use_id"].(string); ok && parent != "" {
		return
	}
	if session, ok := data["session_id"].(string); ok && session != "" && session != p.nativeID {
		p.event(remoteProviderEvent{Type: "error", State: "failed", Error: "native CLI returned a different session ID"})
		p.cancel()
		return
	}
	switch m.Type {
	case "control_response":
		var response struct {
			RequestID string          `json:"request_id"`
			Subtype   string          `json:"subtype"`
			Response  json.RawMessage `json:"response"`
		}
		if json.Unmarshal(m.Response, &response) != nil {
			return
		}
		p.mu.Lock()
		ch := p.pending[response.RequestID]
		p.mu.Unlock()
		if ch != nil {
			out := remoteNativeMessage{Result: response.Response}
			if response.Subtype != "success" {
				out.Error = json.RawMessage(`"rejected"`)
			}
			select {
			case ch <- out:
			default:
			}
		}
	case "control_request":
		var req struct {
			Subtype  string         `json:"subtype"`
			ToolName string         `json:"tool_name"`
			Input    map[string]any `json:"input"`
		}
		if json.Unmarshal(m.Request, &req) != nil {
			return
		}
		if req.Subtype != "can_use_tool" {
			p.write(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "error", "request_id": m.RequestID, "error": "unsupported host control request"}})
			p.event(remoteProviderEvent{Type: "error", Error: "unsupported Claude host control request"})
			return
		}
		kind := req.ToolName
		options := []string{"allow", "deny"}
		if kind == "AskUserQuestion" {
			options = []string{"answer", "deny"}
		}
		p.mu.Lock()
		p.approvals[m.RequestID] = m
		delete(p.approvalClaims, m.RequestID)
		instance := p.newApprovalInstanceLocked(m.RequestID)
		p.mu.Unlock()
		safe := remoteApprovalData(kind, req.Input)
		p.event(remoteProviderEvent{Type: "approval", State: "awaitingApproval", Approval: &remoteProviderApproval{ID: m.RequestID, Instance: instance, Kind: kind, Title: kind, Options: options, Data: safe}})
	case "control_cancel_request":
		p.mu.Lock()
		instance := p.approvalInstances[m.RequestID]
		delete(p.approvals, m.RequestID)
		delete(p.approvalClaims, m.RequestID)
		delete(p.approvalInstances, m.RequestID)
		p.mu.Unlock()
		p.event(remoteProviderEvent{Type: "approvalInvalidated", Text: m.RequestID, ApprovalInstance: instance})
	case "user":
		p.claudeDispatchAck(data, false)
	case "assistant":
		if message, ok := data["message"].(map[string]any); ok {
			if model, ok := message["model"].(string); ok && model != "" {
				p.event(remoteProviderEvent{Type: "model", Model: model})
			}
		}
	case "stream_event":
		event, _ := data["event"].(map[string]any)
		if delta, ok := event["delta"].(map[string]any); ok {
			if text, ok := delta["text"].(string); ok {
				p.event(remoteProviderEvent{Type: "output", Text: text})
			}
		}
		if message, ok := event["message"].(map[string]any); ok {
			if model, ok := message["model"].(string); ok && model != "" {
				p.event(remoteProviderEvent{Type: "model", Model: model})
			}
		}
	case "result":
		p.claudeDispatchAck(data, true)
		isError, _ := data["is_error"].(bool)
		state := "idle"
		if isError {
			state = "failed"
			result, _ := data["result"].(string)
			status, hasStatus := data["api_error_status"].(float64)
			// Older Claude versions report HTTP status only in the terminal result text.
			if p.probe && (status == 402 || !hasStatus && strings.HasPrefix(result, "API Error: 402 ")) {
				p.event(remoteProviderEvent{Type: "error", probeFailureKind: "payment-required"})
			}
		}
		p.mu.Lock()
		p.eventLocked(remoteProviderEvent{Type: "state", State: state})
		p.runID = ""
		p.approvals = map[string]remoteNativeMessage{}
		p.approvalClaims = nil
		p.approvalInstances = nil
		p.mu.Unlock()
	}
}

func remoteApprovalData(kind string, data map[string]any) json.RawMessage {
	safe := map[string]any{}
	for _, k := range []string{"questions", "command", "cwd", "reason", "changes", "permissions", "file_path", "path", "url", "old_string", "new_string", "content", "query", "pattern", "description", "tool_use_id"} {
		if v, ok := data[k]; ok {
			safe[k] = v
		}
	}
	out, _ := json.Marshal(safe)
	return out
}
func (p *remoteNativeProvider) Respond(ctx context.Context, reply remoteProviderReply) error {
	p.replyMu.Lock()
	defer p.replyMu.Unlock()
	if err := remoteCheckAuthority(ctx); err != nil {
		return err
	}
	p.mu.Lock()
	m, ok := p.approvals[reply.RequestID]
	instance := p.approvalInstances[reply.RequestID]
	if !ok || reply.Instance != 0 && reply.Instance != instance {
		p.mu.Unlock()
		return errors.New("native approval is no longer pending")
	}
	claim := &remoteApprovalClaim{held: true}
	if p.approvalClaims == nil {
		p.approvalClaims = map[string]*remoteApprovalClaim{}
	}
	p.approvalClaims[reply.RequestID] = claim
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		if p.approvalClaims[reply.RequestID] == claim {
			delete(p.approvalClaims, reply.RequestID)
		}
		p.mu.Unlock()
	}()
	commit := func() error {
		if err := remoteCheckAuthority(ctx); err != nil {
			return err
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.approvalClaims[reply.RequestID] != claim {
			return errors.New("native approval is no longer pending")
		}
		if _, ok := p.approvals[reply.RequestID]; !ok {
			return errors.New("native approval is no longer pending")
		}
		delete(p.approvals, reply.RequestID)
		delete(p.approvalClaims, reply.RequestID)
		delete(p.approvalInstances, reply.RequestID)
		return nil
	}
	var response any
	if p.agent == "codex" {
		switch m.Method {
		case "item/tool/requestUserInput":
			if reply.Decision != "answer" || len(reply.Answers) == 0 {
				return errors.New("question requires answers")
			}
			answers := map[string]any{}
			for k, v := range reply.Answers {
				answers[k] = map[string]any{"answers": v}
			}
			response = map[string]any{"answers": answers}
		case "item/permissions/requestApproval":
			if reply.Decision != "deny" {
				return errors.New("permission scope approval is unsupported")
			}
			response = map[string]any{"permissions": map[string]any{}, "scope": "turn"}
		default:
			if reply.Decision != "accept" && reply.Decision != "decline" && reply.Decision != "cancel" {
				return errors.New("unsupported native approval decision")
			}
			var params struct {
				AvailableDecisions []json.RawMessage `json:"availableDecisions"`
			}
			if json.Unmarshal(m.Params, &params) == nil && params.AvailableDecisions != nil {
				valid := false
				for _, raw := range params.AvailableDecisions {
					var decision string
					if json.Unmarshal(raw, &decision) == nil && decision == reply.Decision {
						valid = true
					}
				}
				if !valid {
					return errors.New("decision is unavailable for this native approval")
				}
			}
			response = map[string]any{"decision": reply.Decision}
		}
		if attempted, err := p.writeGuarded(map[string]any{"id": m.ID, "result": response}, commit); err != nil {
			return remoteDispatchOutcome(err, attempted)
		}
	} else {
		var req struct {
			ToolName string         `json:"tool_name"`
			Input    map[string]any `json:"input"`
		}
		json.Unmarshal(m.Request, &req)
		decision := map[string]any{}
		switch reply.Decision {
		case "deny":
			decision = map[string]any{"behavior": "deny", "message": "Denied by user"}
		case "allow":
			if req.ToolName == "AskUserQuestion" {
				return errors.New("question requires answers")
			}
			decision = map[string]any{"behavior": "allow", "updatedInput": req.Input}
		case "answer":
			if req.ToolName != "AskUserQuestion" || len(reply.Answers) == 0 {
				return errors.New("invalid question response")
			}
			answers := map[string]string{}
			for k, v := range reply.Answers {
				answers[k] = strings.Join(v, ", ")
			}
			req.Input["answers"] = answers
			decision = map[string]any{"behavior": "allow", "updatedInput": req.Input}
		default:
			return errors.New("unsupported native approval decision")
		}
		if attempted, err := p.writeGuarded(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": reply.RequestID, "response": decision}}, commit); err != nil {
			return remoteDispatchOutcome(err, attempted)
		}
	}
	p.event(remoteProviderEvent{Type: "approvalResolved", State: "running", Text: reply.RequestID, ApprovalInstance: instance})
	return nil
}

func remoteCodexPaymentFailure(value any) bool {
	err, _ := value.(map[string]any)
	info, _ := err["codexErrorInfo"].(map[string]any)
	for _, kind := range []string{"httpConnectionFailed", "responseTooManyFailedAttempts"} {
		detail, _ := info[kind].(map[string]any)
		if code, _ := detail["httpStatusCode"].(float64); code == 402 {
			return true
		}
	}
	return false
}

func logRemoteProbeFailure(agent, kind string) {
	switch agent {
	case "codex", "claude", "openclaw", "hermes", "pi", "opencode":
	case "claude-cli":
		agent = "claude"
	default:
		agent = "other"
	}
	switch kind {
	case "setup", "turn-start", "timeout", "canceled", "tool-attempt", "output-limit", "output-failed", "protocol-invalid", "native-codex-error", "payment-required", "failed-state", "stopped-state":
	default:
		kind = "other"
	}
	slog.Warn("remote.authorization.probe.failed", "agent", agent, "kind", kind)
}

func remoteProbeStartupTimeout(agent string) time.Duration {
	if remoteProviderBinary(agent) == "openclaw" {
		return 70 * time.Second
	}
	return 0
}

func remoteProbeTimeout(agent string) time.Duration {
	if binary := remoteProviderBinary(agent); binary == "codex" || binary == "openclaw" {
		// Some authorized local models need longer to read the native probe context.
		return 120 * time.Second
	}
	if remoteACPAgent(remoteProviderBinary(agent)) {
		return 100 * time.Second
	}
	return 45 * time.Second
}

func probeRemoteProvider(ctx context.Context, agent, cwd string) (info remoteProviderInfo, err error) {
	failureKind := "setup"
	defer func() {
		if err != nil {
			logRemoteProbeFailure(agent, failureKind)
		}
	}()
	info, err = remoteProviderDetailsContext(ctx, agent)
	if err != nil {
		return info, err
	}
	startup := remoteProbeStartupTimeout(agent)
	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()
	if startup == 0 {
		var stop context.CancelFunc
		lifetime, stop = context.WithTimeout(lifetime, remoteProbeTimeout(agent))
		defer stop()
	}
	launch := lifetime
	stopLaunch := func() {}
	if startup > 0 {
		launch, stopLaunch = context.WithTimeout(lifetime, startup)
		// After initialization, keep the owned native processes until the probe ends.
		launch = context.WithValue(launch, remoteProviderLifetimeKey{}, lifetime)
	}
	defer stopLaunch()
	events := make(chan remoteProviderEvent, 128)
	agent = remoteProviderBinary(agent)
	p, err := launchRemoteProvider(launch, agent, cwd, "", func(e remoteProviderEvent) {
		select {
		case events <- e:
		default:
			cancel()
		}
	}, info.command, true)
	if err != nil {
		return info, err
	}
	defer p.Close()
	stopLaunch()
	ctx = lifetime
	if startup > 0 {
		var stopTurn context.CancelFunc
		ctx, stopTurn = context.WithTimeout(lifetime, remoteProbeTimeout(agent))
		defer stopTurn()
	}
	if err = p.StartTurn(ctx, remoteProviderInput{Text: "This is an authorization connectivity check. Do not call any tools or read files. Reply with exactly NEWNOA_REMOTE_OK."}); err != nil {
		failureKind = "turn-start"
		switch ctx.Err() {
		case context.DeadlineExceeded:
			failureKind = "timeout"
		case context.Canceled:
			failureKind = "canceled"
		}
		return info, err
	}
	var output strings.Builder
	for {
		select {
		case <-ctx.Done():
			failureKind = "canceled"
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				failureKind = "timeout"
			}
			return info, errors.New("native conversation probe timed out or was canceled")
		case e := <-events:
			switch e.Type {
			case "approval":
				failureKind = "tool-attempt"
				return info, errors.New("native conversation probe attempted a tool or approval")
			case "output":
				if output.Len()+len(e.Text) > 4096 {
					failureKind = "output-limit"
					return info, errors.New("native probe output exceeded limit")
				}
				output.WriteString(e.Text)
			case "error":
				failureKind = e.probeFailureKind
				if failureKind == "payment-required" {
					return info, errors.New("Model request rejected (HTTP 402). Check your available points and API key points limit, then retry.")
				}
				return info, errors.New("native conversation probe failed")
			case "state":
				if e.State == "failed" || e.State == "stopped" {
					failureKind = e.State + "-state"
					return info, errors.New("native conversation probe failed")
				}
				if e.State == "idle" && strings.Contains(output.String(), "NEWNOA_REMOTE_OK") {
					return info, nil
				}
			}
		}
	}
}
