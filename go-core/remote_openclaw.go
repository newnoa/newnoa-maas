package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Foreground, owned companion only. No shared/OpenClaw user Gateway is adopted.
type remoteGatewayProcess struct {
	done       chan struct{}
	cancel     context.CancelFunc
	owned      remoteOwnedProcess
	cleanupErr error
}

func (g *remoteGatewayProcess) Close() error {
	g.cancel()
	select {
	case <-g.done:
		return errors.Join(g.cleanupErr, verifyRemoteProcessExit(g.owned))
	case <-time.After(3 * time.Second):
		return errors.New("Native Gateway cleanup timed out")
	}
}

func remoteOpenClawPlatform() bool {
	return remoteOpenClawVersion(runtime.GOOS) != ""
}

func remoteOpenClawVersion(platform string) string {
	switch platform {
	case "linux":
		return "OpenClaw 2026.11.4 (d843d6a)"
	case "darwin", "windows":
		return "OpenClaw 2026.11.5 (e13cf15)"
	default:
		return ""
	}
}

func startRemoteOpenClaw(ctx context.Context, cwd, nativeID string, emit func(remoteProviderEvent), command remoteCommand, probe bool) (remoteProvider, error) {
	site, ok := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch)
	if !ok {
		return nil, errors.New(remoteSiteConfigurationRequired)
	}
	if !remoteOpenClawPlatform() {
		return nil, errors.New("OpenClaw remote Gateway is not verified on this platform")
	}
	stateDir, _ := ctx.Value(remoteProviderStateDirKey{}).(string)
	slot, _ := ctx.Value(remoteProviderSessionSlotKey{}).(string)
	if probe {
		stateDir, slot = cwd, "authorization"
	}
	if !filepath.IsAbs(stateDir) || slot == "" || slot == "." || slot == ".." || filepath.Base(slot) != slot {
		return nil, errors.New("Native Gateway state directory is unavailable")
	}
	dir := filepath.Join(stateDir, "openclaw", slot)
	if err := (&core{home: site.Home}).safeConfigFile(filepath.Join(dir, "config.json")); err != nil || os.MkdirAll(dir, 0700) != nil {
		return nil, errors.New("OpenClaw state directory is unsafe")
	}
	reserve, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("Cannot reserve a loopback Gateway port")
	}
	port := strings.Split(reserve.Addr().String(), ":")[1]
	host := "127.0.0.1:" + port
	token := randomToken()
	configPath, tokenPath := filepath.Join(dir, "config.json"), filepath.Join(dir, "token")
	config, err := json.Marshal(map[string]any{
		"gateway": map[string]any{"mode": "local", "bind": "loopback", "auth": map[string]string{"mode": "token"}, "controlUi": map[string]bool{"enabled": false}},
		"plugins": map[string]bool{"enabled": false}, "tools": map[string]any{"deny": []string{"*"}},
		"agents": map[string]any{"defaults": map[string]any{"workspace": cwd, "skipBootstrap": true, "thinkingDefault": "off", "model": map[string]string{"primary": "newnoa/" + site.Alias}}},
		"models": map[string]any{"providers": map[string]any{"newnoa": map[string]any{"baseUrl": site.BaseURL + "/v1", "api": "openai-completions", "apiKey": "${NEWNOA_REMOTE_API_KEY}", "models": []map[string]string{{"id": site.Alias, "name": site.Alias}}}}},
	})
	current, userErr := user.Current()
	if err != nil || userErr != nil || atomicFile(configPath, config) != nil || writeRemoteJournal(tokenPath, current.Uid, []byte(token)) != nil {
		reserve.Close()
		return nil, errors.New("Cannot write isolated OpenClaw Gateway configuration")
	}
	env := append(openClawRemoteEnv(dir), "OPENCLAW_STATE_DIR="+dir, "OPENCLAW_CONFIG_PATH="+configPath, "OPENCLAW_GATEWAY_TOKEN="+token, "NEWNOA_REMOTE_API_KEY="+site.Secret, "OPENCLAW_SKIP_CHANNELS=1", "OPENCLAW_DISABLE_BONJOUR=1")
	if base, ok := ctx.Value(remoteExecutorEnvironmentKey{}).([]string); ok {
		env = mergeRemoteEnvironment(base, env)
	}
	reserve.Close()
	gateway, err := launchRemoteGateway(ctx, command.path, cwd, command.args("gateway", "run", "--bind", "loopback", "--auth", "token", "--port", port, "--tailscale", "off"), env)
	if err != nil {
		_ = os.Remove(tokenPath)
		return nil, err
	}
	closeGateway := func() error { return errors.Join(gateway.Close(), removeRemoteGatewayToken(tokenPath)) }
	if err = waitOpenClawReady(ctx, gateway, host); err != nil {
		return nil, errors.Join(err, closeGateway())
	}
	acpCtx := context.WithValue(ctx, remoteGatewayContextKey{}, closeGateway)
	p, err := startRemoteProviderCommand(acpCtx, "openclaw", cwd, nativeID, emit, command.path, command.args("acp", "--url", "ws://"+host, "--token-file", tokenPath, "--no-prefix-cwd"), env, probe)
	if err != nil {
		return nil, errors.Join(err, closeGateway())
	}
	return p, nil
}

func openClawRemoteEnv(home string) []string {
	env := []string{"HOME=" + home, "PATH=" + os.Getenv("PATH")}
	if runtime.GOOS == "windows" {
		for _, name := range []string{"SystemRoot", "WINDIR", "ComSpec"} {
			if value := os.Getenv(name); value != "" {
				env = append(env, name+"="+value)
			}
		}
		for _, name := range []string{"USERPROFILE", "APPDATA", "LOCALAPPDATA", "TEMP", "TMP"} {
			env = append(env, name+"="+home)
		}
	}
	return env
}

type remoteGatewayContextKey struct{}

func removeRemoteGatewayToken(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return errors.New("Native Gateway token removal failed")
	}
	return nil
}

func waitOpenClawReady(ctx context.Context, gateway *remoteGatewayProcess, host string) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for {
		select {
		case <-ctx.Done():
			return errors.New("OpenClaw Gateway readiness timed out")
		case <-gateway.done:
			return errors.New("OpenClaw Gateway exited before readiness")
		case <-ticker.C:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+host+"/readyz", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var readiness struct {
			Ready bool `json:"ready"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&readiness)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK && err == nil && readiness.Ready {
			return nil
		}
	}
}
