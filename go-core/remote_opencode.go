package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
)

func remoteOpenCodePlatform() bool {
	return runtime.GOOS == "linux" || runtime.GOOS == "darwin" || runtime.GOOS == "windows"
}

func startRemoteOpenCode(ctx context.Context, cwd, nativeID string, emit func(remoteProviderEvent), binary string, probe bool) (remoteProvider, error) {
	site, ok := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch)
	if !ok || !validAPIKey(site.Secret) || !providerAlias.MatchString(site.Alias) {
		return nil, errors.New(remoteSiteConfigurationRequired)
	}
	if !remoteOpenCodePlatform() {
		return nil, errors.New("OpenCode remote requires Linux, macOS or Windows")
	}
	state, _ := ctx.Value(remoteProviderStateDirKey{}).(string)
	slot, _ := ctx.Value(remoteProviderSessionSlotKey{}).(string)
	if probe {
		state, slot = cwd, "authorization"
	}
	if !filepath.IsAbs(state) || slot == "" || slot == "." || slot == ".." || filepath.Base(slot) != slot {
		return nil, errors.New("OpenCode native state directory is unavailable")
	}
	dir := filepath.Join(state, "opencode", slot)
	if (&core{home: site.Home}).safeConfigFile(filepath.Join(dir, "config", "opencode", "opencode.json")) != nil || os.MkdirAll(dir, 0700) != nil {
		return nil, errors.New("OpenCode state directory is unsafe")
	}
	const provider = "newnoa-remote"
	model := provider + "/" + site.Alias
	config, err := json.Marshal(map[string]any{
		"model": model, "small_model": model, "enabled_providers": []string{provider},
		"permission": "deny", "plugin": []string{}, "mcp": map[string]any{},
		"provider": map[string]any{provider: map[string]any{
			"name": "NEWNOA", "npm": "@ai-sdk/openai-compatible",
			"options": map[string]string{"baseURL": site.BaseURL + "/v1", "apiKey": "{env:NEWNOA_REMOTE_API_KEY}"},
			"models":  map[string]any{site.Alias: map[string]string{"name": site.Alias}},
		}},
	})
	if err != nil {
		return nil, errors.New("Cannot prepare isolated OpenCode configuration")
	}
	env := []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + dir,
		"XDG_CONFIG_HOME=" + filepath.Join(dir, "config"), "XDG_DATA_HOME=" + filepath.Join(dir, "data"),
		"XDG_CACHE_HOME=" + filepath.Join(dir, "cache"), "XDG_STATE_HOME=" + filepath.Join(dir, "state"),
		"NEWNOA_REMOTE_API_KEY=" + site.Secret, "OPENCODE_CONFIG_CONTENT=" + string(config),
		"OPENCODE_DISABLE_PROJECT_CONFIG=true", "OPENCODE_DISABLE_DEFAULT_PLUGINS=true",
		"OPENCODE_DISABLE_MODELS_FETCH=true", "OPENCODE_DISABLE_AUTOUPDATE=true",
	}
	if runtime.GOOS == "windows" {
		for _, name := range []string{"SystemRoot", "WINDIR", "ComSpec"} {
			if value := os.Getenv(name); value != "" {
				env = append(env, name+"="+value)
			}
		}
		for _, name := range []string{"USERPROFILE", "APPDATA", "LOCALAPPDATA", "TEMP", "TMP"} {
			env = append(env, name+"="+dir)
		}
	}
	return startRemoteProviderCommand(ctx, "opencode", cwd, nativeID, emit, binary, []string{"acp"}, env, probe)
}
