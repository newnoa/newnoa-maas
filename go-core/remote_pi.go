package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

func (c *core) remotePiSelection() (remoteSiteLaunch, []agentConnection, error) {
	var site remoteSiteLaunch
	if err := c.checkPiLocation(); err != nil {
		return site, nil, errors.New(remoteSiteConfigurationChanged)
	}
	j, err := c.readPiJournal()
	if err != nil {
		return site, nil, errors.New(remoteSiteConfigurationChanged)
	}
	if j == nil {
		return site, nil, errors.New(remoteSiteConfigurationRequired)
	}
	raw, exists, err := c.readPiFiles()
	if err != nil || j.Stage != "applied" || j.ProxyPort != c.proxyPortValue() {
		return site, nil, errors.New(remoteSiteConfigurationChanged)
	}
	for i, f := range j.Files {
		if !exists[i] || hash(raw[i]) != f.Installed {
			return site, nil, errors.New(remoteSiteConfigurationChanged)
		}
	}
	var models struct {
		Providers map[string]struct {
			BaseURL string `json:"baseUrl"`
			API     string `json:"api"`
			Key     string `json:"apiKey"`
		} `json:"providers"`
	}
	var defaults struct {
		Provider string `json:"defaultProvider"`
		Model    string `json:"defaultModel"`
	}
	if json.Unmarshal(raw[0], &models) != nil || json.Unmarshal(raw[1], &defaults) != nil {
		return site, nil, errors.New(remoteSiteConfigurationChanged)
	}
	native := "newnoa"
	if j.ProviderCatalog != nil {
		info := &journal{ProviderCatalog: j.ProviderCatalog, Model: j.Model, Alias: j.Alias}
		native, err = setRemoteCatalogDefault(info, &site)
		if err != nil || defaults.Model != j.Alias {
			return site, nil, errors.New(remoteSiteConfigurationChanged)
		}
	}
	provider := models.Providers[native]
	if defaults.Provider != native || provider.BaseURL != c.proxyBaseURL()+"/v1" || provider.API != "openai-completions" || managedAgentKeyHash(provider.Key) != j.APIKeyHash {
		return site, nil, errors.New(remoteSiteConfigurationChanged)
	}
	site.Model, site.Alias, site.Secret, site.BaseURL, site.Home = j.Model, defaults.Model, provider.Key, c.proxyBaseURL(), c.home
	if j.ProviderCatalog != nil {
		return site, []agentConnection{{j.Model, j.Alias, j.APIKeyHash}}, nil
	}
	return site, piJournalConnections(*j), nil
}

func prepareRemotePi(ctx context.Context, site remoteSiteLaunch, nativeID string, probe bool) ([]string, []string, string, func(), error) {
	stateDir, _ := ctx.Value(remoteProviderStateDirKey{}).(string)
	if !filepath.IsAbs(stateDir) {
		return nil, nil, "", nil, errors.New("Pi native state directory is unavailable")
	}
	cfg, err := os.MkdirTemp(stateDir, "pi-config-")
	if err != nil {
		return nil, nil, "", nil, errors.New("Cannot create isolated Pi configuration")
	}
	cleanup := func() { _ = os.RemoveAll(cfg) }
	fail := func(err error) ([]string, []string, string, func(), error) { cleanup(); return nil, nil, "", nil, err }
	models, err := json.Marshal(map[string]any{"providers": map[string]any{"newnoa": map[string]any{"api": "openai-completions", "baseUrl": site.BaseURL + "/v1", "apiKey": "$NEWNOA_REMOTE_API_KEY", "models": []map[string]any{{"id": site.Alias, "name": site.Alias}}}}})
	if err != nil || atomicFile(filepath.Join(cfg, "models.json"), models) != nil {
		return fail(errors.New("Cannot write isolated Pi model configuration"))
	}
	env := []string{"HOME=" + site.Home, "USERPROFILE=" + site.Home, "PATH=" + os.Getenv("PATH"), "SYSTEMROOT=" + os.Getenv("SYSTEMROOT"), "PI_CODING_AGENT_DIR=" + cfg, "NEWNOA_REMOTE_API_KEY=" + site.Secret}
	if base, ok := ctx.Value(remoteExecutorEnvironmentKey{}).([]string); ok {
		env = mergeRemoteEnvironment(base, env)
	}
	args := []string{"--mode", "rpc", "--provider", "newnoa", "--model", site.Alias, "--no-context-files", "--no-extensions", "--no-skills", "--no-prompt-templates"}
	if probe {
		return append(args, "--no-session", "--no-tools", "--thinking", "low"), env, "", cleanup, nil
	}
	store := filepath.Join(stateDir, "pi-sessions")
	if err := (&core{home: stateDir}).safeConfigFile(filepath.Join(store, "session-check")); err != nil || os.MkdirAll(store, 0700) != nil {
		return fail(errors.New("Pi session storage is unavailable"))
	}
	if nativeID == "" {
		nativeID = uuid.NewString()
		args = append(args, "--session-id", nativeID)
	} else {
		if id, err := uuid.Parse(nativeID); err != nil || id.String() != nativeID {
			return fail(errors.New("Invalid Pi native session identity"))
		}
		matches, err := filepath.Glob(filepath.Join(store, "*_"+nativeID+".jsonl"))
		if err != nil || len(matches) != 1 {
			return fail(errors.New("Pi native session file is missing or ambiguous"))
		}
		info, err := os.Lstat(matches[0])
		if err != nil || !info.Mode().IsRegular() {
			return fail(errors.New("Pi native session file is not a regular file"))
		}
		args = append(args, "--session", matches[0])
	}
	// Pi's normal RPC tools lack a permission callback. Do not enable tools without a permission boundary.
	return append(args, "--session-dir", store, "--no-tools"), env, nativeID, cleanup, nil
}

func (p *remoteNativeProvider) initializePi(ctx context.Context, nativeID string) error {
	result, err := p.rpc(ctx, "get_state", map[string]any{})
	if err != nil {
		return err
	}
	var state struct {
		SessionID string `json:"sessionId"`
		Model     struct {
			ID       string `json:"id"`
			Provider string `json:"provider"`
		} `json:"model"`
	}
	if json.Unmarshal(result, &state) != nil || state.Model.Provider != "newnoa" || state.Model.ID != p.siteModel || state.SessionID == "" || (nativeID != "" && nativeID != state.SessionID) {
		return errors.New("Pi did not confirm the managed model and session identity")
	}
	p.mu.Lock()
	p.nativeID = state.SessionID
	p.eventLocked(remoteProviderEvent{Type: "session", Model: state.Model.ID})
	p.mu.Unlock()
	return nil
}

func (p *remoteNativeProvider) startPiTurn(ctx context.Context, in remoteProviderInput) error {
	if len(in.Images) != 0 {
		return errors.New("Pi remote image input is not enabled")
	}
	p.mu.Lock()
	p.runID = uuid.NewString()
	p.piTerminalError = false
	p.mu.Unlock()
	_, err := p.rpcDispatch(ctx, "prompt", map[string]any{"message": in.Text}, in.OperationID)
	if err != nil && !isRemoteDispatchUnknown(err) {
		p.mu.Lock()
		p.runID = ""
		p.mu.Unlock()
	}
	return err
}

func (p *remoteNativeProvider) piMessage(m remoteNativeMessage) {
	var wire struct {
		Command   string          `json:"command"`
		Success   *bool           `json:"success"`
		Data      json.RawMessage `json:"data"`
		Aborted   bool            `json:"aborted"`
		WillRetry bool            `json:"willRetry"`
		Messages  []struct {
			Role string `json:"role"`
			Stop string `json:"stopReason"`
		} `json:"messages"`
		Update struct {
			Type  string `json:"type"`
			Delta string `json:"delta"`
		} `json:"assistantMessageEvent"`
	}
	if json.Unmarshal(m.Raw, &wire) != nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch m.Type {
	case "response":
		id := remoteMessageID(m.ID)
		p.pruneDispatchesLocked(time.Now())
		if wire.Success == nil {
			return
		}
		accepted := *wire.Success
		if wire.Command == "prompt" && accepted {
			var data struct {
				Disposition string `json:"disposition"`
			}
			accepted = json.Unmarshal(wire.Data, &data) == nil && data.Disposition == "started"
		}
		if dispatch, ok := p.dispatches[id]; ok && dispatch.method == wire.Command {
			status := "rejected"
			if accepted {
				status = "providerAccepted"
			}
			p.settleDispatchLocked(id, status)
		}
		if waiter, ok := p.piWaiters[id]; ok && waiter.command == wire.Command {
			m.Result = wire.Data
			if !accepted {
				m.Error = json.RawMessage(`true`)
			}
			select {
			case waiter.reply <- m:
			default:
			}
		}
	case "message_update":
		if p.runID != "" && wire.Update.Type == "text_delta" {
			p.eventLocked(remoteProviderEvent{Type: "output", Text: wire.Update.Delta})
		}
	case "agent_end":
		if p.runID != "" {
			p.piTerminalError = false
			if !wire.WillRetry {
				for i := len(wire.Messages) - 1; i >= 0; i-- {
					if wire.Messages[i].Role == "assistant" {
						p.piTerminalError = wire.Messages[i].Stop == "error"
						break
					}
				}
			}
		}
	case "agent_settled":
		if p.runID == "" {
			return
		}
		switch {
		case wire.Aborted:
			p.eventLocked(remoteProviderEvent{Type: "state", State: "stopped"})
		case p.piTerminalError:
			p.eventLocked(remoteProviderEvent{Type: "error", State: "failed", Error: "Pi model request failed"})
		default:
			p.eventLocked(remoteProviderEvent{Type: "state", State: "idle"})
		}
		p.runID = ""
	}
}
