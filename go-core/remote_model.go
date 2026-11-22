package main

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
)

func (c *core) changeRemoteModel(ctx context.Context, m *remoteManager, target *remoteSession, p remotePayload) error {
	if err := remoteCheckAuthority(ctx); err != nil {
		return err
	}
	if _, err := uuid.Parse(p.KeyID); err != nil {
		return errors.New("Invalid API key identifier")
	}
	if _, err := uuid.Parse(p.ModelID); err != nil {
		return errors.New("Invalid model identifier")
	}
	m.mu.Lock()
	session := m.session
	m.mu.Unlock()
	if session == "" {
		return errors.New("Current account session is required to change model")
	}
	owner, err := c.remoteAccount(session)
	if err != nil || owner != m.owner {
		return errors.New("Account authorization changed")
	}
	var secret struct {
		Secret string `json:"secret"`
	}
	if err = c.remoteHTTP(ctx, "POST", "/me/api-keys/"+p.KeyID+"/copy-secret", session, nil, map[string]any{}, &secret); err != nil {
		return err
	}
	if err = c.validateAgentSelection(ctx, session, p.KeyID, secret.Secret, []string{p.ModelID}); err != nil {
		return err
	}
	if !validAPIKey(secret.Secret) {
		return errors.New("Authorized key secret is unavailable")
	}
	m.mu.Lock()
	id, agent, cwd, nativeID := target.ID, target.Agent, target.Cwd, target.NativeID
	resumeID := remoteNativeResumeID(target)
	provider := m.providers[id]
	m.mu.Unlock()
	if agent != "codex" && agent != "claude-cli" {
		return errors.New("This native executor does not support remote model switching")
	}
	if nativeID == "" || provider == nil {
		return errors.New("Native session is unavailable for model restoration")
	}
	c.remoteConfigMu.Lock()
	defer c.remoteConfigMu.Unlock()
	if err = remoteCheckAuthority(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err = remoteCheckAuthority(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return errors.New("Remote account runtime has closed")
	}
	m.mu.Lock()
	sameSession := m.session == session
	m.mu.Unlock()
	if !sameSession {
		return errors.New("Account changed during model authorization")
	}
	if c.server == nil {
		return errors.New("Start the local model proxy before changing to a NewNoa model")
	}
	if _, err = c.remoteSiteSelectionLocked(ctx, session, agent); err != nil {
		return err
	}
	if err = c.validateAgentSelection(ctx, session, p.KeyID, secret.Secret, []string{p.ModelID}); err != nil {
		return err
	}
	if err = remoteCheckAuthority(ctx); err != nil {
		return err
	}
	preview, err := c.previewWithAPIKey(agent, p.ModelID, secret.Secret, p.ModelID)
	if err != nil {
		return err
	}
	defer func() {
		if c.pending != nil && c.pending.Digest == preview.Digest {
			c.pending = nil
		}
	}()
	if err = remoteCheckAuthority(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	m.changedLocked(target, "", "model.changeStage", map[string]any{"stage": "restarting"})
	m.generations[id] = uuid.NewString()
	m.mu.Unlock()
	if err = remoteCheckAuthority(ctx); err != nil {
		return err
	}
	if err = provider.Close(); err != nil {
		return errors.New("Old native session could not be stopped safely")
	}
	m.mu.Lock()
	if m.closed || m.providers[id] != provider {
		m.mu.Unlock()
		return errors.New("Remote native owner changed during model restart")
	}
	owned := target.OwnedProcess
	delete(m.providers, id)
	target.OwnedProcess = remoteOwnedProcess{}
	m.changedLocked(target, "", "model.changeStage", map[string]any{"stage": "stopped"})
	if saveErr := m.saveLocked(); saveErr != nil {
		m.providers[id] = provider
		target.OwnedProcess = owned
		target.State = "unknown"
		m.setFailureLocked("Native model restart ownership could not be persisted")
		m.mu.Unlock()
		return saveErr
	}
	m.mu.Unlock()
	if err = remoteCheckAuthority(ctx); err != nil {
		return err
	}
	if err = c.apply(preview.Digest, preview.Content); err != nil {
		return err
	}
	c.modelAliasMu.Lock()
	if err = remoteCheckAuthority(ctx); err != nil {
		c.modelAliasMu.Unlock()
		return err
	}
	if c.modelAliases == nil {
		c.modelAliases = map[string]agentModelRoute{}
	}
	for k := range c.modelAliases {
		if strings.HasPrefix(k, agent+"\n") {
			delete(c.modelAliases, k)
		}
	}
	c.modelAliases[agent+"\n"+p.ModelID] = agentModelRoute{Model: p.ModelID, Keys: map[string]bool{managedAgentKeyHash(managedAgentKey(secret.Secret)): true}}
	c.modelAliasMu.Unlock()
	generation := uuid.NewString()
	m.mu.Lock()
	m.generations[id] = generation
	target.Model = ""
	target.ConfiguredModel = ""
	target.ConfiguredModelID = ""
	target.ConfiguredKeyID = ""
	m.changedLocked(target, "", "model.changeStage", map[string]any{"stage": "resuming"})
	m.mu.Unlock()
	if err = remoteCheckAuthority(ctx); err != nil {
		return err
	}
	selection, err := c.remoteSiteSelectionLocked(ctx, session, agent)
	if err != nil {
		return err
	}
	if m.mintSiteCredential != nil {
		selection, err = m.mintSiteCredential(ctx, id, agent, selection)
		if err != nil {
			return err
		}
	}
	ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, selection)
	resumed, err := m.start(m.providerContext(ctx, id, generation), agent, cwd, resumeID, func(e remoteProviderEvent) { m.providerEvent(id, generation, e) })
	if err != nil {
		if selection.Revoke != nil {
			err = errors.Join(err, selection.Revoke(context.Background()))
		}
		return err
	}
	m.mu.Lock()
	if m.closed || ctx.Err() != nil || m.generations[id] != generation || target.State == "failed" || target.State == "unknown" {
		m.mu.Unlock()
		_ = resumed.Close()
		return errors.New("Remote host closed while restoring native session")
	}
	m.providers[id] = resumed
	if owned, ok := resumed.(interface{ OwnedProcess() remoteOwnedProcess }); ok {
		target.OwnedProcess = owned.OwnedProcess()
	}
	if fingerprint, e := c.remoteExecutorFingerprint(agent); e == nil {
		m.executors[agent].Identity = fingerprint
		target.SiteIdentity = fingerprint
	} else {
		m.executors[agent].Authorized = false
	}
	target.ConfiguredModel = selection.Alias
	target.ConfiguredModelID = selection.Model
	target.ConfiguredKeyID = selection.KeyID
	target.Source = "newnoa"
	target.State = "idle"
	m.changedLocked(target, "", "model.changeStage", map[string]any{"stage": "restored", "actualModelConfirmed": target.Model != ""})
	m.mu.Unlock()
	return nil
}
