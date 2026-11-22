package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type remoteHTTPError struct{ Status int }

func (e *remoteHTTPError) Error() string {
	return fmt.Sprintf("Remote control authority rejected the request (HTTP %d)", e.Status)
}
func (c *core) remoteHTTP(ctx context.Context, method, path, session string, m *remoteManager, body, out any) error {
	if _, err := controlBase(c.control); err != nil {
		return err
	}
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.control, "/")+"/api/v1"+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	r.Header.Set("Authorization", "Bearer "+session)
	r.Header.Set("Content-Type", "application/json")
	if m != nil {
		if path == "/me/points/live-ticket" {
			r.Header.Set("X-NewNoa-Host-Id", m.hostID)
		}
		r.Header.Set("X-NewNoa-Host-Secret", m.secret)
		r.Header.Set("X-NewNoa-Host-Epoch", m.epoch)
	}
	client := connectorControlClient()
	client.Timeout = 8 * time.Second
	res, err := client.Do(r)
	if err != nil {
		return errors.New("Remote control authority is unavailable")
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return &remoteHTTPError{Status: res.StatusCode}
	}
	if out != nil {
		decoder := json.NewDecoder(io.LimitReader(res.Body, 1<<20))
		if decoder.Decode(out) != nil {
			return errors.New("Invalid remote authority response")
		}
	}
	return nil
}
func (c *core) remoteAccount(session string) (string, error) {
	if session == "" || c.historySessionRevoked(session) {
		return "", errors.New("Login required for remote control")
	}
	var account struct {
		ID string `json:"id"`
	}
	if err := c.remoteHTTP(context.Background(), "GET", "/me", session, nil, nil, &account); err != nil {
		return "", err
	}
	if !nodePattern.MatchString(account.ID) {
		return "", errors.New("Invalid authenticated account")
	}
	// The API applies edition/surface policy; a website login cannot become desktop control.
	if err := c.remoteHTTP(context.Background(), "GET", "/me/remote-hosts", session, nil, nil, nil); err != nil {
		return "", err
	}
	return account.ID, nil
}
func (c *core) remoteForAccount(owner string) (*remoteManager, error) {
	c.remoteMu.Lock()
	defer c.remoteMu.Unlock()
	if c.remote != nil {
		c.remote.mu.Lock()
		closed := c.remote.closed
		c.remote.mu.Unlock()
		if c.remote.owner == owner && !closed {
			return c.remote, nil
		}
		if err := c.remote.shutdown(); err != nil {
			return nil, err
		}
		c.remote = nil
	}
	m, err := newRemoteManager(c.home, owner)
	if err != nil {
		return nil, err
	}
	m.configurationGate = &c.remoteConfigMu
	m.checkSite = func(ctx context.Context, agent string) (remoteSiteLaunch, error) {
		m.mu.Lock()
		session := m.session
		identity := ""
		authorized := false
		if e := m.executors[agent]; e != nil {
			identity = e.Identity
			authorized = e.Authorized
		}
		m.mu.Unlock()
		if !authorized {
			return remoteSiteLaunch{}, errors.New("Authorize this executor with a real conversation probe")
		}
		selection, err := c.remoteSiteSelection(ctx, session, agent)
		if err != nil {
			return selection, err
		}
		if agent == "hermes-agent" {
			selection, err = c.remoteHermesContext(ctx, session, selection)
			if err != nil {
				return selection, err
			}
		}
		current, err := c.remoteExecutorFingerprint(agent)
		if err != nil || current != identity {
			return selection, errors.New(remoteSiteConfigurationChanged)
		}
		return selection, nil
	}
	m.mintSiteCredential = func(ctx context.Context, sessionID, agent string, site remoteSiteLaunch) (remoteSiteLaunch, error) {
		return c.mintRemoteAgentCredential(ctx, m, sessionID, agent, site)
	}
	m.checkAuthorization = func(agent, identity string) error {
		current, err := c.remoteExecutorFingerprint(agent)
		if err != nil {
			return err
		}
		if current != identity {
			return errors.New("Executor or configuration changed; authorize again")
		}
		return nil
	}
	m.modelChange = func(ctx context.Context, s *remoteSession, p remotePayload) error {
		return c.changeRemoteModel(ctx, m, s, p)
	}
	c.remote = m
	go c.watchRemoteLocal(m.ctx, m, c.remoteLocalFiles())
	return m, nil
}
func (c *core) stopRemote() error {
	c.remoteMu.Lock()
	defer c.remoteMu.Unlock()
	if c.remote == nil {
		return nil
	}
	err := c.remote.shutdown()
	if err == nil {
		c.remote = nil
	}
	return err
}
func (c *core) remoteRPC(method string, raw json.RawMessage) (any, error) {
	return c.remoteRPCOrigin(method, raw, "")
}
func (c *core) remoteRPCOrigin(method string, raw json.RawMessage, origin string) (any, error) {
	if dockerRuntime() || c.publicStateless {
		return nil, errors.New("Remote agent control is unavailable in this runtime")
	}
	var p struct {
		Session     string          `json:"session"`
		Agent       string          `json:"agent"`
		Enabled     bool            `json:"enabled"`
		Command     json.RawMessage `json:"command"`
		OperationID string          `json:"operationId"`
	}
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if strictRemoteJSON(raw, &p) != nil {
		return nil, errors.New("Invalid remote RPC parameters")
	}
	if method == "remote.shutdown" {
		err := c.stopRemote()
		return map[string]bool{"stopped": err == nil}, err
	}
	owner, err := c.remoteAccount(p.Session)
	if err != nil {
		return nil, err
	}
	m, err := c.remoteForAccount(owner)
	if err != nil {
		return nil, err
	}
	if !m.identityValid() {
		return nil, errors.New("Operating system user changed; authorize again")
	}
	switch method {
	case "remote.events.ticket":
		return c.issueRemoteEventsTicket(m, p.Session, origin)
	case "remote.operation.get":
		m.mu.Lock()
		defer m.mu.Unlock()
		m.pruneTransientLocked()
		r := m.operations[p.OperationID]
		if r == nil {
			return nil, errors.New("Remote operation receipt not found")
		}
		return publicRemoteReceipt(*r), nil
	case "agent.remote.status", "remote.snapshot":
		m.mu.Lock()
		hosting, hostSession := m.enabled, m.session
		m.mu.Unlock()
		// A second device can observe this host, but may not refresh grants using its own token.
		if !hosting || hostSession == p.Session {
			c.refreshRemoteSiteState(context.Background(), m, p.Session)
		}
		return m.snapshot(), nil
	case "agent.remote.authorize":
		c.mu.Lock()
		if c.nativeKeyMigrationErr != nil {
			c.nativeKeyMigrationErr = c.migrateAgentKeys()
		}
		migrationErr := c.nativeKeyMigrationErr
		c.mu.Unlock()
		if migrationErr != nil {
			return nil, migrationErr
		}
		m.mu.Lock()
		e := m.executors[p.Agent]
		if runtime.GOOS == "darwin" && p.Agent == "openclaw" && e != nil && !e.Supported && e.Error != "" {
			message := e.Error
			m.mu.Unlock()
			return nil, errors.New(message)
		}
		supported := e != nil && e.Supported
		m.mu.Unlock()
		if !supported {
			return nil, errors.New("This executor does not have a verified remote adapter")
		}
		c.refreshRemoteSiteState(context.Background(), m, p.Session)
		m.mu.Lock()
		configurationError := m.executors[p.Agent].ConfigurationError
		m.mu.Unlock()
		if configurationError != "" {
			return nil, errors.New(configurationError)
		}
		m.mu.Lock()
		e = m.executors[p.Agent]
		if e == nil || !e.Supported {
			m.mu.Unlock()
			return nil, errors.New("This executor does not have a verified remote adapter")
		}
		if e.Authorized || e.State == "authorizing" {
			result := *e
			register := e.Authorized && e.State != "authorizing" && (!m.enabled || e.Error != "")
			m.mu.Unlock()
			if register {
				if err := c.enableRemoteHost(m, p.Session); err != nil {
					return nil, err
				}
				m.mu.Lock()
				e.Error = ""
				m.changedLocked(nil, "", "executor.registration", nil)
				result = *e
				m.mu.Unlock()
			}
			return publicRemoteExecutor(result), nil
		}
		e.State = "authorizing"
		e.Error = ""
		result := *e
		m.changedLocked(nil, "", "executor.authorizing", nil)
		m.mu.Unlock()
		go c.authorizeRemoteExecutor(m, p.Agent, p.Session)
		return publicRemoteExecutor(result), nil
	case "remote.host.enable":
		if !p.Enabled {
			m.mu.Lock()
			id := m.hostID
			m.mu.Unlock()
			err = c.remoteHTTP(context.Background(), "DELETE", "/me/remote-hosts/"+id, p.Session, nil, nil, nil)
			stopErr := m.shutdown()
			if err == nil {
				m.mu.Lock()
				m.revoked = true
				_ = m.saveLocked()
				m.mu.Unlock()
			}
			if err != nil {
				return nil, err
			}
			if stopErr != nil {
				return nil, stopErr
			}
			return m.snapshot(), nil
		}
		c.refreshRemoteSiteState(m.ctx, m, p.Session)
		if err = c.enableRemoteHost(m, p.Session); err != nil {
			return nil, err
		}
		return m.snapshot(), nil
	case "remote.command":
		command, err := parseRemoteCommand(p.Command)
		if err != nil {
			return nil, err
		}
		if !nodePattern.MatchString(command.ExpectedHostEpoch) || command.ExpectedHostEpoch != m.epoch {
			return nil, errors.New("Remote host epoch changed; reconnect before sending")
		}
		return m.commandWithAuthorization(command, func(ctx context.Context) error {
			current, err := c.remoteAccount(p.Session)
			if err != nil {
				return err
			}
			if current != m.owner {
				return errors.New("Remote account changed before command execution")
			}
			return nil
		})
	default:
		return nil, errors.New("Unsupported remote operation")
	}
}
func remoteAuthorizationTimeout(agent string) time.Duration {
	if startup := remoteProbeStartupTimeout(agent); startup > 0 {
		return startup + remoteProbeTimeout(agent) + 15*time.Second
	}
	if remoteACPAgent(remoteProviderBinary(agent)) {
		return remoteProbeTimeout(agent) + 10*time.Second
	}
	return remoteProbeTimeout(agent) + 15*time.Second
}

func (c *core) authorizeRemoteExecutor(m *remoteManager, agent, session string) {
	ctx, cancel := context.WithTimeout(m.ctx, remoteAuthorizationTimeout(agent))
	defer cancel()
	var info remoteProviderInfo
	c.remoteConfigMu.Lock()
	defer c.remoteConfigMu.Unlock()
	selection, siteErr := c.remoteSiteSelection(ctx, session, agent)
	if siteErr == nil && agent == "hermes-agent" {
		selection, siteErr = c.remoteHermesContext(ctx, session, selection)
	}
	if siteErr == nil {
		ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, selection)
	}
	fingerprint, fingerprintErr := c.remoteExecutorFingerprint(agent)
	if siteErr != nil {
		fingerprintErr = siteErr
	}
	dir, err := os.MkdirTemp(filepath.Dir(m.path), "probe-")
	if os.IsNotExist(err) {
		if e := os.MkdirAll(filepath.Dir(m.path), 0700); e == nil {
			dir, err = os.MkdirTemp(filepath.Dir(m.path), "probe-")
		}
	}
	if err == nil {
		defer os.RemoveAll(dir)
	}
	if err == nil && fingerprintErr != nil {
		err = fingerprintErr
	}
	if err == nil {
		info, err = probeRemoteProvider(ctx, agent, dir)
	}
	if err == nil {
		after, e := c.remoteExecutorFingerprint(agent)
		if e != nil || after != fingerprint {
			err = errors.New("Executor identity changed during authorization")
		}
	}
	if err == nil {
		selection, siteErr = c.remoteSiteSelection(ctx, session, agent)
		err = siteErr
	}
	if err == nil {
		var owner string
		owner, err = c.remoteAccount(session)
		if err == nil && owner != m.owner {
			err = errors.New("Account changed during executor authorization")
		}
	}
	m.mu.Lock()
	if m.closed || !m.identityValid() {
		m.mu.Unlock()
		return
	}
	e := m.executors[agent]
	e.SiteConfigured = siteErr == nil
	e.ConfiguredModel = selection.Model
	e.ConfiguredKeyID = selection.KeyID
	e.ConfigurationError = ""
	if siteErr != nil {
		e.ConfigurationError = siteErr.Error()
	}
	if err != nil {
		e.State = "failed"
		e.Error = remoteSafeError(err)
		e.Authorized = false
	} else {
		e.Authorized = true
		e.Identity = fingerprint
		e.State = "authorizing"
		e.Version = info.Version
		e.Capabilities = info.Capabilities
		e.Capabilities["modelChange"] = agent == "codex" || agent == "claude-cli"
		e.Error = ""
	}
	m.changedLocked(nil, "", "executor.authorization", map[string]any{"agent": agent, "authorized": e.Authorized})
	_ = m.saveLocked()
	m.mu.Unlock()
	if err == nil {
		err = c.enableRemoteHost(m, session)
		m.mu.Lock()
		if !m.closed && e.Authorized {
			e.State = "ready"
			if err != nil {
				e.Error = "Host registration failed: " + remoteSafeError(err)
			}
			m.changedLocked(nil, "", "executor.registration", nil)
		}
		m.mu.Unlock()
	}
}
func (c *core) enableRemoteHost(m *remoteManager, session string) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("Remote runtime is closed")
	}
	capabilities := []string{}
	for agent, e := range m.executors {
		if e.Authorized && e.SiteConfigured {
			capabilities = append(capabilities, agent)
		}
	}
	if len(capabilities) == 0 {
		m.mu.Unlock()
		return errors.New("Authorize an executor before enabling remote access")
	}
	hostname, _ := os.Hostname()
	body := map[string]any{"hostId": m.hostID, "hostEpoch": m.epoch, "hostSecret": m.secret, "hostname": hostname, "platform": m.snapshotPlatform(), "capabilities": capabilities}
	m.mu.Unlock()
	if err := c.remoteHTTP(m.ctx, "POST", "/me/remote-hosts/register", session, nil, body, nil); err != nil {
		return err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("Remote runtime closed during registration")
	}
	// Only successful, explicit host registration changes the relay credential.
	if m.enabled && m.session == session {
		m.mu.Unlock()
		return nil
	}
	if m.relayCancel != nil {
		m.relayCancel()
	}
	m.session = session
	m.enabled = true
	ctx, cancel := context.WithCancel(m.ctx)
	m.relayCancel = cancel
	m.changedLocked(nil, "", "host.lifecycle", map[string]any{"enabled": true})
	err := m.saveLocked()
	if err != nil {
		m.enabled = false
		m.relayCancel = nil
	}
	m.mu.Unlock()
	if err != nil {
		cancel()
		return err
	}
	go c.runRemoteRelay(ctx, m, session)
	return nil
}
func (m *remoteManager) snapshotPlatform() string { return runtime.GOOS }
func (c *core) fetchRemoteCommands(ctx context.Context, m *remoteManager, session string) error {
	path := "/me/remote-hosts/" + m.hostID
	for ctx.Err() == nil {
		var poll struct {
			Commands []struct {
				Command       remoteCommandEnvelope `json:"command"`
				Grant         string                `json:"grant"`
				CommandDigest string                `json:"commandDigest"`
				ExpiresAt     string                `json:"expiresAt"`
			} `json:"commands"`
		}
		err := c.remoteHTTP(ctx, "POST", path+"/poll", session, m, map[string]any{}, &poll)
		if err == nil {
			for _, item := range poll.Commands {
				commandSession := session
				var verified struct {
					Valid     bool                  `json:"valid"`
					Command   remoteCommandEnvelope `json:"command"`
					Scope     string                `json:"scope"`
					HostEpoch string                `json:"hostEpoch"`
				}
				body := map[string]any{"grant": item.Grant, "operationId": item.Command.OperationID, "commandDigest": item.CommandDigest}
				verifyErr := c.remoteHTTP(ctx, "POST", path+"/verify-grant", session, m, body, &verified)
				if verifyErr != nil || !verified.Valid || verified.HostEpoch != m.epoch || verified.Scope != item.Command.Type || remoteDigest(verified.Command) != remoteDigest(item.Command) {
					continue
				}
				_, commandErr := m.commandWithAuthorization(verified.Command, func(execCtx context.Context) error {
					var again struct {
						Valid     bool                  `json:"valid"`
						Command   remoteCommandEnvelope `json:"command"`
						HostEpoch string                `json:"hostEpoch"`
						Scope     string                `json:"scope"`
					}
					if err := c.remoteHTTP(execCtx, "POST", path+"/verify-grant", commandSession, m, body, &again); err != nil {
						return err
					}
					if !again.Valid || again.HostEpoch != m.epoch || again.Scope != item.Command.Type || remoteDigest(again.Command) != remoteDigest(item.Command) {
						return errors.New("Control grant expired or changed before execution")
					}
					return nil
				})
				if commandErr != nil {
					// Admission failures have no local operation; API retries delivery until this receipt is acknowledged.
					rejected := remoteReceipt{OperationID: verified.Command.OperationID, Status: "rejected", SessionID: verified.Command.SessionID, Error: remoteSafeError(commandErr)}
					err = c.remoteHTTP(ctx, "POST", path+"/publish", commandSession, m, map[string]any{"snapshot": m.snapshot(), "receipts": []remoteReceipt{rejected}}, nil)
					if err != nil {
						break
					}
				} else {
					m.mu.Lock()
					if m.relayOperations == nil {
						m.relayOperations = map[string]bool{}
					}
					m.relayOperations[verified.Command.OperationID] = true
					m.signalChangesLocked()
					m.mu.Unlock()
				}
			}
		}
		if err != nil || len(poll.Commands) < 16 {
			return err
		}
	}
	return ctx.Err()
}
