package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pelletier/go-toml/v2"
	"gopkg.in/yaml.v3"
)

const remoteSiteConfigurationRequired = "Configure this executor with a current site API key and authorized model before enabling remote access"
const remoteSiteConfigurationChanged = "Site agent configuration changed; configure and authorize this executor again"
const remoteSiteKeyUnavailable = "Configured site API key or authorized models are unavailable; update the agent configuration"

type remoteSiteLaunchKey struct{}
type remoteSiteLaunch struct {
	KeyID, Model, Alias, Secret, BaseURL, Home string
	CredentialID                               string
	Revision                                   int64
	Revoke                                     func(context.Context) error
	Effort, Owner                              string
	ContextLength                              int64
}

type remoteAgentCredential struct {
	refreshMu                 sync.Mutex
	sessionID                 string
	agent                     string
	keyID                     string
	model                     string
	alias                     string
	localHash                 string
	credentialID              string
	upstreamToken             string
	accountSession            string
	expiresAt                 time.Time
	revision                  int64
	retiredID, retiredSession string
	retiredUntil              time.Time
}

type issuedAgentLease struct {
	ID        string `json:"credentialId"`
	Secret    string `json:"credential"`
	ExpiresAt string `json:"expiresAt"`
	Revision  int64  `json:"revision"`
}

func agentCredentialPath(hostID, sessionID string) string {
	return "/me/remote-hosts/" + hostID + "/sessions/" + sessionID + "/credentials"
}
func (c *core) issueAgentLease(ctx context.Context, m *remoteManager, login, hostID, epoch, sessionID, keyID, deployment, agent string, revision int64) (issuedAgentLease, time.Time, error) {
	var lease issuedAgentLease
	body := map[string]any{"provider": agent, "deploymentId": deployment, "parentKeyId": keyID, "hostEpoch": epoch}
	if revision != 0 {
		body["revision"] = revision
	}
	if err := c.remoteHTTP(ctx, http.MethodPost, agentCredentialPath(hostID, sessionID), login, m, body, &lease); err != nil {
		return lease, time.Time{}, err
	}
	now := time.Now()
	expiry, err := time.Parse(time.RFC3339, lease.ExpiresAt)
	if err != nil || !expiry.After(now) || expiry.After(now.Add(time.Hour+5*time.Second)) || !nodePattern.MatchString(lease.ID) || lease.Revision < 1 || revision != 0 && revision != lease.Revision || !strings.HasPrefix(lease.Secret, "ra_") || len(lease.Secret) < 35 || len(lease.Secret) > 256 || !validAPIKey(lease.Secret) {
		return lease, time.Time{}, errors.New("Remote authority returned an invalid agent credential")
	}
	return lease, expiry, nil
}
func (c *core) mintRemoteAgentCredential(ctx context.Context, m *remoteManager, sessionID, agent string, site remoteSiteLaunch) (remoteSiteLaunch, error) {
	if m == nil || !nodePattern.MatchString(m.hostID) || !nodePattern.MatchString(sessionID) ||
		!nodePattern.MatchString(site.KeyID) || !nodePattern.MatchString(site.Model) ||
		!map[string]bool{"codex": true, "claude-cli": true, "pi-agent": true, "openclaw": true, "hermes-agent": true, "opencode": true}[agent] {
		return site, errors.New("Remote agent credential binding is incomplete")
	}
	m.mu.Lock()
	accountSession, hostID, epoch := m.session, m.hostID, m.epoch
	m.mu.Unlock()
	if accountSession == "" || epoch == "" {
		return site, errors.New("Remote account session or host epoch is unavailable")
	}
	credential, expires, err := c.issueAgentLease(ctx, m, accountSession, hostID, epoch, sessionID, site.KeyID, site.Model, agent, 0)
	if err != nil {
		return site, err
	}
	localToken := "rla_" + randomToken()
	localHash := hash([]byte(localToken))
	record := &remoteAgentCredential{
		sessionID: sessionID, agent: agent, keyID: site.KeyID, model: site.Model, alias: site.Alias,
		localHash: localHash, credentialID: credential.ID, upstreamToken: credential.Secret,
		accountSession: accountSession, expiresAt: expires, revision: credential.Revision,
	}
	m.mu.Lock()
	if m.closed || m.siteCredentials == nil || m.session != accountSession {
		m.mu.Unlock()
		revokeErr := c.remoteHTTP(ctx, http.MethodDelete, agentCredentialPath(hostID, sessionID)+"/"+credential.ID, accountSession, m, nil, nil)
		return site, errors.Join(errors.New("Remote agent credential registry is unavailable"), revokeErr)
	}
	m.siteCredentials[localHash] = record
	m.mu.Unlock()
	c.modelAliasMu.Lock()
	if c.modelAliases == nil {
		c.modelAliases = map[string]agentModelRoute{}
	}
	index := "remote-" + localHash + "\n" + site.Alias
	route := c.modelAliases[index]
	if route.Keys == nil {
		route.Keys = map[string]bool{}
	}
	route.Model = site.Model
	route.Keys[localHash] = true
	c.modelAliases[index] = route
	c.modelAliasMu.Unlock()
	site.CredentialID = credential.ID
	site.Secret = localToken
	site.Revision = credential.Revision
	site.Revoke = func(ctx context.Context) error {
		return c.revokeRemoteAgentCredential(ctx, m, record)
	}
	return site, nil
}

func (c *core) revokePreviousAgentLease(ctx context.Context, m *remoteManager, record *remoteAgentCredential) error {
	if record.retiredID == "" {
		return nil
	}
	if record.retiredUntil.After(time.Now()) {
		path := agentCredentialPath(m.hostID, record.sessionID) + "/" + record.retiredID
		if err := c.remoteHTTP(ctx, http.MethodDelete, path, record.retiredSession, m, nil, nil); err != nil {
			return err
		}
	}
	record.retiredID, record.retiredSession = "", ""
	return nil
}
func (c *core) revokeRemoteAgentCredential(ctx context.Context, m *remoteManager, record *remoteAgentCredential) error {
	if record == nil {
		return nil
	}
	if m == nil || !nodePattern.MatchString(m.hostID) || !nodePattern.MatchString(record.sessionID) {
		return errors.New("Remote agent credential binding is invalid")
	}
	record.refreshMu.Lock()
	defer record.refreshMu.Unlock()
	m.mu.Lock()
	hostID, accountSession, credentialID := m.hostID, record.accountSession, record.credentialID
	delete(m.siteCredentials, record.localHash)
	m.mu.Unlock()
	c.modelAliasMu.Lock()
	index := "remote-" + record.localHash + "\n" + record.alias
	if route, ok := c.modelAliases[index]; ok {
		delete(route.Keys, record.localHash)
		if len(route.Keys) == 0 {
			delete(c.modelAliases, index)
		} else {
			c.modelAliases[index] = route
		}
	}
	c.modelAliasMu.Unlock()
	if accountSession == "" {
		return errors.New("Remote account session is unavailable for credential revocation")
	}
	path := agentCredentialPath(hostID, record.sessionID) + "/" + credentialID
	return errors.Join(c.remoteHTTP(ctx, http.MethodDelete, path, accountSession, m, nil, nil), c.revokePreviousAgentLease(ctx, m, record))
}

func (c *core) remoteAgentAuthorization(ctx context.Context, token string) (string, bool, error) {
	if !strings.HasPrefix(token, "rla_") {
		return "", false, nil
	}
	c.remoteMu.Lock()
	m := c.remote
	c.remoteMu.Unlock()
	if m == nil {
		return "", true, errors.New("Remote agent session is unavailable")
	}
	localHash := hash([]byte(token))
	m.mu.Lock()
	record := m.siteCredentials[localHash]
	active := !m.closed && m.enabled && record != nil && m.sessions[record.sessionID] != nil
	m.mu.Unlock()
	if !active {
		return "", true, errors.New("Remote agent session has ended")
	}
	record.refreshMu.Lock()
	defer record.refreshMu.Unlock()
	m.mu.Lock()
	if m.siteCredentials[localHash] != record || m.closed {
		m.mu.Unlock()
		return "", true, errors.New("Remote agent session has ended")
	}
	accountSession, hostID, epoch := m.session, m.hostID, m.epoch
	m.mu.Unlock()
	if err := c.revokePreviousAgentLease(ctx, m, record); err != nil {
		return "", true, err
	}
	if accountSession == record.accountSession && record.expiresAt.After(time.Now().Add(5*time.Minute)) {
		return record.upstreamToken, true, nil
	}
	if accountSession == "" || hostID == "" || epoch == "" {
		return "", true, errors.New("Remote account session is unavailable")
	}
	next, expires, err := c.issueAgentLease(ctx, m, accountSession, hostID, epoch, record.sessionID, record.keyID, record.model, record.agent, record.revision)
	if err != nil {
		return "", true, err
	}
	record.retiredID, record.retiredSession, record.retiredUntil = record.credentialID, record.accountSession, record.expiresAt
	record.credentialID, record.upstreamToken, record.expiresAt, record.accountSession = next.ID, next.Secret, expires, accountSession
	if err = c.revokePreviousAgentLease(ctx, m, record); err != nil {
		return "", true, err
	}
	return record.upstreamToken, true, nil
}

func remoteCredentialFreeEnvironment(home string) ([]string, error) {
	if !filepath.IsAbs(home) {
		return nil, errors.New("Remote session home is unavailable")
	}
	if err := os.MkdirAll(home, 0700); err != nil {
		return nil, errors.New("Remote session home cannot be created")
	}

	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "USERPROFILE=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "CODEX_HOME=" + filepath.Join(home, ".codex"), "CLAUDE_CONFIG_DIR=" + filepath.Join(home, ".claude")}
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
		if value := os.Getenv(name); value != "" {
			env = append(env, name+"="+value)
		}
	}
	if runtime.GOOS == "windows" {
		for _, name := range []string{"SystemRoot", "WINDIR", "ComSpec"} {
			if value := os.Getenv(name); value != "" {
				env = append(env, name+"="+value)
			}
		}
		for _, name := range []string{"APPDATA", "LOCALAPPDATA", "TEMP", "TMP"} {
			env = append(env, name+"="+home)
		}
	}
	return env, nil
}

// The managed hashes prove which files we wrote; the API remains the key/model authority.
func (c *core) remoteSiteSelection(ctx context.Context, session, agent string) (remoteSiteLaunch, error) {
	c.mu.Lock()
	result, err := c.remoteSiteSelectionLocked(ctx, session, agent)
	c.mu.Unlock()
	return result, err
}
func (c *core) remoteSiteSelectionLocked(ctx context.Context, session, agent string) (remoteSiteLaunch, error) {
	if c.nativeKeyMigrationErr != nil {
		return remoteSiteLaunch{}, c.nativeKeyMigrationErr
	}
	selection, entries, err := c.remoteManagedSelection(agent)
	if err != nil {
		return selection, err
	}
	return c.authorizeRemoteSiteSelection(ctx, session, selection, entries)
}

func (c *core) remoteManagedSelection(agent string) (remoteSiteLaunch, []agentConnection, error) {
	if agent == "pi-agent" {
		return c.remotePiSelection()
	}
	var result remoteSiteLaunch
	if agent != "codex" && agent != "claude-cli" && agent != "hermes-agent" && agent != "openclaw" && agent != "opencode" {
		return result, nil, errors.New("This executor does not have a verified remote adapter")
	}
	path, jpath, err := c.agentPaths(agent)
	if err != nil {
		return result, nil, err
	}
	if c.safeConfigFile(path) != nil || c.safeConfigFile(jpath) != nil {
		return result, nil, errors.New(remoteSiteConfigurationChanged)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return result, nil, errors.New(remoteSiteConfigurationRequired)
	}
	j, err := c.managedConnectionJournal(agent, raw, true)
	if err != nil {
		return result, nil, errors.New(remoteSiteConfigurationChanged)
	}
	if j == nil || j.Stage != "applied" || j.Model == "" || len(j.APIKeyHash) != 64 {
		return result, nil, errors.New(remoteSiteConfigurationRequired)
	}
	result = remoteSiteLaunch{Model: j.Model, Alias: j.Alias, BaseURL: c.proxyBaseURL(), Home: c.home}
	if agent == "opencode" {
		return c.remoteOpenCodeSelection(raw, j, result)
	}
	if agent == "codex" {
		var config map[string]any
		if toml.Unmarshal(raw, &config) != nil || config["model_provider"] != "newnoa" {
			return result, nil, errors.New(remoteSiteConfigurationChanged)
		}
		alias, _ := config["model"].(string)
		result.Alias = alias
		effort, _ := config["model_reasoning_effort"].(string)
		if validateAgentReasoning(agent, effort) == nil {
			result.Effort = effort
		}
		var auth struct {
			Key  string `json:"OPENAI_API_KEY"`
			Mode string `json:"auth_mode"`
		}
		authPath := filepath.Join(c.home, ".codex", "auth.json")
		if c.safeConfigFile(authPath) != nil {
			return result, nil, errors.New(remoteSiteConfigurationChanged)
		}
		data, e := os.ReadFile(authPath)
		if e != nil || json.Unmarshal(data, &auth) != nil || auth.Mode != "apikey" {
			return result, nil, errors.New(remoteSiteConfigurationChanged)
		}
		result.Secret = auth.Key
	} else if agent == "openclaw" {
		if c.checkOpenClawLocation() != nil {
			return result, nil, errors.New(remoteSiteConfigurationChanged)
		}
		var config struct {
			Models struct {
				Providers map[string]struct {
					BaseURL string `json:"baseUrl"`
					Key     string `json:"apiKey"`
					API     string `json:"api"`
				} `json:"providers"`
			} `json:"models"`
			Agents struct {
				Defaults struct {
					Model struct {
						Primary   string   `json:"primary"`
						Fallbacks []string `json:"fallbacks"`
					} `json:"model"`
				} `json:"defaults"`
			} `json:"agents"`
		}
		if json.Unmarshal(raw, &config) != nil {
			return result, nil, errors.New(remoteSiteConfigurationChanged)
		}
		native := "newnoa"
		if j.ProviderCatalog != nil {
			var err error
			native, err = setRemoteCatalogDefault(j, &result)
			if err != nil {
				return result, nil, err
			}
		}
		provider := config.Models.Providers[native]
		alias, ok := strings.CutPrefix(config.Agents.Defaults.Model.Primary, native+"/")
		if !ok || len(config.Agents.Defaults.Model.Fallbacks) != 0 || provider.BaseURL != result.BaseURL+"/v1" || provider.API != "openai-completions" {
			return result, nil, errors.New(remoteSiteConfigurationChanged)
		}
		result.Alias, result.Secret = alias, provider.Key
	} else if agent == "hermes-agent" {
		if c.checkHermesLocation() != nil {
			return result, nil, errors.New(remoteSiteConfigurationChanged)
		}
		var config struct {
			Model struct {
				Provider string `yaml:"provider"`
				Default  string `yaml:"default"`
			} `yaml:"model"`
			Providers map[string]struct {
				BaseURL string `yaml:"base_url"`
				Key     string `yaml:"api_key"`
				Mode    string `yaml:"api_mode"`
				Model   string `yaml:"default_model"`
			} `yaml:"providers"`
		}
		if yaml.Unmarshal(raw, &config) != nil || !strings.HasPrefix(config.Model.Provider, "custom:newnoa-") {
			return result, nil, errors.New(remoteSiteConfigurationChanged)
		}
		native := strings.TrimPrefix(config.Model.Provider, "custom:")
		if j.ProviderCatalog != nil {
			want, err := setRemoteCatalogDefault(j, &result)
			if err != nil || native != want {
				return result, nil, errors.New(remoteSiteConfigurationChanged)
			}
		}
		provider := config.Providers[native]
		if provider.BaseURL != result.BaseURL+"/v1" || provider.Mode != "chat_completions" || config.Model.Default != provider.Model || provider.Model != j.Alias {
			return result, nil, errors.New(remoteSiteConfigurationChanged)
		}
		result.Alias, result.Secret = config.Model.Default, provider.Key
	} else {
		var config struct {
			Env map[string]string `json:"env"`
		}
		if json.Unmarshal(raw, &config) != nil || config.Env["ANTHROPIC_BASE_URL"] != result.BaseURL {
			return result, nil, errors.New(remoteSiteConfigurationChanged)
		}
		result.Secret = config.Env["ANTHROPIC_API_KEY"]
		result.Alias = config.Env["ANTHROPIC_MODEL"]
		if result.Alias == "" {
			for _, entry := range journalConnections(*j) {
				if entry.Model == j.Model {
					result.Alias = entry.Alias
					break
				}
			}
		}
	}
	if managedAgentKeyHash(result.Secret) != j.APIKeyHash || j.ProviderCatalog != nil && result.Alias != j.Alias {
		return result, nil, errors.New(remoteSiteConfigurationChanged)
	}
	entries := journalConnections(*j)
	if agent == "hermes-agent" || j.ProviderCatalog != nil {
		// Only the selected pair is in scope for native launch authorization.
		entries = []agentConnection{{Model: j.Model, Alias: j.Alias, APIKeyHash: j.APIKeyHash}}
	}
	return result, entries, nil
}

func (c *core) authorizeRemoteSiteSelection(ctx context.Context, session string, result remoteSiteLaunch, entries []agentConnection) (remoteSiteLaunch, error) {
	keyHash := managedAgentKeyHash(result.Secret)
	if !validAPIKey(result.Secret) || result.Alias == "" {
		return result, errors.New(remoteSiteConfigurationChanged)
	}
	matchedDefault := false
	models := []string{}
	for _, entry := range entries {
		if entry.APIKeyHash != keyHash {
			return result, errors.New(remoteSiteConfigurationChanged)
		}
		models = append(models, entry.Model)
		if entry.Alias == result.Alias {
			result.Model = entry.Model
			matchedDefault = true
		}
	}
	if len(models) == 0 || !matchedDefault {
		return result, errors.New(remoteSiteConfigurationRequired)
	}
	if result.Owner != "" {
		var account struct{ ID string }
		if c.remoteHTTP(ctx, "GET", "/me", session, nil, nil, &account) != nil || account.ID != result.Owner {
			return result, errors.New(remoteSiteKeyUnavailable)
		}
	}
	var keys []struct {
		ID            string   `json:"id"`
		Prefix        string   `json:"prefix"`
		Paused        bool     `json:"paused"`
		Expired       bool     `json:"expired"`
		Revoked       any      `json:"revoked_at"`
		ExpiresAt     string   `json:"expires_at"`
		DeploymentIDs []string `json:"deploymentIds"`
	}
	if err := c.remoteHTTP(ctx, "GET", "/me/api-keys", session, nil, nil, &keys); err != nil {
		return result, errors.New(remoteSiteKeyUnavailable)
	}
	for _, key := range keys {
		if !nodePattern.MatchString(key.ID) || result.KeyID != "" && result.KeyID != key.ID || key.Paused || key.Expired || key.Revoked != nil || key.Prefix == "" || !referencePrefixMatches(result.Secret, key.Prefix) {
			continue
		}
		if key.ExpiresAt != "" {
			expires, e := time.Parse(time.RFC3339, key.ExpiresAt)
			if e != nil || !expires.After(time.Now()) {
				continue
			}
		}
		allowed := map[string]bool{}
		for _, id := range key.DeploymentIDs {
			allowed[id] = true
		}
		complete := true
		for _, id := range models {
			if !nodePattern.MatchString(id) || !allowed[id] {
				complete = false
			}
		}
		if !complete {
			continue
		}
		var secret struct {
			Secret string `json:"secret"`
		}
		if c.remoteHTTP(ctx, "POST", "/me/api-keys/"+key.ID+"/copy-secret", session, nil, map[string]any{}, &secret) == nil && agentKeyMatches(result.Secret, secret.Secret) {
			result.KeyID = key.ID
			return result, nil
		}
	}
	return result, errors.New(remoteSiteKeyUnavailable)
}

func (c *core) refreshRemoteSiteState(ctx context.Context, m *remoteManager, session string) {
	for _, agent := range []string{"codex", "claude-cli", "pi-agent", "openclaw", "hermes-agent", "opencode"} {
		selection, err := c.remoteSiteSelection(ctx, session, agent)
		m.mu.Lock()
		identity := ""
		if e := m.executors[agent]; e != nil && e.Authorized {
			identity = e.Identity
		}
		m.mu.Unlock()
		if err == nil && identity != "" {
			current, e := c.remoteExecutorFingerprint(agent)
			if e != nil || current != identity {
				err = errors.New(remoteSiteConfigurationChanged)
			}
		}
		m.mu.Lock()
		e := m.executors[agent]
		if e != nil {
			before := *e
			e.SiteConfigured = err == nil
			e.ConfiguredModel = ""
			e.ConfiguredKeyID = ""
			e.ConfigurationError = ""
			if err != nil {
				e.ConfigurationError = err.Error()
				if e.Authorized {
					e.State = "failed"
					e.Error = ""
				}
				e.Authorized = false
			} else {
				e.ConfiguredModel = selection.Model
				e.ConfiguredKeyID = selection.KeyID
			}
			if before.SiteConfigured != e.SiteConfigured || before.ConfigurationError != e.ConfigurationError || before.ConfiguredModel != e.ConfiguredModel || before.ConfiguredKeyID != e.ConfiguredKeyID || before.Authorized != e.Authorized {
				m.changedLocked(nil, "", "executor.configuration", map[string]any{"agent": agent, "siteConfigured": e.SiteConfigured})
			}
		}
		m.mu.Unlock()
	}
}
