package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pelletier/go-toml/v2"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const maxFrame = 1 << 20

var buildVersion = "0.4.6"

func inferenceRequestLimit(path string) int64 {
	path = strings.TrimPrefix(path, "/evaluation")
	if path == imageEditPath || path == "/v1/chat/completions" || path == "/v1/audio/transcriptions" || path == "/v1/responses" || path == "/v1/messages" {
		return modelTestRequestLimit
	}
	return maxFrame
}

func readFrame(r io.Reader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	if n == 0 || n > maxFrame {
		return nil, errors.New("invalid frame length")
	}
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}
func writeFrame(w io.Writer, b []byte) error {
	return writeFrameLimit(w, b, maxFrame)
}
func writeFrameLimit(w io.Writer, b []byte, limit int) error {
	if len(b) == 0 || len(b) > limit {
		return errors.New("invalid frame length")
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}
func hash(b []byte) string { v := sha256.Sum256(b); return hex.EncodeToString(v[:]) }
func randomToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

type preview struct {
	Configuration       *agentExtendedConfiguration `json:"configuration,omitempty"`
	Operation           string                      `json:"operation,omitempty"`
	Capabilities        agentCapabilities           `json:"capabilities"`
	Model               string                      `json:"model"`
	Alias               string                      `json:"alias"`
	Agent               string                      `json:"agent"`
	Digest              string                      `json:"digest"`
	Content             string                      `json:"content"`
	Path                string                      `json:"path"`
	Existed             bool                        `json:"-"`
	SourceDigest        string                      `json:"-"`
	Paths               []string                    `json:"paths,omitempty"`
	piFiles             []piFile
	hanaFiles           []hanaFile
	hanaServiceChange   *hanaServiceChange
	providerAction      string
	hanaIDs             []string
	hanaJournalDigest   string
	apiKey              string
	companions          []managedFile
	providerCatalog     *providerCatalog
	providerRoutes      []agentConnection
	restoreLastProvider bool
}
type journal struct {
	ProviderCatalog   *providerCatalog            `json:"providerCatalog,omitempty"`
	Configuration     *agentExtendedConfiguration `json:"configuration,omitempty"`
	Companions        []managedFile               `json:"companions,omitempty"`
	Connections       []agentConnection           `json:"connections,omitempty"`
	PreviousInstalled string                      `json:"previousInstalled,omitempty"`
	ProxyPort         int                         `json:"proxyPort,omitempty"`
	Model             string                      `json:"model,omitempty"`
	Alias             string                      `json:"alias,omitempty"`
	APIKeyHash        string                      `json:"apiKeyHash,omitempty"`
	Original          []byte                      `json:"original"`
	Existed           bool                        `json:"existed"`
	Installed         string                      `json:"installed"`
	Stage             string                      `json:"stage"`
}
type core struct {
	nativeReferenceCache    map[string]string
	nativeKeyMigrationErr   error
	noaTasks                noaTasks
	compressionMu           sync.RWMutex
	compression             *proxyCompressionStatus
	publicResources         *publicResourceSampler
	publicStateless         bool
	publicInternalToken     string
	publicSVGInference      http.Handler
	publicSVGContext        context.Context
	publicSVGCancel         context.CancelFunc
	publicSVGCore           *core
	publicSVGSlots          chan struct{}
	updateGate              runtimeUpdateGate
	home, gateway, localKey string
	control                 string
	connector               *connector
	server                  *http.Server
	proxyListener           net.Listener
	proxyServerListener     net.Listener
	proxyReserved           bool
	proxyPort               int
	startupReady            bool
	startupError            string
	endpointIP              string
	proxyLAN                bool
	proxySessionMu          sync.RWMutex
	proxySession            string
	proxySessionContext     context.Context
	proxySessionCancel      context.CancelFunc
	historyIdentityMu       sync.Mutex
	historyIdentityHash     [32]byte
	historyIdentityAccount  string
	historyIdentityVersion  uint64
	historyRevokedHash      [32]byte
	history                 *chatHistory
	historyEvents           historyEventHub
	historyMu               sync.Mutex
	hostedMu                sync.Mutex
	hostedActive            map[string]bool
	hostedCancel            context.CancelFunc
	hostedDone              chan struct{}
	onlineSupply            *supplyPolicy
	modelTestServer         *http.Server
	modelTestURL            string
	modelTestToken          string
	modelTestContext        context.Context
	modelTestCancel         context.CancelFunc
	svgMu                   sync.Mutex
	svgWorkers              sync.WaitGroup
	svgStores               map[string]*svgTestStore
	mu                      sync.Mutex
	remoteMu                sync.Mutex
	remoteConfigMu          sync.Mutex
	remote                  *remoteManager
	supplyMu                sync.Mutex
	pending                 *preview
	pendingAgentSelection   *agentSelectionAuthorization
	modelAliasMu            sync.RWMutex
	modelAliases            map[string]agentModelRoute
	gatewaySelector         *gatewaySelector
}

type agentModelRoute struct {
	Model string
	Keys  map[string]bool
}

func newCore(home, gateway string) *core {
	control := os.Getenv("NEWNOA_CONTROL_URL")
	if control == "" {
		control = "https://api.example.invalid"
	}
	c := &core{home: home, gateway: gateway, localKey: randomToken(), control: control, proxyLAN: true}
	c.gatewaySelector = newGatewaySelector(control)
	c.loadProxyEndpoint()
	c.loadCompression()
	if dockerRuntime() {
		c.proxyLAN = true
	}
	if !dockerRuntime() {
		c.nativeKeyMigrationErr = c.migrateAgentKeys()
		c.loadPersistedAliases()
	}
	return c
}

func (c *core) loadPersistedAliases() {
	// Hana may start after Core. Live-service journals contain only previously
	// confirmed key-bound aliases; Java still authorizes the actual request.
	if j, err := c.readHanaJournal(); err == nil && j != nil && j.Stage == "applied" && (j.Service || c.checkHanaFiles(j, false) == nil) {
		c.modelAliases = map[string]agentModelRoute{}
		for _, connection := range journalConnections(*j.providerJournal()) {
			index := "hana-agent\n" + connection.Alias
			route := c.modelAliases[index]
			if route.Keys == nil {
				route.Keys = map[string]bool{}
			}
			route.Model = connection.Model
			route.Keys[connection.APIKeyHash] = true
			c.modelAliases[index] = route
		}
	}
	for _, agent := range []string{"codex", "claude-cli", "claude-desktop", "opencode", "openclaw", "hermes-agent"} {
		config, path, err := c.agentPaths(agent)
		if err != nil {
			continue
		}
		if providerListAgent(agent) && (c.safeConfigFile(path) != nil || c.safeConfigFile(config) != nil) {
			continue
		}
		if agent == "opencode" && c.checkOpenCodeLocation() != nil {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var j journal
		if json.Unmarshal(data, &j) == nil && j.Stage == "applied" {
			if j.ProviderCatalog != nil {
				raw, err := os.ReadFile(config)
				if err != nil || hash(raw) != j.Installed {
					continue
				}
			}
			for _, connection := range journalConnections(j) {
				if c.modelAliases == nil {
					c.modelAliases = map[string]agentModelRoute{}
				}
				key := agent + "\n" + connection.Alias
				route := c.modelAliases[key]
				if route.Keys == nil {
					route.Keys = map[string]bool{}
				}
				route.Model = connection.Model
				route.Keys[connection.APIKeyHash] = true
				c.modelAliases[key] = route
			}
		}
	}
	if j, err := c.readPiJournal(); err == nil && j != nil && j.Stage == "applied" {
		if c.modelAliases == nil {
			c.modelAliases = map[string]agentModelRoute{}
		}
		if j.ProviderCatalog != nil {
			if _, _, err = c.remotePiSelection(); err != nil {
				return
			}
		}
		for _, entry := range piJournalConnections(*j) {
			key := "pi-agent\n" + entry.Alias
			route := c.modelAliases[key]
			if route.Keys == nil {
				route.Keys = map[string]bool{}
			}
			route.Model = entry.Model
			route.Keys[entry.APIKeyHash] = true
			c.modelAliases[key] = route
		}
	}
}
func (c *core) configPath() string  { return filepath.Join(c.home, ".codex", "config.toml") }
func (c *core) journalPath() string { return filepath.Join(c.home, ".newnoa", "config-journal.json") }
func (c *core) checkCodexLocation() error {
	if home := os.Getenv("CODEX_HOME"); home != "" && filepath.Clean(home) != filepath.Join(c.home, ".codex") {
		return errors.New("custom Codex configuration paths require manual configuration")
	}
	return nil
}
func (c *core) safeConfigFile(path string) error {
	// shortcut: parent checks cannot stop same-user replacement races; use anchored I/O for hostile local processes.
	for parent := filepath.Dir(path); ; parent = filepath.Dir(parent) {
		info, err := os.Lstat(parent)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return errors.New("symlinks and non-directory configuration parents are not supported")
		}
		if parent == filepath.Clean(c.home) {
			break
		}
		if parent == filepath.Dir(parent) {
			return errors.New("configuration path is outside home")
		}
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("symlinks and non-regular files are not supported")
	}
	limit := int64(maxFrame)
	if path == filepath.Join(c.home, ".codex", "newnoa-models.json") {
		limit = codexCatalogMaxBytes
	}
	if strings.HasPrefix(filepath.Base(path), "chat-history.sqlite") && (filepath.Dir(path) == filepath.Join(c.home, ".newnoa") || filepath.Dir(path) == defaultHistoryDatabase(c.home).SQLiteDirectory) {
		limit = 1<<63 - 1
	}
	if path == filepath.Join(c.home, ".newnoa", "pi-agent-journal.json") {
		limit *= 4
	}
	if path == filepath.Join(c.home, ".newnoa", "hana-agent-journal.json") {
		limit *= 6
	}
	if path == filepath.Join(c.home, ".newnoa", "claude-desktop-journal.json") {
		limit *= 8
	}
	if path == c.journalPath() || path == filepath.Join(c.home, ".newnoa", "claude-cli-journal.json") || path == filepath.Join(c.home, ".newnoa", "opencode-journal.json") || path == filepath.Join(c.home, ".newnoa", "openclaw-journal.json") || path == filepath.Join(c.home, ".newnoa", "hermes-agent-journal.json") {
		limit *= 2
	}
	if path == c.journalPath() {
		limit = codexJournalMaxBytes
	}
	if info.Size() > limit {
		return errors.New("configuration too large")
	}
	return nil
}
func (c *core) preview(agent, model string) (preview, error) {
	return c.previewWithAPIKey(agent, model, c.localKey)
}
func (c *core) previewWithAPIKey(agent, model, apiKey string, aliases ...string) (preview, error) {
	apiKey = managedAgentKey(apiKey)
	if !validAPIKey(apiKey) {
		return preview{}, errors.New("valid NEWNOA API key required")
	}
	alias := model
	if len(aliases) > 0 && aliases[0] != "" {
		alias = aliases[0]
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_.:/-]{1,100}$`).MatchString(alias) {
		return preview{}, errors.New("invalid Agent model alias")
	}
	if agent == "hermes-agent" {
		return c.previewHermes(model, apiKey, alias)
	}
	if agent == "hana-agent" {
		return c.previewHana(model, apiKey, alias)
	}
	if agent == "pi-agent" {
		return c.previewPi(model, apiKey, alias)
	}
	if agent == "openclaw" {
		return c.previewOpenClaw(model, apiKey, alias)
	}
	if agent == "opencode" {
		return c.previewOpenCode(model, apiKey, alias)
	}
	if agent == "claude-cli" {
		return c.previewClaude(model, apiKey, alias)
	}
	if agent == "claude-desktop" {
		return c.previewClaudeDesktop(model, apiKey, alias)
	}
	if agent != "codex" {
		return preview{}, errors.New("only Codex Responses configuration is supported")
	}
	if err := c.checkCodexLocation(); err != nil {
		return preview{}, err
	}
	p := c.configPath()
	if err := c.safeConfigFile(p); err != nil {
		return preview{}, err
	}
	raw, err := os.ReadFile(p)
	existed := err == nil
	if err != nil && !os.IsNotExist(err) {
		return preview{}, err
	}
	var parsed map[string]any
	if err = toml.Unmarshal(raw, &parsed); err != nil {
		return preview{}, errors.New("existing TOML is invalid; no changes made")
	}
	if providers, ok := parsed["model_providers"].(map[string]any); ok {
		if _, exists := providers["newnoa"]; exists {
			managed, err := c.managedConnectionJournal(agent, raw, existed)
			if err != nil {
				return preview{}, err
			}
			if managed == nil {
				return preview{}, errors.New("existing newnoa provider must be restored or removed before applying")
			}
		}
	}
	if _, err := c.managedConnectionJournal(agent, raw, existed); err != nil {
		return preview{}, err
	}
	lines := strings.Split(string(raw), "\n")
	root := true
	managedProvider := false
	out := []string{}
	re := regexp.MustCompile(`^\s*(model|model_provider|cli_auth_credentials_store)\s*=\s*"([^"\\]|\\.)*"\s*(#.*)?$`)
	managedRoot := regexp.MustCompile(`^(model|model_provider|cli_auth_credentials_store)\s*=`)
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "[") {
			root = false
			managedProvider = trim == "[model_providers.newnoa]" || strings.HasPrefix(trim, "[model_providers.newnoa.")
		}
		if managedProvider {
			continue
		}
		if root && managedRoot.MatchString(trim) {
			if !re.MatchString(line) {
				return preview{}, errors.New("complex root model settings require manual editing")
			}
			continue
		}
		out = append(out, line)
	}
	content := fmt.Sprintf("model = %q\nmodel_provider = \"newnoa\"\ncli_auth_credentials_store = \"file\"\n", alias) + strings.Join(out, "\n") + fmt.Sprintf("\n[model_providers.newnoa]\nname = \"NEWNOA\"\nbase_url = %q\nwire_api = \"responses\"\nrequires_openai_auth = true\n", c.proxyBaseURL()+"/v1")
	var check map[string]any
	if toml.Unmarshal([]byte(content), &check) != nil {
		return preview{}, errors.New("configuration cannot be updated losslessly")
	}
	if len(content) > maxFrame {
		return preview{}, errors.New("configuration too large")
	}
	pr := preview{Model: model, Alias: alias, Agent: agent, Digest: randomToken(), SourceDigest: hash(raw), Content: content, Path: p, Existed: existed, apiKey: apiKey}
	auth, err := c.prepareCodexAuth(apiKey)
	if err != nil {
		return preview{}, err
	}
	pr.companions = []managedFile{auth}
	pr, err = c.preserveCodexCatalog(pr)
	if err != nil {
		return preview{}, err
	}
	pr.Paths = []string{p, auth.Path}
	c.pending = &pr
	return pr, nil
}
func atomicFile(path string, data []byte) error {
	if info, err := os.Lstat(path); err == nil && info.Mode() == 0600 {
		if current, err := os.ReadFile(path); err == nil && bytes.Equal(current, data) {
			return nil
		}
	}
	return writeAtomicFile(path, data, nil)
}

func writeAtomicFile(path string, data []byte, createTemp func(string) (*os.File, error)) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	if createTemp == nil {
		createTemp = func(dir string) (*os.File, error) { return os.CreateTemp(dir, ".newnoa-*") }
	}
	f, err := createTemp(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return publishAtomicFile(f.Name(), path)
}
func (c *core) apply(digest, content string) error {
	if c.pending == nil || c.pending.Digest != digest || c.pending.Content != content {
		return errors.New("preview is missing or changed")
	}
	if c.pending.Agent == "pi-agent" {
		return c.applyPi()
	}
	if c.pending.Agent == "hana-agent" {
		return c.applyHana()
	}
	if c.pending.Agent == "codex" {
		if err := c.checkCodexLocation(); err != nil {
			return err
		}
	}
	if c.pending.Agent == "claude-cli" {
		if err := c.checkClaudeLocation(); err != nil {
			return err
		}
	}
	if c.pending.Agent == "opencode" {
		if err := c.checkOpenCodeLocation(); err != nil {
			return err
		}
	}
	if c.pending.Agent == "hermes-agent" {
		if err := c.checkHermesLocation(); err != nil {
			return err
		}
	}
	if c.pending.Agent == "openclaw" {
		if err := c.checkOpenClawLocation(); err != nil {
			return err
		}
	}
	if c.pending.Agent == "claude-desktop" {
		if err := c.checkDesktopLocation(); err != nil {
			return err
		}
	}
	configPath, journalPath, err := c.agentPaths(c.pending.Agent)
	if err != nil {
		return err
	}
	if err := c.safeConfigFile(configPath); err != nil {
		return err
	}
	if err := c.safeConfigFile(journalPath); err != nil {
		return err
	}
	raw, err := os.ReadFile(configPath)
	existed := err == nil
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if hash(raw) != c.pending.SourceDigest || existed != c.pending.Existed {
		return errors.New("configuration changed since preview; preview again")
	}
	previous, err := c.managedConnectionJournal(c.pending.Agent, raw, existed)
	if err != nil {
		return err
	}
	if c.pending.providerCatalog != nil && previous != nil && previous.ProviderCatalog != nil && previous.ProviderCatalog.Owner != c.pending.providerCatalog.Owner {
		return errors.New("Managed providers belong to another account")
	}
	if c.pending.restoreLastProvider {
		if previous == nil || previous.ProviderCatalog == nil {
			return errors.New("Provider configuration changed since preview; preview again")
		}
		previous.Stage = "prepared"
		data, err := json.Marshal(previous)
		if err != nil {
			return err
		}
		if err := atomicFile(journalPath, data); err != nil {
			return err
		}
		if err := c.restoreAgent(c.pending.Agent); err != nil {
			return err
		}
		c.pending = nil
		return nil
	}
	if err := c.checkManagedFiles(c.pending.companions, false); err != nil {
		return err
	}
	j := journal{ProxyPort: c.proxyPortValue(), Model: c.pending.Model, Alias: c.pending.Alias, APIKeyHash: managedAgentKeyHash(c.pending.apiKey), Original: raw, Existed: existed, Installed: hash([]byte(content)), Stage: "prepared"}
	j.Configuration = c.pending.Configuration
	j.Companions = append([]managedFile(nil), c.pending.companions...)
	if previous != nil {
		j.Original, j.Existed, j.PreviousInstalled = previous.Original, previous.Existed, previous.Installed
		for i := range j.Companions {
			for _, old := range previous.Companions {
				if old.Path == j.Companions[i].Path {
					j.Companions[i].Original, j.Companions[i].Existed, j.Companions[i].PreviousInstalled = old.Original, old.Existed, old.Installed
				}
			}
		}
		if c.pending.Agent == "opencode" || c.pending.Agent == "hermes-agent" || c.pending.Agent == "openclaw" && previous.APIKeyHash == j.APIKeyHash {
			j.Connections = journalConnections(*previous)
		}
	}
	if c.pending.Agent == "opencode" || c.pending.Agent == "hermes-agent" || c.pending.Agent == "openclaw" {
		entry := agentConnection{Model: j.Model, Alias: j.Alias, APIKeyHash: j.APIKeyHash}
		found := false
		for _, existing := range j.Connections {
			if existing == entry {
				found = true
			}
		}
		if !found {
			j.Connections = append(j.Connections, entry)
		}
	}
	if c.pending.Agent == "opencode" && previous != nil {
		j.Model, j.Alias, j.APIKeyHash = previous.Model, previous.Alias, previous.APIKeyHash
	}
	if c.pending.providerCatalog != nil {
		j.ProviderCatalog = c.pending.providerCatalog
		j.Model, j.Alias, j.APIKeyHash = c.pending.Model, c.pending.Alias, managedAgentKeyHash(c.pending.apiKey)
		j.Connections = c.pending.providerRoutes
	}
	data, _ := json.Marshal(j)
	if c.pending.Agent == "codex" && len(data) > codexJournalMaxBytes {
		return errors.New("Codex recovery journal is too large; reduce the model catalog before applying")
	}
	if err = atomicFile(journalPath, data); err != nil {
		return err
	}
	if err = atomicFile(configPath, []byte(content)); err != nil {
		return err
	}
	for _, file := range c.pending.companions {
		if err = atomicFile(file.Path, file.Content); err != nil {
			return err
		}
	}
	j.Stage = "applied"
	j.PreviousInstalled = ""
	for i := range j.Companions {
		j.Companions[i].PreviousInstalled = ""
	}
	data, _ = json.Marshal(j)
	if err = atomicFile(journalPath, data); err != nil {
		return err
	}
	c.pending = nil
	return nil
}
func (c *core) restoreOnStartup(wsl bool) error {
	if !wsl {
		return c.restore()
	}
	var result error
	for _, agent := range []string{"codex", "claude-cli", "opencode", "openclaw", "pi-agent", "hermes-agent"} {
		j, err := c.readProviderJournal(agent)
		if err != nil {
			result = errors.Join(result, err)
		} else if j != nil && j.Stage != "applied" {
			result = errors.Join(result, c.restoreAgent(agent))
		}
	}
	return result
}

func (c *core) restore() error {
	if dockerRuntime() {
		return nil
	}
	return errors.Join(c.restoreAgent("codex"), c.restoreAgent("claude-cli"), c.restoreAgent("claude-desktop"), c.recoverProviderCatalog("opencode"), c.recoverProviderCatalog("openclaw"), c.recoverProviderCatalog("pi-agent"), c.recoverProviderCatalog("hermes-agent"), c.recoverProviderCatalog("hana-agent"))
}
func (c *core) restoreAgent(agent string) error {
	if agent == "hana-agent" {
		return c.restoreHana()
	}
	if agent == "pi-agent" {
		return c.restorePi()
	}
	configPath, journalPath, err := c.agentPaths(agent)
	if err != nil {
		return err
	}
	if err := c.safeConfigFile(journalPath); err != nil {
		return err
	}
	data, err := os.ReadFile(journalPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var j journal
	if json.Unmarshal(data, &j) != nil {
		return errors.New("invalid recovery journal")
	}
	if err = c.validateCompanionPaths(agent, j.Companions); err != nil {
		return err
	}
	if err = c.checkManagedFiles(j.Companions, true); err != nil {
		return err
	}
	if err = c.safeConfigFile(configPath); err != nil {
		return err
	}
	raw, err := os.ReadFile(configPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	exists := err == nil
	if exists == j.Existed && hash(raw) == hash(j.Original) && (j.Stage == "prepared" || len(j.Companions) > 0) {
		if err = c.restoreManagedFiles(j.Companions); err != nil {
			return err
		}
		return os.Remove(journalPath)
	}
	if !exists || (hash(raw) != j.Installed && !(j.Stage == "prepared" && j.PreviousInstalled != "" && hash(raw) == j.PreviousInstalled)) {
		return errors.New("configuration modified outside NEWNOA; recovery requires manual review")
	}
	if j.Existed {
		err = atomicFile(configPath, j.Original)
	} else {
		err = os.Remove(configPath)
	}
	if err != nil {
		return err
	}
	if err = c.restoreManagedFiles(j.Companions); err != nil {
		return err
	}
	return os.Remove(journalPath)
}
func (c *core) close() {
	if c.publicSVGCore != nil {
		c.publicSVGCore.cancelSVGTests()
		if c.publicSVGCore.publicSVGCancel != nil {
			c.publicSVGCore.publicSVGCancel()
		}
		c.publicSVGCore.svgWorkers.Wait()
		c.publicSVGCore = nil
	}
	if c.publicSVGCancel != nil {
		c.publicSVGCancel()
	}
	c.cancelSVGTests()
	if c.modelTestCancel != nil {
		c.modelTestCancel()
		c.modelTestContext, c.modelTestCancel = nil, nil
	}
	if c.server != nil {
		c.server.Close()
		c.server = nil
		c.historyEvents.publishService()
	}
	if c.proxyServerListener != nil {
		c.proxyServerListener.Close()
		c.proxyServerListener = nil
	}
	if !c.proxyReserved {
		c.proxyListener = nil
	}
	gatewayGRPCConnections.closeIdle()
}

// resolveManagedModel maps an Agent-facing alias back to its authorized deployment ID.
// It only rewrites a request when the selected API key was used for that alias.
func (c *core) resolveManagedModel(r *http.Request, authorization string) error {
	if r.Method != http.MethodPost || (!strings.HasPrefix(authorization, "Bearer nref_") && r.URL.Path != "/v1/chat/completions" && r.URL.Path != "/v1/responses" && r.URL.Path != "/v1/messages" && r.URL.Path != "/v1/audio/transcriptions" && !isImagePath(r.URL.Path)) {
		return nil
	}
	raw, err := readInferenceBody(r)
	if err != nil {
		return err
	}
	var body map[string]json.RawMessage
	var requested string
	asr := r.URL.Path == "/v1/audio/transcriptions"
	if asr {
		requested, err = transcriptionModel(raw, r.Header.Get("Content-Type"))
	} else if json.Unmarshal(raw, &body) == nil {
		if diagnostic, ok := r.Context().Value(inferenceDiagnosticContext{}).(*inferenceDiagnostic); ok && r.URL.Path == "/v1/responses" {
			var session string
			if json.Unmarshal(body["prompt_cache_key"], &session) == nil && nodePattern.MatchString(session) {
				diagnostic.sessionID = session
			}
		}
		err = json.Unmarshal(body["model"], &requested)
	}
	if err != nil || requested == "" {
		if strings.HasPrefix(authorization, "Bearer nref_") {
			return errors.New("Native configuration requires a managed model")
		}
		return nil
	}
	originalRequested := requested
	requested = strings.TrimPrefix(requested, "newnoa/")
	key := managedAgentKeyHash(strings.TrimPrefix(authorization, "Bearer "))
	c.modelAliasMu.RLock()
	resolved := ""
	for aliasKey, route := range c.modelAliases {
		if strings.HasSuffix(aliasKey, "\n"+requested) && route.Keys[key] {
			resolved = route.Model
			break
		}
	}
	c.modelAliasMu.RUnlock()
	if resolved == "" && strings.HasPrefix(authorization, "Bearer nref_") {
		return errors.New("Model is outside this native configuration")
	}
	if resolved == "" && requested == originalRequested {
		return nil
	}
	if resolved == "" {
		resolved = requested
	}
	var updated []byte
	if asr {
		var contentType string
		updated, contentType, err = replaceTranscriptionModel(raw, r.Header.Get("Content-Type"), resolved)
		if err == nil {
			r.Header.Set("Content-Type", contentType)
		}
	} else {
		body["model"], _ = json.Marshal(resolved)
		updated, err = json.Marshal(body)
	}
	if err != nil {
		return err
	}
	setInferenceBody(r, updated)
	return nil
}
func (c *core) startProxy(secret string) error { return c.startProxyAt(secret, c.proxyPortValue()) }
func (c *core) startProxyAt(secret string, port int) error {
	return c.startProxyOn(secret, port, c.proxyLAN)
}
func (c *core) startProxyOn(secret string, port int, allowLAN bool) error {
	allowLAN = dockerRuntime() || allowLAN
	if port < 1024 || port > 65535 {
		return errors.New("proxy port must be an integer from 1024 to 65535")
	}
	if secret != "" && !validAPIKey(secret) {
		return errors.New("valid NEWNOA key required")
	}
	if c.server != nil {
		return errors.New("proxy already running; stop before changing key")
	}
	if err := c.canChangeProxyPort(port); err != nil {
		return err
	}
	u, err := url.Parse(c.gateway)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost"))) {
		return errors.New("gateway must use HTTPS or loopback HTTP")
	}
	inference, err := privateInferenceWithSelector(c.control, c.gateway, c.gatewaySelector)
	if err != nil {
		return err
	}
	var public http.Handler
	if serviceSecret := os.Getenv("NEWNOA_PUBLIC_PROXY_SECRET"); serviceSecret != "" {
		publicControl := os.Getenv("NEWNOA_PUBLIC_CONTROL_URL")
		if publicControl == "" {
			publicControl = c.control
		}
		shared := &core{home: c.home, control: publicControl, gateway: c.gateway, gatewaySelector: c.gatewaySelector}
		public, err = shared.publicProxyHandler(serviceSecret, os.Getenv("NEWNOA_INTERNAL_TOKEN"))
		if err != nil {
			return err
		}
		c.publicSVGCore = shared
	}
	_, _ = c.historyStore()
	forward := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		out := r.Clone(r.Context())
		c.attachProxySession(out)
		c.hostedInference(inference).ServeHTTP(w, out)
	})
	listener := c.proxyListener
	reserved := listener != nil
	if listener != nil && (listener.Addr().(*net.TCPAddr).Port != port || listener.Addr().(*net.TCPAddr).IP.IsUnspecified() != allowLAN) {
		listener.Close()
		listener, err = reserveProxyListener(port, allowLAN)
		if err != nil {
			c.proxyListener = nil
			c.proxyReserved = false
			return err
		}
		c.proxyListener = listener
	}
	if listener == nil {
		listener, err = reserveProxyListener(port, allowLAN)
		if err != nil {
			return err
		}
	}
	c.proxyReserved = reserved
	serverListener := listener
	if reserved {
		serverListener, err = serveReservedProxyListener(listener.(*net.TCPListener))
		if err != nil {
			return fmt.Errorf("create proxy listener: %w", err)
		}
	}
	var lanTLS *tls.Config
	if allowLAN {
		cert, err := c.lanCertificate(port)
		if err != nil {
			serverListener.Close()
			return fmt.Errorf("prepare LAN TLS certificate: %w", err)
		}
		lanTLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	}
	if port != c.proxyPortValue() {
		c.pending = nil
	}
	c.proxyPort = port
	c.proxyLAN = allowLAN
	c.server = &http.Server{TLSConfig: lanTLS, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Minute, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384, Handler: instrumentCoreHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, forwarded := r.Header[http.CanonicalHeaderKey("X-Newnoa-Public-Proxy")]; forwarded {
			if public == nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			public.ServeHTTP(w, r)
			return
		}
		if r.Header.Get("Origin") != "" {
			http.Error(w, "Untrusted proxy origin", http.StatusForbidden)
			return
		}
		target, hostErr := url.Parse("//" + r.Host)
		trustedHost := false
		if hostErr == nil && target.Host == r.Host && target.User == nil {
			ip := net.ParseIP(target.Hostname())
			trustedHost = target.Hostname() == "localhost" || ip != nil && (ip.IsLoopback() || allowLAN && ip.IsPrivate())
		}
		if !trustedHost {
			http.Error(w, "Untrusted proxy host", http.StatusForbidden)
			return
		}
		w.Header().Set("X-Newnoa-Proxy", hash([]byte(c.localKey)))
		auth, credentialErr := proxyAuthorization(r.Header)
		if credentialErr != nil {
			http.Error(w, "Conflicting credentials", http.StatusUnauthorized)
			return
		}
		if allowLAN && auth == "" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if secret != "" && (auth == "" || auth == "Bearer "+c.localKey) {
			auth = "Bearer " + secret
		}
		if !allowLAN {
			host, _, _ := net.SplitHostPort(r.RemoteAddr)
			if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
				http.Error(w, "Unauthorized", 401)
				return
			}
		}
		if !((r.Method == "GET" && r.URL.Path == "/v1/models") || (r.Method == "POST" && historyInferencePath(r.URL.Path))) {
			http.NotFound(w, r)
			return
		}
		modelAuthorization := auth
		if key, ok := strings.CutPrefix(auth, "Bearer "); ok && strings.HasPrefix(key, "rla_") {
			ip, _, _ := net.SplitHostPort(r.RemoteAddr)
			if !net.ParseIP(ip).IsLoopback() {
				http.Error(w, "Remote agent requires loopback", http.StatusUnauthorized)
				return
			}
			upstream, matched, err := c.remoteAgentAuthorization(r.Context(), key)
			if !matched || err != nil {
				http.Error(w, "Remote agent credential unavailable", http.StatusUnauthorized)
				return
			}
			auth = "Bearer " + upstream
		}
		if r.Method == http.MethodPost {
			key, bearer := strings.CutPrefix(auth, "Bearer ")
			if !bearer || key == "" || len(key) > 256 || strings.ContainsAny(key, " \t\r\n") {
				http.Error(w, "API key required", http.StatusUnauthorized)
				return
			}
			release := reserveInferenceUpload(w, r, r.RemoteAddr)
			if release == nil {
				return
			}
			defer release()
		} else {
			r.Body = http.MaxBytesReader(w, r.Body, inferenceRequestLimit(r.URL.Path))
		}
		if err := c.resolveManagedModel(r, modelAuthorization); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			return
		}
		r.Header.Del("X-Api-Key")
		r.Header.Del("api-key")
		if ref, local := strings.CutPrefix(auth, "Bearer nref_"); local {
			upstream, err := c.localReferenceAuthorization(r.Context(), r.RemoteAddr, "nref_"+ref)
			if err != nil {
				http.Error(w, "Native configuration authorization failed", http.StatusUnauthorized)
				return
			}
			auth = "Bearer " + upstream
		}
		r.Header.Set("Authorization", auth)
		c.localInference(forward).ServeHTTP(w, r)
	}))}
	server := c.server
	c.proxyServerListener = serverListener
	if !reserved {
		c.proxyListener = serverListener
	}
	if lanTLS != nil {
		go server.Serve(lanTLSListener{serverListener, lanTLS})
	} else {
		go server.Serve(serverListener)
	}
	c.historyEvents.publishService()

	return nil
}
func (c *core) handle(method string, raw json.RawMessage) (result any, retErr error) {
	if method == "update.prepare" {
		if err := c.updateGate.prepare(); err != nil {
			return nil, err
		}
		return map[string]any{"ready": true}, nil
	}
	if method == "update.cancel" {
		c.updateGate.cancel()
		return map[string]any{"ready": false}, nil
	}
	if !c.updateGate.begin() {
		return nil, errors.New("Client update installation is starting")
	}
	defer c.updateGate.end()
	if method == "noa.start" || method == "noa.poll" || method == "noa.cancel" {
		return c.noaTaskCommand(method, raw)
	}
	if method == "proxy.compression.stats" {
		return c.compressionStatsCommand(raw)
	}
	if strings.HasPrefix(method, "remote.") || strings.HasPrefix(method, "agent.remote.") {
		return c.remoteRPC(method, raw)
	}
	if strings.HasPrefix(method, "svg.tests.") {
		return c.svgCommand(method, raw)
	}
	if dockerRuntime() && strings.HasPrefix(method, "agent.") {
		return nil, errors.New("Agent operations are unavailable in Docker")
	}
	var p struct {
		Secret          string                      `json:"secret"`
		NodeID          string                      `json:"nodeId"`
		Credential      string                      `json:"credential"`
		OwnerID         string                      `json:"ownerId"`
		Purpose         string                      `json:"purpose"`
		Agent           string                      `json:"agent"`
		Model           string                      `json:"model"`
		Alias           string                      `json:"alias"`
		ReasoningEffort string                      `json:"reasoningEffort"`
		Endpoint        string                      `json:"endpoint"`
		Method          string                      `json:"deploymentMethod"`
		SourceType      string                      `json:"sourceType"`
		APIKey          string                      `json:"apiKey"`
		ProviderAction  string                      `json:"providerAction"`
		ProviderModels  []providerModel             `json:"providerModels"`
		KeyID           string                      `json:"keyId"`
		Configuration   *agentExtendedConfiguration `json:"configuration"`
		Session         string                      `json:"session"`
		Digest          string                      `json:"digest"`
		Nonce           string                      `json:"nonce"`
		AllowLAN        *bool                       `json:"allowLAN"`
		EndpointIP      *string                     `json:"endpointIP"`
	}
	if len(raw) > 0 && json.Unmarshal(raw, &p) != nil {
		return nil, errors.New("invalid params")
	}
	if method == "model.assess" {
		policy, err := c.supplyPolicy(p.Session)
		if err != nil {
			return nil, err
		}
		if len(p.APIKey) > 4096 {
			return nil, errors.New("invalid upstream API key")
		}
		return assessSupplyEndpoint(policy, p.Endpoint, p.APIKey)
	}
	if method == "model.list" {
		if p.SourceType == "online" {
			return listModelsWithPolicy(p.Endpoint, p.APIKey, supplyPolicy{edition: "online"})
		}
		policy, err := c.supplyPolicy(p.Session)
		if err != nil {
			return nil, err
		}
		return listModelsWithPolicy(p.Endpoint, p.APIKey, policy)
	}
	if method == "model.service.check" {
		policy, err := c.supplyPolicy(p.Session)
		if err != nil {
			return nil, err
		}
		return map[string]any{"available": modelServiceAvailable(p.Endpoint, policy)}, nil
	}
	if method == "connector.ensure" {
		return c.ensureLocalSupply(p.Session)
	}
	if method == "agent.preview" || method == "agent.apply" || method == "agent.restore" {
		c.remoteConfigMu.Lock()
		defer c.remoteConfigMu.Unlock()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if method == "agent.preview" || method == "agent.apply" || method == "agent.restore" {
		agent, action := p.Agent, p.ProviderAction
		if method == "agent.apply" && c.pending != nil {
			agent, action = c.pending.Agent, c.pending.providerAction
		}
		defer func() {
			if retErr != nil {
				retErr = &agentConfigError{error: retErr, agent: agent, action: action}
			}
		}()
		c.remoteMu.Lock()
		m := c.remote
		c.remoteMu.Unlock()
		if m != nil {
			m.mu.Lock()
			busy := m.providerBusyLocked(agent)
			m.mu.Unlock()
			if busy {
				return nil, errors.New("This executor has an active hosted session")
			}
		}
	}
	switch method {
	case "proxy.session":
		c.setProxySession(p.Session)
		return map[string]bool{"configured": p.Session != ""}, nil
	case "startup.status", "startup.bind":
		return c.startupCommand(method, raw)
	case "model.test.endpoint":
		if len(raw) > 0 && string(raw) != "{}" {
			return nil, errors.New("model test endpoint accepts no parameters")
		}
		return c.modelTestEndpoint()
	case "history.events.ticket":
		return c.historyEventTicket(raw, "")
	case "history.database.status", "history.database.test", "history.database.configure", "history.query", "history.filters", "history.accounts", "history.sync.status", "history.export", "history.export.status", "history.export.cancel", "history.export.directory", "history.export.configure", "history.settings", "history.configure", "history.list", "history.conversation", "history.get", "history.content", "history.delete", "history.sync":
		return c.historyCommand(method, raw)
	case "handshake":
		capabilities := []string{"proxy.start", "proxy.stop", "proxy.status", "proxy.diagnose", "proxy.compression.get", "proxy.compression.set", "proxy.compression.stats", "agent.detect", "agent.providers", "agent.version", "agent.preview", "agent.apply", "agent.restore", "connector.ensure", "connector.start", "connector.stop", "connector.status", "deployment.context_probe", "model.list", "model.service.check"}
		if dockerRuntime() || c.publicStateless {
			capabilities = slices.DeleteFunc(capabilities, func(method string) bool {
				return strings.HasPrefix(method, "agent.") || strings.HasPrefix(method, "remote.")
			})
		}
		return map[string]any{"version": 1, "build": buildVersion, "nonce": p.Nonce, "capabilities": capabilities}, nil
	case "connector.status":
		if c.connector == nil {
			return map[string]any{"state": "stopped"}, nil
		}
		return c.connector.status(), nil
	case "connector.start":
		if err := c.startConnector(p.NodeID, p.Credential, p.OwnerID, p.Purpose); err != nil {
			return nil, err
		}
		return c.connector.status(), nil
	case "connector.stop":
		if c.connector != nil {
			c.connector.stop()
		}
		return map[string]any{"state": "stopping"}, nil
	case "proxy.diagnose":
		if len(raw) > 0 && string(raw) != "{}" {
			return nil, errors.New("diagnostics accepts no parameters")
		}
		return c.diagnoseProxy(), nil
	case "proxy.endpoint":
		var settings struct {
			IP       *string `json:"ip"`
			TestOnly bool    `json:"testOnly"`
		}
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&settings) != nil || decoder.Decode(new(any)) != io.EOF || settings.IP == nil {
			return nil, errors.New("invalid endpoint settings")
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(raw, &fields)
		if testOnly, ok := fields["testOnly"]; ok && string(testOnly) == "null" {
			return nil, errors.New("invalid endpoint settings")
		}
		return c.configureProxyEndpoint(*settings.IP, settings.TestOnly)
	case "proxy.compression.get":
		return c.compressionStatus(), nil
	case "proxy.compression.set":
		return c.configureCompression(raw)
	case "proxy.status":
		return map[string]any{"compression": c.compressionStatus(), "running": c.server != nil, "address": c.proxyListenAddress(), "baseUrls": c.proxyEndpoints(), "lanTLS": true, "port": c.proxyPortValue(), "allowLAN": c.proxyLAN, "endpointIP": c.endpointIP, "runtimeMode": runtimeMode()}, nil
	case "proxy.start":
		secret, port, err := c.proxyStartParams(raw)
		if err != nil {
			return nil, err
		}
		previousEndpoint, previousPort, previousLAN := c.endpointIP, c.proxyPort, c.proxyLAN
		if p.EndpointIP != nil {
			c.endpointIP = *p.EndpointIP
		}
		allowLAN := c.proxyLAN
		if p.AllowLAN != nil {
			allowLAN = *p.AllowLAN
		}
		err = c.startProxyOn(secret, port, allowLAN)
		if err == nil {
			if p.EndpointIP != nil && c.endpointIP != "" {
				_, err = c.configureProxyEndpoint(c.endpointIP, false)
			} else {
				err = c.saveProxyEndpoint(c.endpointIP)
			}
			if err != nil {
				c.close()
			}
		}
		if err != nil {
			c.endpointIP, c.proxyPort, c.proxyLAN = previousEndpoint, previousPort, previousLAN
		}
		return map[string]any{"running": c.server != nil, "address": c.proxyAddress(), "port": c.proxyPortValue()}, err
	case "proxy.stop":
		c.close()
		return map[string]any{"running": false}, nil
	case "agent.detect":
		result, err := c.detectAgent(p.Agent)
		if err == nil && providerListAgent(p.Agent) {
			// The account-bound catalog, not general agent detection, exposes managed selections.
			delete(result, "model")
			delete(result, "configuredModels")
		}
		return result, err
	case "agent.providers":
		return c.managedProviderStatus(p.Agent, p.Session)
	case "agent.version":
		return c.agentVersion(p.Agent), nil
	case "deployment.context_probe":
		policy, err := c.supplyPolicy(p.Session)
		if err != nil {
			return nil, err
		}
		return probeContextWithPolicy(p.Endpoint, p.Method, p.APIKey, p.Model, policy)
	case "agent.preview":
		c.pending = nil
		c.pendingAgentSelection = nil
		if p.ProviderAction != "" {
			if !providerListAgent(p.Agent) || p.Configuration != nil {
				return nil, errors.New("Managed provider list is unavailable for this agent")
			}
			owner, err := c.providerAccount(p.Session)
			if err != nil {
				return nil, err
			}
			selection := &agentSelectionAuthorization{Session: p.Session, Owner: owner, KeyID: p.KeyID, Secret: p.APIKey}
			if p.ProviderAction == "add" || p.ProviderAction == "default" {
				if len(p.ProviderModels) == 0 || len(p.ProviderModels) > 64 {
					return nil, errors.New("Invalid managed model selection")
				}
				for _, model := range p.ProviderModels {
					selection.Models = append(selection.Models, model.ID)
				}
				if err = c.validateAgentSelection(context.Background(), p.Session, p.KeyID, p.APIKey, selection.Models); err != nil {
					return nil, err
				}
			}
			pr, err := c.previewManagedProvider(p.Agent, owner, p.KeyID, p.APIKey, p.ProviderAction, p.ProviderModels)
			if err != nil {
				return nil, err
			}
			c.pendingAgentSelection = selection
			c.pending.providerAction = p.ProviderAction
			pr.Content = ""
			return pr, nil
		}
		if p.Configuration != nil {
			modelIDs := p.Configuration.modelsForAuthorization()
			if err := c.validateAgentSelection(context.Background(), p.Session, p.KeyID, p.APIKey, modelIDs); err != nil {
				c.pending = nil
				return nil, err
			}
			pr, err := c.previewExtendedAgent(p.Agent, p.APIKey, *p.Configuration)
			if err != nil {
				return nil, err
			}
			c.pendingAgentSelection = &agentSelectionAuthorization{Session: p.Session, KeyID: p.KeyID, Secret: p.APIKey, Models: modelIDs}
			pr.Operation, pr.Capabilities = agentOperation(p.Agent)
			pr.Content = ""
			return pr, nil
		}
		if p.Agent == "opencode" {
			if err := c.validateAgentSelection(context.Background(), p.Session, p.KeyID, p.APIKey, []string{p.Model}); err != nil {
				c.pending = nil
				return nil, err
			}
		}
		if err := validateAgentReasoning(p.Agent, p.ReasoningEffort); err != nil {
			c.pending = nil
			return nil, err
		}
		pr, err := c.previewWithAPIKey(p.Agent, p.Model, p.APIKey, p.Alias)
		if err == nil && p.ReasoningEffort != "" {
			pr, err = c.withCodexReasoning(pr, p.ReasoningEffort)
		}
		if err == nil && p.Agent == "opencode" {
			c.pendingAgentSelection = &agentSelectionAuthorization{Session: p.Session, KeyID: p.KeyID, Secret: p.APIKey, Models: []string{p.Model}}
		}
		pr.Operation, pr.Capabilities = agentOperation(p.Agent)
		pr.Content = ""
		return pr, err
	case "agent.apply":
		if c.pendingAgentSelection != nil {
			selection := c.pendingAgentSelection
			if selection.Session != p.Session {
				c.pending = nil
				c.pendingAgentSelection = nil
				return nil, errors.New("Account changed since configuration preview")
			}
			var err error
			if selection.Owner != "" {
				var owner string
				owner, err = c.providerAccount(p.Session)
				if err == nil && owner != selection.Owner {
					err = errors.New("Account changed since configuration preview")
				}
			}
			if err == nil && len(selection.Models) > 0 {
				err = c.validateAgentSelection(context.Background(), p.Session, selection.KeyID, selection.Secret, selection.Models)
			}
			if err != nil {
				c.pending = nil
				c.pendingAgentSelection = nil
				return nil, err
			}
		}
		if c.server == nil && (c.pending == nil || c.pending.providerCatalog == nil) {
			return nil, errors.New("start the local proxy first")
		}
		if c.pending == nil {
			return nil, errors.New("preview required")
		}
		pending := c.pending
		keyHash := managedAgentKeyHash(pending.apiKey)
		connections := []agentConnection{{Model: pending.Model, Alias: pending.Alias, APIKeyHash: keyHash}}
		if pending.providerCatalog != nil {
			connections = pending.providerRoutes
		}
		if pending.Configuration != nil {
			connections = pending.Configuration.connections(keyHash)
		}
		c.modelAliasMu.RLock()
		for existingAlias, route := range c.modelAliases {
			if (pending.providerCatalog != nil || pending.Agent == "codex" || pending.Agent == "claude-cli" || pending.Agent == "claude-desktop") && strings.HasPrefix(existingAlias, pending.Agent+"\n") {
				continue
			}
			if (pending.Agent == "pi-agent" || pending.Agent == "openclaw") && strings.HasPrefix(existingAlias, pending.Agent+"\n") && !route.Keys[keyHash] {
				continue
			}
			for _, connection := range connections {
				if strings.HasSuffix(existingAlias, "\n"+connection.Alias) && route.Model != connection.Model && (route.Keys[connection.APIKeyHash] || existingAlias == pending.Agent+"\n"+connection.Alias) {
					c.modelAliasMu.RUnlock()
					return map[string]any{"applied": false}, errors.New("this Agent model alias already refers to a different deployment; choose another alias")
				}
			}
		}
		c.modelAliasMu.RUnlock()
		if err := c.apply(p.Digest, pending.Content); err != nil {
			return map[string]any{"applied": false}, err
		}
		c.modelAliasMu.Lock()
		if c.modelAliases == nil {
			c.modelAliases = map[string]agentModelRoute{}
		}
		if pending.providerCatalog != nil || pending.Agent == "codex" || pending.Agent == "claude-cli" || pending.Agent == "claude-desktop" || pending.Agent == "pi-agent" || pending.Agent == "openclaw" {
			for key := range c.modelAliases {
				if strings.HasPrefix(key, pending.Agent+"\n") {
					delete(c.modelAliases, key)
				}
			}
		}
		if pending.providerCatalog != nil {
			for _, entry := range pending.providerRoutes {
				index := pending.Agent + "\n" + entry.Alias
				route := c.modelAliases[index]
				if route.Keys == nil {
					route.Keys = map[string]bool{}
				}
				route.Model = entry.Model
				route.Keys[entry.APIKeyHash] = true
				c.modelAliases[index] = route
			}
			c.modelAliasMu.Unlock()
			c.pendingAgentSelection = nil
			return map[string]any{"applied": true}, nil
		}
		if pending.Agent == "pi-agent" || pending.Agent == "openclaw" {
			var connections []agentConnection
			if pending.Agent == "pi-agent" {
				j, err := c.readPiJournal()
				if err != nil || j == nil {
					c.modelAliasMu.Unlock()
					return map[string]any{"applied": true}, errors.New("applied configuration route reload failed")
				}
				connections = piJournalConnections(*j)
			} else {
				_, jpath, _ := c.agentPaths("openclaw")
				raw, err := os.ReadFile(jpath)
				var j journal
				if err != nil || json.Unmarshal(raw, &j) != nil {
					c.modelAliasMu.Unlock()
					return map[string]any{"applied": true}, errors.New("applied configuration route reload failed")
				}
				connections = journalConnections(j)
			}
			for _, entry := range connections {
				c.modelAliases[pending.Agent+"\n"+entry.Alias] = agentModelRoute{Model: entry.Model, Keys: map[string]bool{entry.APIKeyHash: true}}
			}
			c.modelAliasMu.Unlock()
			c.pendingAgentSelection = nil
			return map[string]any{"applied": true}, nil
		}
		if pending.Configuration != nil {
			for _, connection := range pending.Configuration.connections(keyHash) {
				c.modelAliases[pending.Agent+"\n"+connection.Alias] = agentModelRoute{Model: connection.Model, Keys: map[string]bool{keyHash: true}}
			}
			c.modelAliasMu.Unlock()
			c.pendingAgentSelection = nil
			return map[string]any{"applied": true}, nil
		}
		aliasKey := pending.Agent + "\n" + pending.Alias
		route := c.modelAliases[aliasKey]
		route.Model = pending.Model
		if route.Keys == nil {
			route.Keys = map[string]bool{}
		}
		route.Keys[keyHash] = true
		c.modelAliases[aliasKey] = route
		c.modelAliasMu.Unlock()
		return map[string]any{"applied": true}, nil
	case "agent.restore":
		c.pending, c.pendingAgentSelection = nil, nil
		if providerListAgent(p.Agent) {
			j, err := c.readProviderJournal(p.Agent)
			if err != nil {
				return nil, err
			}
			if j != nil && j.ProviderCatalog != nil {
				owner, err := c.providerAccount(p.Session)
				if err != nil {
					return nil, err
				}
				if owner != j.ProviderCatalog.Owner {
					return nil, errors.New("Managed providers belong to another account")
				}
				if j.Stage == "applied" {
					return nil, errors.New("Use the managed provider list to remove connections")
				}
			}
		}
		err := c.restoreAgent(p.Agent)
		if err == nil {
			c.modelAliasMu.Lock()
			for key := range c.modelAliases {
				if strings.HasPrefix(key, p.Agent+"\n") {
					delete(c.modelAliases, key)
				}
			}
			c.modelAliasMu.Unlock()
		}
		return map[string]any{"restored": err == nil}, err
	default:
		return nil, errors.New("method not allowed")
	}
}
func main() {
	if len(os.Args) == 2 && (os.Args[1] == "--noa-agent-check" || os.Args[1] == "--noa-agent-notices") {
		if err := noaAgentCLI(os.Args[1], os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	shutdownTelemetry, telemetryErr := initCoreTelemetry()
	defer shutdownTelemetry()
	if telemetryErr != nil {
		fmt.Fprintln(os.Stderr, "OpenTelemetry disabled: initialization failed")
	}
	if os.Getenv("NEWNOA_PUBLIC_PROXY_ENABLED") == "true" {
		if err := servePublicProxy(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	port := defaultProxyPort
	if configured := os.Getenv("NEWNOA_PROXY_PORT"); configured != "" {
		parsed, parseErr := strconv.Atoi(configured)
		if parseErr != nil || parsed < 1024 || parsed > 65535 {
			fmt.Fprintln(os.Stderr, "Core cannot start: NEWNOA_PROXY_PORT must be an integer from 1024 to 65535")
			os.Exit(1)
		}
		port = parsed
	}
	if address := os.Getenv("NEWNOA_BROWSER_CORE_ADDR"); address != "" {
		control := os.Getenv("NEWNOA_CONTROL_URL")
		if control == "" {
			control = "https://api.example.invalid"
		}
		if err := serveBrowserAPI(address, control, port); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	home, err := os.UserHomeDir()
	if isolated := os.Getenv("NEWNOA_CORE_TEST_HOME"); isolated != "" {
		home = isolated
		_, err = os.Stat(home)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "home unavailable")
		os.Exit(1)
	}
	wsl := runtime.GOOS == "linux" && os.Getenv("NEWNOA_WSL_AGENT_RUNTIME") == "1"
	if wsl {
		lock, lockErr := lockWSLRuntime(home)
		if lockErr != nil {
			fmt.Fprintln(os.Stderr, "WSL Core cannot acquire its account state lock")
			os.Exit(1)
		}
		defer lock.Close()
	}
	gateway := os.Getenv("NEWNOA_GATEWAY_URL")
	if gateway == "" {
		gateway = "https://server.example.invalid"
	}
	c := newCore(home, gateway)
	_ = c.bindStartupPort(port)
	defer c.shutdown()
	if err = c.restoreOnStartup(wsl); err != nil {
		fmt.Fprintln(os.Stderr, "configuration recovery requires review")
	}
	if _, err = c.historyStore(); err != nil {
		fmt.Fprintln(os.Stderr, "Call record maintenance could not start")
	}
	stopSignal := signal.NotifyContext
	signalCtx, signalStop := stopSignal(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer signalStop()
	go func() { <-signalCtx.Done(); os.Stdin.Close() }()
	reader := bufio.NewReader(os.Stdin)
	for {
		frame, err := readFrame(reader)
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid RPC frame")
			return
		}
		var req struct {
			Version  int             `json:"version"`
			ID       string          `json:"id"`
			Method   string          `json:"method"`
			Deadline int64           `json:"deadline_ms"`
			Params   json.RawMessage `json:"params"`
		}
		if json.Unmarshal(frame, &req) != nil || req.Version != 1 || len(req.ID) == 0 || len(req.ID) > 80 {
			return
		}
		var result any
		var callErr error
		if req.Deadline < time.Now().UnixMilli() {
			callErr = errors.New("deadline exceeded")
		} else {
			result, callErr = c.handle(req.Method, req.Params)
		}
		res := map[string]any{"id": req.ID}
		if callErr != nil {
			msg := browserLocalMessage(req.Method, req.Params, callErr)
			res["error"] = map[string]any{"code": "CORE_ERROR", "message": msg.English, "publicMessage": true, "localizedMessages": msg}
		} else {
			res["result"] = result
		}
		out, err := json.Marshal(res)
		if err != nil {
			return
		}
		responseLimit := maxFrame
		if strings.HasPrefix(req.Method, "svg.tests.") {
			responseLimit = 8 * maxFrame
		}
		if err = writeFrameLimit(os.Stdout, out, responseLimit); err != nil {
			return
		}
	}
}
