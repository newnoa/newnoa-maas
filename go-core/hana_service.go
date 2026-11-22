package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"time"
)

var (
	errHanaStart        = errors.New("Open HanaAgent on this computer, then refresh the connection")
	errHanaPrivate      = errors.New("HanaAgent connection settings must be a private regular file")
	errHanaAuth         = errors.New("HanaAgent denied local settings access. Reopen HanaAgent and retry")
	errHanaProtocol     = errors.New("HanaAgent could not confirm the provider settings. Check the local provider list")
	errHanaConflict     = errors.New("This provider was changed in HanaAgent. Check its settings before retrying")
	errHanaLegacySealed = errors.New("HanaAgent encrypted a previous file-based configuration. Review the existing NEWNOA provider in HanaAgent")
)

type hanaProviderService struct {
	port  int
	token string
}
type hanaServiceChange struct {
	id, secret string
}
type hanaServiceTransition struct {
	ID     string       `json:"id"`
	Before *hanaJournal `json:"before,omitempty"`
}

func (c *core) hanaService() (*hanaProviderService, error) {
	path := filepath.Join(c.home, ".hanako", "server-info.json")
	if err := c.safeConfigFile(path); err != nil {
		return nil, errHanaPrivate
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, errHanaStart
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		return nil, errHanaPrivate
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, errHanaPrivate
	}
	var s struct {
		Port     int
		Token    string
		ServerID string
	}
	if json.Unmarshal(raw, &s) != nil || s.Port < 1 || s.Port > 65535 || s.Token == "" || len(s.Token) > 2048 || strings.ContainsAny(s.Token, "\r\n") {
		return nil, errHanaStart
	}
	h := &hanaProviderService{s.Port, s.Token}
	var identity struct{ ServerID string }
	if err := h.request("GET", "server/identity", nil, &identity); err != nil {
		return nil, err
	}
	if s.ServerID == "" || identity.ServerID != s.ServerID {
		return nil, errHanaAuth
	}
	return h, nil
}

// Use the same local bearer and loopback address as the Hana CLI. Neither its
// advertised LAN host nor redirects may receive a bearer or a NEWNOA key.
var hanaHTTP = &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func (h *hanaProviderService) request(method, path string, body, result any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return errHanaProtocol
		}
	}
	req, err := http.NewRequest(method, fmt.Sprintf("http://127.0.0.1:%d/api/%s", h.port, path), bytes.NewReader(data))
	if err != nil {
		return errHanaProtocol
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := hanaHTTP.Do(req)
	if err != nil {
		return errHanaStart
	}
	defer resp.Body.Close()
	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return errHanaAuth
	}
	if resp.StatusCode != 200 {
		return errHanaProtocol
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8*maxFrame+1))
	if err != nil || len(raw) > 8*maxFrame || json.Unmarshal(raw, result) != nil {
		return errHanaProtocol
	}
	return nil
}

type hanaSummary struct {
	Name      string                     `json:"display_name"`
	URL       string                     `json:"base_url"`
	API       string                     `json:"api"`
	Auth      string                     `json:"auth_type"`
	Headers   map[string]json.RawMessage `json:"headers"`
	ModelList []struct {
		ID    string   `json:"id"`
		Name  string   `json:"name"`
		Input []string `json:"input"`
		API   string   `json:"api"`
	} `json:"models"`
}

func (h *hanaProviderService) summary() (map[string]hanaSummary, error) {
	var result struct{ Providers map[string]hanaSummary }
	if err := h.request("GET", "providers/summary", nil, &result); err != nil {
		return nil, err
	}
	if result.Providers == nil {
		return nil, errHanaProtocol
	}
	return result.Providers, nil
}

func hanaRow(j *hanaJournal, id string) *managedProvider {
	if j != nil && j.ProviderCatalog != nil {
		for _, row := range j.ProviderCatalog.Providers {
			if row.Native == id {
				return &row
			}
		}
	}
	return nil
}

func hanaDisplayName(id string) string { return "NEWNOA · " + strings.TrimPrefix(id, "newnoa-")[:8] }

func (h *hanaProviderService) match(j *hanaJournal, id string, state map[string]hanaSummary) (string, error) {
	row := hanaRow(j, id)
	observed, exists := state[id]
	if row == nil {
		if exists {
			return "", errHanaConflict
		}
		return "", nil
	}
	if !exists || observed.Name != hanaDisplayName(id) || observed.API != "openai-completions" || observed.URL != fmt.Sprintf("http://127.0.0.1:%d/v1", j.ProxyPort) || observed.Auth != "api-key" || len(observed.Headers) != 0 || len(observed.ModelList) != len(row.Models) {
		return "", errHanaConflict
	}
	for i, m := range row.Models {
		if observed.ModelList[i].ID != m.Alias || observed.ModelList[i].Name != m.Alias || !slices.Equal(observed.ModelList[i].Input, []string{"text"}) || observed.ModelList[i].API != "" {
			return "", errHanaConflict
		}
	}
	// Only request the credential of a previously journaled NEWNOA provider.
	var key struct {
		Secret string `json:"api_key"`
	}
	if err := h.request("GET", "providers/"+id+"/api-key", nil, &key); err != nil {
		return "", err
	}
	if !validAPIKey(key.Secret) || j.KeyHashes[row.KeyID] != managedAgentKeyHash(key.Secret) {
		return "", errHanaConflict
	}
	return key.Secret, nil
}

func (h *hanaProviderService) providerInputs(j *hanaJournal) (map[string]json.RawMessage, error) {
	rows := map[string]json.RawMessage{}
	state, err := h.summary()
	if err != nil {
		return nil, err
	}
	if j != nil {
		for _, row := range j.ProviderCatalog.Providers {
			secret, err := h.match(j, row.Native, state)
			if err != nil {
				return nil, err
			}
			rows[row.Native], _ = json.Marshal(map[string]string{"api_key": secret})
		}
	}
	return rows, nil
}

func validHanaServiceJournal(j *hanaJournal) bool {
	if j == nil || !j.Service || j.ProviderCatalog == nil || !nodePattern.MatchString(j.ProviderCatalog.Owner) || len(j.ProviderCatalog.Providers) > 32 || j.ProxyPort < 1024 || j.ProxyPort > 65535 {
		return false
	}
	for _, row := range j.ProviderCatalog.Providers {
		if !hanaNativeID.MatchString(row.Native) || !nodePattern.MatchString(row.KeyID) || len(j.KeyHashes[row.KeyID]) != 64 || len(row.Models) == 0 || len(row.Models) > 64 {
			return false
		}
	}
	if j.Transition == nil {
		return j.Stage == "applied"
	}
	t := j.Transition
	return j.Stage == "prepared" && hanaNativeID.MatchString(t.ID) && (t.Before == nil || t.Before.Transition == nil && validHanaServiceJournal(t.Before) && t.Before.ProviderCatalog.Owner == j.ProviderCatalog.Owner)
}

func (c *core) previewHanaService(p *preview, old *hanaJournal, h *hanaProviderService, keyID, secret string) (preview, error) {
	var id string
	for _, r := range p.providerCatalog.Providers {
		if r.KeyID == keyID {
			id = r.Native
			break
		}
	}
	if old != nil {
		p.hanaJournalDigest = old.digest
		if id == "" {
			for _, r := range old.ProviderCatalog.Providers {
				if r.KeyID == keyID {
					id = r.Native
				}
			}
		}
	}
	if !hanaNativeID.MatchString(id) {
		return preview{}, errHanaProtocol
	}
	state, err := h.summary()
	if err != nil {
		return preview{}, err
	}
	if _, err = h.match(old, id, state); err != nil {
		return preview{}, err
	}
	p.hanaServiceChange = &hanaServiceChange{id: id, secret: secret}
	c.pending = p
	return *p, nil
}

func (c *core) writeHanaServiceJournal(j *hanaJournal) error {
	_, path := c.hanaPaths()
	if j == nil || j.Transition == nil && len(j.ProviderCatalog.Providers) == 0 {
		err := os.Remove(path)
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if len(data) > 6*maxFrame {
		return errors.New("HanaAgent configuration too large")
	}
	return atomicFile(path, data)
}

// GET reconciles a lost PUT response or an interrupted local journal write.
// Repeating an opaque write or reverting the entire encrypted catalog is unnecessary.
func (c *core) reconcileHanaService(next *hanaJournal) (*hanaJournal, error) {
	h, err := c.hanaService()
	if err != nil {
		return nil, err
	}
	state, err := h.summary()
	if err != nil {
		return nil, err
	}
	t := next.Transition
	if _, err = h.match(next, t.ID, state); err == nil {
		next.Stage, next.Transition = "applied", nil
	} else if errors.Is(err, errHanaConflict) {
		if _, err = h.match(t.Before, t.ID, state); err != nil {
			return nil, err
		}
		next = t.Before
	} else {
		return nil, err
	}
	if err = c.writeHanaServiceJournal(next); err != nil {
		return nil, err
	}
	if next == nil || len(next.ProviderCatalog.Providers) == 0 {
		return nil, nil
	}
	data, _ := json.Marshal(next)
	next.digest = hash(data)
	return next, nil
}

func (c *core) applyHanaService(p *preview) error {
	change := p.hanaServiceChange
	actual, err := c.readHanaJournal()
	if err != nil {
		return err
	}
	if actual == nil && p.hanaJournalDigest != "" || actual != nil && actual.digest != p.hanaJournalDigest {
		return errHanaConflict
	}
	h, err := c.hanaService()
	if err != nil {
		return err
	}
	state, err := h.summary()
	if err != nil {
		return err
	}
	if _, err = h.match(actual, change.id, state); err != nil {
		return err
	}
	next := &hanaJournal{Service: true, LocalKeyReferences: actual == nil || actual.LocalKeyReferences, Stage: "prepared", ProxyPort: c.proxyPortValue(), ProviderCatalog: p.providerCatalog, Connections: p.providerRoutes, KeyHashes: map[string]string{}, Transition: &hanaServiceTransition{ID: change.id, Before: actual}}
	var patch any
	for _, row := range p.providerCatalog.Providers {
		if actual != nil {
			next.KeyHashes[row.KeyID] = actual.KeyHashes[row.KeyID]
		}
		if row.Native != change.id {
			continue
		}
		next.KeyHashes[row.KeyID] = managedAgentKeyHash(change.secret)
		patch = hanaReferencePatch(row, c.proxyBaseURL(), change.secret)
	}
	if !validHanaServiceJournal(next) {
		return errHanaProtocol
	}
	if err = c.writeHanaServiceJournal(next); err != nil {
		return err
	}
	var response struct{ OK bool }
	writeErr := h.request("PUT", "config", map[string]any{"providers": map[string]any{change.id: patch}}, &response)
	resolved, err := c.reconcileHanaService(next)
	if err != nil {
		return err
	}
	if resolved == nil && len(p.providerCatalog.Providers) == 0 || resolved != nil && reflect.DeepEqual(resolved.ProviderCatalog, p.providerCatalog) && slices.Equal(resolved.Connections, p.providerRoutes) {
		c.pending = nil
		return nil
	}
	if writeErr != nil {
		return writeErr
	}
	return errHanaProtocol
}

var knownHanaErrors = map[string]string{
	"Selected API key no longer authorizes one or more models": "此密钥已无权使用所选模型，请刷新密钥与模型列表。",
	"Selected API key is paused, expired or revoked":           "所选密钥已暂停、过期或撤销，请刷新并选择有效密钥。",
	"Selected API key has expired":                             "所选密钥已过期，请选择有效密钥。",
	"API key does not match the selected account key":          "密钥与当前账号不匹配，请刷新密钥列表。",
	"Invalid managed model selection":                          "所选模型标识不受支持，请刷新模型列表并重新选择。",
	errHanaStart.Error():                                       "请先在这台电脑启动 HanaAgent，再刷新连接。",
	errHanaPrivate.Error():                                     "无法安全读取 HanaAgent 的连接文件，请检查文件所有者和访问权限。",
	errHanaAuth.Error():                                        "HanaAgent 未授权访问本机配置，请重新打开 HanaAgent 后重试。",
	errHanaProtocol.Error():                                    "HanaAgent 未能确认配置，请检查 HanaAgent 的提供商列表。",
	errHanaConflict.Error():                                    "此连接已在 HanaAgent 中更改，请核对其配置后重试。",
	errHanaLegacySealed.Error():                                "HanaAgent 已加密旧版文件配置，请先在 HanaAgent 中核对原有 NEWNOA 提供商。",
	"initialize HanaAgent before adding NEWNOA models":         "请先在这台电脑启动 HanaAgent 完成初始化，再添加连接。",
	"HanaAgent requires Provider Catalog v2; upgrade and initialize HanaAgent first": "HanaAgent 配置格式不受支持，请在 HanaAgent 中确认版本并完成初始化。",
	"Login required for provider configuration":                                      "请重新登录当前客户端，再配置 HanaAgent。",
	"Managed providers belong to another account":                                    "这些配置属于其他账号，请切换到原账号。",
	"configuration changed since preview; preview again":                             "配置已更改，请刷新后重新确认。",
	"configuration modified outside NEWNOA; recovery requires manual review":         "本机配置已被修改，请在 HanaAgent 中核对后重试。",
	"custom HanaAgent paths require manual configuration":                            "HanaAgent 使用了自定义目录，请在 HanaAgent 中配置连接。",
}

func hanaReferencePatch(row managedProvider, baseURL, secret string) map[string]any {
	models := make([]map[string]any, 0, len(row.Models))
	for _, model := range row.Models {
		models = append(models, map[string]any{"id": model.Alias, "name": model.Alias, "input": []string{"text"}})
	}
	return map[string]any{"display_name": hanaDisplayName(row.Native), "auth_type": "api-key", "api": "openai-completions", "base_url": baseURL + "/v1", "api_key": secret, "models": models}
}

// The native service owns the encrypted store. Migrate only journaled records
// through its private settings API and confirm the installed values afterwards.
func (c *core) migrateHanaServiceKeys() error {
	j, err := c.readHanaJournal()
	if err != nil {
		return err
	}
	if j == nil || j.LocalKeyReferences {
		return nil
	}
	service, err := c.hanaService()
	if err != nil {
		return err
	}
	state, err := service.summary()
	if err != nil {
		return err
	}
	for _, row := range j.ProviderCatalog.Providers {
		key, err := service.match(j, row.Native, state)
		if err != nil {
			return err
		}
		ref := managedAgentKey(key)
		if ref == key {
			if !strings.HasPrefix(ref, "nref_") {
				return errHanaConflict
			}
			continue
		}
		var ack struct{ OK bool }
		patch := hanaReferencePatch(row, fmt.Sprintf("http://127.0.0.1:%d", j.ProxyPort), ref)
		if err = service.request("PUT", "config", map[string]any{"providers": map[string]any{row.Native: patch}}, &ack); err != nil {
			return err
		}
		check, e := service.summary()
		if e != nil {
			return e
		}
		installed, e := service.match(j, row.Native, check)
		if e != nil || installed != ref {
			return errHanaConflict
		}
	}
	j.LocalKeyReferences = true
	return c.writeHanaServiceJournal(j)
}
