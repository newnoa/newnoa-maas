package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

type providerModel struct {
	ID    string `json:"id"`
	Alias string `json:"alias"`
}

type managedProvider struct {
	KeyID  string          `json:"keyId"`
	Native string          `json:"native"`
	Models []providerModel `json:"models"`
}

// Journal encoding includes only the native identifier, never the API key itself.
type providerCatalog struct {
	Owner      string            `json:"owner"`
	DefaultKey string            `json:"defaultKey"`
	Providers  []managedProvider `json:"providers"`
}

var hanaProviderAlias = regexp.MustCompile(`^[a-zA-Z0-9_.:/-]{1,100}$`)
var providerAlias = regexp.MustCompile(`^[a-zA-Z0-9_.:-]{1,100}$`)

func (c *core) providerAccount(session string) (string, error) {
	if session == "" || c.historySessionRevoked(session) {
		return "", errors.New("Login required for provider configuration")
	}
	var account struct{ ID string }
	if err := c.remoteHTTP(context.Background(), "GET", "/me", session, nil, nil, &account); err != nil {
		return "", err
	}
	if !nodePattern.MatchString(account.ID) {
		return "", errors.New("Invalid authenticated account")
	}
	return account.ID, nil
}

func (c *core) previewManagedProvider(agent, owner, keyID, secret, action string, wanted []providerModel) (preview, error) {
	secret = managedAgentKey(secret)
	if !providerListAgent(agent) {
		return preview{}, errors.New("Managed provider list is unavailable for this agent")
	}
	if !nodePattern.MatchString(owner) || !nodePattern.MatchString(keyID) ||
		(action != "add" && action != "default" && action != "remove" && action != "remove-model") ||
		(action == "remove" && len(wanted) != 0) || ((action == "default" || action == "remove-model") && len(wanted) != 1) ||
		(action == "add" && (len(wanted) < 1 || len(wanted) > 64)) {
		return preview{}, errors.New("Invalid managed provider selection")
	}
	aliasPattern := providerAlias
	if agent == "hana-agent" {
		aliasPattern = hanaProviderAlias
	}
	for _, model := range wanted {
		if !nodePattern.MatchString(model.ID) || action == "add" && !aliasPattern.MatchString(model.Alias) {
			return preview{}, errors.New("Invalid managed model selection")
		}
	}
	var err error
	switch agent {
	case "hana-agent":
		err = c.checkHanaLocation()
		if action == "default" {
			return preview{}, errors.New("Select the default model in HanaAgent")
		}
	case "openclaw":
		err = c.checkOpenClawLocation()
	case "pi-agent":
		err = c.checkPiLocation()
	case "hermes-agent":
		err = c.checkHermesLocation()
	default:
		err = c.checkOpenCodeLocation()
	}
	if err != nil {
		return preview{}, err
	}
	path, _, err := c.agentPaths(agent)
	if err != nil {
		return preview{}, err
	}
	var raw []byte
	var exists bool
	var previous *journal
	var piPaths []string
	var piFiles []piFile
	var settings, models, providers map[string]json.RawMessage
	var hermesDoc, hermesProviders *yaml.Node
	var hermesDefault string
	var hana *hanaJournal
	var service *hanaProviderService
	if agent == "hana-agent" {
		raw, err = os.ReadFile(path)
		if os.IsNotExist(err) {
			return preview{}, errors.New("initialize HanaAgent before adding NEWNOA models")
		}
		if err != nil {
			return preview{}, err
		}
		exists = true
		hana, err = c.readHanaJournal()
		if err != nil {
			return preview{}, err
		}
		if hana != nil && (hana.Stage != "applied" || hana.ProxyPort != c.proxyPortValue()) {
			return preview{}, errors.New("restore the previous change first")
		}
		if bytes.HasPrefix(raw, []byte("HANA-SECRET-")) || hana != nil && hana.Service {
			if hana != nil && !hana.Service {
				return preview{}, errHanaLegacySealed
			}
			service, err = c.hanaService()
			if err == nil {
				providers, err = service.providerInputs(hana)
			}
		} else {
			settings, providers, err = hanaCatalog(raw)
			if err == nil && hana != nil {
				err = c.checkHanaFiles(hana, false)
			}
		}
		if err != nil {
			return preview{}, err
		}
		if hana != nil {
			previous = hana.providerJournal()
		}
	} else if agent == "pi-agent" {
		piPaths, _ = c.piPaths()
		contents, found, err := c.readPiFiles()
		if err != nil {
			return preview{}, err
		}
		piFiles = make([]piFile, len(contents))
		for i := range contents {
			piFiles[i] = piFile{Original: contents[i], Existed: found[i]}
		}
		models, err = openClawObject(map[string]json.RawMessage{"file": jsonOrEmpty(contents[0], found[0])}, "file")
		if err != nil {
			return preview{}, err
		}
		settings, err = openClawObject(map[string]json.RawMessage{"file": jsonOrEmpty(contents[1], found[1])}, "file")
		if err != nil {
			return preview{}, err
		}
		providers, err = openClawObject(models, "providers")
		if err != nil {
			return preview{}, err
		}
		if j, err := c.readPiJournal(); err != nil {
			return preview{}, err
		} else if j != nil {
			if j.Stage != "applied" || j.ProxyPort != c.proxyPortValue() {
				return preview{}, errors.New("restore the previous change first")
			}
			for i, actual := range contents {
				if !found[i] || hash(actual) != j.Files[i].Installed {
					return preview{}, errors.New("configuration modified outside NEWNOA; recovery requires manual review")
				}
			}
			previous = &journal{ProviderCatalog: j.ProviderCatalog, Model: j.Model, Alias: j.Alias, APIKeyHash: j.APIKeyHash, Connections: piJournalConnections(*j)}
		}
	} else {
		raw, err = os.ReadFile(path)
		exists = err == nil
		if err != nil && !os.IsNotExist(err) {
			return preview{}, err
		}
		previous, err = c.managedConnectionJournal(agent, raw, exists)
		if err != nil {
			return preview{}, err
		}
		switch agent {
		case "openclaw":
			settings, models, providers, err = openClawProviderSettings(raw, exists)
		case "hermes-agent":
			hermesDoc, hermesProviders, providers, hermesDefault, err = hermesProviderSettings(raw, exists)
			if err == nil && previous == nil {
				for name := range providers {
					if hermesProviderConflict(name) {
						return preview{}, errors.New("existing NEWNOA provider requires manual configuration")
					}
				}
			}
		default:
			settings, providers, err = openCodeProviders(raw, exists)
		}
	}
	if err != nil {
		return preview{}, err
	}
	catalog := &providerCatalog{Owner: owner}
	j := journal{}
	if previous != nil {
		j = *previous
		if j.ProviderCatalog != nil {
			*catalog = *j.ProviderCatalog
			catalog.Providers = slices.Clone(j.ProviderCatalog.Providers)
			for i := range catalog.Providers {
				catalog.Providers[i].Models = slices.Clone(catalog.Providers[i].Models)
			}
			if catalog.Owner != owner {
				return preview{}, errors.New("Managed providers belong to another account")
			}
		} else {
			// The deployed single-provider format can be adopted without renaming its native ID.
			connection := managedProvider{KeyID: keyID, Native: "newnoa"}
			if agent == "hermes-agent" {
				connection.Native = hermesDefault
			}
			keyHash := managedAgentKeyHash(secret)
			if action != "add" || !validAPIKey(secret) || providerConfigSecret(agent, providers[connection.Native]) != secret {
				return preview{}, errors.New("Add the original API key first to adopt the existing configuration")
			}
			legacy := journalConnections(j)
			if agent == "hermes-agent" && len(legacy) != 1 {
				return preview{}, errors.New("Existing multi-provider Hermes configuration needs explicit migration; no changes made")
			}
			for _, model := range legacy {
				if model.APIKeyHash != keyHash {
					return preview{}, errors.New("Add the original API key first to adopt the existing configuration")
				}
				connection.Models = append(connection.Models, providerModel{model.Model, model.Alias})
			}
			if len(connection.Models) == 0 {
				return preview{}, errors.New("Add the original API key first to adopt the existing configuration")
			}
			if agent != "hana-agent" {
				catalog.DefaultKey = keyID
			}
			catalog.Providers = append(catalog.Providers, connection)
		}
	}
	row := slices.IndexFunc(catalog.Providers, func(p managedProvider) bool { return p.KeyID == keyID })
	p := preview{Agent: agent, Digest: randomToken(), SourceDigest: hash(raw), Path: path, Existed: exists, providerCatalog: catalog, piFiles: piFiles, Paths: piPaths}
	if action != "add" && row == -1 {
		return preview{}, errors.New("Managed provider no longer exists")
	}
	if action == "remove" {
		if len(catalog.Providers) == 1 {
			p.restoreLastProvider = true
			if service != nil {
				p.providerCatalog.Providers = nil
				return c.previewHanaService(&p, hana, service, keyID, "")
			}
			if agent == "hana-agent" {
				return c.finishHanaProvider(&p, hana, settings, providers)
			}
			c.pending = &p
			return p, nil
		}
		if catalog.DefaultKey == keyID {
			return preview{}, errors.New("Choose another default model before removing this provider")
		}
		delete(providers, catalog.Providers[row].Native)
		catalog.Providers = slices.Delete(catalog.Providers, row, row+1)
	} else if action == "remove-model" {
		connection := &catalog.Providers[row]
		i := slices.IndexFunc(connection.Models, func(m providerModel) bool { return m.ID == wanted[0].ID })
		if i == -1 || len(connection.Models) == 1 || (keyID == catalog.DefaultKey && connection.Models[i].Alias == j.Alias) {
			return preview{}, errors.New("Choose another default or remove the connection instead")
		}
		var provider, models map[string]json.RawMessage
		if json.Unmarshal(providers[connection.Native], &provider) != nil || provider == nil {
			return preview{}, errors.New("Invalid managed provider models")
		}
		if agent == "openclaw" || agent == "pi-agent" {
			var listed []map[string]json.RawMessage
			if json.Unmarshal(provider["models"], &listed) != nil {
				return preview{}, errors.New("Invalid managed provider models")
			}
			alias, _ := json.Marshal(connection.Models[i].Alias)
			listed = slices.DeleteFunc(listed, func(row map[string]json.RawMessage) bool { return string(row["id"]) == string(alias) })
			provider["models"], _ = json.Marshal(listed)
		} else if agent != "hana-agent" {
			if agent == "hermes-agent" && provider["models"] == nil {
				return preview{}, errors.New("Managed Hermes models are missing")
			}
			if json.Unmarshal(provider["models"], &models) != nil || models == nil {
				return preview{}, errors.New("Invalid managed provider models")
			}
			delete(models, connection.Models[i].Alias)
			provider["models"], _ = json.Marshal(models)
			if agent == "hermes-agent" {
				provider["default_model"], _ = json.Marshal(connection.Models[0].Alias)
				if i == 0 {
					provider["default_model"], _ = json.Marshal(connection.Models[1].Alias)
				}
			}
		}
		connection.Models = slices.Delete(connection.Models, i, i+1)
		providers[connection.Native], _ = json.Marshal(provider)
	} else if action == "add" {
		if !validAPIKey(secret) {
			return preview{}, errors.New("Invalid managed provider key")
		}
		if row == -1 {
			if len(catalog.Providers) >= 32 {
				return preview{}, errors.New("Too many managed providers")
			}
			native := "newnoa-" + randomToken()[:16]
			var nativeProviders map[string]json.RawMessage
			if value := settings["providers"]; value != nil && (json.Unmarshal(value, &nativeProviders) != nil || nativeProviders == nil) {
				return preview{}, errors.New("Existing native providers must be a JSON object")
			}
			if providers[native] != nil || nativeProviders[native] != nil {
				return preview{}, errors.New("Existing native provider conflicts with this selection")
			}
			row = len(catalog.Providers)
			catalog.Providers = append(catalog.Providers, managedProvider{KeyID: keyID, Native: native})
		}
		connection := &catalog.Providers[row]
		if old := providers[connection.Native]; old != nil && providerConfigSecret(agent, old) != secret {
			return preview{}, errors.New("API key changed; remove this connection before adding it again")
		}
		for _, item := range wanted {
			if !slices.ContainsFunc(connection.Models, func(m providerModel) bool { return m.ID == item.ID }) {
				connection.Models = append(connection.Models, item)
			}
		}
		if len(connection.Models) > 64 {
			return preview{}, errors.New("Too many models in one provider")
		}
		models := map[string]any{}
		for _, model := range connection.Models {
			if models[model.Alias] != nil {
				return preview{}, errors.New("A provider model alias already exists")
			}
			models[model.Alias] = map[string]string{"name": model.Alias}
		}
		if agent == "hana-agent" {
			providers[connection.Native], _ = json.Marshal(map[string]string{"api_key": secret})
		} else if agent == "opencode" {
			providers[connection.Native], _ = json.Marshal(map[string]any{
				"npm": "@ai-sdk/openai-compatible", "name": "NEWNOA",
				"options": map[string]string{"baseURL": c.proxyBaseURL() + "/v1", "apiKey": secret}, "models": models,
			})
		} else if agent == "hermes-agent" {
			providers[connection.Native], _ = json.Marshal(map[string]any{
				"name": connection.Native, "base_url": c.proxyBaseURL() + "/v1", "api_key": secret, "api_mode": "chat_completions",
				"default_model": connection.Models[0].Alias, "models": models, "extra_body": map[string]int{"max_tokens": 4096},
			})
		} else {
			listed := make([]map[string]any, 0, len(connection.Models))
			for _, model := range connection.Models {
				listed = append(listed, map[string]any{"id": model.Alias, "name": model.Alias, "input": []string{"text"}})
			}
			providers[connection.Native], _ = json.Marshal(map[string]any{"baseUrl": c.proxyBaseURL() + "/v1", "apiKey": secret, "api": "openai-completions", "models": listed})
		}
		if catalog.DefaultKey == "" && agent != "hana-agent" {
			catalog.DefaultKey = keyID
			j.Model, j.Alias = wanted[0].ID, wanted[0].Alias
			if err := setCatalogDefault(agent, settings, hermesDoc, connection.Native, j.Alias); err != nil {
				return preview{}, err
			}
		}
	} else {
		connection := catalog.Providers[row]
		i := slices.IndexFunc(connection.Models, func(m providerModel) bool { return m.ID == wanted[0].ID })
		if i == -1 || providerConfigSecret(agent, providers[connection.Native]) != secret {
			return preview{}, errors.New("Default must be an added model with the current API key")
		}
		catalog.DefaultKey = keyID
		j.Model, j.Alias = connection.Models[i].ID, connection.Models[i].Alias
		if err := setCatalogDefault(agent, settings, hermesDoc, connection.Native, j.Alias); err != nil {
			return preview{}, err
		}
	}
	routes := map[string]string{}
	for _, provider := range catalog.Providers {
		key := providerConfigSecret(agent, providers[provider.Native])
		if !validAPIKey(key) {
			return preview{}, errors.New("Managed provider is missing")
		}
		if provider.KeyID == catalog.DefaultKey {
			p.Model, p.Alias, p.apiKey = j.Model, j.Alias, key
		}
		for _, model := range provider.Models {
			if previous := routes[model.Alias]; previous != "" && previous != model.ID {
				return preview{}, errors.New("This model alias already refers to a different deployment")
			}
			routes[model.Alias] = model.ID
			p.providerRoutes = append(p.providerRoutes, agentConnection{model.ID, model.Alias, managedAgentKeyHash(key)})
		}
	}
	if service != nil {
		for _, row := range catalog.Providers {
			if row.KeyID == keyID {
				secret = providerConfigSecret(agent, providers[row.Native])
				break
			}
		}
		return c.previewHanaService(&p, hana, service, keyID, secret)
	}
	if agent == "hana-agent" {
		return c.finishHanaProvider(&p, hana, settings, providers)
	}
	if agent == "hermes-agent" {
		content, err := finishHermesProviderCatalog(hermesDoc, hermesProviders, providers, previous, catalog, j.Alias)
		if err != nil || len(content) > maxFrame/3 {
			return preview{}, errors.New("Invalid or oversized Hermes provider configuration")
		}
		p.Content = string(content)
		c.pending = &p
		return p, nil
	}
	if agent == "pi-agent" {
		models["providers"], _ = json.Marshal(providers)
		var enabled []string
		if old, exists := settings["enabledModels"]; exists && json.Unmarshal(old, &enabled) != nil {
			return preview{}, errors.New("Pi enabled models must be an array of strings")
		}
		// Catalog edits must not remove model patterns from other providers.
		enabled = slices.DeleteFunc(enabled, func(pattern string) bool {
			provider, _, ok := strings.Cut(pattern, "/")
			return ok && previous != nil && ((previous.ProviderCatalog == nil && provider == "newnoa") || (previous.ProviderCatalog != nil && slices.ContainsFunc(previous.ProviderCatalog.Providers, func(row managedProvider) bool { return row.Native == provider })))
		})
		for _, row := range catalog.Providers {
			for _, model := range row.Models {
				enabled = append(enabled, row.Native+"/"+model.Alias)
			}
		}
		settings["enabledModels"], _ = json.Marshal(enabled)
		combined := map[string]json.RawMessage{}
		for i, file := range []map[string]json.RawMessage{models, settings} {
			content, err := json.MarshalIndent(file, "", "  ")
			if err != nil || len(content) > maxFrame/3 {
				return preview{}, errors.New("Pi configuration is too large")
			}
			p.piFiles[i].Content = append(content, '\n')
			p.piFiles[i].Installed = hash(p.piFiles[i].Content)
			combined[filepath.Base(piPaths[i])] = content
		}
		settings = combined
	} else if agent == "openclaw" {
		models["providers"], _ = json.Marshal(providers)
		settings["models"], _ = json.Marshal(models)
	} else {
		settings["provider"], _ = json.Marshal(providers)
	}
	content, err := json.MarshalIndent(settings, "", "  ")
	if err != nil || len(content) > maxFrame/3 {
		return preview{}, errors.New("Managed configuration is too large")
	}
	p.Content = string(content) + "\n"
	c.pending = &p
	return p, nil
}

func jsonOrEmpty(raw []byte, exists bool) json.RawMessage {
	if exists {
		return raw
	}
	return json.RawMessage(`{}`)
}

func setCatalogDefault(agent string, settings map[string]json.RawMessage, hermes *yaml.Node, native, alias string) error {
	if agent == "hermes-agent" {
		return setHermesDefault(hermes.Content[0], native, alias)
	}
	if agent == "pi-agent" {
		settings["defaultProvider"], _ = json.Marshal(native)
		settings["defaultModel"], _ = json.Marshal(alias)
		return nil
	}
	if agent == "openclaw" {
		return setOpenClawDefault(settings, native, alias)
	}
	settings["model"], _ = json.Marshal(native + "/" + alias)
	settings["small_model"] = settings["model"]
	return nil
}

func (c *core) remoteOpenCodeSelection(raw []byte, j *journal, selection remoteSiteLaunch) (remoteSiteLaunch, []agentConnection, error) {
	if j.ProviderCatalog == nil {
		return selection, nil, errors.New(remoteSiteConfigurationRequired)
	}
	if err := c.checkOpenCodeLocation(); err != nil {
		return selection, nil, errors.New(remoteSiteConfigurationChanged)
	}
	settings, providers, err := openCodeProviders(raw, true)
	if err != nil {
		return selection, nil, errors.New(remoteSiteConfigurationChanged)
	}
	catalog := j.ProviderCatalog
	var model string
	if json.Unmarshal(settings["model"], &model) == nil && nodePattern.MatchString(catalog.Owner) && nodePattern.MatchString(catalog.DefaultKey) {
		for _, provider := range catalog.Providers {
			if provider.KeyID != catalog.DefaultKey || model != provider.Native+"/"+j.Alias {
				continue
			}
			var config struct {
				NPM     string
				Options struct{ BaseURL, APIKey string }
			}
			if json.Unmarshal(providers[provider.Native], &config) != nil || config.NPM != "@ai-sdk/openai-compatible" || config.Options.BaseURL != selection.BaseURL+"/v1" || managedAgentKeyHash(config.Options.APIKey) != j.APIKeyHash {
				break
			}
			for _, item := range provider.Models {
				if item.ID == j.Model && item.Alias == j.Alias {
					selection.Owner, selection.KeyID, selection.Secret = catalog.Owner, provider.KeyID, config.Options.APIKey
					return selection, []agentConnection{{Model: j.Model, Alias: j.Alias, APIKeyHash: j.APIKeyHash}}, nil
				}
			}
			break
		}
	}
	return selection, nil, errors.New(remoteSiteConfigurationChanged)
}

func providerConfigSecret(agent string, value json.RawMessage) string {
	var p struct {
		APIKey    string
		HermesKey string `json:"api_key"`
		Options   map[string]string
	}
	_ = json.Unmarshal(value, &p)
	if agent == "hermes-agent" || agent == "hana-agent" {
		return p.HermesKey
	}
	if agent != "opencode" {
		return p.APIKey
	}
	return p.Options["apiKey"]
}

func (c *core) readProviderJournal(agent string) (*journal, error) {
	if agent == "hana-agent" {
		j, err := c.readHanaJournal()
		if err != nil || j == nil {
			return nil, err
		}
		return j.providerJournal(), nil
	}
	if agent == "pi-agent" {
		j, err := c.readPiJournal()
		if err != nil || j == nil {
			return nil, err
		}
		return &journal{ProviderCatalog: j.ProviderCatalog, Stage: j.Stage, Model: j.Model}, nil
	}
	_, path, err := c.agentPaths(agent)
	if err != nil {
		return nil, err
	}
	if err := c.safeConfigFile(path); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var j journal
	if json.Unmarshal(data, &j) != nil {
		return nil, errors.New("Invalid provider journal")
	}
	return &j, nil
}

func (c *core) managedProviderStatus(agent, session string) (map[string]any, error) {
	if _, capabilities := agentOperation(agent); !capabilities.ProviderList {
		return nil, errors.New("Managed provider list is unavailable for this agent")
	}
	owner, err := c.providerAccount(session)
	if err != nil {
		return nil, err
	}
	state, err := c.detectAgent(agent)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"state": state["configurationState"]}
	if state["configurationState"] != "applied" {
		return result, nil
	}
	j, err := c.readProviderJournal(agent)
	if err != nil || j == nil {
		return nil, errors.New("Managed provider status changed; refresh again")
	}
	if j.ProviderCatalog == nil {
		result["legacy"] = true
		if agent == "hermes-agent" && len(journalConnections(*j)) > 1 {
			result["legacyManual"] = true
		}
		return result, nil
	}
	if owner != j.ProviderCatalog.Owner {
		return map[string]any{"state": "other_account"}, nil
	}
	rows := make([]map[string]any, 0, len(j.ProviderCatalog.Providers))
	for _, provider := range j.ProviderCatalog.Providers {
		models := make([]string, 0, len(provider.Models))
		for _, model := range provider.Models {
			models = append(models, model.ID)
		}
		rows = append(rows, map[string]any{"keyId": provider.KeyID, "models": models})
	}
	result["providers"], result["defaultKey"], result["defaultModel"] = rows, j.ProviderCatalog.DefaultKey, j.Model
	return result, nil
}

// A clean managed catalog is persistent; only interrupted or legacy writes use startup recovery.
func (c *core) recoverProviderCatalog(agent string) error {
	j, err := c.readProviderJournal(agent)
	if err != nil || j == nil {
		return err
	}
	if j.ProviderCatalog != nil && j.Stage == "applied" {
		return nil
	}
	return c.restoreAgent(agent)
}

func setRemoteCatalogDefault(j *journal, selection *remoteSiteLaunch) (string, error) {
	catalog := j.ProviderCatalog
	if catalog != nil && nodePattern.MatchString(catalog.Owner) && nodePattern.MatchString(catalog.DefaultKey) {
		for _, provider := range catalog.Providers {
			if provider.KeyID == catalog.DefaultKey && slices.Contains(provider.Models, providerModel{j.Model, j.Alias}) {
				selection.Owner, selection.KeyID = catalog.Owner, provider.KeyID
				return provider.Native, nil
			}
		}
	}
	return "", errors.New(remoteSiteConfigurationChanged)
}
