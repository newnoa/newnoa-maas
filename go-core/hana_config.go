package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

type hanaFile struct {
	Original          []byte `json:"original"`
	Existed           bool   `json:"existed"`
	Installed         string `json:"installed"`
	PreviousInstalled string `json:"previousInstalled,omitempty"`
	Removed           bool   `json:"removed,omitempty"`
	Content           []byte `json:"-"`
}

type hanaJournal struct {
	LocalKeyReferences bool                   `json:"localKeyReferences,omitempty"`
	Service            bool                   `json:"service,omitempty"`
	KeyHashes          map[string]string      `json:"keyHashes,omitempty"`
	Transition         *hanaServiceTransition `json:"transition,omitempty"`

	digest          string
	ProviderCatalog *providerCatalog  `json:"providerCatalog,omitempty"`
	Connections     []agentConnection `json:"connections,omitempty"`
	IDs             []string          `json:"ids,omitempty"`
	ProxyPort       int               `json:"proxyPort,omitempty"`
	Model           string            `json:"model,omitempty"`
	Alias           string            `json:"alias,omitempty"`
	APIKeyHash      string            `json:"apiKeyHash,omitempty"`
	Files           []hanaFile        `json:"files"`
	Stage           string            `json:"stage"`
}

func (j *hanaJournal) providerJournal() *journal {
	return &journal{Stage: j.Stage, ProviderCatalog: j.ProviderCatalog, Model: j.Model, Alias: j.Alias, APIKeyHash: j.APIKeyHash, Connections: j.Connections}
}

func (c *core) hanaPaths(ids ...string) ([]string, string) {
	dir := filepath.Join(c.home, ".hanako")
	if len(ids) == 0 {
		ids = []string{"newnoa"}
	}
	paths := []string{filepath.Join(dir, "provider-catalog.json")}
	for _, id := range ids {
		pluginDir := filepath.Join(dir, "provider-plugins", id)
		paths = append(paths, filepath.Join(pluginDir, "manifest.json"), filepath.Join(pluginDir, "providers", id+".json"))
	}
	return paths, filepath.Join(c.home, ".newnoa", "hana-agent-journal.json")
}

func hanaCatalog(raw []byte) (map[string]json.RawMessage, map[string]json.RawMessage, error) {
	var catalog, providers map[string]json.RawMessage
	if json.Unmarshal(raw, &catalog) != nil || string(catalog["catalogVersion"]) != "2" {
		return nil, nil, errors.New("HanaAgent requires Provider Catalog v2; upgrade and initialize HanaAgent first")
	}
	if json.Unmarshal(catalog["providers"], &providers) != nil || providers == nil {
		return nil, nil, errors.New("HanaAgent providers must be a JSON object")
	}
	return catalog, providers, nil
}

func hanaPlugin(id, name, baseURL string, models []providerModel) []any {
	listed := make([]map[string]any, 0, len(models))
	for _, model := range models {
		listed = append(listed, map[string]any{"id": model.Alias, "name": model.Alias, "input": []string{"text"}})
	}
	return []any{map[string]any{"id": id, "type": "provider-plugin", "schemaVersion": 1, "provider": id},
		map[string]any{"id": id, "displayName": name, "authType": "api-key", "defaultBaseUrl": baseURL + "/v1", "defaultApi": "openai-completions", "models": listed}}
}

func (c *core) checkHanaLocation() error {
	if home := os.Getenv("HANA_HOME"); home != "" && filepath.Clean(home) != filepath.Join(c.home, ".hanako") {
		return errors.New("custom HanaAgent paths require manual configuration")
	}
	paths, journalPath := c.hanaPaths()
	for _, file := range append(paths, journalPath) {
		if err := c.safeConfigFile(file); err != nil {
			return err
		}
	}
	return nil
}

func (c *core) previewHana(model, apiKey, alias string) (preview, error) {
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`).MatchString(model) {
		return preview{}, errors.New("invalid model identifier")
	}
	if err := c.checkHanaLocation(); err != nil {
		return preview{}, err
	}
	paths, journalPath := c.hanaPaths()
	if _, err := os.Stat(journalPath); err == nil {
		return preview{}, errors.New("restore the previous change first")
	} else if !os.IsNotExist(err) {
		return preview{}, err
	}
	raw, err := os.ReadFile(paths[0])
	if os.IsNotExist(err) {
		return preview{}, errors.New("initialize HanaAgent before adding NEWNOA models")
	}
	if err != nil {
		return preview{}, err
	}
	catalog, providers, err := hanaCatalog(raw)
	if err != nil {
		return preview{}, err
	}
	if _, exists := providers["newnoa"]; exists {
		return preview{}, errors.New("existing newnoa provider must be restored or removed before applying")
	}
	for _, file := range paths[1:] {
		if _, err = os.Stat(file); err == nil {
			return preview{}, errors.New("existing HanaAgent newnoa plugin requires manual configuration")
		} else if !os.IsNotExist(err) {
			return preview{}, err
		}
	}
	var meta map[string]json.RawMessage
	if value, exists := catalog["meta"]; exists {
		if json.Unmarshal(value, &meta) != nil || meta == nil {
			return preview{}, errors.New("HanaAgent catalog metadata must be a JSON object")
		}
		if value, exists = meta["deletedProviders"]; exists {
			var deleted []string
			if json.Unmarshal(value, &deleted) != nil {
				return preview{}, errors.New("invalid HanaAgent deleted provider list")
			}
			kept := []string{}
			for _, provider := range deleted {
				if provider != "newnoa" {
					kept = append(kept, provider)
				}
			}
			meta["deletedProviders"], _ = json.Marshal(kept)
			catalog["meta"], _ = json.Marshal(meta)
		}
	}
	// Hanako separates the plugin definition from its catalog credential overlay.
	providers["newnoa"], _ = json.Marshal(map[string]string{"api_key": apiKey})
	catalog["providers"], _ = json.Marshal(providers)
	values := append([]any{catalog}, hanaPlugin("newnoa", "NEWNOA", c.proxyBaseURL(), []providerModel{{model, alias}})...)
	files := make([]hanaFile, len(paths))
	for i, value := range values {
		content, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return preview{}, err
		}
		if len(content)+1 > maxFrame {
			return preview{}, errors.New("HanaAgent configuration too large")
		}
		files[i] = hanaFile{Content: append(content, '\n')}
		files[i].Installed = hash(files[i].Content)
	}
	files[0].Original, files[0].Existed = raw, true
	pr := preview{Agent: "hana-agent", Model: model, Alias: alias, Digest: randomToken(), Content: string(files[0].Content), Path: paths[0], Paths: paths, hanaFiles: files, apiKey: apiKey}
	c.pending = &pr
	return pr, nil
}

func (c *core) readHanaFiles(ids ...string) ([][]byte, []bool, error) {
	if err := c.checkHanaLocation(); err != nil {
		return nil, nil, err
	}
	paths, _ := c.hanaPaths(ids...)
	raw, exists := make([][]byte, len(paths)), make([]bool, len(paths))
	for i, path := range paths {
		if err := c.safeConfigFile(path); err != nil {
			return nil, nil, err
		}
		data, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			return nil, nil, err
		}
		raw[i], exists[i] = data, err == nil
	}
	return raw, exists, nil
}

func (c *core) applyHana() error {
	if c.pending.hanaServiceChange != nil {
		return c.applyHanaService(c.pending)
	}
	p := c.pending
	raw, exists, err := c.readHanaFiles(p.hanaIDs...)
	if err != nil {
		return err
	}
	paths, journalPath := c.hanaPaths(p.hanaIDs...)
	previous, err := c.readHanaJournal()
	if err != nil {
		return err
	}
	if previous == nil && p.hanaJournalDigest != "" || previous != nil && previous.digest != p.hanaJournalDigest {
		return errors.New("configuration changed since preview; preview again")
	}
	if len(c.pending.hanaFiles) != len(raw) {
		return errors.New("invalid HanaAgent preview")
	}
	for i, file := range c.pending.hanaFiles {
		if exists[i] != file.Existed || hash(raw[i]) != hash(file.Original) {
			return errors.New("configuration changed since preview; preview again")
		}
	}
	if p.restoreLastProvider {
		if previous == nil || previous.ProviderCatalog == nil {
			return errors.New("invalid HanaAgent preview")
		}
		previous.Stage = "prepared"
		data, _ := json.Marshal(previous)
		if err := atomicFile(journalPath, data); err != nil {
			return err
		}
		if err := c.restoreHana(); err != nil {
			return err
		}
		c.pending = nil
		return nil
	}
	j := hanaJournal{
		ProxyPort: c.proxyPortValue(), Model: p.Model, Alias: p.Alias,
		Files: slices.Clone(p.hanaFiles), Stage: "prepared", IDs: p.hanaIDs,
		ProviderCatalog: p.providerCatalog, Connections: p.providerRoutes,
	}
	if p.apiKey != "" {
		j.APIKeyHash = managedAgentKeyHash(p.apiKey)
	}
	if previous != nil {
		for i, old := range previous.Files {
			j.Files[i].Original, j.Files[i].Existed, j.Files[i].PreviousInstalled = old.Original, old.Existed, old.Installed
		}
	}
	data, _ := json.Marshal(j)
	if len(data) > 6*maxFrame {
		return errors.New("HanaAgent configuration too large")
	}
	if err = atomicFile(journalPath, data); err != nil {
		return err
	}
	// Publish the catalog credentials after the plugin definitions.
	for n := 1; n <= len(paths); n++ {
		i := n % len(paths)
		f := p.hanaFiles[i]
		if f.Removed {
			if exists[i] {
				err = os.Remove(paths[i])
			}
		} else if !exists[i] || hash(raw[i]) != f.Installed {
			err = atomicFile(paths[i], f.Content)
		}
		if err != nil {
			return err
		}
	}
	if j.ProviderCatalog != nil {
		kept := []hanaFile{j.Files[0]}
		var ids []string
		for i, id := range j.IDs {
			if !j.Files[1+2*i].Removed {
				ids = append(ids, id)
				kept = append(kept, j.Files[1+2*i:3+2*i]...)
			}
		}
		for i, id := range j.IDs {
			if j.Files[1+2*i].Removed {
				c.removeEmptyHanaPlugin(id)
			}
		}
		j.Files, j.IDs = kept, ids
	}
	for i := range j.Files {
		j.Files[i].PreviousInstalled = ""
	}
	j.Stage = "applied"
	data, _ = json.Marshal(j)
	if err = atomicFile(journalPath, data); err != nil {
		return err
	}
	c.pending = nil
	return nil
}

func (c *core) readHanaJournal() (*hanaJournal, error) {
	if err := c.checkHanaLocation(); err != nil {
		return nil, err
	}
	_, journalPath := c.hanaPaths()
	raw, err := os.ReadFile(journalPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var j hanaJournal
	if json.Unmarshal(raw, &j) != nil {
		return nil, errors.New("invalid HanaAgent recovery journal")
	}
	if j.Service {
		if !validHanaServiceJournal(&j) {
			return nil, errors.New("invalid HanaAgent recovery journal")
		}
		j.digest = hash(raw)
		if j.Transition != nil {
			return c.reconcileHanaService(&j)
		}
		return &j, nil
	}
	if len(j.Files) != 1+2*max(1, len(j.IDs)) || len(j.IDs) > 32 || (j.Stage != "prepared" && j.Stage != "applied") {
		return nil, errors.New("invalid HanaAgent recovery journal")
	}
	for i, id := range j.IDs {
		if (id != "newnoa" && !hanaNativeID.MatchString(id)) || slices.Contains(j.IDs[:i], id) {
			return nil, errors.New("invalid HanaAgent recovery journal")
		}
	}
	for _, file := range j.Files {
		digest, err := hex.DecodeString(file.Installed)
		if err != nil || len(digest) != 32 || len(file.Original) > maxFrame {
			return nil, errors.New("invalid HanaAgent recovery journal")
		}
	}
	j.digest = hash(raw)
	return &j, nil
}

var hanaNativeID = regexp.MustCompile(`^newnoa-[0-9a-f]{16}$`)

func (c *core) checkHanaFiles(j *hanaJournal, recovery bool) error {
	if j.Service {
		service, err := c.hanaService()
		if err != nil {
			return err
		}
		_, err = service.providerInputs(j)
		return err
	}
	raw, exists, err := c.readHanaFiles(j.IDs...)
	if err != nil {
		return err
	}
	for i, file := range j.Files {
		if exists[i] == !file.Removed && hash(raw[i]) == file.Installed {
			continue
		}
		if recovery && (exists[i] == file.Existed && hash(raw[i]) == hash(file.Original) || exists[i] && j.Stage == "prepared" && hash(raw[i]) == file.PreviousInstalled) {
			continue
		}
		return errors.New("configuration modified outside NEWNOA; recovery requires manual review")
	}
	return nil
}

func (c *core) removeEmptyHanaPlugin(id string) {
	if !hanaNativeID.MatchString(id) {
		return
	}
	paths, _ := c.hanaPaths(id)
	for _, path := range []string{filepath.Dir(paths[2]), filepath.Dir(paths[1])} {
		// os.Remove leaves directories with user-owned files untouched.
		_ = os.Remove(path)
	}
}

func (c *core) restoreHana() error {
	j, err := c.readHanaJournal()
	if err != nil || j == nil {
		return err
	}
	if j.Service {
		return errors.New("Use the managed provider list to remove connections")
	}
	if err := c.checkHanaFiles(j, true); err != nil {
		return err
	}
	raw, exists, err := c.readHanaFiles(j.IDs...)
	if err != nil {
		return err
	}
	paths, journalPath := c.hanaPaths(j.IDs...)
	for i, file := range j.Files {
		if exists[i] == file.Existed && hash(raw[i]) == hash(file.Original) {
			continue
		}
		if file.Existed {
			err = atomicFile(paths[i], file.Original)
		} else {
			err = os.Remove(paths[i])
		}
		if err != nil {
			return err
		}
	}
	for _, id := range j.IDs {
		c.removeEmptyHanaPlugin(id)
	}
	return os.Remove(journalPath)
}

func (c *core) detectHana() (map[string]any, error) {
	_, exists, err := c.readHanaFiles()
	if err != nil {
		return nil, err
	}
	paths, _ := c.hanaPaths()
	result := map[string]any{"agent": "hana-agent", "exists": exists[0], "path": paths[0], "paths": paths, "supportedProtocol": "chat/completions", "experimental": true, "configurationState": "not_applied", "proxyRunning": c.server != nil}
	j, err := c.readHanaJournal()
	if err != nil || j == nil {
		return result, err
	}
	if j.Service {
		err := c.checkHanaFiles(j, false)
		if errors.Is(err, errHanaConflict) {
			result["configurationState"] = "conflict"
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		result["configurationState"] = "applied"
		return result, nil
	}
	if err := c.checkHanaFiles(j, false); err == nil && j.Stage == "applied" {
		result["configurationState"], result["model"] = "applied", j.Model
		paths, _ := c.hanaPaths(j.IDs...)
		result["paths"] = paths
		var models []string
		for _, row := range journalConnections(*j.providerJournal()) {
			if !slices.Contains(models, row.Model) {
				models = append(models, row.Model)
			}
		}
		result["configuredModels"] = models
	} else if err := c.checkHanaFiles(j, true); err == nil {
		result["configurationState"] = "recovery_required"
	} else {
		result["configurationState"] = "conflict"
	}
	return result, nil
}

func (c *core) finishHanaProvider(p *preview, previous *hanaJournal, settings, providers map[string]json.RawMessage) (preview, error) {
	var ids []string
	if previous != nil {
		ids = slices.Clone(previous.IDs)
		if len(ids) == 0 {
			ids = []string{"newnoa"}
		}
		p.hanaJournalDigest = previous.digest
	}
	active := map[string]managedProvider{}
	for _, row := range p.providerCatalog.Providers {
		if row.Native != "newnoa" && !hanaNativeID.MatchString(row.Native) {
			return preview{}, errors.New("invalid HanaAgent recovery journal")
		}
		if !slices.Contains(ids, row.Native) {
			ids = append(ids, row.Native)
		}
		active[row.Native] = row
	}
	raw, exists, err := c.readHanaFiles(ids...)
	if err != nil {
		return preview{}, err
	}
	if !exists[0] || hash(raw[0]) != p.SourceDigest {
		return preview{}, errors.New("configuration changed since preview; preview again")
	}
	if previous != nil {
		for i, f := range previous.Files {
			if !exists[i] || hash(raw[i]) != f.Installed {
				return preview{}, errors.New("configuration changed since preview; preview again")
			}
		}
	}
	p.hanaIDs = ids
	p.Paths, _ = c.hanaPaths(ids...)
	settings["providers"], _ = json.Marshal(providers)
	values := []any{settings}
	for i, id := range ids {
		if previous == nil || 1+2*i >= len(previous.Files) {
			_, err := os.Lstat(filepath.Dir(p.Paths[1+2*i]))
			if err == nil {
				return preview{}, errors.New("existing HanaAgent newnoa plugin requires manual configuration")
			}
			if !os.IsNotExist(err) {
				return preview{}, err
			}
		}
		row, ok := active[id]
		if !ok {
			values = append(values, nil, nil)
			continue
		}
		name := "NEWNOA"
		if id != "newnoa" {
			name += " · " + strings.TrimPrefix(id, "newnoa-")[:8]
		}
		values = append(values, hanaPlugin(id, name, c.proxyBaseURL(), row.Models)...)
	}
	for i, value := range values {
		file := hanaFile{Original: raw[i], Existed: exists[i], Removed: value == nil}
		if value != nil {
			content, err := json.MarshalIndent(value, "", "  ")
			if err != nil || len(content)+1 > maxFrame {
				return preview{}, errors.New("HanaAgent configuration too large")
			}
			file.Content = append(content, '\n')
		}
		file.Installed = hash(file.Content)
		p.hanaFiles = append(p.hanaFiles, file)
	}
	p.Content = string(p.hanaFiles[0].Content)
	c.pending = p
	return *p, nil
}
