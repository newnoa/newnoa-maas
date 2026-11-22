package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
)

type piFile struct {
	Original          []byte `json:"original"`
	Existed           bool   `json:"existed"`
	PreviousInstalled string `json:"previousInstalled,omitempty"`
	Installed         string `json:"installed"`
	Content           []byte `json:"-"`
}

type piJournal struct {
	ProviderCatalog *providerCatalog  `json:"providerCatalog,omitempty"`
	ProxyPort       int               `json:"proxyPort,omitempty"`
	Model           string            `json:"model,omitempty"`
	Alias           string            `json:"alias,omitempty"`
	APIKeyHash      string            `json:"apiKeyHash,omitempty"`
	Connections     []agentConnection `json:"connections,omitempty"`
	Files           []piFile          `json:"files"`
	Stage           string            `json:"stage"`
}

func (c *core) piPaths() ([]string, string) {
	dir := filepath.Join(c.home, ".pi", "agent")
	return []string{filepath.Join(dir, "models.json"), filepath.Join(dir, "settings.json")}, filepath.Join(c.home, ".newnoa", "pi-agent-journal.json")
}

func (c *core) checkPiLocation() error {
	if os.Getenv("PI_CODING_AGENT_DIR") != "" {
		return errors.New("custom Pi Agent paths require manual configuration")
	}
	paths, _ := c.piPaths()
	for _, path := range paths {
		if err := c.safeConfigFile(path); err != nil {
			return err
		}
	}
	authPath := filepath.Join(c.home, ".pi", "agent", "auth.json")
	if err := c.safeConfigFile(authPath); err != nil {
		return err
	}
	raw, err := os.ReadFile(authPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var auth map[string]json.RawMessage
	if json.Unmarshal(raw, &auth) != nil || auth == nil {
		return errors.New("Pi Agent authentication requires a JSON object; configure manually")
	}
	if _, exists := auth["newnoa"]; exists {
		return errors.New("existing newnoa authentication requires manual configuration")
	}
	return nil
}

func (c *core) previewPi(model, apiKey string, aliases ...string) (preview, error) {
	alias := model
	if len(aliases) > 0 && aliases[0] != "" {
		alias = aliases[0]
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`).MatchString(model) {
		return preview{}, errors.New("invalid model identifier")
	}
	if err := c.checkPiLocation(); err != nil {
		return preview{}, err
	}
	paths, journalPath := c.piPaths()
	if err := c.safeConfigFile(journalPath); err != nil {
		return preview{}, err
	}
	managed, err := c.readPiJournal()
	if err != nil {
		return preview{}, err
	}
	if managed != nil {
		if managed.ProviderCatalog != nil {
			return preview{}, errors.New("Use the managed provider list to change this configuration")
		}
		if managed.Stage != "applied" || managed.ProxyPort != c.proxyPortValue() {
			return preview{}, errors.New("restore the previous change first")
		}
		raw, exists, err := c.readPiFiles()
		if err != nil {
			return preview{}, err
		}
		for i, file := range managed.Files {
			if !exists[i] || hash(raw[i]) != file.Installed {
				return preview{}, errors.New("configuration modified outside NEWNOA; recovery requires manual review")
			}
		}
	}
	objects := make([]map[string]json.RawMessage, 2)
	files := make([]piFile, 2)
	for i, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			return preview{}, err
		}
		files[i] = piFile{Original: raw, Existed: err == nil}
		objects[i] = map[string]json.RawMessage{}
		if files[i].Existed && (json.Unmarshal(raw, &objects[i]) != nil || objects[i] == nil) {
			return preview{}, errors.New("Pi Agent configuration requires strict JSON objects")
		}
	}
	providers := map[string]json.RawMessage{}
	if raw, exists := objects[0]["providers"]; exists && (json.Unmarshal(raw, &providers) != nil || providers == nil) {
		return preview{}, errors.New("Pi Agent providers must be a JSON object")
	}
	var previousModels json.RawMessage
	if old, exists := providers["newnoa"]; exists {
		if managed == nil {
			return preview{}, errors.New("existing newnoa provider must be restored or removed before applying")
		}
		if managed.APIKeyHash == managedAgentKeyHash(apiKey) {
			var provider map[string]json.RawMessage
			if json.Unmarshal(old, &provider) != nil {
				return preview{}, errors.New("invalid managed provider")
			}
			previousModels = provider["models"]
		}
	}
	entries, err := managedModelEntries(previousModels, alias)
	if err != nil {
		return preview{}, err
	}
	providers["newnoa"], _ = json.Marshal(map[string]any{"baseUrl": c.proxyBaseURL() + "/v1", "api": "openai-completions", "apiKey": apiKey, "models": entries})
	objects[0]["providers"], _ = json.Marshal(providers)
	objects[1]["defaultProvider"] = json.RawMessage(`"newnoa"`)
	objects[1]["defaultModel"], _ = json.Marshal(alias)
	var listed []struct{ ID string }
	json.Unmarshal(entries, &listed)
	enabled := []string{}
	for _, m := range listed {
		enabled = append(enabled, "newnoa/"+m.ID)
	}
	objects[1]["enabledModels"], _ = json.Marshal(enabled)
	combined := map[string]json.RawMessage{}
	for i, object := range objects {
		content, err := json.MarshalIndent(object, "", "  ")
		if err != nil {
			return preview{}, err
		}
		files[i].Content = append(content, '\n')
		files[i].Installed = hash(files[i].Content)
		combined[filepath.Base(paths[i])] = content
	}
	content, err := json.MarshalIndent(combined, "", "  ")
	if err != nil {
		return preview{}, err
	}
	if len(content) > maxFrame/3 {
		return preview{}, errors.New("combined Pi Agent configuration too large for preview")
	}
	pr := preview{Model: model, Alias: alias, Agent: "pi-agent", Digest: randomToken(), Path: paths[0], Paths: paths, Content: string(content) + "\n", piFiles: files, apiKey: apiKey}
	c.pending = &pr
	return pr, nil
}

func (c *core) readPiFiles() ([][]byte, []bool, error) {
	paths, _ := c.piPaths()
	contents, exists := make([][]byte, 2), make([]bool, 2)
	for i, path := range paths {
		if err := c.safeConfigFile(path); err != nil {
			return nil, nil, err
		}
		raw, err := os.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			return nil, nil, err
		}
		contents[i], exists[i] = raw, err == nil
	}
	return contents, exists, nil
}

func (c *core) applyPi() error {
	if err := c.checkPiLocation(); err != nil {
		return err
	}
	paths, journalPath := c.piPaths()
	if err := c.safeConfigFile(journalPath); err != nil {
		return err
	}
	previous, err := c.readPiJournal()
	if err != nil {
		return err
	}
	if previous != nil && (previous.Stage != "applied" || previous.ProxyPort != c.proxyPortValue()) {
		return errors.New("restore the previous change first")
	}
	raw, exists, err := c.readPiFiles()
	if err != nil {
		return err
	}
	for i, file := range c.pending.piFiles {
		if previous != nil && (!exists[i] || hash(raw[i]) != previous.Files[i].Installed) {
			return errors.New("configuration modified outside NEWNOA; recovery requires manual review")
		}
		if exists[i] != file.Existed || hash(raw[i]) != hash(file.Original) {
			return errors.New("configuration changed since preview; preview again")
		}
	}
	if previous != nil && previous.ProviderCatalog != nil && (c.pending.providerCatalog == nil || previous.ProviderCatalog.Owner != c.pending.providerCatalog.Owner) {
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
		if err := c.restorePi(); err != nil {
			return err
		}
		c.pending = nil
		return nil
	}
	j := piJournal{ProxyPort: c.proxyPortValue(), Model: c.pending.Model, Alias: c.pending.Alias, APIKeyHash: managedAgentKeyHash(c.pending.apiKey), Files: c.pending.piFiles, Stage: "prepared"}
	if previous != nil {
		for i := range j.Files {
			j.Files[i].Original = previous.Files[i].Original
			j.Files[i].Existed = previous.Files[i].Existed
			j.Files[i].PreviousInstalled = previous.Files[i].Installed
		}
		if previous.APIKeyHash == j.APIKeyHash {
			j.Connections = piJournalConnections(*previous)
		}
	}
	entry := agentConnection{Model: j.Model, Alias: j.Alias, APIKeyHash: j.APIKeyHash}
	found := false
	for _, v := range j.Connections {
		if v == entry {
			found = true
		}
	}
	if !found {
		j.Connections = append(j.Connections, entry)
	}
	if c.pending.providerCatalog != nil {
		j.ProviderCatalog, j.Connections = c.pending.providerCatalog, c.pending.providerRoutes
	}
	data, _ := json.Marshal(j)
	if len(data) > 4*maxFrame {
		return errors.New("Pi Agent recovery journal is too large")
	}
	if err = atomicFile(journalPath, data); err != nil {
		return err
	}
	for i, path := range paths {
		if err = atomicFile(path, c.pending.piFiles[i].Content); err != nil {
			return err
		}
	}
	j.Stage = "applied"
	for i := range j.Files {
		j.Files[i].PreviousInstalled = ""
	}
	data, _ = json.Marshal(j)
	if err = atomicFile(journalPath, data); err != nil {
		return err
	}
	c.pending = nil
	return nil
}

func (c *core) readPiJournal() (*piJournal, error) {
	_, path := c.piPaths()
	if err := c.safeConfigFile(path); err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var j piJournal
	if json.Unmarshal(raw, &j) != nil || len(j.Files) != 2 || (j.Stage != "prepared" && j.Stage != "applied") {
		return nil, errors.New("invalid Pi Agent recovery journal")
	}
	for _, file := range j.Files {
		if len(file.Installed) != 64 || len(file.PreviousInstalled) != 0 && len(file.PreviousInstalled) != 64 || len(file.Original) > maxFrame {
			return nil, errors.New("invalid Pi Agent recovery journal")
		}
	}
	return &j, nil
}

func piJournalConnections(j piJournal) []agentConnection {
	if len(j.Connections) > 0 {
		return j.Connections
	}
	if j.Model != "" && j.Alias != "" && len(j.APIKeyHash) == 64 {
		return []agentConnection{{Model: j.Model, Alias: j.Alias, APIKeyHash: j.APIKeyHash}}
	}
	return nil
}

func (c *core) restorePi() error {
	j, err := c.readPiJournal()
	if err != nil || j == nil {
		return err
	}
	paths, journalPath := c.piPaths()
	raw, exists, err := c.readPiFiles()
	if err != nil {
		return err
	}
	// Preflight both files before restoring either; original bytes are also valid after an interrupted restore.
	for i, file := range j.Files {
		if exists[i] == file.Existed && hash(raw[i]) == hash(file.Original) {
			continue
		}
		if !exists[i] || hash(raw[i]) != file.Installed && (file.PreviousInstalled == "" || hash(raw[i]) != file.PreviousInstalled) {
			return errors.New("configuration modified outside NEWNOA; recovery requires manual review")
		}
	}
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
	return os.Remove(journalPath)
}

func (c *core) detectPi() (map[string]any, error) {
	if err := c.checkPiLocation(); err != nil {
		return nil, err
	}
	paths, _ := c.piPaths()
	raw, exists, err := c.readPiFiles()
	if err != nil {
		return nil, err
	}
	result := map[string]any{"agent": "pi-agent", "exists": exists[0] || exists[1], "path": paths[0], "paths": paths, "supportedProtocol": "chat/completions", "experimental": true, "configurationState": "not_applied", "proxyRunning": c.server != nil}
	j, err := c.readPiJournal()
	if err != nil || j == nil {
		return result, err
	}
	allInstalled := true
	for i, file := range j.Files {
		installed := exists[i] && hash(raw[i]) == file.Installed
		original := exists[i] == file.Existed && hash(raw[i]) == hash(file.Original)
		previous := exists[i] && file.PreviousInstalled != "" && hash(raw[i]) == file.PreviousInstalled
		if !installed && !original && !previous {
			result["configurationState"] = "conflict"
			return result, nil
		}
		allInstalled = allInstalled && installed
	}
	result["configurationState"] = "recovery_required"
	if allInstalled && j.Stage == "applied" {
		result["configurationState"] = "applied"
		if j.Model != "" {
			result["model"] = j.Model
		}
	}
	return result, nil
}
