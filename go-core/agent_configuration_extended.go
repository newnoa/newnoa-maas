package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pelletier/go-toml/v2"
)

type agentRoleConfiguration struct {
	Role        string `json:"role"`
	DisplayName string `json:"displayName"`
	ModelID     string `json:"modelId"`
	OneMillion  bool   `json:"oneMillion"`
}
type agentFallbackConfiguration struct {
	ModelID    string `json:"modelId"`
	OneMillion bool   `json:"oneMillion"`
}
type agentCatalogModel struct {
	Alias           string `json:"alias,omitempty"`
	ModelID         string `json:"modelId"`
	DisplayName     string `json:"displayName"`
	ContextWindow   int64  `json:"contextWindow"`
	ReasoningEffort string `json:"reasoningEffort"`
}
type agentExtendedConfiguration struct {
	DefaultModel   string                     `json:"defaultModel,omitempty"`
	DefaultEffort  string                     `json:"defaultEffort,omitempty"`
	Roles          []agentRoleConfiguration   `json:"roles,omitempty"`
	Fallback       agentFallbackConfiguration `json:"fallback"`
	Models         []agentCatalogModel        `json:"models,omitempty"`
	SubagentModel  string                     `json:"subagentModel,omitempty"`
	SubagentEffort string                     `json:"subagentEffort,omitempty"`
}

const codexCatalogMaxBytes = 8 * maxFrame
const codexJournalMaxBytes = 12 * maxFrame

func (m agentCatalogModel) mappingID() string {
	if m.Alias != "" {
		return m.Alias
	}
	return m.ModelID
}

func (d agentExtendedConfiguration) defaultCatalogModel() agentCatalogModel {
	for _, m := range d.Models {
		if m.mappingID() == d.DefaultModel {
			return m
		}
	}
	return d.Models[0]
}

func (d agentExtendedConfiguration) modelsForAuthorization() []string {
	out := []string{}
	seen := map[string]bool{}
	add := func(id string) {
		if id != "" && !seen[id] {
			out = append(out, id)
			seen[id] = true
		}
	}
	for _, r := range d.Roles {
		add(r.ModelID)
	}
	add(d.Fallback.ModelID)
	for _, m := range d.Models {
		add(m.ModelID)
	}
	return out
}
func agentRoleModel(id string, oneMillion bool) string {
	if id != "" && oneMillion {
		return id + "[1m]"
	}
	return id
}
func (d agentExtendedConfiguration) connections(keyHash string) []agentConnection {
	out := []agentConnection{}
	seen := map[string]bool{}
	add := func(id, alias string) {
		if id != "" && !seen[alias] {
			out = append(out, agentConnection{Model: id, Alias: alias, APIKeyHash: keyHash})
			seen[alias] = true
		}
	}
	for _, r := range d.Roles {
		add(r.ModelID, agentRoleModel(r.ModelID, r.OneMillion))
	}
	add(d.Fallback.ModelID, agentRoleModel(d.Fallback.ModelID, d.Fallback.OneMillion))
	for _, m := range d.Models {
		add(m.ModelID, m.mappingID())
	}
	return out
}

func (d agentExtendedConfiguration) validate(agent string) error {
	validID := regexp.MustCompile(`^[a-zA-Z0-9_-]{1,100}$`)
	check := func(id string, oneMillion bool) error {
		if id == "" {
			if oneMillion {
				return errors.New("1M requires a selected model")
			}
			return nil
		}
		if !validID.MatchString(id) {
			return errors.New("invalid authorized model identifier")
		}
		return nil
	}
	name := func(s string) error {
		if !utf8.ValidString(s) || utf8.RuneCountInString(s) > 100 || strings.ContainsAny(s, "\x00\r\n") {
			return errors.New("invalid model display name")
		}
		return nil
	}
	switch agent {
	case "claude-cli":
		if len(d.Models) > 0 || d.DefaultModel != "" || d.DefaultEffort != "" || d.SubagentModel != "" || d.SubagentEffort != "" {
			return errors.New("Codex configuration fields cannot be used for Claude")
		}
		if len(d.Roles) > 5 {
			return errors.New("Claude supports exactly five managed role names")
		}
		seen := map[string]bool{}
		for _, r := range d.Roles {
			if r.Role != "sonnet" && r.Role != "opus" && r.Role != "fable" && r.Role != "haiku" && r.Role != "subagent" {
				return errors.New("unsupported Claude model role")
			}
			if seen[r.Role] {
				return errors.New("duplicate Claude model role")
			}
			seen[r.Role] = true
			if err := check(r.ModelID, r.OneMillion); err != nil {
				return err
			}
			if err := name(r.DisplayName); err != nil {
				return err
			}
			if r.Role == "subagent" && r.DisplayName != "" {
				return errors.New("Claude subagent role does not have a display-name override")
			}
			if r.ModelID == "" && r.DisplayName != "" {
				return errors.New("display name requires a selected model")
			}
		}
		return check(d.Fallback.ModelID, d.Fallback.OneMillion)
	case "codex":
		if len(d.Roles) > 0 || d.Fallback.ModelID != "" || d.Fallback.OneMillion {
			return errors.New("Claude configuration fields cannot be used for Codex")
		}
		if len(d.Models) == 0 {
			return errors.New("Codex catalog must contain at least one authorized model mapping")
		}
		seen := map[string]bool{}
		validAlias := regexp.MustCompile(`^[a-zA-Z0-9_.:/-]{1,100}$`)
		for _, m := range d.Models {
			if m.ModelID == "" {
				return errors.New("Codex catalog model is required")
			}
			if err := check(m.ModelID, false); err != nil {
				return err
			}
			if !validAlias.MatchString(m.mappingID()) {
				return errors.New("invalid Codex mapping alias")
			}
			if seen[m.mappingID()] {
				return errors.New("duplicate Codex catalog model")
			}
			seen[m.mappingID()] = true
			if err := name(m.DisplayName); err != nil {
				return err
			}
			if m.ContextWindow < 0 || m.ContextWindow > 2000000 {
				return errors.New("context window must be between zero and 2000000")
			}
			if err := validateAgentReasoning("codex", m.ReasoningEffort); err != nil {
				return err
			}
		}
		if d.DefaultModel != "" && !seen[d.DefaultModel] {
			return errors.New("Codex default model must belong to the current catalog")
		}
		if err := validateAgentReasoning("codex", d.DefaultEffort); err != nil {
			return err
		}
		if d.SubagentModel != "" && !seen[d.SubagentModel] {
			return errors.New("Codex subagent model must belong to the current catalog")
		}
		if d.SubagentModel == "" && d.SubagentEffort != "" {
			return errors.New("subagent reasoning requires a subagent model")
		}
		return validateAgentReasoning("codex", d.SubagentEffort)
	default:
		return errors.New("extended model configuration is supported only for Codex and Claude Code")
	}
}

func (c *core) previewExtendedAgent(agent, apiKey string, draft agentExtendedConfiguration) (preview, error) {
	c.pending = nil
	if err := draft.validate(agent); err != nil {
		return preview{}, err
	}
	first := "newnoa-managed"
	ids := draft.modelsForAuthorization()
	if len(ids) > 0 {
		first = ids[0]
	}
	if agent == "codex" {
		first = draft.defaultCatalogModel().ModelID
	}
	alias := ""
	if agent == "codex" {
		alias = draft.defaultCatalogModel().mappingID()
	}
	pr, err := c.previewWithAPIKey(agent, first, apiKey, alias)
	if err != nil {
		return preview{}, err
	}
	fail := func(err error) (preview, error) { c.pending = nil; return preview{}, err }
	if agent == "claude-cli" {
		var settings map[string]json.RawMessage
		if err = json.Unmarshal([]byte(pr.Content), &settings); err != nil {
			return fail(err)
		}
		var env map[string]json.RawMessage
		if err = json.Unmarshal(settings["env"], &env); err != nil {
			return fail(err)
		}
		for _, role := range []string{"SONNET", "OPUS", "FABLE", "HAIKU"} {
			delete(env, "ANTHROPIC_DEFAULT_"+role+"_MODEL")
			delete(env, "ANTHROPIC_DEFAULT_"+role+"_MODEL_NAME")
		}
		delete(env, "CLAUDE_CODE_SUBAGENT_MODEL")
		delete(env, "ANTHROPIC_MODEL")
		delete(env, "ANTHROPIC_SMALL_FAST_MODEL")
		set := func(k, v string) {
			if v != "" {
				env[k], _ = json.Marshal(v)
			}
		}
		for _, r := range draft.Roles {
			key := "ANTHROPIC_DEFAULT_" + strings.ToUpper(r.Role) + "_MODEL"
			if r.Role == "subagent" {
				key = "CLAUDE_CODE_SUBAGENT_MODEL"
			}
			set(key, agentRoleModel(r.ModelID, r.OneMillion))
			if r.Role != "subagent" {
				set(key+"_NAME", r.DisplayName)
			}
		}
		set("ANTHROPIC_MODEL", agentRoleModel(draft.Fallback.ModelID, draft.Fallback.OneMillion))
		settings["env"], _ = json.Marshal(env)
		content, e := json.MarshalIndent(settings, "", "  ")
		if e != nil {
			return fail(e)
		}
		pr.Content = string(content) + "\n"
		if len(ids) == 0 {
			pr.Model = ""
			pr.Alias = ""
		}
	} else {
		catalogPath := filepath.Join(c.home, ".codex", "newnoa-models.json")
		file, e := c.readManagedFile(catalogPath)
		if e != nil {
			return fail(e)
		}
		models := []map[string]any{}
		for index, m := range draft.Models {
			display := m.DisplayName
			if display == "" {
				display = m.ModelID
			}
			reasoning := []map[string]string{}
			for _, effort := range []string{"none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"} {
				reasoning = append(reasoning, map[string]string{"effort": effort, "description": effort})
			}
			item := map[string]any{"base_instructions": codexExtendedBaseInstructions, "slug": m.mappingID(), "display_name": display, "description": nil, "supported_reasoning_levels": reasoning, "shell_type": "unified_exec", "visibility": "list", "supported_in_api": true, "priority": index, "upgrade": nil, "support_verbosity": false, "default_verbosity": nil, "apply_patch_tool_type": nil, "truncation_policy": map[string]any{"mode": "bytes", "limit": 10000}, "experimental_supported_tools": []string{}, "input_modalities": []string{"text", "image"}}
			if m.ContextWindow > 0 {
				item["context_window"] = m.ContextWindow
			}
			models = append(models, item)
		}
		file.Content, err = json.MarshalIndent(map[string]any{"models": models}, "", "  ")
		if err != nil {
			return fail(err)
		}
		file.Content = append(file.Content, '\n')
		file.Installed = hash(file.Content)
		if len(file.Content) > codexCatalogMaxBytes {
			return fail(errors.New("Codex catalog is too large"))
		}
		roots := map[string]string{"model_catalog_json": strconv.Quote(catalogPath), "model_context_window": "", "model_reasoning_effort": ""}
		selected := draft.defaultCatalogModel()
		if selected.ContextWindow > 0 {
			roots["model_context_window"] = strconv.FormatInt(selected.ContextWindow, 10)
		}
		effort := draft.DefaultEffort
		if effort == "" && draft.DefaultModel == "" {
			effort = selected.ReasoningEffort
		}
		if effort != "" {
			roots["model_reasoning_effort"] = strconv.Quote(effort)
		}
		pr.Content, err = replaceAgentTomlFields(pr.Content, "", roots)
		if err != nil {
			return fail(err)
		}
		agents := map[string]string{"default_subagent_model": "", "default_subagent_reasoning_effort": ""}
		if draft.SubagentModel != "" {
			agents["default_subagent_model"] = strconv.Quote(draft.SubagentModel)
		}
		if draft.SubagentEffort != "" {
			agents["default_subagent_reasoning_effort"] = strconv.Quote(draft.SubagentEffort)
		}
		pr.Content, err = replaceAgentTomlFields(pr.Content, "agents", agents)
		if err != nil {
			return fail(err)
		}
		if len(pr.companions) > 1 {
			pr.companions = pr.companions[:1]
		}
		pr.companions = append(pr.companions, file)
		pr.Paths = []string{pr.Path, pr.companions[0].Path, file.Path}
	}
	if len(pr.Content) > maxFrame {
		return fail(errors.New("configuration too large"))
	}
	pr.Configuration = &draft
	c.pending = &pr
	return pr, nil
}

func replaceAgentTomlFields(content, section string, fields map[string]string) (string, error) {
	lines := strings.Split(content, "\n")
	out := []string{}
	current := ""
	inserted := false
	appendFields := func() {
		for _, key := range []string{"model_catalog_json", "model_context_window", "model_reasoning_effort", "default_subagent_model", "default_subagent_reasoning_effort"} {
			if value := fields[key]; value != "" {
				out = append(out, key+" = "+value)
			}
		}
		inserted = true
	}
	if section == "" {
		appendFields()
	}
	scalar := regexp.MustCompile(`^\s*[a-zA-Z0-9_]+\s*=\s*("([^"\\]|\\.)*"|'[^']*'|-?[0-9]+)\s*(#.*)?$`)
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "[") {
			current = strings.TrimSpace(strings.SplitN(trim, "#", 2)[0])
			current = strings.TrimSuffix(strings.TrimPrefix(current, "["), "]")
			out = append(out, line)
			if current == section && !inserted {
				appendFields()
			}
			continue
		}
		if section != "" && current == "" && regexp.MustCompile(`^agents\s*=`).MatchString(trim) {
			return "", errors.New("inline agents settings require manual conversion before managed subagent configuration")
		}
		if current == section {
			key, _, ok := strings.Cut(trim, "=")
			key = strings.TrimSpace(key)
			if _, managed := fields[key]; ok && managed {
				if !scalar.MatchString(line) {
					return "", errors.New("complex native model settings require manual editing")
				}
				continue
			}
		}
		out = append(out, line)
	}
	if !inserted {
		hasValue := false
		for _, v := range fields {
			hasValue = hasValue || v != ""
		}
		if hasValue {
			out = append(out, "["+section+"]")
			appendFields()
		}
	}
	result := strings.Join(out, "\n")
	var check map[string]any
	if toml.Unmarshal([]byte(result), &check) != nil {
		return "", errors.New("native settings could not be updated without losing unrelated configuration")
	}
	return result, nil
}

func (c *core) preserveCodexCatalog(pr preview) (preview, error) {
	if pr.Agent != "codex" {
		return pr, nil
	}
	raw, err := os.ReadFile(pr.Path)
	if err != nil && !os.IsNotExist(err) {
		return preview{}, err
	}
	var config map[string]any
	if toml.Unmarshal(raw, &config) != nil {
		return preview{}, errors.New("invalid native config")
	}
	path, _ := config["model_catalog_json"].(string)
	if path != filepath.Join(c.home, ".codex", "newnoa-models.json") {
		return pr, nil
	}
	file, err := c.readManagedFile(path)
	if err != nil {
		return preview{}, err
	}
	if !file.Existed {
		return preview{}, errors.New("managed Codex catalog is missing")
	}
	file.Content = append([]byte(nil), file.Original...)
	file.Installed = hash(file.Content)
	pr.companions = append(pr.companions, file)
	pr.Paths = append(pr.Paths, file.Path)
	return pr, nil
}
