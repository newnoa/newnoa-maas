package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
)

func TestAgentExtendedClaudeRolesAndClearing(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	path, _, _ := c.agentPaths("claude-cli")
	original := []byte(`{"permissions":{"allow":["Read"]},"env":{"CUSTOM":"keep","ANTHROPIC_SMALL_FAST_MODEL":"old"}}`)
	if err := atomicFile(path, original); err != nil {
		t.Fatal(err)
	}
	draft := agentExtendedConfiguration{Roles: []agentRoleConfiguration{{Role: "sonnet", ModelID: "deployment-one", DisplayName: "Main", OneMillion: true}, {Role: "fable", ModelID: "deployment-two", DisplayName: "Fast"}, {Role: "subagent", ModelID: "deployment-two"}}, Fallback: agentFallbackConfiguration{ModelID: "deployment-three"}}
	p, err := c.previewExtendedAgent("claude-cli", c.localKey, draft)
	if err != nil {
		t.Fatal(err)
	}
	var settings struct {
		Env         map[string]string
		Permissions map[string]any
	}
	json.Unmarshal([]byte(p.Content), &settings)
	if settings.Env["ANTHROPIC_DEFAULT_SONNET_MODEL"] != "deployment-one[1m]" || settings.Env["ANTHROPIC_DEFAULT_FABLE_MODEL_NAME"] != "Fast" || settings.Env["CLAUDE_CODE_SUBAGENT_MODEL"] != "deployment-two" || settings.Env["ANTHROPIC_MODEL"] != "deployment-three" || settings.Env["CUSTOM"] != "keep" {
		t.Fatalf("role projection incorrect: %+v", settings.Env)
	}
	if _, ok := settings.Env["ANTHROPIC_SMALL_FAST_MODEL"]; ok {
		t.Fatal("legacy small-fast override retained")
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	detected, err := c.detectAgent("claude-cli")
	if err != nil {
		t.Fatal(err)
	}
	if detected["configuration"] == nil {
		t.Fatal("extended mapping missing after restart detection")
	}
	p, err = c.previewExtendedAgent("claude-cli", c.localKey, agentExtendedConfiguration{})
	if err != nil {
		t.Fatal(err)
	}
	settings.Env = nil
	json.Unmarshal([]byte(p.Content), &settings)
	for k := range settings.Env {
		if strings.Contains(k, "_MODEL") {
			t.Fatalf("managed override survived clear: %s", k)
		}
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("claude-cli"); err != nil {
		t.Fatal(err)
	}
	restored, _ := os.ReadFile(path)
	if string(restored) != string(original) {
		t.Fatal("original configuration not restored")
	}
}

func TestAgentExtendedCodexCatalogAndRestore(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	original := []byte("# keep\nsandbox_mode = \"read-only\"\n[agents]\nmax_depth = 2\n[agents.reviewer]\ndescription = \"keep role\"\n")
	if err := atomicFile(c.configPath(), original); err != nil {
		t.Fatal(err)
	}
	draft := agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: "deployment-one", DisplayName: "First", ContextWindow: 200000, ReasoningEffort: "high"}, {ModelID: "deployment-two", DisplayName: "Second"}}, SubagentModel: "deployment-two", SubagentEffort: "low"}
	p, err := c.previewExtendedAgent("codex", c.localKey, draft)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err = toml.Unmarshal([]byte(p.Content), &config); err != nil {
		t.Fatal(err)
	}
	if config["model"] != "deployment-one" || config["sandbox_mode"] != "read-only" || config["model_catalog_json"] != filepath.Join(c.home, ".codex", "newnoa-models.json") {
		t.Fatalf("bad native config: %+v", config)
	}
	agents := config["agents"].(map[string]any)
	if agents["default_subagent_model"] != "deployment-two" || agents["max_depth"] != int64(2) || agents["reviewer"] == nil {
		t.Fatalf("native subagent config lost fields: %+v", agents)
	}
	if len(p.companions) != 2 {
		t.Fatal("catalog not bound to transaction")
	}
	var catalog struct{ Models []map[string]any }
	if json.Unmarshal(p.companions[1].Content, &catalog) != nil || len(catalog.Models) != 2 || catalog.Models[0]["slug"] != "deployment-one" || catalog.Models[0]["shell_type"] != "unified_exec" {
		t.Fatal("invalid native catalog")
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	if _, err = c.detectAgent("codex"); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("codex"); err != nil {
		t.Fatal(err)
	}
	restored, _ := os.ReadFile(c.configPath())
	if string(restored) != string(original) {
		t.Fatal("original config not restored")
	}
	if _, err = os.Stat(filepath.Join(c.home, ".codex", "newnoa-models.json")); !os.IsNotExist(err) {
		t.Fatal("managed catalog not removed on restore")
	}
}

func TestAgentExtendedValidation(t *testing.T) {
	cases := []struct {
		name, agent string
		draft       agentExtendedConfiguration
	}{{"blank1m", "claude-cli", agentExtendedConfiguration{Roles: []agentRoleConfiguration{{Role: "sonnet", OneMillion: true}}}}, {"duplicateRole", "claude-cli", agentExtendedConfiguration{Roles: []agentRoleConfiguration{{Role: "sonnet"}, {Role: "sonnet"}}}}, {"unknownRole", "claude-cli", agentExtendedConfiguration{Roles: []agentRoleConfiguration{{Role: "system", ModelID: "dep"}}}}, {"duplicateModel", "codex", agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: "dep"}, {ModelID: "dep"}}}}, {"badWindow", "codex", agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: "dep", ContextWindow: 2000001}}}}, {"missingSubagent", "codex", agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: "dep"}}, SubagentModel: "other"}}, {"badEffort", "codex", agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: "dep", ReasoningEffort: "extreme"}}}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			if _, err := c.previewExtendedAgent(tc.agent, c.localKey, tc.draft); err == nil {
				t.Fatal("invalid extended draft accepted")
			}
			if c.pending != nil {
				t.Fatal("invalid draft left a reusable preview")
			}
		})
	}
}

func TestAgentExtendedCatalogExternalEditAndLegacyRecovery(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	draft := agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: "deployment-one"}}}
	p, err := c.previewExtendedAgent("codex", c.localKey, draft)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	p, err = c.previewWithAPIKey("codex", "deployment-two", c.localKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.companions) != 2 {
		t.Fatal("legacy preview orphaned managed catalog recovery")
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	p, err = c.previewExtendedAgent("codex", c.localKey, draft)
	if err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(c.home, ".codex", "newnoa-models.json")
	if err = atomicFile(catalogPath, []byte(`{"external":"preserve me"}`)); err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err == nil {
		t.Fatal("external companion change was overwritten")
	}
	if err = c.restoreAgent("codex"); err == nil {
		t.Fatal("external companion change was silently restored")
	}
}

func TestAgentExtendedOneMillionAliasConnections(t *testing.T) {
	draft := agentExtendedConfiguration{Roles: []agentRoleConfiguration{{Role: "sonnet", ModelID: "dep", OneMillion: true}, {Role: "haiku", ModelID: "dep"}}, Fallback: agentFallbackConfiguration{ModelID: "fallback", OneMillion: true}}
	j := journal{Configuration: &draft, APIKeyHash: hash([]byte("key"))}
	routes := journalConnections(j)
	if len(routes) != 3 {
		t.Fatalf("lost per-role request aliases: %+v", routes)
	}
	found := false
	for _, r := range routes {
		if r.Model == "dep" && r.Alias == "dep[1m]" && r.APIKeyHash == j.APIKeyHash {
			found = true
		}
	}
	if !found {
		t.Fatal("1M request alias does not resolve authorized model")
	}
}

func TestAgentExtendedCodexNativeCatalog(t *testing.T) {
	if os.Getenv("NEWNOA_AGENT_NATIVE_SCHEMA_TEST") != "1" {
		t.Skip("explicit opt-in installed native parser check; no model inference")
	}
	binary, err := exec.LookPath("codex")
	if err != nil {
		t.Fatal(err)
	}
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	draft := agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: "deployment-one", DisplayName: "First native model", ContextWindow: 200000, ReasoningEffort: "high"}, {ModelID: "deployment-two", DisplayName: "Second native model", ReasoningEffort: "low"}}, SubagentModel: "deployment-two", SubagentEffort: "low"}
	draft.DefaultModel = "deployment-two"
	draft.DefaultEffort = "ultra"
	draft.SubagentEffort = "max"
	p, err := c.previewExtendedAgent("codex", c.localKey, draft)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "app-server", "--stdio")
	cmd.Env = append(os.Environ(), "CODEX_HOME="+filepath.Join(c.home, ".codex"))
	prepareRemoteProviderCommand(cmd)
	defer cleanupRemoteProviderCommand(cmd)
	stdin, e := cmd.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	stdout, e := cmd.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { cmd.Cancel(); cmd.Wait() }()
	encoder := json.NewEncoder(stdin)
	decoder := json.NewDecoder(stdout)
	call := func(id, method string, params any) json.RawMessage {
		if e = encoder.Encode(map[string]any{"id": id, "method": method, "params": params}); e != nil {
			t.Fatal(e)
		}
		for {
			var msg remoteNativeMessage
			if e = decoder.Decode(&msg); e != nil {
				t.Fatal(e)
			}
			if remoteMessageID(msg.ID) != id {
				continue
			}
			if len(msg.Error) > 0 {
				t.Fatal(remoteSafeError(errors.New(strings.ReplaceAll(string(msg.Error), c.localKey, "[REDACTED]"))))
			}
			return msg.Result
		}
	}
	call("1", "initialize", map[string]any{"clientInfo": map[string]string{"name": "newnoa_schema_test", "version": "1.0"}})
	encoder.Encode(map[string]any{"method": "initialized"})
	raw := call("2", "model/list", map[string]any{"includeHidden": false})
	call("3", "thread/start", map[string]any{"cwd": c.home, "ephemeral": true})
	var result struct {
		Data []struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
		} `json:"data"`
	}
	if err = json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 2 || result.Data[0].ID != "deployment-one" || result.Data[0].DisplayName != "First native model" {
		t.Fatalf("installed Codex did not consume native ordered catalog: %s", raw)
	}
}

func TestAgentExtendedCodex64RowRecovery(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	catalogPath := filepath.Join(c.home, ".codex", "newnoa-models.json")
	originalCatalog := []byte(`{"external":"` + strings.Repeat("x", 1200000) + `"}`)
	originalConfig := []byte("# external configuration\nmodel_catalog_json = " + strconv.Quote(catalogPath) + "\n")
	if err := atomicFile(c.configPath(), originalConfig); err != nil {
		t.Fatal(err)
	}
	if err := atomicFile(catalogPath, originalCatalog); err != nil {
		t.Fatal(err)
	}
	draft := agentExtendedConfiguration{}
	for i := 0; i < 64; i++ {
		draft.Models = append(draft.Models, agentCatalogModel{ModelID: fmt.Sprintf("deployment-%d", i), DisplayName: strings.Repeat("界", 100)})
	}
	p, err := c.previewExtendedAgent("codex", c.localKey, draft)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.companions[1].Content) <= maxFrame {
		t.Fatal("fixture did not exercise large native catalog")
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	p, err = c.previewExtendedAgent("codex", c.localKey, agentExtendedConfiguration{Models: draft.Models[:1]})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("codex"); err != nil {
		t.Fatal(err)
	}
	actual, _ := os.ReadFile(catalogPath)
	if string(actual) != string(originalCatalog) {
		t.Fatal("large original catalog was not restored")
	}
	actual, _ = os.ReadFile(c.configPath())
	if string(actual) != string(originalConfig) {
		t.Fatal("original config was not restored")
	}
}

func TestAgentExtendedOversizedJournalDoesNotWrite(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	catalogPath := filepath.Join(c.home, ".codex", "newnoa-models.json")
	originalCatalog := []byte(`{"external":"` + strings.Repeat("x", codexCatalogMaxBytes-32) + `"}`)
	originalConfig := []byte("external = " + strconv.Quote(strings.Repeat("x", maxFrame-512)) + "\nmodel_catalog_json = " + strconv.Quote(catalogPath) + "\n")
	if err := atomicFile(c.configPath(), originalConfig); err != nil {
		t.Fatal(err)
	}
	if err := atomicFile(catalogPath, originalCatalog); err != nil {
		t.Fatal(err)
	}
	p, err := c.previewExtendedAgent("codex", c.localKey, agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: "deployment-one"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err == nil {
		t.Fatal("oversized recovery journal was accepted")
	}
	actual, _ := os.ReadFile(c.configPath())
	if string(actual) != string(originalConfig) {
		t.Fatal("configuration changed before journal size rejection")
	}
	actual, _ = os.ReadFile(catalogPath)
	if string(actual) != string(originalCatalog) {
		t.Fatal("catalog changed before journal size rejection")
	}
}

func TestAgentExtendedCodexExplicitDefaultAndMappingAliases(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	var draft agentExtendedConfiguration
	raw := `{"models":[{"modelId":"deployment-one","alias":"quick","displayName":"Quick","contextWindow":64000},{"modelId":"deployment-one","alias":"deep","displayName":"Deep","contextWindow":200000}],"defaultModel":"deep","defaultEffort":"ultra","subagentModel":"quick","subagentEffort":"max"}`
	if err := json.Unmarshal([]byte(raw), &draft); err != nil {
		t.Fatal(err)
	}
	p, err := c.previewExtendedAgent("codex", c.localKey, draft)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err = toml.Unmarshal([]byte(p.Content), &config); err != nil {
		t.Fatal(err)
	}
	if config["model"] != "deep" || config["model_reasoning_effort"] != "ultra" || config["model_context_window"] != int64(200000) {
		t.Fatalf("wrong default: %+v", config)
	}
	agents := config["agents"].(map[string]any)
	if agents["default_subagent_model"] != "quick" || agents["default_subagent_reasoning_effort"] != "max" {
		t.Fatalf("wrong subagent: %+v", agents)
	}
	var catalog struct{ Models []map[string]any }
	if err = json.Unmarshal(p.companions[1].Content, &catalog); err != nil {
		t.Fatal(err)
	}
	if catalog.Models[0]["slug"] != "quick" || catalog.Models[1]["slug"] != "deep" {
		t.Fatal("mapping aliases missing")
	}
	routes := p.Configuration.connections("key-hash")
	if len(routes) != 2 || routes[0].Model != "deployment-one" || routes[1].Model != "deployment-one" || routes[1].Alias != "deep" {
		t.Fatalf("wrong routes: %+v", routes)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	if _, err = c.detectAgent("codex"); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("codex"); err != nil {
		t.Fatal(err)
	}
}

func TestAgentExtendedCodexMoreThan100Mappings(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	draft := agentExtendedConfiguration{}
	for i := 0; i < 150; i++ {
		draft.Models = append(draft.Models, agentCatalogModel{ModelID: fmt.Sprintf("deployment-%d", i), DisplayName: "Model"})
	}
	p, err := c.previewExtendedAgent("codex", c.localKey, draft)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(p.Digest, p.Content); err != nil {
		t.Fatal(err)
	}
	if err = c.restoreAgent("codex"); err != nil {
		t.Fatal(err)
	}
}

func TestAgentExtendedCodexChecksEveryMappingAliasBeforeApply(t *testing.T) {
	const keyID = "11111111-1111-4111-8111-111111111111"
	const modelA = "22222222-2222-4222-8222-222222222222"
	const modelB = "33333333-3333-4333-8333-333333333333"
	const secret = "nn_mapping_authorization_fixture_1234567890"
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/copy-secret") {
			json.NewEncoder(w).Encode(map[string]string{"secret": secret})
			return
		}
		json.NewEncoder(w).Encode([]any{map[string]any{"id": keyID, "deploymentIds": []string{modelA, modelB}}})
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	c.control = api.URL
	c.server = &http.Server{}
	c.modelAliases = map[string]agentModelRoute{"pi-agent\nshared-alias": {Model: modelB, Keys: map[string]bool{hash([]byte(secret)): true}}}
	draft := agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: modelA, Alias: "default-alias"}, {ModelID: modelA, Alias: "shared-alias"}}, DefaultModel: "default-alias", SubagentModel: "shared-alias", SubagentEffort: "max"}
	raw, _ := json.Marshal(map[string]any{"agent": "codex", "apiKey": secret, "session": "account-session", "keyId": keyID, "configuration": draft})
	result, err := c.handle("agent.preview", raw)
	if err != nil {
		t.Fatal(err)
	}
	p := result.(preview)
	raw, _ = json.Marshal(map[string]string{"digest": p.Digest, "session": "account-session"})
	if _, err = c.handle("agent.apply", raw); err == nil || !strings.Contains(err.Error(), "alias already refers") {
		t.Fatal("accepted conflicting non-default mapping alias")
	}
	if _, err = os.Stat(c.configPath()); !os.IsNotExist(err) {
		t.Fatal("wrote configuration before detecting conflict")
	}
	if c.modelAliases["pi-agent\nshared-alias"].Model != modelB {
		t.Fatal("changed existing route")
	}
}

func TestAgentExtendedCodexAuthorizationUsesDeploymentsNotMappingAliases(t *testing.T) {
	draft := agentExtendedConfiguration{Models: []agentCatalogModel{{ModelID: "deployment-one", Alias: "quick"}, {ModelID: "deployment-one", Alias: "deep"}}, DefaultModel: "deep", SubagentModel: "quick"}
	ids := draft.modelsForAuthorization()
	if len(ids) != 1 || ids[0] != "deployment-one" {
		t.Fatalf("mapping alias leaked into deployment authorization: %v", ids)
	}
}
