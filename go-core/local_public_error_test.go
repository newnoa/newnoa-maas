package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPublicLocalErrorsUseOnlyKnownReasonsAndProducts(t *testing.T) {
	const internal = "Bearer sk-secret-session-value at /home/fixture/config.yaml https://upstream.private.example:39001"
	raw, _ := json.Marshal(map[string]any{"agent": internal, "providerAction": internal, "name": internal, "model": internal})
	for _, stage := range []struct{ method, zh, en string }{
		{"agent.preview", "智能体", "the agent"}, {"remote.command", "远程会话", "remote session"},
		{"history.export", "导出", "export"}, {"proxy.start", "端口", "port"}, {"connector.ensure", "本机模型", "model connection"},
		{"svg.tests.create", "SVG", "SVG"}, {"model.list", "模型服务", "model service"}, {"account.refresh", "账号", "account"}, {internal, "本机操作", "local operation"},
	} {
		message := browserLocalMessage(stage.method, raw, errors.New(internal))
		if !strings.Contains(message.Chinese, stage.zh) || !strings.Contains(message.English, stage.en) {
			t.Errorf("wrong context for %q: %+v", stage.method, message)
		}
		for _, lang := range []string{"en", "zh-CN"} {
			w := httptest.NewRecorder()
			writeLocalError(w, 400, lang, message)
			var dto struct {
				Error struct {
					Message       string
					PublicMessage bool
				}
			}
			if json.Unmarshal(w.Body.Bytes(), &dto) != nil || !dto.Error.PublicMessage || dto.Error.Message != message.in(lang) {
				t.Fatal("invalid localized error DTO")
			}
			for _, secret := range []string{"Bearer", "sk-secret", "/home/fixture", "upstream.private", "39001"} {
				if strings.Contains(w.Body.String(), secret) {
					t.Errorf("private detail escaped %q", stage.method)
				}
			}
		}
	}
	valid := json.RawMessage(`{"agent":"hana-agent","providerAction":"add"}`)
	if msg := browserLocalMessage("agent.preview", valid, errors.New(internal)); !strings.Contains(msg.Chinese, "HanaAgent添加连接") || !strings.Contains(msg.English, "add the connection for HanaAgent") {
		t.Fatal(msg)
	}
	// The renderer sends only a digest on apply. Retain the agent and actual
	// managed-provider action captured under the Core lock.
	if msg := browserLocalMessage("agent.apply", raw, &agentConfigError{error: errors.New(internal), agent: "claude-cli", action: "remove-model"}); !strings.Contains(msg.English, "remove the model from Claude Code") || !strings.Contains(msg.Chinese, "Claude Code移除模型") {
		t.Fatal(msg)
	}
	if msg := browserLocalMessage("agent.preview", valid, errHanaStart); msg.English != errHanaStart.Error() || !strings.Contains(msg.Chinese, "启动 HanaAgent") {
		t.Fatal(msg)
	}
	if msg := browserLocalMessage("remote.command", nil, errors.New(remoteSessionLimitError)); msg.English != remoteSessionLimitError || !strings.Contains(msg.Chinese, "8 条") {
		t.Fatal(msg)
	}
}

func TestBrowserCoreAndAccountNeverRelayLocalErrorsInEitherLanguage(t *testing.T) {
	const pathSecret = "never-show-this-private-file"
	for _, lang := range []string{"zh-CN", "en"} {
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]string{"id": "11111111-1111-4111-8111-111111111111", "language": lang})
		}))
		t.Cleanup(api.Close)
		c := newCore(t.TempDir(), api.URL)
		c.control = api.URL
		// Non-file parent causes the same path-bearing filesystem failure as a broken local installation.
		c.home = filepath.Join(c.home, pathSecret)
		if err := os.WriteFile(c.home, []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest("POST", "/api/local/core", strings.NewReader(`{"method":"agent.detect","params":{"agent":"codex"}}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Language", lang)
		w := httptest.NewRecorder()
		browserLocal(c, api.URL).ServeHTTP(w, req)
		if w.Code != 400 || strings.Contains(w.Body.String(), pathSecret) || !strings.Contains(w.Body.String(), `"publicMessage":true`) || !strings.Contains(w.Body.String(), "Codex") {
			t.Fatal("unsafe or unidentified agent detection", w.Body.String())
		}
		req = httptest.NewRequest(http.MethodGet, "/api/local/session", nil)
		req.Header.Set("Accept-Language", lang)
		w = httptest.NewRecorder()
		accountError(w, req, 503, "dial network at /"+pathSecret)
		want := "会话"
		if lang == "en" {
			want = "session"
		}
		if w.Code != 503 || !strings.Contains(w.Body.String(), want) || strings.Contains(w.Body.String(), pathSecret) || !strings.Contains(w.Body.String(), `"publicMessage":true`) {
			t.Fatal("unsafe account failure", w.Body.String())
		}
		c.startupError = "listen tcp 192.0.2.199:31919: " + pathSecret
		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/api/local/startup", nil)
		req.Header.Set("Accept-Language", lang)
		browserStartup(c).ServeHTTP(w, req)
		if w.Code != 200 || strings.Contains(w.Body.String(), "192.0.2.199") || strings.Contains(w.Body.String(), pathSecret) || !strings.Contains(w.Body.String(), `"publicMessage":true`) {
			t.Fatal("unsafe startup status", w.Body.String())
		}
	}
}

func TestHistoricalRemoteErrorsAreFilteredAtReadAndRelay(t *testing.T) {
	const sensitive = "SMTP password at /home/legacy/private, http://192.0.2.55:7890"
	c, m, _, _ := remoteRPCFixture(t)
	m.mu.Lock()
	m.failure = sensitive
	m.executors["codex"].Error = sensitive
	m.executors["codex"].ConfigurationError = sensitive
	m.sessions["archivable"] = &remoteSession{ID: "archivable", State: "failed", Agent: "codex", Error: sensitive}
	m.operations["old-failure"] = &remoteReceipt{OperationID: "old-failure", SessionID: "archivable", Status: "rejected", Error: sensitive, created: time.Now()}
	m.events = []remoteEvent{{Type: "operation.rejected", Payload: map[string]any{"error": sensitive}}}
	m.relayOperations = map[string]bool{"old-failure": true}
	snapshot := m.snapshotLocked()
	m.mu.Unlock()
	raw, _ := json.Marshal(snapshot)
	if strings.Contains(string(raw), sensitive) || !strings.Contains(string(raw), remotePublicFailure) {
		t.Fatal("historical snapshot exposed or silently dropped remote diagnostics")
	}
	result, err := callRemoteRPC(t, c, "remote.operation.get", map[string]any{"operationId": "old-failure"})
	if err != nil || result.(remoteReceipt).Error != remotePublicFailure {
		t.Fatal("historical receipt exposed raw diagnostics")
	}
	var delivered bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Snapshot map[string]any
			Receipts []remoteReceipt
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Receipts) != 1 || body.Receipts[0].Error != remotePublicFailure {
			t.Error("unfiltered relay receipt")
		} else {
			raw, _ := json.Marshal(body.Snapshot)
			if strings.Contains(string(raw), sensitive) {
				t.Error("unfiltered relay snapshot")
			}
			delivered = true
		}
		json.NewEncoder(w).Encode(map[string]bool{"published": true})
	}))
	defer api.Close()
	c.control = api.URL
	if err := c.publishChangedRemoteState(context.Background(), m, "session", nil); err != nil || !delivered {
		t.Fatal("could not verify remote publication", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sessions["archivable"].Error != sensitive || m.events[0].Payload["error"] != sensitive {
		t.Fatal("read projection altered historical sources")
	}
}

func TestStoredJobDiagnosticsDoNotEscapeStatus(t *testing.T) {
	const internal = "open /home/test/private-database: fixture-internal"
	job := historyJob{Error: internal, kind: "export", Path: "user-selected-export"}
	out := job.publicStatus()
	if out.Error == internal || !strings.Contains(out.Error, "export") || out.Path != job.Path || job.Error != internal {
		t.Fatal("incorrect history status projection")
	}
	svg := svgPublic(&svgTest{Error: internal, Content: "user SVG content"}, true)
	if svg["error"] == internal || svg["content"] != "user SVG content" {
		t.Fatal("incorrect SVG status projection")
	}
	if remotePublicMessage("Host registration failed: Remote control authority rejected the request (HTTP 429) at /tmp/private") != remotePublicFailure {
		t.Fatal("accepted a raw historic authority body")
	}
}
