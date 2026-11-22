package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHistoryManagerLocalOfflineAndMigration(t *testing.T) {
	online := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !online {
			w.WriteHeader(503)
			return
		}
		if r.URL.Path != "/api/v1/me" {
			t.Errorf("local query made cloud call: %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"id": "00000000-0000-0000-0000-000000000001"})
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	configureTestHistorySQLite(t, c.home, "00000000-0000-0000-0000-000000000001")
	c.control = server.URL
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	if err = h.save("00000000-0000-0000-0000-000000000001", map[string]any{"id": "a", "model": "m", "agent": "a", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "literal % _ [needle]", "response": "response", "keyId": "old-key", "keyName": "Revoked key"}, false); err != nil {
		t.Fatal(err)
	}
	value, err := c.historyCommand("history.query", json.RawMessage(`{"session":"verified","q":"[needle]"}`))
	if err != nil {
		t.Fatal(err)
	}
	if value.(historyQueryResult).Total != 1 {
		t.Fatal(value)
	}
	online = false
	filters, err := c.historyCommand("history.filters", json.RawMessage(`{"session":"verified","kind":"models","keyId":"old-key"}`))
	if err != nil || len(filters.(historyFilterResult).Items) != 1 || filters.(historyFilterResult).Items[0].ID != "m" {
		t.Fatal("offline local cascade", filters, err)
	}
	if _, err = c.historyCommand("history.filters", json.RawMessage(`{"session":"forged","kind":"keys"}`)); err == nil {
		t.Fatal("forged filter session accepted")
	}
	if _, err = c.historyCommand("history.query", json.RawMessage(`{"session":"verified"}`)); err != nil {
		t.Fatal("verified offline query", err)
	}
	if _, err = c.historyCommand("history.query", json.RawMessage(`{"session":"forged"}`)); err == nil {
		t.Fatal("unverified offline session accepted")
	}
	manager, err := h.manager.forAccount("00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	job := manager.jobs[manager.busy]
	manager.mu.Unlock()
	if job != nil {
		select {
		case <-job.done:
		case <-time.After(5 * time.Second):
			t.Fatal("initial legacy migration did not finish")
		}
	}
	target := manager.config
	target.SQLiteDirectory = filepath.Join(c.home, "relocated")
	if err = manager.migrate(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	result, err := manager.store.Query(context.Background(), "00000000-0000-0000-0000-000000000001", historyQuery{})
	if err != nil || result.Total != 1 {
		t.Fatal(result, err)
	}
	target.SQLiteDirectory = "relative"
	if err = manager.migrate(context.Background(), target); err == nil {
		t.Fatal("invalid migration accepted")
	}
	result, err = manager.store.Query(context.Background(), "00000000-0000-0000-0000-000000000001", historyQuery{})
	if err != nil || result.Total != 1 {
		t.Fatal("failed migration replaced active store", result, err)
	}
}

func TestHistoryArchiveChecksAndJobLock(t *testing.T) {
	raw := []byte("中文 bounded archive")
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	compressed := encoder.EncodeAll(raw, nil)
	encoder.Close()
	item := historyArchive{Codec: "zstd-v1", RawBytes: len(raw), CompressedBytes: len(compressed), SHA256: historyHash(raw), BodyBase64: base64.StdEncoding.EncodeToString(compressed)}
	got, err := decodeHistoryArchive(item, 256<<10)
	if err != nil || string(got) != string(raw) {
		t.Fatal(string(got), err)
	}
	item.SHA256 = strings.Repeat("0", 64)
	if _, err = decodeHistoryArchive(item, 256<<10); err == nil {
		t.Fatal("corrupt block accepted")
	}
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err := newHistoryManager(t.TempDir(), h)
	if err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()
	_, id, err := m.start("export")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.start("sync"); err == nil {
		t.Fatal("sync/export lock bypass")
	}
	m.finish(id, context.Canceled, historyExportResult{})
	if _, _, err = m.start("migration"); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryJournalLegacyWALAndOutboxesPreserved(t *testing.T) {
	home := t.TempDir()
	old, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	if err = old.save("viewer", map[string]any{"id": "legacy", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "WAL record"}, true); err != nil {
		t.Fatal(err)
	}
	if _, err = old.db.Exec("INSERT INTO private_usage_outbox(request_id,node_id,payload) VALUES('request','node','{}')"); err != nil {
		t.Fatal(err)
	}
	old.db.Close()
	c := newCore(home, "")
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	defer func() {
		if h.manager.store != nil {
			h.manager.store.Close()
		}
	}()
	var count int
	if err = h.db.QueryRow("SELECT count(*) FROM private_usage_outbox").Scan(&count); err != nil || count != 1 {
		t.Fatal("outbox lost", count, err)
	}
	if err = h.db.QueryRow("SELECT count(*) FROM exchanges WHERE pending=1").Scan(&count); err != nil || count != 1 {
		t.Fatal("pending lost", count, err)
	}
	if _, err = os.Stat(filepath.Join(home, ".newnoa", "chat-history.sqlite")); err != nil {
		t.Fatal("source removed", err)
	}
	if _, err = os.Stat(filepath.Join(defaultHistoryDatabase(home).SQLiteDirectory, "chat-history.sqlite")); err != nil {
		t.Fatal("new journal missing", err)
	}
}

func TestHistoryManagerCaptureJournalSurvivesFailedStore(t *testing.T) {
	c := newCore(t.TempDir(), "")
	configureTestHistorySQLite(t, c.home, "00000000-0000-0000-0000-000000000001")
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err := h.manager.forAccount("00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	m.store.Close()
	record := map[string]any{"id": "pending", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "recover me"}
	if err = h.save("00000000-0000-0000-0000-000000000001", record, false); err == nil {
		t.Fatal("closed selected store failure hidden")
	}
	var count int
	if err = h.db.QueryRow("SELECT count(*) FROM history_pending").Scan(&count); err != nil || count != 1 {
		t.Fatal("local journal lost", count, err)
	}
	h.db.Close()
	c.history = nil
	h, err = c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err = h.manager.forAccount("00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()
	if record, err = m.store.Get(context.Background(), "00000000-0000-0000-0000-000000000001", "own", "pending"); err != nil || record["request"] != "recover me" {
		t.Fatal(record, err)
	}
}

func TestHistoryManagerResumesInterruptedMigration(t *testing.T) {
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	m, err := newHistoryManager(home, h)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.commit(context.Background(), "viewer", "own", "record", "r", map[string]any{"id": "r", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "survives restart"}); err != nil {
		t.Fatal(err)
	}
	target := m.config
	target.SQLiteDirectory = filepath.Join(home, "resumed")
	raw, _ := json.Marshal(map[string]any{"config": target, "owner": "viewer"})
	if err = atomicFile(filepath.Join(home, ".newnoa", "history-migration.json"), raw); err != nil {
		t.Fatal(err)
	}
	m.store.Close()
	h.db.Close()
	h, err = openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err = newHistoryManager(home, h)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { m.store.Close() }()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		m.mu.Lock()
		job := *m.jobs[m.migration]
		m.mu.Unlock()
		if job.State != "running" {
			if job.State != "complete" {
				t.Fatal(job)
			}
			record, err := m.store.Get(context.Background(), "viewer", "own", "r")
			if err != nil || record["request"] != "survives restart" {
				t.Fatal(record, err)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("migration did not resume")
}

func TestHistoryAccountIsolationExportSnapshotAndRecovery(t *testing.T) {
	accountA := "00000000-0000-0000-0000-000000000001"
	accountB := "00000000-0000-0000-0000-000000000002"
	home := t.TempDir()
	c := newCore(home, "")
	configureTestHistorySQLite(t, home, accountA)
	configureTestHistorySQLite(t, home, accountB)
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	a, err := h.manager.forAccount(accountA)
	if err != nil {
		t.Fatal(err)
	}
	defer a.store.Close()
	b, err := h.manager.forAccount(accountB)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { b.store.Close() }()
	record := func(id, body string) map[string]any {
		return map[string]any{"id": id, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": body}
	}
	if err = h.save(accountA, record("a", "account A private"), false); err != nil {
		t.Fatal(err)
	}
	if err = h.save(accountB, record("b", "account B snapshot"), false); err != nil {
		t.Fatal(err)
	}
	target := b.config
	target.SQLiteDirectory = filepath.Join(home, "account-b-database")
	if err = b.migrate(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	leaked, err := b.store.Query(context.Background(), accountA, historyQuery{})
	if err != nil || leaked.Total != 0 {
		t.Fatal("account A migrated into B database", leaked, err)
	}
	if a.config.SQLiteDirectory == b.config.SQLiteDirectory {
		t.Fatal("account settings leaked")
	}
	if a.configPath() == b.configPath() {
		t.Fatal("config path shared")
	}
	if info, err := os.Stat(b.configPath()); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("private config permission", err)
	}
	_, id, err := b.start("export", accountB)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = a.start("sync", accountA); err == nil {
		t.Fatal("cross-account global task lock bypass")
	}
	if err = b.commit(context.Background(), accountB, "own", "record", "new", record("new", "backdated capture during export")); err != nil {
		t.Fatal(err)
	}
	if err = b.commit(context.Background(), accountB, "own", "delete", "b", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := b.store.Query(context.Background(), accountB, historyQuery{})
	if err != nil || snapshot.Total != 1 || snapshot.Items[0]["id"] != "b" {
		t.Fatal("export membership changed", snapshot, err)
	}
	result, err := writeHistoryExport(context.Background(), b.store, accountB, historyQuery{}, "json", b.config.ExportDirectory, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	b.finish(id, nil, result)
	raw, err := os.ReadFile(result.Path)
	if err != nil || !strings.Contains(string(raw), "account B snapshot") || strings.Contains(string(raw), "backdated capture") {
		t.Fatal("mixed export snapshot", string(raw), err)
	}
	current, err := b.store.Query(context.Background(), accountB, historyQuery{})
	if err != nil || current.Total != 1 || current.Items[0]["id"] != "new" {
		t.Fatal("queued mutation lost", current, err)
	}
	a.mu.Lock()
	if job := a.masked()["export"].(historyJob); job.JobID != "" {
		t.Fatal("other account export exposed", job)
	}
	a.mu.Unlock()
	restarted, err := newAccountHistoryManager(home, h, accountB, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.store.Close()
	restarted.mu.Lock()
	job := restarted.masked()["export"].(historyJob)
	restarted.mu.Unlock()
	if job.State != "complete" || job.Path != result.Path {
		t.Fatal("completed export not recovered", job)
	}
}

func TestHistoryKnownRevocationClearsOnlyMatchingVerifiedSession(t *testing.T) {
	status := atomic.Int32{}
	status.Store(200)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/me" {
			json.NewEncoder(w).Encode(map[string]any{"id": "00000000-0000-0000-0000-000000000001"})
			return
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	c.control = server.URL
	a := &browserAccount{core: c}
	request := httptest.NewRequest("GET", "/", nil)
	if err := a.authenticate(request, "old", server.URL); err != nil {
		t.Fatal(err)
	}
	ctx, release, err := a.begin("old", server.URL, true)
	if err != nil {
		t.Fatal(err)
	}
	release()
	_ = ctx
	configureTestHistorySQLite(t, c.home, "00000000-0000-0000-0000-000000000001")
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err := h.manager.forAccount("00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()
	status.Store(401)
	if err = c.syncHistory(context.Background(), m, m.owner, "old", "own", "job"); err == nil {
		t.Fatal("401 accepted")
	}
	if _, err = c.historyIdentity("old"); err == nil {
		t.Fatal("known revoked cache accepted")
	}
	if _, _, err = a.begin("old", server.URL, true); err == nil {
		t.Fatal("browser cached revoked identity accepted")
	}
	if _, err = c.historyIdentity("new"); err != nil {
		t.Fatal(err)
	}
	c.revokeHistorySession("old")
	if _, err = c.historyIdentity("new"); err != nil {
		t.Fatal("stale 401 cleared new identity", err)
	}
}

type historyImportFault struct {
	historyRepository
	calls, failAt int
}

func (s *historyImportFault) Save(ctx context.Context, viewer, scope string, value map[string]any) error {
	s.calls++
	if s.calls == s.failAt {
		return errors.New("[TEST] injected import interruption")
	}
	return s.historyRepository.Save(ctx, viewer, scope, value)
}
func TestHistoryLegacyImportPersistsRowCheckpoint(t *testing.T) {
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err := newHistoryManager(home, h)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		id := strconv.Itoa(i)
		if err = h.save("viewer", map[string]any{"id": id, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "legacy row " + id}, false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = h.db.Exec("DELETE FROM history_meta WHERE key='legacy-imported:'"); err != nil {
		t.Fatal(err)
	}
	fault := &historyImportFault{historyRepository: m.store, failAt: 2}
	m.store = fault
	if err = m.restore(context.Background()); err == nil {
		t.Fatal("injected failure ignored")
	}
	var checkpoint int
	if err = h.db.QueryRow("SELECT CAST(value AS INTEGER) FROM history_meta WHERE key='legacy-import-cursor:'").Scan(&checkpoint); err != nil || checkpoint != 1 {
		t.Fatal("committed import checkpoint lost", checkpoint, err)
	}
	next, err := openHistoryRepository(context.Background(), m.config)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	retry := &historyImportFault{historyRepository: next}
	m.store = retry
	if err = m.restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if retry.calls != 4 {
		t.Fatal("import restarted from row zero", retry.calls)
	}
	result, err := next.Query(context.Background(), "viewer", historyQuery{})
	if err != nil || result.Total != 5 {
		t.Fatal(result, err)
	}
}

func TestHistoryColdInitializationDoesNotBlockInference(t *testing.T) {
	c := newCore(t.TempDir(), "")
	configureTestHistorySQLite(t, c.home, "00000000-0000-0000-0000-000000000001")
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer c.shutdown()
	defer h.db.Close()
	historyPolicyFixture(t, h, "local")
	account := "00000000-0000-0000-0000-000000000001"
	h.manager.childMu.Lock()
	locked := true
	defer func() {
		if locked {
			h.manager.childMu.Unlock()
		}
	}()
	handler := h.capture(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("immediate inference")) }), account, "key")
	request := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"m","input":"hello"}`))
	request.Header.Set("Authorization", "Bearer test")
	before := time.Now()
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if time.Since(before) > time.Second {
		t.Fatal("cold account database blocked inference")
	}
	var pending int
	if err = h.db.QueryRow("SELECT count(*) FROM history_pending WHERE viewer=?", account).Scan(&pending); err != nil || pending != 1 {
		t.Fatal("cold capture was not journaled", pending, err)
	}
	h.manager.childMu.Unlock()
	locked = false
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	job := m.jobs[m.busy]
	m.mu.Unlock()
	if job != nil {
		select {
		case <-job.done:
		case <-time.After(5 * time.Second):
			t.Fatal("background migration stalled")
		}
	}
	m.mu.Lock()
	result, err := m.store.Query(context.Background(), account, historyQuery{})
	m.mu.Unlock()
	if err != nil || result.Total != 1 {
		t.Fatal("cold queued history lost", result, err)
	}
}

func TestHistoryUpgradeFromTrueLegacyJournalKeepsAllRows(t *testing.T) {
	home := t.TempDir()
	account := "00000000-0000-0000-0000-000000000001"
	other := "00000000-0000-0000-0000-000000000002"
	old, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 63; i++ {
		id := fmt.Sprintf("legacy-%03d", i)
		if err = old.save(account, map[string]any{"id": id, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "pre-manager row " + id}, false); err != nil {
			t.Fatal(err)
		}
	}
	if err = old.save(other, map[string]any{"id": "foreign", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "other account remains private"}, false); err != nil {
		t.Fatal(err)
	}
	old.db.Close()
	c := newCore(home, "")
	defer c.shutdown()
	configureTestHistorySQLite(t, home, account)
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	active, err := m.store.Query(context.Background(), account, historyQuery{Q: "pre-manager row", Limit: 100})
	job := m.jobs[m.busy]
	m.mu.Unlock()
	if err != nil || active.Total != 63 {
		t.Fatal("old active records unavailable during upgrade", active, err)
	}
	if job != nil {
		select {
		case <-job.done:
		case <-time.After(5 * time.Second):
			t.Fatal("legacy background migration stalled")
		}
		if job.State != "complete" {
			t.Fatal(job)
		}
	}
	m.mu.Lock()
	final, err := m.store.Query(context.Background(), account, historyQuery{Limit: 100})
	foreign, foreignErr := m.store.Query(context.Background(), other, historyQuery{})
	m.mu.Unlock()
	if err != nil || final.Total != 63 || len(final.Items) != 63 {
		t.Fatal("legacy rows disappeared after cutover", final, err)
	}
	if foreignErr != nil || foreign.Total != 0 {
		t.Fatal("another account copied", foreign, foreignErr)
	}
	var original int
	if err = h.db.QueryRow("SELECT count(*) FROM exchanges").Scan(&original); err != nil || original != 64 {
		t.Fatal("legacy source not preserved", original, err)
	}
}

func TestDockerHistoryJournalAllowsOnlyApprovedDataDirectory(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	c := newCore(t.TempDir(), "")
	if err := c.safeConfigFile("/data/sqlite/chat-history.sqlite"); err == nil {
		t.Fatal("generic configuration boundary widened outside home")
	}
	if err := c.safeHistoryJournalFile("/data/sqlite/chat-history.sqlite"); err != nil {
		t.Fatal("approved Docker journal rejected", err)
	}
	for _, path := range []string{"/data/other/chat-history.sqlite", "/etc/passwd", "/data/sqlite/other.sqlite", "/data/sqlite/chat-history.sqlite-copy"} {
		if err := c.safeHistoryJournalFile(path); err == nil {
			t.Fatal("unexpected history path accepted", path)
		}
	}
}
