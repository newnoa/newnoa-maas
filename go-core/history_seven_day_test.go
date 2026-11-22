package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSQLiteHistoryDefaultsAndLegacyUpgrade(t *testing.T) {
	home := t.TempDir()
	config := defaultHistoryDatabase(home)
	if config.RetentionDays != 7 || len(historyRetentionChoices("sqlite")) != 7 {
		t.Fatalf("SQLite must default to 7 days and offer seven choices: %+v", config)
	}
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	config.RetentionDays = 90
	raw, _ := json.Marshal(config)
	if err = atomicFile(filepath.Join(home, ".newnoa", "history-database.json"), raw); err != nil {
		t.Fatal(err)
	}
	m, err := newHistoryManager(home, h)
	if err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()
	if m.config.RetentionDays != 7 {
		t.Fatal("old SQLite policy not normalized", m.config)
	}
	raw, err = os.ReadFile(m.configPath())
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(raw, &config) != nil || config.RetentionDays != 90 {
		t.Fatal("upgrade rewrote the saved configuration", string(raw))
	}
}

func seedExpiredHistory(t *testing.T, m *historyManager, account, id string, stamp time.Time) {
	t.Helper()
	value := map[string]any{"id": id, "createdAt": stamp.UTC().Format(time.RFC3339Nano), "model": "[TEST] model", "agent": "[TEST] agent", "request": "[TEST] body"}
	// Seed the on-disk format written by older Core versions, bypassing the new ingest cutoff.
	raw, _ := json.Marshal(value)
	s := m.store.(*sqlHistoryRepository)
	_, err := s.db.Exec("INSERT INTO nn_history_records(viewer,scope,id,created,storage_id,caller_id,key_name,payload,metadata,search_text) VALUES(?,'own',?,?,?,'','',?,?,'[TEST] body')", account, id, stamp.UTC().Format("2006-01-02T15:04:05.000000000Z"), id, string(raw), string(raw))
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.journal.db.Exec("INSERT INTO exchanges(account,id,created,model,agent,payload,pending) VALUES(?,?,?,'[TEST] model','[TEST] agent',?,1)", account, id, stamp.UTC().Format(time.RFC3339Nano), string(raw))
	if err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteHistoryMaintenancePhysicallyRemovesCopiesAndPreservesOutboxes(t *testing.T) {
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
	defer m.store.Close()
	now := time.Now().UTC()
	cutoff := now.Add(-7 * 24 * time.Hour)
	seedExpiredHistory(t, m, "viewer", "old", cutoff.Add(-time.Nanosecond))
	seedExpiredHistory(t, m, "viewer", "boundary", cutoff)
	seedExpiredHistory(t, m, "other", "other", cutoff.Add(-time.Hour))
	if _, err = h.db.Exec("INSERT INTO hosted_usage_outbox(request_id,account,model_id,revision,key_id,report_token,payload) VALUES('old','viewer','[TEST] model',1,'[TEST] key','[TEST] token','{}'); INSERT INTO private_usage_outbox(request_id,node_id,payload) VALUES('old','[TEST] node','{}')"); err != nil {
		t.Fatal(err)
	}
	m.owner = "viewer"
	cache := filepath.Join(home, ".newnoa", "history-blocks", "viewer")
	if err = os.MkdirAll(cache, 0700); err != nil {
		t.Fatal(err)
	}
	block := filepath.Join(cache, strings.Repeat("a", 64))
	if err = os.WriteFile(block, []byte("[TEST] cached body"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = m.maintain(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"nn_history_records", "exchanges"} {
		db := h.db.DB
		if table == "nn_history_records" {
			db = m.store.(*sqlHistoryRepository).db
		}
		var count int
		if err = db.QueryRow("SELECT count(*) FROM " + table + " WHERE id='old'").Scan(&count); err != nil || count != 0 {
			t.Fatal("expired body still physically present", table, count, err)
		}
		if err = db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 2 {
			t.Fatal("boundary/other account removed", table, count, err)
		}
	}
	for _, table := range []string{"hosted_usage_outbox", "private_usage_outbox"} {
		var count int
		if err = h.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 1 {
			t.Fatal("settlement outbox lost", table, count, err)
		}
	}
	if _, err = os.Stat(block); !os.IsNotExist(err) {
		t.Fatal("unused body block left", err)
	}
	value := map[string]any{"id": "old", "createdAt": cutoff.Add(-time.Hour).Format(time.RFC3339Nano), "request": "[TEST] replay"}
	if err = m.commit(context.Background(), "viewer", "own", "record", "old", value); err != nil {
		t.Fatal(err)
	}
	if _, err = m.store.Get(context.Background(), "viewer", "own", "old"); err == nil {
		t.Fatal("sync resurrected expired row")
	}
}

func TestHistoryMaintenanceRunsWithoutQueriesAndHonorsBusyExternalPolicy(t *testing.T) {
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
	defer m.store.Close()
	now := time.Now().UTC()
	seedExpiredHistory(t, m, "viewer", "old", now.Add(-8*24*time.Hour))
	m.config.Engine = "postgres"
	if err = m.maintain(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var count int
	h.db.QueryRow("SELECT count(*) FROM exchanges").Scan(&count)
	if count != 1 {
		t.Fatal("SQLite policy touched external account")
	}
	m.config.Engine = "sqlite"
	m.busy = "[TEST] export"
	if err = m.maintain(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	h.db.QueryRow("SELECT count(*) FROM exchanges").Scan(&count)
	if count != 1 {
		t.Fatal("maintenance crossed job lock")
	}
	m.busy = ""
	ticks := make(chan time.Time)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); m.runMaintenance(ctx, ticks, nil) }()
	defer func() { cancel(); <-done }()
	deadline := time.After(3 * time.Second)
	for {
		h.db.QueryRow("SELECT count(*) FROM exchanges").Scan(&count)
		if count == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("startup maintenance needed a query")
		case <-time.After(5 * time.Millisecond):
		}
	}
	seedExpiredHistory(t, m, "viewer", "next", now.Add(-6*24*time.Hour))
	ticks <- now.Add(2 * 24 * time.Hour)
	deadline = time.After(3 * time.Second)
	for {
		h.db.QueryRow("SELECT count(*) FROM exchanges").Scan(&count)
		if count == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("periodic maintenance did not run")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestCoreStartupRetentionAndRestartKeepRecoverySource(t *testing.T) {
	home := t.TempDir()
	account := "00000000-0000-0000-0000-000000000001"
	old, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	expired := map[string]any{"id": "[TEST] expired", "createdAt": time.Now().Add(-8 * 24 * time.Hour).UTC().Format(time.RFC3339Nano), "request": "[TEST] legacy body", "model": "[TEST] model", "agent": "[TEST] agent"}
	if err = old.save(account, expired, true); err != nil {
		t.Fatal(err)
	}
	if _, err = old.db.Exec("INSERT INTO private_usage_outbox(request_id,node_id,payload) VALUES('[TEST] pending','[TEST] node','{}')"); err != nil {
		t.Fatal(err)
	}
	old.db.Close()
	configureTestHistorySQLite(t, home, account)
	for i := 0; i < 2; i++ {
		c := newCore(home, "")
		h, err := c.historyStore()
		if err != nil {
			t.Fatal(err)
		}
		waitHistoryBackfill(t, h)
		var count int
		if err = h.db.QueryRow("SELECT count(*) FROM exchanges").Scan(&count); err != nil || count != 0 {
			t.Fatal("startup/restart retained expired body", i, count, err)
		}
		if err = h.db.QueryRow("SELECT count(*) FROM private_usage_outbox").Scan(&count); err != nil || count != 1 {
			t.Fatal("startup lost settlement report", count, err)
		}
		m, err := h.manager.forAccount(account)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = m.store.Get(context.Background(), account, "own", "[TEST] expired"); err == nil {
			t.Fatal("legacy import resurrected expired record")
		}
		c.shutdown()
		m.store.Close()
		h.db.Close()
	}
	source, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer source.db.Close()
	var count int
	if err = source.db.QueryRow("SELECT count(*) FROM exchanges").Scan(&count); err != nil || count != 1 {
		t.Fatal("recovery source destroyed", count, err)
	}
}

func TestSQLiteMigrationFiltersOldRecoveryRecordsAndPendingReplay(t *testing.T) {
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
	defer func() { m.store.Close() }()
	now := time.Now().UTC()
	seedExpiredHistory(t, m, "viewer", "[TEST] expired", now.Add(-8*24*time.Hour))
	seedExpiredHistory(t, m, "viewer", "[TEST] recent", now)
	// Legacy view represents a preserved migration source; cutover must count only eligible rows.
	source, err := openLegacyHistoryRepository(h)
	if err != nil {
		t.Fatal(err)
	}
	m.store.Close()
	m.store = source
	if _, err = h.db.Exec("INSERT OR IGNORE INTO history_viewers(viewer) VALUES('viewer')"); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"id": "[TEST] replay", "createdAt": now.Add(-8 * 24 * time.Hour).Format(time.RFC3339Nano), "request": "[TEST] old replay"})
	if _, err = h.db.Exec("INSERT INTO history_pending(viewer,scope,kind,id,payload) VALUES('viewer','own','record','[TEST] replay',?)", string(raw)); err != nil {
		t.Fatal(err)
	}
	target := m.config
	target.SQLiteDirectory = filepath.Join(home, "new-database")
	if err = m.migrate(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = m.store.(*sqlHistoryRepository).db.QueryRow("SELECT count(*) FROM nn_history_records").Scan(&count); err != nil || count != 1 {
		t.Fatal("migration copied expired data", count, err)
	}
	if err = h.db.QueryRow("SELECT count(*) FROM exchanges").Scan(&count); err != nil || count != 2 {
		t.Fatal("migration deleted recovery source", count, err)
	}
}

func TestHistoryMigrationPinsSevenDaySnapshotAcrossAdvancingTime(t *testing.T) {
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
	defer func() { m.store.Close() }()
	// At migration start this row was inside retention; wall time has since advanced two hours.
	now := time.Now().UTC()
	stamp := now.Add(-7*24*time.Hour - time.Hour)
	startedAt := stamp.Add(7*24*time.Hour - time.Hour)
	seedExpiredHistory(t, m, "viewer", "[TEST] crossed-cutoff", stamp)
	if _, err = h.db.Exec("INSERT INTO history_viewers(viewer) VALUES('viewer')"); err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), historyRetentionContextKey{}, startedAt)
	target := m.config
	target.SQLiteDirectory = filepath.Join(home, "migration-crossing-cutoff")
	if err = m.migrate(ctx, target); err != nil {
		t.Fatal("migration lost its initial retention snapshot", err)
	}
	s := m.store.(*sqlHistoryRepository)
	var count int
	if err = s.db.QueryRow("SELECT count(*) FROM nn_history_records WHERE id='[TEST] crossed-cutoff'").Scan(&count); err != nil || count != 1 {
		t.Fatal("moving cutoff skipped snapshot row", count, err)
	}
	if _, err = s.Get(context.Background(), "viewer", "own", "[TEST] crossed-cutoff"); err == nil {
		t.Fatal("normal reads retained pinned migration cutoff")
	}
	if err = m.maintain(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if err = s.db.QueryRow("SELECT count(*) FROM nn_history_records").Scan(&count); err != nil || count != 0 {
		t.Fatal("post-migration cleanup retained expired snapshot", count, err)
	}
}

func TestHistoryDatabaseOptInAndPersistedSQLiteRetention(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	coordinator, err := newHistoryCoordinator(home, h)
	if err != nil {
		t.Fatal(err)
	}
	h.manager = coordinator
	const account = "00000000-0000-0000-0000-000000000001"
	if off, err := coordinator.accountRecordingDisabled(account); err != nil || !off {
		t.Fatal("uninitialized account must not capture history", off, err)
	}
	m, err := coordinator.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	if m.config.Engine != "off" || m.config.RetentionDays != 7 {
		t.Fatalf("fresh config: %+v", m.config)
	}
	if _, err = os.Stat(m.configPath()); !os.IsNotExist(err) {
		t.Fatal("default wrote a user config", err)
	}
	target := defaultHistoryDatabase(home)
	target.RetentionDays = 2
	if err = m.migrate(ctx, target); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(m.configPath())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	seedExpiredHistory(t, m, account, "[TEST] old", now.Add(-49*time.Hour))
	seedExpiredHistory(t, m, account, "[TEST] recent", now.Add(-47*time.Hour))
	m.store.Close()
	m, err = newAccountHistoryManager(home, h, account, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { m.store.Close() }()
	after, err := os.ReadFile(m.configPath())
	if err != nil || string(after) != string(before) || m.config != target {
		t.Fatal("upgrade rewrote saved config", err)
	}
	s := m.store.(*sqlHistoryRepository)
	for table, db := range map[string]*sql.DB{"nn_history_records": s.db, "exchanges": h.db.DB} {
		var count int
		if err = db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 1 {
			t.Fatal(table, count, err)
		}
	}
	seedExpiredHistory(t, m, account, "[TEST] expired before even the next hourly run", now.Add(-72*time.Hour))
	if _, err = m.store.Get(ctx, account, "own", "[TEST] expired before even the next hourly run"); err == nil {
		t.Fatal("expired record is visible")
	}
	if err = m.maintain(ctx, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = s.db.QueryRow("SELECT count(*) FROM nn_history_records").Scan(&count); err != nil || count != 0 {
		t.Fatal("hourly maintenance ignored selected retention", count, err)
	}
	seedExpiredHistory(t, m, account, "[TEST] do not resurrect on extension", now.Add(-72*time.Hour))
	if _, err = h.db.Exec("INSERT INTO history_sync_cursors(viewer,scope,engine,cursor) VALUES(?,'own',?,'9')", account, m.cursorEngine()); err != nil {
		t.Fatal(err)
	}
	seedExpiredHistory(t, m, account, "[TEST] shorten policy on save", now.Add(-36*time.Hour))
	target.RetentionDays = 7
	if err = m.migrate(ctx, target); err != nil {
		t.Fatal(err)
	}
	if _, err = m.store.Get(ctx, account, "own", "[TEST] do not resurrect on extension"); err == nil {
		t.Fatal("longer retention resurrected expired data")
	}
	target.RetentionDays = 1
	if err = m.migrate(ctx, target); err != nil {
		t.Fatal(err)
	}
	s = m.store.(*sqlHistoryRepository)
	if err = s.db.QueryRow("SELECT count(*) FROM nn_history_records").Scan(&count); err != nil || count != 0 {
		t.Fatal("shorter retention did not physically delete on save", count, err)
	}
	if _, err = h.db.Exec("UPDATE history_sync_cursors SET cursor='12' WHERE viewer=? AND scope='own' AND engine=?", account, m.cursorEngine()); err != nil {
		t.Fatal(err)
	}
	seedExpiredHistory(t, m, account, "[TEST] expired while disabled", now.Add(-36*time.Hour))
	if _, err = m.disableRecording(); err != nil {
		t.Fatal(err)
	}
	target.RetentionDays = 7
	if err = m.migrate(ctx, target); err != nil {
		t.Fatal(err)
	}
	if _, err = m.store.Get(ctx, account, "own", "[TEST] expired while disabled"); err == nil {
		t.Fatal("reopening with longer retention resurrected expired data")
	}
	var cursor string
	if err = h.db.QueryRow("SELECT cursor FROM history_sync_cursors WHERE viewer=? AND scope='own' AND engine=?", account, m.cursorEngine()).Scan(&cursor); err != nil || cursor != "12" {
		t.Fatal("retention changes rewound the sync cursor", cursor, err)
	}
}
