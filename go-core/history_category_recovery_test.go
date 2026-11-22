package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func categoryRecoveryValue(id, category string) map[string]any {
	return map[string]any{"id": id, "modelCategory": category, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "[TEST] recovery", "agent": "[TEST]", "request": "[TEST] exact 中文 request", "response": "data: exact\r\n\n", "pointsConsumed": nil}
}

func categoryRecoveryExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func categoryRecoveryCount(t *testing.T, db *sql.DB, query string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(query).Scan(&got); err != nil || got != want {
		t.Fatalf("%s: count=%d want=%d error=%v", query, got, want, err)
	}
}

func TestCategoryRecoverySaveAndDeleteRollbackPreserveIdentityAndBody(t *testing.T) {
	ctx := context.Background()
	r, err := openSQLHistory(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.(*sqlHistoryRepository)
	value := categoryRecoveryValue("rollback", "image")
	categoryRecoveryExec(t, s.db, "CREATE TRIGGER recovery_fail_insert BEFORE INSERT ON nn_history_image_records BEGIN SELECT RAISE(ABORT,'[TEST] leaf write failed');END")
	if err = s.Save(ctx, "viewer", "own", value); err == nil {
		t.Fatal("leaf fault must reject publication")
	}
	for _, table := range []string{"nn_history_record_locations", "nn_history_image_records", "nn_history_image_body_blocks", "nn_history_image_body_refs"} {
		categoryRecoveryCount(t, s.db, "SELECT count(*) FROM "+table, 0)
	}
	categoryRecoveryExec(t, s.db, "DROP TRIGGER recovery_fail_insert")
	if err = s.Save(ctx, "viewer", "own", value); err != nil {
		t.Fatal(err)
	}
	categoryRecoveryExec(t, s.db, "CREATE TRIGGER recovery_fail_delete BEFORE UPDATE ON nn_history_image_records WHEN NEW.deleted=1 BEGIN SELECT RAISE(ABORT,'[TEST] tombstone failed');END")
	if err = s.Delete(ctx, "viewer", "own", "rollback"); err == nil {
		t.Fatal("failed tombstone must not drop body references")
	}
	got, err := s.Get(ctx, "viewer", "own", "rollback")
	if err != nil || got["request"] != value["request"] {
		t.Fatalf("failed delete damaged body: %v %v", got, err)
	}
	categoryRecoveryExec(t, s.db, "DROP TRIGGER recovery_fail_delete")
	categoryRecoveryExec(t, s.db, "UPDATE nn_history_record_locations SET model_category='invalid'")
	if _, err = s.Get(ctx, "viewer", "own", "rollback"); err == nil {
		t.Fatal("invalid location must not guess a leaf")
	}
	if err = s.Delete(ctx, "viewer", "own", "rollback"); err == nil {
		t.Fatal("invalid location must not claim successful cleanup")
	}
	categoryRecoveryExec(t, s.db, "UPDATE nn_history_record_locations SET model_category='image'")
	invalid := categoryRecoveryValue("invalid", "video")
	if err = s.Save(ctx, "viewer", "own", invalid); err == nil {
		t.Fatal("unsupported category accepted")
	}
	if err = s.Delete(ctx, "viewer", "own", "absent"); err != nil {
		t.Fatal("unknown delete must be idempotent", err)
	}
}

func TestCategoryRecoveryRenamedLegacySourceReopensWithoutResurrectingDeletedRecord(t *testing.T) {
	ctx := context.Background()
	config := defaultHistoryDatabase(t.TempDir())
	r, err := openSQLHistoryTable(ctx, config, "nn_history_records")
	if err != nil {
		t.Fatal(err)
	}
	value := categoryRecoveryValue("renamed", "image")
	if err = r.Save(ctx, "viewer", "others", value); err != nil {
		t.Fatal(err)
	}
	source := r.(*sqlHistoryRepository)
	categoryRecoveryExec(t, source.db, "ALTER TABLE nn_history_records RENAME TO nn_history_records_recovery")
	r.Close()
	r, err = openSQLHistory(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(ctx, "viewer", "others", "renamed")
	if err != nil || got["request"] != value["request"] || got["pointsConsumed"] != nil {
		t.Fatalf("rename recovery lost exact body/snapshot: %v %v", got, err)
	}
	if err = r.Delete(ctx, "viewer", "others", "renamed"); err != nil {
		t.Fatal(err)
	}
	r.Close()
	r, err = openSQLHistory(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err = r.Save(ctx, "viewer", "others", value); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Get(ctx, "viewer", "others", "renamed"); err == nil {
		t.Fatal("replay after reopening resurrected a deleted record")
	}
	categoryRecoveryCount(t, r.(*sqlHistoryRepository).db, "SELECT count(*) FROM sqlite_master WHERE name='nn_history_records_recovery'", 0)
}

func TestCategoryRecoveryLegacyConflictingLeafKeepsSourceAndRollsBackCheckpoint(t *testing.T) {
	ctx := context.Background()
	config := defaultHistoryDatabase(t.TempDir())
	r, err := openSQLHistoryTable(ctx, config, "nn_history_records")
	if err != nil {
		t.Fatal(err)
	}
	value := categoryRecoveryValue("conflict", "text")
	if err = r.Save(ctx, "viewer", "own", value); err != nil {
		t.Fatal(err)
	}
	r.Close()
	r, err = openSQLHistoryTable(ctx, config, "nn_history_text_records")
	if err != nil {
		t.Fatal(err)
	}
	other := categoryRecoveryValue("conflict", "text")
	other["request"] = "[TEST] conflicting target must never win"
	if err = r.Save(ctx, "viewer", "own", other); err != nil {
		t.Fatal(err)
	}
	r.Close()
	if repo, err := openSQLHistory(ctx, config); err == nil {
		repo.Close()
		t.Fatal("mismatched target must stop the read barrier")
	}
	db, err := sql.Open("sqlite", filepath.Join(config.SQLiteDirectory, "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	categoryRecoveryCount(t, db, "SELECT count(*) FROM nn_history_records", 1)
	categoryRecoveryCount(t, db, "SELECT count(*) FROM nn_history_record_locations", 0)
	categoryRecoveryExec(t, db, "DELETE FROM nn_history_text_records")
	db.Close()
	r, err = openSQLHistory(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := r.Get(ctx, "viewer", "own", "conflict")
	if err != nil || got["request"] != value["request"] {
		t.Fatalf("source not recovered after repairing conflict: %v %v", got, err)
	}
}

func TestCategoryRecoveryMigrationRejectsDamagedSourceOrCommittedCheckpoint(t *testing.T) {
	for _, fault := range []string{"metadata", "missing-block", "target-body-write", "location-write", "checkpoint-metadata", "checkpoint-missing-leaf"} {
		t.Run(fault, func(t *testing.T) {
			ctx := context.Background()
			config := defaultHistoryDatabase(t.TempDir())
			source, err := openSQLHistoryTable(ctx, config, "nn_history_records")
			if err != nil {
				t.Fatal(err)
			}
			value := categoryRecoveryValue("damaged", "image")
			if err = source.Save(ctx, "viewer", "own", value); err != nil {
				t.Fatal(err)
			}
			source.Close()
			target, err := openSQLHistoryTable(ctx, config, "nn_history_image_records")
			if err != nil {
				t.Fatal(err)
			}
			if fault == "checkpoint-metadata" {
				if err = target.Save(ctx, "viewer", "own", value); err != nil {
					t.Fatal(err)
				}
			}
			target.Close()
			db, err := sql.Open("sqlite", filepath.Join(config.SQLiteDirectory, "history.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var originalMetadata string
			if err = db.QueryRow("SELECT metadata FROM nn_history_records").Scan(&originalMetadata); err != nil {
				t.Fatal(err)
			}
			var block historyBodyBlock
			switch fault {
			case "metadata":
				categoryRecoveryExec(t, db, "UPDATE nn_history_records SET metadata='{' ")
			case "missing-block":
				if err = db.QueryRow("SELECT hash,raw_size,data FROM nn_history_body_blocks LIMIT 1").Scan(&block.Hash, &block.RawSize, &block.Data); err != nil {
					t.Fatal(err)
				}
				categoryRecoveryExec(t, db, "DELETE FROM nn_history_body_blocks WHERE hash=?", block.Hash)
			case "target-body-write":
				categoryRecoveryExec(t, db, "CREATE TRIGGER recovery_body_fail BEFORE INSERT ON nn_history_image_body_blocks BEGIN SELECT RAISE(ABORT,'[TEST] body write failed');END")
			case "location-write":
				categoryRecoveryExec(t, db, "CREATE TABLE nn_history_record_locations(viewer TEXT,scope TEXT,id TEXT,model_category TEXT,PRIMARY KEY(viewer,scope,id));CREATE TRIGGER recovery_claim_fail BEFORE INSERT ON nn_history_record_locations BEGIN SELECT RAISE(ABORT,'[TEST] identity write failed');END")
			default:
				categoryRecoveryExec(t, db, "CREATE TABLE nn_history_record_locations(viewer TEXT,scope TEXT,id TEXT,model_category TEXT,PRIMARY KEY(viewer,scope,id));INSERT INTO nn_history_record_locations VALUES('viewer','own','damaged','image')")
				if fault == "checkpoint-metadata" {
					categoryRecoveryExec(t, db, "UPDATE nn_history_image_records SET metadata='{}'")
				}
			}
			if repo, err := openSQLHistory(ctx, config); err == nil {
				repo.Close()
				t.Fatal("damaged migration escaped read barrier", fault)
			}
			categoryRecoveryCount(t, db, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='nn_history_records'", 1)
			categoryRecoveryCount(t, db, "SELECT count(*) FROM nn_history_records", 1)
			switch fault {
			case "metadata":
				categoryRecoveryExec(t, db, "UPDATE nn_history_records SET metadata=?", originalMetadata)
			case "missing-block":
				categoryRecoveryExec(t, db, "INSERT INTO nn_history_body_blocks(viewer,hash,raw_size,data) VALUES('viewer',?,?,?)", block.Hash, block.RawSize, block.Data)
			case "target-body-write":
				categoryRecoveryExec(t, db, "DROP TRIGGER recovery_body_fail")
			case "location-write":
				categoryRecoveryExec(t, db, "DROP TRIGGER recovery_claim_fail")
			case "checkpoint-metadata":
				categoryRecoveryExec(t, db, "UPDATE nn_history_image_records SET metadata=?", originalMetadata)
			case "checkpoint-missing-leaf":
				categoryRecoveryExec(t, db, "DELETE FROM nn_history_record_locations")
			}
			repo, err := openSQLHistory(ctx, config)
			if err != nil {
				t.Fatal("repair could not resume", err)
			}
			defer repo.Close()
			got, err := repo.Get(ctx, "viewer", "own", "damaged")
			if err != nil || got["request"] != value["request"] || got["response"] != value["response"] {
				t.Fatalf("repaired migration changed body: %v %v", got, err)
			}
		})
	}
}

const categoryRecoveryExchangeSchema = "(account TEXT NOT NULL,id TEXT NOT NULL,created TEXT NOT NULL,model TEXT NOT NULL,agent TEXT NOT NULL,payload TEXT NOT NULL,pending INTEGER NOT NULL DEFAULT 0,deleted_at TEXT,PRIMARY KEY(account,id))"

func categoryRecoveryLegacyJournal(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "legacy.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	categoryRecoveryExec(t, db, "CREATE TABLE exchanges"+categoryRecoveryExchangeSchema)
	return db
}

func TestCategoryRecoveryJournalConflictAndInvalidPayloadRemainRepairable(t *testing.T) {
	for _, fault := range []string{"target-conflict", "invalid-json", "location-write"} {
		t.Run(fault, func(t *testing.T) {
			db := categoryRecoveryLegacyJournal(t)
			value := categoryRecoveryValue("legacy", "image")
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			categoryRecoveryExec(t, db, "INSERT INTO exchanges(rowid,account,id,created,model,agent,payload,pending) VALUES(91,'viewer','legacy',?,'[TEST] model','[TEST]',?,1)", value["createdAt"], string(raw))
			switch fault {
			case "target-conflict":
				categoryRecoveryExec(t, db, "CREATE TABLE image_exchanges"+categoryRecoveryExchangeSchema)
				categoryRecoveryExec(t, db, "INSERT INTO image_exchanges SELECT account,id,created,model,agent,'{}',pending,deleted_at FROM exchanges")
			case "invalid-json":
				categoryRecoveryExec(t, db, "UPDATE exchanges SET payload='{' ")
			case "location-write":
				categoryRecoveryExec(t, db, "CREATE TABLE history_exchange_locations(seq INTEGER PRIMARY KEY AUTOINCREMENT,account TEXT NOT NULL,id TEXT NOT NULL,model_category TEXT NOT NULL,UNIQUE(account,id));CREATE TRIGGER recovery_locations_fail BEFORE INSERT ON history_exchange_locations BEGIN SELECT RAISE(ABORT,'[TEST] location full');END")
			}
			if err = splitHistoryJournal(db); err == nil {
				t.Fatal("migration fault was ignored")
			}
			categoryRecoveryCount(t, db, "SELECT count(*) FROM exchanges", 1)
			categoryRecoveryCount(t, db, "SELECT count(*) FROM history_exchange_locations", 0)
			switch fault {
			case "target-conflict":
				categoryRecoveryExec(t, db, "DELETE FROM image_exchanges")
			case "invalid-json":
				categoryRecoveryExec(t, db, "UPDATE exchanges SET payload=?", string(raw))
			case "location-write":
				categoryRecoveryCount(t, db, "SELECT count(*) FROM image_exchanges", 0)
				categoryRecoveryExec(t, db, "DROP TRIGGER recovery_locations_fail")
			}
			if err = splitHistoryJournal(db); err != nil {
				t.Fatal(err)
			}
			var restored string
			var seq, pending int
			if err = db.QueryRow("SELECT rowid,payload,pending FROM exchanges WHERE account='viewer' AND id='legacy'").Scan(&seq, &restored, &pending); err != nil || restored != string(raw) || seq != 91 || pending != 1 {
				t.Fatalf("recovery lost exact payload/sequence: %d %d %s %v", seq, pending, restored, err)
			}
			if _, err = db.Exec("UPDATE exchanges SET id='changed'"); err == nil {
				t.Fatal("compatibility view allowed identity changes")
			}
			if _, err = db.Exec("UPDATE exchanges SET payload=?", `{"modelCategory":"text"}`); err == nil {
				t.Fatal("compatibility view allowed category changes")
			}
		})
	}
}

func TestCategoryRecoveryJournalFailedViewTriggerCreationKeepsLegacyTable(t *testing.T) {
	db := categoryRecoveryLegacyJournal(t)
	categoryRecoveryExec(t, db, "INSERT INTO exchanges VALUES('viewer','one','2026-11-17T00:00:00Z','[TEST]','[TEST]','{\"modelCategory\":\"text\"}',0,NULL)")
	categoryRecoveryExec(t, db, "CREATE TABLE blocker(id INTEGER);CREATE TRIGGER exchanges_insert AFTER INSERT ON blocker BEGIN SELECT 1;END")
	if err := splitHistoryJournal(db); err == nil {
		t.Fatal("trigger-name conflict was ignored")
	}
	var kind string
	if err := db.QueryRow("SELECT type FROM sqlite_master WHERE name='exchanges'").Scan(&kind); err != nil || kind != "table" {
		t.Fatalf("failed DDL destroyed recovery table: %q %v", kind, err)
	}
	categoryRecoveryCount(t, db, "SELECT count(*) FROM exchanges", 1)
	categoryRecoveryExec(t, db, "DROP TRIGGER exchanges_insert")
	if err := splitHistoryJournal(db); err != nil {
		t.Fatal(err)
	}
	categoryRecoveryExec(t, db, "DELETE FROM exchanges WHERE account='viewer' AND id='one'")
	if err := splitHistoryJournal(db); err != nil {
		t.Fatal(err)
	}
	categoryRecoveryCount(t, db, "SELECT count(*) FROM exchanges", 0)
}

func TestCategoryRecoveryJournalSequenceAndCountConflictsDoNotDiscardSource(t *testing.T) {
	for _, fault := range []string{"sequence", "extra-leaf"} {
		t.Run(fault, func(t *testing.T) {
			db := categoryRecoveryLegacyJournal(t)
			categoryRecoveryExec(t, db, "INSERT INTO exchanges(rowid,account,id,created,model,agent,payload,pending,deleted_at) VALUES(8,'viewer','source','2026-11-17T00:00:00Z','[TEST]','[TEST]','{\"modelCategory\":\"image\"}',0,NULL)")
			if fault == "sequence" {
				categoryRecoveryExec(t, db, "CREATE TABLE history_exchange_locations(seq INTEGER PRIMARY KEY AUTOINCREMENT,account TEXT NOT NULL,id TEXT NOT NULL,model_category TEXT NOT NULL,UNIQUE(account,id));INSERT INTO history_exchange_locations VALUES(8,'other','occupied','text')")
			} else {
				categoryRecoveryExec(t, db, "CREATE TABLE text_exchanges"+categoryRecoveryExchangeSchema)
				categoryRecoveryExec(t, db, "INSERT INTO text_exchanges VALUES('other','orphan','2026-11-17T00:00:00Z','[TEST]','[TEST]','{}',0,NULL)")
			}
			if err := splitHistoryJournal(db); err == nil {
				t.Fatal("journal identity conflict ignored")
			}
			categoryRecoveryCount(t, db, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='exchanges'", 1)
			categoryRecoveryCount(t, db, "SELECT count(*) FROM exchanges", 1)
			if fault == "sequence" {
				categoryRecoveryCount(t, db, "SELECT count(*) FROM image_exchanges", 0)
				categoryRecoveryExec(t, db, "DELETE FROM history_exchange_locations WHERE account='other'")
			} else {
				categoryRecoveryExec(t, db, "DELETE FROM text_exchanges WHERE account='other'")
			}
			if err := splitHistoryJournal(db); err != nil {
				t.Fatal("identity repair failed", err)
			}
			categoryRecoveryCount(t, db, "SELECT count(*) FROM exchanges WHERE rowid=8 AND id='source'", 1)
		})
	}
}

func TestCategoryRecoveryPendingWritesAndUpgradeRollbackAtomically(t *testing.T) {
	ctx := context.Background()
	for _, fault := range []string{"entity-insert", "queue-reference"} {
		t.Run(fault, func(t *testing.T) {
			h, err := openChatHistory(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer h.db.Close()
			categoryRecoveryExec(t, h.db.DB, historyManagerSchema)
			if err = initializeCategoryPending(h.db.DB); err != nil {
				t.Fatal(err)
			}
			trigger := "CREATE TRIGGER recovery_pending_fail BEFORE INSERT ON history_image_pending_entities BEGIN SELECT RAISE(ABORT,'[TEST] entity failed');END"
			if fault == "queue-reference" {
				trigger = "CREATE TRIGGER recovery_pending_fail BEFORE UPDATE ON history_pending BEGIN SELECT RAISE(ABORT,'[TEST] reference failed');END"
			}
			categoryRecoveryExec(t, h.db.DB, trigger)
			value := categoryRecoveryValue("pending", "image")
			if _, err = queueHistoryEvent(ctx, h.db.DB, "viewer", "own", "record", "pending", value); err == nil {
				t.Fatal("faulted pending transaction succeeded")
			}
			categoryRecoveryCount(t, h.db.DB, "SELECT count(*) FROM history_pending", 0)
			categoryRecoveryCount(t, h.db.DB, "SELECT count(*) FROM history_image_pending_entities", 0)
			raw, _ := json.Marshal(value)
			categoryRecoveryExec(t, h.db.DB, "INSERT INTO history_pending(seq,viewer,scope,kind,id,payload) VALUES(77,'viewer','own','record','pending',?)", string(raw))
			if err = initializeCategoryPending(h.db.DB); err == nil {
				t.Fatal("faulted upgrade succeeded")
			}
			categoryRecoveryCount(t, h.db.DB, "SELECT count(*) FROM history_image_pending_entities", 0)
			var stored string
			if err = h.db.QueryRow("SELECT payload FROM history_pending WHERE seq=77").Scan(&stored); err != nil || stored != string(raw) {
				t.Fatal("failed upgrade changed source", err)
			}
			categoryRecoveryExec(t, h.db.DB, "DROP TRIGGER recovery_pending_fail")
			if err = initializeCategoryPending(h.db.DB); err != nil {
				t.Fatal(err)
			}
			if err = h.db.QueryRow("SELECT payload FROM history_pending WHERE seq=77").Scan(&stored); err != nil {
				t.Fatal(err)
			}
			resolved, err := resolveHistoryEvent(ctx, h.db.DB, 77, stored)
			if err != nil || resolved != string(raw) {
				t.Fatalf("pending retry lost exact event %v", err)
			}
			categoryRecoveryExec(t, h.db.DB, "DELETE FROM history_pending WHERE seq=77")
			categoryRecoveryCount(t, h.db.DB, "SELECT count(*) FROM history_image_pending_entities", 0)
		})
	}
}

func TestCategoryRecoveryPendingOwnerDrainDoesNotLoseBrokenOrForeignEvents(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	configureTestHistorySQLite(t, home, "alice")
	m, err := newAccountHistoryManager(home, h, "alice", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()
	foreign := categoryRecoveryValue("foreign", "text")
	if _, err = queueHistoryEvent(ctx, h.db.DB, "bob", "own", "record", "foreign", foreign); err != nil {
		t.Fatal(err)
	}
	value := categoryRecoveryValue("retry", "image")
	seq, err := queueHistoryEvent(ctx, h.db.DB, "alice", "own", "record", "retry", value)
	if err != nil {
		t.Fatal(err)
	}
	var reference, raw string
	if err = h.db.QueryRow("SELECT payload FROM history_pending WHERE seq=?", seq).Scan(&reference); err != nil {
		t.Fatal(err)
	}
	if err = h.db.QueryRow("SELECT payload FROM history_image_pending_entities WHERE seq=?", seq).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	categoryRecoveryExec(t, h.db.DB, "DELETE FROM history_image_pending_entities WHERE seq=?", seq)
	if _, err = m.drainOnePending(ctx); err == nil {
		t.Fatal("missing event entity must block acknowledgement")
	}
	categoryRecoveryCount(t, h.db.DB, "SELECT count(*) FROM history_pending", 2)
	categoryRecoveryExec(t, h.db.DB, "INSERT INTO history_image_pending_entities(seq,payload) VALUES(?,?)", seq, raw)
	if more, err := m.drainOnePending(ctx); err != nil || !more {
		t.Fatal("repaired owner event did not drain", err)
	}
	if more, err := m.drainOnePending(ctx); err != nil || more {
		t.Fatal("owner drain consumed a foreign event", err)
	}
	categoryRecoveryCount(t, h.db.DB, "SELECT count(*) FROM history_pending WHERE viewer='bob'", 1)
	got, err := m.store.Get(ctx, "alice", "own", "retry")
	if err != nil || got["request"] != value["request"] {
		t.Fatal("retry did not restore body", err)
	}
	for _, invalid := range []string{"{", `{"categoryRef":"video"}`} {
		if _, err = resolveHistoryEvent(ctx, h.db.DB, seq, invalid); err == nil {
			t.Fatal("invalid pending reference accepted", invalid)
		}
	}
	if got, err := resolveHistoryEvent(ctx, h.db.DB, seq, `{"kind":"delete"}`); err != nil || got != `{"kind":"delete"}` {
		t.Fatal("non-record legacy event changed", got, err)
	}
	if strings.Contains(reference, "exact") || !strings.Contains(reference, "categoryRef") {
		t.Fatal("shared queue reference leaked body", reference)
	}
	if _, err = queueHistoryEvent(ctx, h.db.DB, "alice", "own", "record", "bad", map[string]any{"request": make(chan int)}); err == nil {
		t.Fatal("unencodable event was queued")
	}
	categoryRecoveryCount(t, h.db.DB, "SELECT count(*) FROM history_pending", 1)
}

func TestCategoryRecoveryPendingSchemaFailuresAreExplicit(t *testing.T) {
	for _, statement := range []string{
		"CREATE VIEW history_text_pending_entities AS SELECT 1 AS seq,'{}' AS payload",
		"CREATE VIEW history_pending AS SELECT 1 AS seq,'record' AS kind,'{}' AS payload",
		"CREATE TABLE history_pending(seq INTEGER PRIMARY KEY,payload TEXT)",
		"CREATE TABLE history_pending(seq INTEGER PRIMARY KEY,kind TEXT,payload TEXT);INSERT INTO history_pending VALUES(1,'record','{')",
		"CREATE TABLE history_pending(seq TEXT PRIMARY KEY,kind TEXT,payload TEXT);INSERT INTO history_pending VALUES('not-a-sequence','record','{}')",
		"CREATE TABLE history_pending(seq INTEGER PRIMARY KEY,kind TEXT,payload TEXT);INSERT INTO history_pending VALUES(1,'record','[]')",
	} {
		t.Run(fmt.Sprint(len(statement)), func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "pending.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			categoryRecoveryExec(t, db, statement)
			if err = initializeCategoryPending(db); err == nil {
				t.Fatal("invalid pending schema/payload accepted")
			}
		})
	}
}

func TestCategoryRecoveryJournalMalformedSchemasPreserveOriginalRows(t *testing.T) {
	for _, fault := range []string{"missing-journal", "missing-column", "null-payload", "null-target", "bad-leaf-schema", "update-trigger-conflict"} {
		t.Run(fault, func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "schema.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			switch fault {
			case "missing-journal":
			case "missing-column":
				categoryRecoveryExec(t, db, "CREATE TABLE exchanges(account TEXT,id TEXT,payload TEXT);INSERT INTO exchanges VALUES('viewer','one','{}')")
			case "null-payload":
				categoryRecoveryExec(t, db, "CREATE TABLE exchanges(account TEXT,id TEXT,created TEXT,model TEXT,agent TEXT,payload TEXT,pending INTEGER,deleted_at TEXT);INSERT INTO exchanges VALUES('viewer','one','2026-11-17T00:00:00Z','[TEST]','[TEST]',NULL,0,NULL)")
			default:
				categoryRecoveryExec(t, db, "CREATE TABLE exchanges"+categoryRecoveryExchangeSchema)
				categoryRecoveryExec(t, db, "INSERT INTO exchanges VALUES('viewer','one','2026-11-17T00:00:00Z','[TEST]','[TEST]','{\"modelCategory\":\"image\"}',0,NULL)")
				if fault == "bad-leaf-schema" {
					categoryRecoveryExec(t, db, "CREATE TABLE text_exchanges(account TEXT)")
				} else if fault == "null-target" {
					categoryRecoveryExec(t, db, "CREATE TABLE image_exchanges(account TEXT,id TEXT,created TEXT,model TEXT,agent TEXT,payload TEXT,pending INTEGER,deleted_at TEXT,PRIMARY KEY(account,id));INSERT INTO image_exchanges SELECT account,id,created,model,agent,NULL,pending,deleted_at FROM exchanges")
				} else {
					categoryRecoveryExec(t, db, "CREATE TABLE blocker(id INTEGER);CREATE TRIGGER exchanges_update AFTER INSERT ON blocker BEGIN SELECT 1;END")
				}
			}
			if err = splitHistoryJournal(db); err == nil {
				t.Fatal("malformed journal accepted", fault)
			}
			if fault != "missing-journal" {
				categoryRecoveryCount(t, db, "SELECT count(*) FROM exchanges", 1)
				categoryRecoveryCount(t, db, "SELECT count(*) FROM sqlite_master WHERE name='exchanges' AND type='table'", 1)
			}
		})
	}
}

func TestCategoryRecoveryCancelledQueueAndInvalidImageLeafFailExplicitly(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	categoryRecoveryExec(t, h.db.DB, historyManagerSchema)
	if err = initializeCategoryPending(h.db.DB); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = queueHistoryEvent(ctx, h.db.DB, "viewer", "own", "record", "cancelled", categoryRecoveryValue("cancelled", "text")); err == nil {
		t.Fatal("cancelled queue write succeeded")
	}
	categoryRecoveryCount(t, h.db.DB, "SELECT count(*) FROM history_pending", 0)
	categoryRecoveryExec(t, h.db.DB, "DROP TABLE history_pending")
	if _, err = queueHistoryEvent(context.Background(), h.db.DB, "viewer", "own", "delete", "missing-table", map[string]any{}); err == nil {
		t.Fatal("failed queue INSERT succeeded")
	}
	config := defaultHistoryDatabase(t.TempDir())
	r, err := openSQLHistoryTable(context.Background(), config, "nn_history_text_records")
	if err != nil {
		t.Fatal(err)
	}
	s := r.(*sqlHistoryRepository)
	categoryRecoveryExec(t, s.db, "CREATE VIEW nn_history_image_records AS SELECT 1 AS id")
	r.Close()
	if repo, err := openSQLHistory(context.Background(), config); err == nil {
		repo.Close()
		t.Fatal("unusable image leaf must prevent partially initialized repository")
	}
}

func TestCategoryRecoveryLegacyCleanupResumesAfterInterruptedBatch(t *testing.T) {
	ctx := context.Background()
	r, err := openSQLHistory(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.(*sqlHistoryRepository)
	for i := 0; i < 300; i++ {
		if err = s.Save(ctx, "viewer", "own", categoryRecoveryValue(fmt.Sprint(i), "text")); err != nil {
			t.Fatal(err)
		}
	}
	categoryRecoveryExec(t, s.db, "CREATE TABLE nn_history_records_recovery AS SELECT * FROM nn_history_records")
	categoryRecoveryExec(t, s.db, "CREATE TRIGGER cleanup_interrupted BEFORE DELETE ON nn_history_records_recovery WHEN OLD.rowid=257 BEGIN SELECT RAISE(ABORT,'[TEST] interrupted cleanup');END")
	if err = s.retireCategoryRecovery(ctx); err == nil {
		t.Fatal("cleanup fault must be reported")
	}
	categoryRecoveryCount(t, s.db, "SELECT count(*) FROM nn_history_records_recovery", 44)
	categoryRecoveryCount(t, s.db, "SELECT count(*) FROM nn_history_records", 300)
	for i := 0; i < 300; i++ {
		value, e := s.Get(ctx, "viewer", "own", fmt.Sprint(i))
		if e != nil || value["request"] != "[TEST] exact 中文 request" || value["response"] != "data: exact\r\n\n" {
			t.Fatalf("published body lost: %d %v", i, e)
		}
	}
	categoryRecoveryExec(t, s.db, "DROP TRIGGER cleanup_interrupted")
	if err = s.retireCategoryRecovery(ctx); err != nil {
		t.Fatal(err)
	}
	categoryRecoveryCount(t, s.db, "SELECT count(*) FROM sqlite_master WHERE name='nn_history_records_recovery'", 0)
	categoryRecoveryCount(t, s.db, "SELECT count(*) FROM nn_history_records", 300)
}

func TestCategoryRecoveryCleanupRejectsClosedDatabase(t *testing.T) {
	r, err := openSQLHistory(context.Background(), defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	s := r.(*sqlHistoryRepository)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.retireCategoryRecovery(context.Background()); err == nil {
		t.Fatal("closed database must not report cleanup success")
	}
}

func TestCategoryRecoveryTriggerFailureKeepsPublishedBodies(t *testing.T) {
	ctx := context.Background()
	r, err := openSQLHistory(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.(*sqlHistoryRepository)
	value := categoryRecoveryValue("trigger-retry", "text")
	if err = s.Save(ctx, "viewer", "own", value); err != nil {
		t.Fatal(err)
	}
	categoryRecoveryExec(t, s.db, "DROP VIEW nn_history_records")
	categoryRecoveryExec(t, s.db, "CREATE TABLE nn_history_records AS SELECT * FROM nn_history_text_records")
	categoryRecoveryExec(t, s.db, "PRAGMA query_only=ON")
	if err = s.createCategoryViewTriggers(ctx); err == nil {
		t.Fatal("read-only schema must reject trigger publication")
	}
	categoryRecoveryExec(t, s.db, "PRAGMA query_only=OFF")
	if err = s.initializeCategoryStorage(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "viewer", "own", "trigger-retry")
	if err != nil || got["request"] != value["request"] {
		t.Fatalf("body changed after trigger retry: %v", err)
	}
}
