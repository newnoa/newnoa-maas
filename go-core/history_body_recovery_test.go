package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func recoveryBody(t *testing.T, id, category string) map[string]any {
	t.Helper()
	return map[string]any{"id": id, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": category, "model": "[TEST] model", "agent": "[TEST] agent", "request": "[TEST] immutable request\x00中文", "response": "data: exact\r\n\r\n", "pointsConsumed": "0.000000000"}
}
func recoveryExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}
func recoveryCount(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestHistoryBodyPackRollbackPreservesPreviousPins(t *testing.T) {
	ctx := context.Background()
	db, store, err := openJournalBodies(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	value := recoveryBody(t, "one", "text")
	header, err := store.Pack(ctx, "records", "viewer", "own", "one", value)
	if err != nil {
		t.Fatal(err)
	}
	before := recoveryCount(t, db, "SELECT count(*) FROM nn_history_text_body_refs")
	recoveryExec(t, db, `CREATE TRIGGER fail_ref BEFORE INSERT ON nn_history_text_body_refs WHEN NEW.id='one' BEGIN SELECT RAISE(ABORT,'[TEST] interrupted publication'); END`)
	changed := recoveryBody(t, "one", "text")
	changed["request"] = "replacement requires new blocks"
	if _, err = store.Pack(ctx, "records", "viewer", "own", "one", changed); err == nil {
		t.Fatal("partial body publication accepted")
	}
	if recoveryCount(t, db, "SELECT count(*) FROM nn_history_text_body_refs") != before {
		t.Fatal("old durable pins lost on rollback")
	}
	got, err := store.Unpack(ctx, "viewer", header)
	if err != nil || got["request"] != value["request"] {
		t.Fatal("rollback lost original body", err)
	}
	recoveryExec(t, db, "DROP TRIGGER fail_ref")
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = store.categories["text"].DeleteRefsTx(ctx, tx, "records", "viewer", "own", "one"); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	if err = store.Collect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Unpack(ctx, "viewer", header); err != nil {
		t.Fatal("rolled-back deletion released pins", err)
	}
	if _, err = store.PackTx(ctx, nil, "records", "viewer", "own", "one", value); err == nil {
		t.Fatal("accepted missing transaction")
	}
}

func TestHistoryBodyDedupRejectsCorruptDurableBytesWithoutReplacingPins(t *testing.T) {
	ctx := context.Background()
	db, store, err := openJournalBodies(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	value := recoveryBody(t, "one", "image")
	header, err := store.Pack(ctx, "journal", "viewer", "own", "one", value)
	if err != nil {
		t.Fatal(err)
	}
	var hash string
	var data []byte
	if err = db.QueryRow("SELECT hash,data FROM nn_history_image_body_blocks LIMIT 1").Scan(&hash, &data); err != nil {
		t.Fatal(err)
	}
	recoveryExec(t, db, "UPDATE nn_history_image_body_blocks SET data=? WHERE hash=?", []byte("bad zstd"), hash)
	before := recoveryCount(t, db, "SELECT count(*) FROM nn_history_image_body_refs")
	if _, err = store.Pack(ctx, "journal", "viewer", "own", "one", value); err == nil {
		t.Fatal("corrupt existing dedup block reused")
	}
	if recoveryCount(t, db, "SELECT count(*) FROM nn_history_image_body_refs") != before {
		t.Fatal("failed dedup unpinned existing body")
	}
	recoveryExec(t, db, "UPDATE nn_history_image_body_blocks SET data=? WHERE hash=?", data, hash)
	got, err := store.Unpack(ctx, "viewer", header)
	if err != nil || got["response"] != value["response"] {
		t.Fatal("repair cannot recover original", err)
	}
	if _, err = store.Unpack(ctx, "stranger", header); err == nil {
		t.Fatal("cross-account block fallback")
	}
	if err = store.DeleteRefs(ctx, "journal", "viewer", "own", "one"); err != nil {
		t.Fatal(err)
	}
	if err = store.Collect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Unpack(ctx, "viewer", header); err == nil {
		t.Fatal("missing block hidden as empty body")
	}
	if _, err = store.Unpack(ctx, "viewer", "{bad"); err == nil {
		t.Fatal("corrupt composite header accepted")
	}
}

func TestHistorySQLMigrationRollbackRetainsPlaintextUntilVerifiedCommit(t *testing.T) {
	ctx := context.Background()
	r, err := openSQLHistory(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.(*sqlHistoryRepository)
	leaf := s.leaves["text"]
	for _, id := range []string{"first", "second"} {
		value := recoveryBody(t, id, "text")
		if err = s.Save(ctx, "viewer", "own", value); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(value)
		recoveryExec(t, s.db, "UPDATE nn_history_text_records SET payload=?,body_version=0,search_text='old duplicate' WHERE id=?", string(raw), id)
	}
	recoveryExec(t, s.db, `CREATE TRIGGER fail_migration BEFORE UPDATE ON nn_history_text_records WHEN NEW.body_version=1 BEGIN SELECT RAISE(ABORT,'[TEST] commit interrupted'); END`)
	if _, err = leaf.migrateBodiesBatch(ctx, 10); err == nil {
		t.Fatal("failed migration committed")
	}
	if recoveryCount(t, s.db, "SELECT count(*) FROM nn_history_text_records WHERE body_version=0 AND search_text='old duplicate'") != 2 {
		t.Fatal("failure rewrote source rows")
	}
	recoveryExec(t, s.db, "DROP TRIGGER fail_migration")
	if err = leaf.migrateBodies(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		value, err := s.Get(ctx, "viewer", "own", id)
		if err != nil || value["request"] != "[TEST] immutable request\x00中文" {
			t.Fatal("retry lost body", err)
		}
	}
	if recoveryCount(t, s.db, "SELECT count(*) FROM nn_history_text_records WHERE body_version=1 AND search_text=''") != 2 {
		t.Fatal("retry did not retire duplicate plaintext")
	}
	if _, err = leaf.migrateBodiesBatch(ctx, 0); err == nil {
		t.Fatal("unbounded migration accepted")
	}
}

func TestHistorySQLSearchFailsOnCorruptMetadataAndRecoversAfterRepair(t *testing.T) {
	ctx := context.Background()
	r, err := openSQLHistory(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.(*sqlHistoryRepository)
	value := recoveryBody(t, "one", "text")
	if err = s.Save(ctx, "viewer", "own", value); err != nil {
		t.Fatal(err)
	}
	var meta string
	if err = s.db.QueryRow("SELECT metadata FROM nn_history_text_records WHERE id='one'").Scan(&meta); err != nil {
		t.Fatal(err)
	}
	recoveryExec(t, s.db, "UPDATE nn_history_text_records SET metadata='{bad' WHERE id='one'")
	if _, err = s.leaves["text"].queryBodies(ctx, "viewer", historyQuery{Scope: "own", Q: "immutable", Limit: 10}); err == nil {
		t.Fatal("corrupt metadata silently returned")
	}
	recoveryExec(t, s.db, "UPDATE nn_history_text_records SET metadata=? WHERE id='one'", meta)
	result, err := s.Query(ctx, "viewer", historyQuery{Scope: "own", Q: "immutable", Limit: 10})
	if err != nil || result.Total != 1 {
		t.Fatal("metadata repair did not restore search", err)
	}
	if _, err = s.leaves["text"].queryBodies(ctx, "viewer", historyQuery{Scope: "own", Q: "immutable", Before: "broken cursor", Limit: 10}); err == nil {
		t.Fatal("invalid cursor accepted")
	}
	recoveryExec(t, s.db, "UPDATE nn_history_text_body_blocks SET raw_size=raw_size+1")
	if _, err = s.leaves["text"].queryBodies(ctx, "viewer", historyQuery{Scope: "own", Q: "immutable", Limit: 10}); err == nil {
		t.Fatal("corrupt block accepted during search")
	}
}

func TestHistoryJournalBodyMigrationResumesAfterCursorWriteFailure(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistoryAt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	value := recoveryBody(t, "one", "image")
	raw, _ := json.Marshal(value)
	recoveryExec(t, h.db.DB, "INSERT INTO exchanges(account,id,created,model,agent,payload) VALUES('viewer','one',?,'model','agent',?)", value["createdAt"], string(raw))
	recoveryExec(t, h.db.DB, "CREATE TABLE history_body_migration(kind TEXT PRIMARY KEY,cursor INTEGER NOT NULL)")
	recoveryExec(t, h.db.DB, `CREATE TRIGGER fail_cursor BEFORE INSERT ON history_body_migration BEGIN SELECT RAISE(ABORT,'[TEST] cursor unavailable'); END`)
	if _, err = h.migrateJournalBodies(ctx, 10); err == nil {
		t.Fatal("failed cursor write accepted")
	}
	var retained string
	if err = h.db.QueryRow("SELECT payload FROM exchanges WHERE id='one'").Scan(&retained); err != nil || retained != string(raw) {
		t.Fatal("header advanced before cursor transaction", err)
	}
	recoveryExec(t, h.db.DB, "DROP TRIGGER fail_cursor")
	n, err := h.migrateJournalBodies(ctx, 10)
	if err != nil || n != 1 {
		t.Fatal("failed migration not retryable", n, err)
	}
	if err = h.db.QueryRow("SELECT payload FROM exchanges WHERE id='one'").Scan(&retained); err != nil {
		t.Fatal(err)
	}
	got, err := h.decodeBody(ctx, "viewer", retained)
	if err != nil || got["request"] != value["request"] {
		t.Fatal("retry changed immutable body", err)
	}
	changed := recoveryBody(t, "one", "image")
	changed["response"] = "changed"
	if _, err = h.encodeBody(ctx, "journal", "viewer", "own", "one", changed); err == nil {
		t.Fatal("same identity body conflict accepted")
	}
	if _, err = h.migrateJournalBodies(ctx, 257); err == nil {
		t.Fatal("unbounded batch accepted")
	}
}

func TestHistoryJournalCollectionRetainsLivePinsAfterStorageFailure(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistoryAt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	for _, cat := range []string{"text", "image"} {
		value := recoveryBody(t, cat, cat)
		header, err := h.encodeBody(ctx, "journal", "viewer", "own", cat, value)
		if err != nil {
			t.Fatal(err)
		}
		recoveryExec(t, h.db.DB, "INSERT INTO exchanges(account,id,created,model,agent,payload) VALUES('viewer',?,?,'model','agent',?)", cat, value["createdAt"], header)
	}
	recoveryExec(t, h.db.DB, "UPDATE exchanges SET deleted_at=CURRENT_TIMESTAMP WHERE id='image'")
	recoveryExec(t, h.db.bodyDB, `CREATE TRIGGER fail_gc BEFORE DELETE ON nn_history_image_body_refs BEGIN SELECT RAISE(ABORT,'[TEST] body storage unavailable'); END`)
	if err = h.collectJournalBodies(ctx, 10); err == nil {
		t.Fatal("failed pin retirement accepted")
	}
	if recoveryCount(t, h.db.bodyDB, "SELECT count(*) FROM nn_history_image_body_refs") == 0 {
		t.Fatal("failed cleanup lost recovery pin")
	}
	recoveryExec(t, h.db.bodyDB, "DROP TRIGGER fail_gc")
	if err = h.collectJournalBodies(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if recoveryCount(t, h.db.bodyDB, "SELECT count(*) FROM nn_history_image_body_blocks") != 0 {
		t.Fatal("deleted body not collected")
	}
	var raw string
	if err = h.db.QueryRow("SELECT payload FROM exchanges WHERE id='text'").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if got, err := h.decodeBody(ctx, "viewer", raw); err != nil || got["request"] != "[TEST] immutable request\x00中文" {
		t.Fatal("other category damaged by GC", err)
	}
	if err = h.collectJournalBodies(ctx, 0); err == nil {
		t.Fatal("invalid collection batch accepted")
	}
}

func TestHistoryJournalRelocationRejectsUnsafeOrCorruptBodyDatabase(t *testing.T) {
	for _, kind := range []string{"symlink", "directory", "corrupt", "image-schema"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "history.sqlite")
			switch kind {
			case "symlink":
				target := filepath.Join(t.TempDir(), "real")
				if err := os.WriteFile(target, []byte("sentinel"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err := os.WriteFile(path, []byte("not sqlite"), 0600); err != nil {
					t.Fatal(err)
				}
			case "image-schema":
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				recoveryExec(t, db, "CREATE VIEW nn_history_image_body_refs AS SELECT 'broken' AS hash")
				db.Close()
			}
			db, _, err := openJournalBodies(directory)
			if err == nil {
				db.Close()
				t.Fatal("unsafe/corrupt body store opened")
			}
		})
	}
}

func TestHistoryBodyCodecRejectsAmbiguousAndUnavailableBodies(t *testing.T) {
	raw, blocks, err := historyBodyEncode(map[string]any{"request": "[TEST] fixed exact bytes"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = historyBodyEncode(map[string]any{historyBodyMarker: nil}); err == nil {
		t.Fatal("reserved marker accepted")
	}
	if _, _, err = historyBodyEncode(map[string]any{"metadata": make(chan int)}); err == nil {
		t.Fatal("non-JSON metadata accepted")
	}
	validLoader := func(hash string) (historyBodyBlock, error) {
		for _, b := range blocks {
			if b.Hash == hash {
				return b, nil
			}
		}
		return historyBodyBlock{}, fmt.Errorf("unknown test block")
	}
	if got, err := historyBodyDecode(raw, validLoader); err != nil || got["request"] != "[TEST] fixed exact bytes" {
		t.Fatal("valid control archive failed", err)
	}
	for _, header := range []string{"null", `{"_newnoaBody":[]}`, `{"_newnoaBody":{"version":2,"fields":{}}}`, `{"_newnoaBody":{"version":1,"fields":{"unknown":{}}}}`, strings.Replace(raw, `"_newnoaBody":`, `"request":"ambiguous","_newnoaBody":`, 1)} {
		if _, err = historyBodyDecode(header, validLoader); err == nil {
			t.Fatal("invalid or ambiguous manifest accepted")
		}
	}
	if _, err = historyBodyDecode(raw, nil); err == nil {
		t.Fatal("unavailable loader accepted")
	}
	for _, kind := range []string{"hash", "size", "compressed-limit", "decompressed-hash"} {
		b := blocks[0]
		switch kind {
		case "hash":
			b.Hash = strings.Repeat("0", 64)
		case "size":
			b.RawSize++
		case "compressed-limit":
			b.Data = make([]byte, historyBodyChunkMax+1025)
		case "decompressed-hash":
			_, other, e := historyBodyEncode(map[string]any{"request": strings.Repeat("x", b.RawSize)})
			if e != nil {
				t.Fatal(e)
			}
			b.Data = other[0].Data
		}
		if _, err = historyBodyDecode(raw, func(string) (historyBodyBlock, error) { return b, nil }); err == nil {
			t.Fatal("corrupt block accepted", kind)
		}
	}
}

func TestHistoryJournalRelocationFailureKeepsBothRecoverySources(t *testing.T) {
	h, err := openChatHistoryAt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	value := recoveryBody(t, "one", "text")
	if err = h.save("viewer", value, false); err != nil {
		t.Fatal(err)
	}
	var hash string
	var bytes []byte
	if err = h.db.bodyDB.QueryRow("SELECT hash,data FROM nn_history_text_body_blocks LIMIT 1").Scan(&hash, &bytes); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	db, _, err := openJournalBodies(target)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	recoveryExec(t, db, `CREATE TRIGGER no_destination BEFORE INSERT ON nn_history_text_body_blocks BEGIN SELECT RAISE(ABORT,'[TEST] destination unavailable'); END`)
	if err = migrateJournalBodyDatabase(h, target); err == nil {
		t.Fatal("failed destination relocation accepted")
	}
	if got, err := h.get("viewer", "one"); err != nil || got["request"] != value["request"] {
		t.Fatal("relocation failure damaged source", err)
	}
	recoveryExec(t, db, "DROP TRIGGER no_destination")
	recoveryExec(t, h.db.bodyDB, "UPDATE nn_history_text_body_blocks SET data=? WHERE hash=?", []byte("bad"), hash)
	if err = migrateJournalBodyDatabase(h, target); err == nil {
		t.Fatal("corrupt source relocated")
	}
	recoveryExec(t, h.db.bodyDB, "UPDATE nn_history_text_body_blocks SET data=? WHERE hash=?", bytes, hash)
	if err = migrateJournalBodyDatabase(h, target); err != nil {
		t.Fatal("relocation not retryable after storage repair", err)
	}
}

func TestHistoryJournalLegacyUpgradeDoesNotMarkCorruptBodiesComplete(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistoryAt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	legacy, err := newSQLHistoryBodyStore(ctx, h.db.bodyDB, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	value := recoveryBody(t, "one", "image")
	header, err := legacy.Pack(ctx, "journal", "viewer", "own", "one", value)
	if err != nil {
		t.Fatal(err)
	}
	recoveryExec(t, h.db.DB, "INSERT INTO exchanges(account,id,created,model,agent,payload) VALUES('viewer','one',?,'model','agent',?)", value["createdAt"], header)
	var hash string
	var bytes []byte
	if err = h.db.bodyDB.QueryRow("SELECT hash,data FROM nn_history_body_blocks LIMIT 1").Scan(&hash, &bytes); err != nil {
		t.Fatal(err)
	}
	recoveryExec(t, h.db.bodyDB, "UPDATE nn_history_body_blocks SET data=? WHERE hash=?", []byte("corrupt legacy"), hash)
	if err = h.migrateLegacyJournalBodyCategories(); err == nil {
		t.Fatal("corrupt published legacy upgrade accepted")
	}
	if recoveryCount(t, h.db.bodyDB, "SELECT count(*) FROM nn_history_journal_body_category_migration") != 0 {
		t.Fatal("failed body upgrade marked complete")
	}
	recoveryExec(t, h.db.bodyDB, "UPDATE nn_history_body_blocks SET data=? WHERE hash=?", bytes, hash)
	recoveryExec(t, h.db.bodyDB, `CREATE TRIGGER fail_legacy_pin BEFORE INSERT ON nn_history_image_body_refs BEGIN SELECT RAISE(ABORT,'[TEST] new store pin unavailable'); END`)
	if err = h.migrateLegacyJournalBodyCategories(); err == nil {
		t.Fatal("failed category body publication accepted")
	}
	if _, err = legacy.Unpack(ctx, "viewer", header); err != nil {
		t.Fatal("failed upgrade damaged legacy recovery source", err)
	}
	recoveryExec(t, h.db.bodyDB, "DROP TRIGGER fail_legacy_pin")
	if err = h.migrateLegacyJournalBodyCategories(); err != nil {
		t.Fatal("repaired legacy body cannot upgrade", err)
	}
	if got, err := h.decodeBody(ctx, "viewer", header); err != nil || got["pointsConsumed"] != "0.000000000" {
		t.Fatal("legacy upgrade changed known zero", err)
	}
	if err = h.migrateLegacyJournalBodyCategories(); err != nil {
		t.Fatal("completed upgrade not idempotent", err)
	}
}

func TestHistoryJournalMissingPendingEntityPreventsCursorAdvance(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistoryAt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	recoveryExec(t, h.db.DB, historyManagerSchema)
	if err = initializeCategoryPending(h.db.DB); err != nil {
		t.Fatal(err)
	}
	value := recoveryBody(t, "pending", "image")
	seq, err := queueHistoryEvent(ctx, h.db.DB, "viewer", "others", "record", "pending", value)
	if err != nil {
		t.Fatal(err)
	}
	var entity string
	if err = h.db.QueryRow("SELECT payload FROM history_image_pending_entities WHERE seq=?", seq).Scan(&entity); err != nil {
		t.Fatal(err)
	}
	recoveryExec(t, h.db.DB, "DELETE FROM history_image_pending_entities WHERE seq=?", seq)
	if _, err = h.migrateJournalBodies(ctx, 10); err == nil {
		t.Fatal("missing durable event treated as acknowledged")
	}
	if recoveryCount(t, h.db.DB, "SELECT count(*) FROM history_body_migration WHERE kind='history_pending'") != 0 {
		t.Fatal("failed pending body skipped by cursor")
	}
	if _, err = h.encodeBody(ctx, "pending", "viewer", "others", "pending", value); err == nil {
		t.Fatal("missing prior event allowed replacement")
	}
	recoveryExec(t, h.db.DB, "INSERT INTO history_image_pending_entities(seq,payload) VALUES(?,?)", seq, entity)
	if _, err = h.migrateJournalBodies(ctx, 10); err != nil {
		t.Fatal("pending retry after durable repair failed", err)
	}
	var ref string
	if err = h.db.QueryRow("SELECT payload FROM history_pending WHERE seq=?", seq).Scan(&ref); err != nil {
		t.Fatal(err)
	}
	raw, err := resolveHistoryEvent(ctx, h.db.DB, seq, ref)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := h.decodePending(ctx, "viewer", raw)
	if err != nil || !strings.Contains(decoded, "immutable request") {
		t.Fatal("pending body lost", err)
	}
	if _, err = h.decodePending(ctx, "viewer", "{bad"); err == nil {
		t.Fatal("corrupt pending body decoded")
	}
}

func TestHistoryBodyCollectionRollbackKeepsUncollectedOrphansForRetry(t *testing.T) {
	ctx := context.Background()
	db, s, err := openJournalBodies(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	value := recoveryBody(t, "one", "text")
	if _, err = s.Pack(ctx, "journal", "viewer", "own", "one", value); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteRefs(ctx, "journal", "viewer", "own", "one"); err != nil {
		t.Fatal(err)
	}
	before := recoveryCount(t, db, "SELECT count(*) FROM nn_history_text_body_blocks")
	recoveryExec(t, db, `CREATE TRIGGER fail_orphan_delete BEFORE DELETE ON nn_history_text_body_blocks BEGIN SELECT RAISE(ABORT,'[TEST] delete unavailable'); END`)
	if err = s.Collect(ctx); err == nil {
		t.Fatal("failed collector reported success")
	}
	if recoveryCount(t, db, "SELECT count(*) FROM nn_history_text_body_blocks") != before {
		t.Fatal("failed transaction partially collected")
	}
	recoveryExec(t, db, "DROP TRIGGER fail_orphan_delete")
	if err = s.Collect(ctx); err != nil {
		t.Fatal("collector cannot retry", err)
	}
	if recoveryCount(t, db, "SELECT count(*) FROM nn_history_text_body_blocks") != 0 {
		t.Fatal("orphans retained after retry")
	}
}

func TestHistoryLegacyCategoryReadersRemainAvailableDuringBodyMigration(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistoryAt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	repository, err := openLegacyHistoryRepository(h)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	for _, category := range []string{"text", "image"} {
		value := recoveryBody(t, category, category)
		value["model"] = "[TEST] " + category + " model"
		value["keyId"] = "key-" + category
		if err = repository.Save(ctx, "viewer", "own", value); err != nil {
			t.Fatal(err)
		}
	}
	var legacyHash string
	var legacyBytes []byte
	if err = h.db.bodyDB.QueryRow("SELECT hash,data FROM nn_history_text_body_blocks LIMIT 1").Scan(&legacyHash, &legacyBytes); err != nil {
		t.Fatal(err)
	}
	recoveryExec(t, h.db.bodyDB, "UPDATE nn_history_text_body_blocks SET data=? WHERE hash=?", []byte("unreadable legacy block"), legacyHash)
	if err = repository.Enrich(ctx, "viewer", "own", "text", map[string]any{"modelName": "must not publish"}); err == nil {
		t.Fatal("legacy enrichment ignored corrupt source")
	}
	recoveryExec(t, h.db.bodyDB, "UPDATE nn_history_text_body_blocks SET data=? WHERE hash=?", legacyBytes, legacyHash)

	for _, category := range []string{"text", "image"} {
		result, err := repository.Query(ctx, "viewer", historyQuery{Scope: "own", ModelCategory: category, Limit: 10, IncludeContent: true})
		if err != nil || result.Total != 1 || len(result.Items) != 1 || result.Items[0]["id"] != category || result.Items[0]["request"] != "[TEST] immutable request\x00中文" {
			t.Fatalf("legacy category reader mixed or lost body: %s %+v %v", category, result, err)
		}
		filters, err := repository.Filters(ctx, "viewer", historyFilterQuery{Scope: "own", ModelCategory: category, Kind: "models", Limit: 10})
		if err != nil || len(filters.Items) != 1 || filters.Items[0].ID != "[TEST] "+category+" model" {
			t.Fatalf("legacy category facets mixed: %s %+v %v", category, filters, err)
		}
	}
}

func TestHistorySQLPackedCheckpointRepairKeepsIdentityAndOriginalBody(t *testing.T) {
	testHistoryPackedCheckpointRecovery(t, defaultHistoryDatabase(t.TempDir()))
}

func TestHistorySQLPackedCheckpointLive(t *testing.T) {
	for _, engine := range []string{"postgres", "mysql"} {
		t.Run(engine, func(t *testing.T) {
			prefix := "CORE_CATEGORY_" + strings.ToUpper(engine)
			port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
			if port == 0 {
				t.Skip("requires authorized isolated SQL sandbox")
			}
			c := historySandboxDatabase(t, defaultHistoryDatabase(t.TempDir()))
			if c.Database == "newnoa_history" {
				t.Fatal("packed checkpoint live test requires isolated database override")
			}
			c.Engine = engine
			c.Host = os.Getenv(prefix + "_HOST")
			c.Port = port
			c.Username = os.Getenv(prefix + "_USER")
			c.Password = os.Getenv(prefix + "_PASSWORD")
			testHistoryPackedCheckpointRecovery(t, c)
		})
	}
}

func testHistoryPackedCheckpointRecovery(t *testing.T, config historyDatabaseConfig) {
	t.Helper()
	ctx := context.Background()
	r, err := openSQLHistory(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.(*sqlHistoryRepository)
	for _, category := range []string{"text", "image"} {
		t.Run(category, func(t *testing.T) {
			value := recoveryBody(t, "checkpoint-"+category, category)
			id := value["id"]
			if err = s.Save(ctx, "viewer", "own", value); err != nil {
				t.Fatal(err)
			}
			leaf := s.leaves[category]
			var header string
			var block historyBodyBlock
			if err = s.db.QueryRow(leaf.bind("SELECT payload FROM nn_history_records WHERE id=?"), id).Scan(&header); err != nil {
				t.Fatal(err)
			}
			if err = s.db.QueryRow(leaf.bodies.bind("SELECT hash,raw_size,data FROM nn_history_body_blocks WHERE viewer=? LIMIT 1"), "viewer").Scan(&block.Hash, &block.RawSize, &block.Data); err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec(leaf.bind("UPDATE nn_history_records SET body_version=0 WHERE id=?"), id); err != nil {
				t.Fatal(err)
			}
			// Both corruption and a missing durable block must keep the same checkpoint retryable.
			if _, err = s.db.Exec(leaf.bodies.bind("UPDATE nn_history_body_blocks SET data=? WHERE hash=?"), []byte("interrupted durable block"), block.Hash); err != nil {
				t.Fatal(err)
			}
			if _, err = leaf.migrateBodiesBatch(ctx, 10); err == nil {
				t.Fatal("unverified packed checkpoint published")
			}
			var retained string
			if err = s.db.QueryRow(leaf.bind("SELECT payload FROM nn_history_records WHERE id=? AND body_version=0"), id).Scan(&retained); err != nil || retained != header {
				t.Fatal("failed checkpoint discarded its manifest", err)
			}
			if _, err = s.db.Exec(leaf.bodies.bind("DELETE FROM nn_history_body_refs WHERE viewer=? AND id=?"), "viewer", id); err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec(leaf.bodies.bind("DELETE FROM nn_history_body_blocks WHERE viewer=? AND hash=?"), "viewer", block.Hash); err != nil {
				t.Fatal(err)
			}
			if _, err = leaf.migrateBodiesBatch(ctx, 10); err == nil {
				t.Fatal("missing checkpoint block accepted")
			}
			if _, err = s.db.Exec(leaf.bodies.bind("INSERT INTO nn_history_body_blocks(viewer,hash,raw_size,data) VALUES(?,?,?,?)"), "viewer", block.Hash, block.RawSize, block.Data); err != nil {
				t.Fatal(err)
			}
			if n, err := leaf.migrateBodiesBatch(ctx, 10); err != nil || n != 1 {
				t.Fatal("packed checkpoint could not resume", n, err)
			}
			other := "text"
			if category == "text" {
				other = "image"
			}
			if err = s.Enrich(ctx, "viewer", "own", id.(string), map[string]any{"modelCategory": other, "pointsConsumed": "999"}); err == nil {
				t.Fatal("enrichment moved immutable identity to another category")
			}
			if err = s.Enrich(ctx, "stranger", "own", id.(string), map[string]any{"modelName": "intruder"}); err == nil {
				t.Fatal("another owner enriched this record")
			}
			got, err := s.Get(ctx, "viewer", "own", id.(string))
			if err != nil || got["request"] != value["request"] || got["modelCategory"] != category || got["pointsConsumed"] != "0.000000000" {
				t.Fatal("repair or rejected enrichment changed original", got, err)
			}

			if config.Engine != "sqlite" {
				old := time.Now().AddDate(0, 0, -30).UTC().Format("2006-01-02T15:04:05.000000000Z")
				if _, err = s.db.Exec(leaf.bind("UPDATE nn_history_records SET created=? WHERE id=?"), old, id); err != nil {
					t.Fatal(err)
				}
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if err = leaf.Expire(cancelled, "viewer", 7); err == nil {
					t.Fatal("cancelled retention falsely reported success")
				}
				if got, err = leaf.Get(ctx, "viewer", "own", id.(string)); err != nil || got["request"] != value["request"] {
					t.Fatal("cancelled retention damaged body", err)
				}
				if err = leaf.Expire(ctx, "viewer", 7); err != nil {
					t.Fatal("retention cannot retry", err)
				}
				if _, err = leaf.Get(ctx, "viewer", "own", id.(string)); err == nil {
					t.Fatal("expired repaired checkpoint stayed visible")
				}
			}
		})
	}
}

func TestHistoryPendingOnlyRelocationRetriesAfterMissingEntityAndTargetFailure(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistoryAt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	recoveryExec(t, h.db.DB, historyManagerSchema)
	if err = initializeCategoryPending(h.db.DB); err != nil {
		t.Fatal(err)
	}
	value := recoveryBody(t, "pending-relocation", "image")
	header, err := h.encodeBody(ctx, "pending", "viewer", "others", "pending-relocation", value)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := queueEncodedHistoryEvent(ctx, h.db.DB, "viewer", "others", "record", "pending-relocation", value, header)
	if err != nil {
		t.Fatal(err)
	}
	recoveryExec(t, h.db.DB, "DELETE FROM history_image_pending_entities WHERE seq=?", seq)
	target := t.TempDir()
	if err = migrateJournalBodyDatabase(h, target); err == nil {
		t.Fatal("missing pending body skipped during relocation")
	}
	invalid := t.TempDir()
	if err = os.Mkdir(filepath.Join(invalid, "history.sqlite"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = migrateJournalBodyDatabase(h, invalid); err == nil {
		t.Fatal("invalid relocation destination accepted")
	}

	recoveryExec(t, h.db.DB, "INSERT INTO history_image_pending_entities(seq,payload) VALUES(?,?)", seq, header)
	var hash string
	var original []byte
	if err = h.db.bodyDB.QueryRow("SELECT hash,data FROM nn_history_image_body_blocks LIMIT 1").Scan(&hash, &original); err != nil {
		t.Fatal(err)
	}
	recoveryExec(t, h.db.bodyDB, "UPDATE nn_history_image_body_blocks SET data=? WHERE hash=?", []byte("unreadable pending source"), hash)
	if err = migrateJournalBodyDatabase(h, target); err == nil {
		t.Fatal("corrupt pending source relocated")
	}
	recoveryExec(t, h.db.bodyDB, "UPDATE nn_history_image_body_blocks SET data=? WHERE hash=?", original, hash)

	db, store, err := openJournalBodies(target)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	recoveryExec(t, db, `CREATE TRIGGER fail_pending_target BEFORE INSERT ON nn_history_image_body_refs BEGIN SELECT RAISE(ABORT,'[TEST] destination pin rejected'); END`)
	if err = migrateJournalBodyDatabase(h, target); err == nil {
		t.Fatal("failed pending destination reported relocated")
	}
	if got, err := h.decodeBody(ctx, "viewer", header); err != nil || got["response"] != value["response"] {
		t.Fatal("failed relocation lost source pending bytes", err)
	}
	recoveryExec(t, db, "DROP TRIGGER fail_pending_target")
	if err = migrateJournalBodyDatabase(h, target); err != nil {
		t.Fatal("pending-only relocation did not retry", err)
	}
	got, err := store.Unpack(ctx, "viewer", header)
	if err != nil || got["request"] != value["request"] || got["response"] != value["response"] {
		t.Fatal("pending-only relocation changed exact body", err)
	}
	if recoveryCount(t, h.db.DB, "SELECT count(*) FROM history_pending WHERE seq=?", seq) != 1 {
		t.Fatal("relocation acknowledged source pending event")
	}
}
