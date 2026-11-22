package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHistoryPhysicalCategoryStorage(t *testing.T) {
	ctx := context.Background()
	repo, e := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if e != nil {
		t.Fatal(e)
	}
	defer repo.Close()
	s := repo.(*sqlHistoryRepository)
	for _, cat := range []string{"text", "image"} {
		v := map[string]any{"id": cat, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": cat, "request": "[TEST] immutable", "pointsConsumed": nil}
		if e = s.Save(ctx, "viewer", "own", v); e != nil {
			t.Fatal(e)
		}
		var n int
		if e = s.db.QueryRow("SELECT count(*) FROM nn_history_" + cat + "_records").Scan(&n); e != nil || n != 1 {
			t.Fatalf("leaf %s count%d %v", cat, n, e)
		}
	}
	v := map[string]any{"id": "image", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": "text"}
	if s.Save(ctx, "viewer", "own", v) == nil {
		t.Fatal("cross category ID accepted")
	}
	page, e := s.Query(ctx, "viewer", historyQuery{})
	if e != nil || page.Total != 2 {
		t.Fatalf("union %+v %v", page, e)
	}
	if e = s.Delete(ctx, "viewer", "own", "image"); e != nil {
		t.Fatal(e)
	}
	page, e = s.Query(ctx, "viewer", historyQuery{ModelCategory: "image"})
	if e != nil || page.Total != 0 {
		t.Fatalf("delete %+v %v", page, e)
	}
}

func TestHistoryCategoryMigrationResumesAfterSecondBatchFailure(t *testing.T) {
	ctx := context.Background()
	config := defaultHistoryDatabase(t.TempDir())
	old, e := openSQLHistoryTable(ctx, config, "nn_history_records")
	if e != nil {
		t.Fatal(e)
	}
	source := old.(*sqlHistoryRepository)
	for i := 0; i < 140; i++ {
		cat := "text"
		if i%2 == 0 {
			cat = "image"
		}
		viewer, scope := fmt.Sprintf("viewer-%d", i/70), "own"
		if i%70 >= 35 {
			scope = "others"
		}
		if e = source.Save(ctx, viewer, scope, map[string]any{"id": fmt.Sprintf("id%03d", i%35), "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": cat, "request": fmt.Sprintf("[TEST] %d", i), "pointsConsumed": nil, "imageUsage": map[string]any{"textInputTokens": 0, "imageInputTokens": nil}}); e != nil {
			t.Fatal(e)
		}
	}
	source.Close()
	text, e := openSQLHistoryTable(ctx, config, "nn_history_text_records")
	if e != nil {
		t.Fatal(e)
	}
	target := text.(*sqlHistoryRepository)
	if _, e = target.db.Exec("CREATE TRIGGER fail_second_batch BEFORE INSERT ON nn_history_text_records WHEN NEW.viewer='viewer-1' BEGIN SELECT RAISE(ABORT,'[TEST] second batch'); END"); e != nil {
		t.Fatal(e)
	}
	target.Close()
	if r, e := openSQLHistory(ctx, config); e == nil {
		r.Close()
		t.Fatal("missing injected batch failure")
	}
	db, e := sql.Open("sqlite", filepath.Join(config.SQLiteDirectory, "history.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	var n int
	if e = db.QueryRow("SELECT count(*) FROM nn_history_record_locations").Scan(&n); e != nil || n != 64 {
		t.Fatalf("checkpoint=%d %v", n, e)
	}
	if _, e = db.Exec("DROP TRIGGER fail_second_batch"); e != nil {
		t.Fatal(e)
	}
	db.Close()
	r, e := openSQLHistory(ctx, config)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	s := r.(*sqlHistoryRepository)
	for i := 0; i < 4; i++ {
		viewer, scope := fmt.Sprintf("viewer-%d", i/2), "own"
		if i%2 != 0 {
			scope = "others"
		}
		page, e := s.Query(ctx, viewer, historyQuery{Scope: scope})
		if e != nil || page.Total != 35 {
			t.Fatalf("resume %s/%s %+v %v", viewer, scope, page, e)
		}
		value, e := s.Get(ctx, viewer, scope, "id000")
		if e != nil || value["request"] != fmt.Sprintf("[TEST] %d", i*35) {
			t.Fatalf("composite cursor skipped or mixed identities: %v", e)
		}
	}
	for _, cat := range []string{"text", "image"} {
		var count int
		if e = s.db.QueryRow("SELECT count(*) FROM nn_history_" + cat + "_records").Scan(&count); e != nil || count != 70 {
			t.Fatalf("leaf%d %v", count, e)
		}
	}
	value, e := s.Get(ctx, "viewer-0", "own", "id000")
	if e != nil || value["pointsConsumed"] != nil {
		t.Fatalf("nullable snapshot %v %v", value, e)
	}
	if e = s.Delete(ctx, "viewer-0", "own", "id000"); e != nil {
		t.Fatal(e)
	}
	if e = s.Save(ctx, "viewer-0", "own", value); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Get(ctx, "viewer-0", "own", "id000"); e == nil {
		t.Fatal("deleted record revived")
	}
}

func TestJournalPhysicalCategoriesAndPendingReferences(t *testing.T) {
	home := t.TempDir()
	h, e := openChatHistory(home)
	if e != nil {
		t.Fatal(e)
	}
	defer h.db.Close()
	if _, e = h.db.Exec(historyManagerSchema); e != nil {
		t.Fatal(e)
	}
	if e = initializeCategoryPending(h.db.DB); e != nil {
		t.Fatal(e)
	}
	for _, cat := range []string{"text", "image"} {
		value := map[string]any{"id": cat, "model": "[TEST] model", "agent": "[TEST]", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": cat, "request": "[TEST] confidential body", "response": "[TEST] content", "pointsConsumed": nil}
		if e = h.save("viewer", value, false); e != nil {
			t.Fatal(e)
		}
		seq, e := queueHistoryEvent(context.Background(), h.db.DB, "viewer", "own", "record", cat, value)
		if e != nil {
			t.Fatal(e)
		}
		var reference string
		if e = h.db.QueryRow("SELECT payload FROM history_pending WHERE seq=?", seq).Scan(&reference); e != nil || strings.Contains(reference, "confidential") {
			t.Fatalf("body in shared pending %s %v", reference, e)
		}
		raw, e := resolveHistoryEvent(context.Background(), h.db.DB, seq, reference)
		if e != nil || !strings.Contains(raw, "confidential") {
			t.Fatalf("missing entity %s %v", raw, e)
		}
		if _, e = h.db.Exec("DELETE FROM history_pending WHERE seq=?", seq); e != nil {
			t.Fatal(e)
		}
		var n int
		if e = h.db.QueryRow("SELECT count(*) FROM history_"+cat+"_pending_entities WHERE seq=?", seq).Scan(&n); e != nil || n != 0 {
			t.Fatalf("orphaned entity %d %v", n, e)
		}
		if e = h.db.QueryRow("SELECT count(*) FROM " + cat + "_exchanges").Scan(&n); e != nil || n != 1 {
			t.Fatalf("journal leaf %d %v", n, e)
		}
	}
}

func TestCategoryJournalMigrationRetainsIdentityAndOutbox(t *testing.T) {
	home := t.TempDir()
	directory := filepath.Join(home, ".newnoa")
	if e := os.MkdirAll(directory, 0700); e != nil {
		t.Fatal(e)
	}
	db, e := sql.Open("sqlite", filepath.Join(directory, "chat-history.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Exec("CREATE TABLE exchanges(account TEXT NOT NULL,id TEXT NOT NULL,created TEXT NOT NULL,model TEXT NOT NULL,agent TEXT NOT NULL,payload TEXT NOT NULL,pending INTEGER NOT NULL DEFAULT 0,deleted_at TEXT,PRIMARY KEY(account,id))")
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 130; i++ {
		cat := "text"
		if i%2 == 0 {
			cat = "image"
		}
		raw := fmt.Sprintf(`{"id":"id%03d","modelCategory":%q,"request":"[TEST] immutable","pointsConsumed":null}`, i, cat)
		if _, e = db.Exec("INSERT INTO exchanges VALUES('viewer',?,?,?, ?,?,0,NULL)", fmt.Sprintf("id%03d", i), time.Now().UTC().Format(time.RFC3339Nano), "model", "agent", raw); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = db.Exec("CREATE TABLE private_usage_outbox(request_id TEXT NOT NULL,node_id TEXT NOT NULL,payload TEXT NOT NULL,created_at INTEGER NOT NULL DEFAULT(unixepoch()),attempts INTEGER NOT NULL DEFAULT 0,next_attempt INTEGER NOT NULL DEFAULT 0,last_error TEXT,PRIMARY KEY(request_id,node_id));INSERT INTO private_usage_outbox(request_id,node_id,payload,attempts,next_attempt) VALUES('[TEST] request','[TEST] node','{\"imageUsage\":null}',3,99)"); e != nil {
		t.Fatal(e)
	}
	db.Close()
	h, e := openChatHistory(home)
	if e != nil {
		t.Fatal(e)
	}
	var payload string
	var attempts, next int
	if e = h.db.QueryRow("SELECT payload,attempts,next_attempt FROM private_usage_outbox WHERE request_id='[TEST] request'").Scan(&payload, &attempts, &next); e != nil || payload != `{"imageUsage":null}` || attempts != 3 || next != 99 {
		t.Fatalf("outbox changed %v", e)
	}
	var count int
	if e = h.db.QueryRow("SELECT count(*) FROM exchanges").Scan(&count); e != nil || count != 130 {
		t.Fatalf("journal migration %d %v", count, e)
	}
	for _, cat := range []string{"text", "image"} {
		if e = h.db.QueryRow("SELECT count(*) FROM " + cat + "_exchanges").Scan(&count); e != nil || count != 65 {
			t.Fatalf("journal %s %d %v", cat, count, e)
		}
	}
	h.db.Close()
	h, e = openChatHistory(home)
	if e != nil {
		t.Fatal(e)
	}
	defer h.db.Close()
	if e = h.db.QueryRow("SELECT count(*) FROM history_exchange_locations").Scan(&count); e != nil || count != 130 {
		t.Fatalf("restart duplicate IDs %d %v", count, e)
	}
}

func TestJournalPendingUpgradeKeepsExactSequenceAndBody(t *testing.T) {
	h, e := openChatHistory(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer h.db.Close()
	if _, e = h.db.Exec(historyManagerSchema); e != nil {
		t.Fatal(e)
	}
	raw := `{"modelCategory":"image","request":"[TEST] exact","pointsConsumed":null,"imageUsage":{"textInputTokens":0}}`
	if _, e = h.db.Exec("INSERT INTO history_pending(seq,viewer,scope,kind,id,payload) VALUES(77,'viewer','own','record','original',?)", raw); e != nil {
		t.Fatal(e)
	}
	if e = initializeCategoryPending(h.db.DB); e != nil {
		t.Fatal(e)
	}
	var ref string
	if e = h.db.QueryRow("SELECT payload FROM history_pending WHERE seq=77").Scan(&ref); e != nil {
		t.Fatal(e)
	}
	actual, e := resolveHistoryEvent(context.Background(), h.db.DB, 77, ref)
	if e != nil || actual != raw {
		t.Fatalf("upgrade lost exact bytes: %s %v", actual, e)
	}
	if e = initializeCategoryPending(h.db.DB); e != nil {
		t.Fatal(e)
	}
}

func TestCategoryJournalSecondBatchRestart(t *testing.T) {
	home := t.TempDir()
	directory := filepath.Join(home, ".newnoa")
	os.MkdirAll(directory, 0700)
	db, e := sql.Open("sqlite", filepath.Join(directory, "chat-history.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if _, e = db.Exec("CREATE TABLE exchanges(account TEXT NOT NULL,id TEXT NOT NULL,created TEXT NOT NULL,model TEXT NOT NULL,agent TEXT NOT NULL,payload TEXT NOT NULL,pending INTEGER NOT NULL DEFAULT 0,deleted_at TEXT,PRIMARY KEY(account,id));CREATE TABLE image_exchanges(account TEXT NOT NULL,id TEXT NOT NULL,created TEXT NOT NULL,model TEXT NOT NULL,agent TEXT NOT NULL,payload TEXT NOT NULL,pending INTEGER NOT NULL DEFAULT 0,deleted_at TEXT,PRIMARY KEY(account,id));CREATE TRIGGER fail_journal BEFORE INSERT ON image_exchanges WHEN NEW.id>='064' BEGIN SELECT RAISE(ABORT,'[TEST] stop batch');END"); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 130; i++ {
		raw := fmt.Sprintf(`{"id":"%03d","modelCategory":"image","requestId":"00000000-0000-0000-0000-%012d","pointsStatus":"pending","pointsConsumed":null}`, i, i)
		if _, e = db.Exec("INSERT INTO exchanges(rowid,account,id,created,model,agent,payload,pending,deleted_at) VALUES(?,'viewer',?,'2026-11-17T00:00:00Z','model','agent',?,0,NULL)", 1000+i*3, fmt.Sprintf("%03d", i), raw); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = db.Exec(historyManagerSchema); e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec("INSERT INTO history_meta(key,value) VALUES('consumption-backfill-v1',?)", 1000+63*3); e != nil {
		t.Fatal(e)
	}
	if h, e := openChatHistory(home); e == nil {
		h.db.Close()
		t.Fatal("expected failed second journal batch")
	}
	var n int
	if e = db.QueryRow("SELECT count(*) FROM history_exchange_locations").Scan(&n); e != nil || n != 64 {
		t.Fatalf("journal checkpoint %d %v", n, e)
	}
	if _, e = db.Exec("DROP TRIGGER fail_journal"); e != nil {
		t.Fatal(e)
	}
	h, e := openChatHistory(home)
	if e != nil {
		t.Fatal(e)
	}
	defer h.db.Close()
	var maxSeq int
	if e = h.db.QueryRow("SELECT count(*),max(rowid) FROM exchanges").Scan(&n, &maxSeq); e != nil || n != 130 || maxSeq != 1000+129*3 {
		t.Fatalf("journal resume %d %v", n, e)
	}
	for _, i := range []int{0, 63, 64, 129} {
		var seq int
		if e = h.db.QueryRow("SELECT rowid FROM exchanges WHERE id=?", fmt.Sprintf("%03d", i)).Scan(&seq); e != nil || seq != 1000+i*3 {
			t.Fatalf("original sequence id%d=%d %v", i, seq, e)
		}
	}
	for {
		done, e := h.backfillConsumption(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		if done {
			break
		}
	}
	if e = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&n); e != nil || n != 66 {
		t.Fatalf("backfill after original high-water count%d %v", n, e)
	}
	var skipped int
	if e = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending WHERE id<'064'").Scan(&skipped); e != nil || skipped != 0 {
		t.Fatalf("replayed old prefix %d %v", skipped, e)
	}
}

func TestHistoryCategoryLargeSQLiteMigrationAndExport(t *testing.T) {
	if testing.Short() {
		t.Skip("large isolated SQLite migration")
	}
	ctx := context.Background()
	config := defaultHistoryDatabase(t.TempDir())
	r, e := openSQLHistoryTable(ctx, config, "nn_history_records")
	if e != nil {
		t.Fatal(e)
	}
	source := r.(*sqlHistoryRepository)
	body := strings.Repeat("[TEST] large immutable unicode 中文 body\n", 40000)
	for i := 0; i < 80; i++ {
		cat := "text"
		if i%2 == 0 {
			cat = "image"
		}
		value := map[string]any{"id": fmt.Sprintf("[TEST] large%03d", i), "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": cat, "request": body, "pointsConsumed": "0.000000000", "imageUsage": map[string]any{"textInputTokens": 0, "imageInputTokens": nil}}
		if e = source.Save(ctx, "viewer", "own", value); e != nil {
			t.Fatal(e)
		}
	}
	source.Close()
	r, e = openSQLHistory(ctx, config)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	s := r.(*sqlHistoryRepository)
	var kind string
	if e = s.db.QueryRow("SELECT type FROM sqlite_master WHERE name='nn_history_records'").Scan(&kind); e != nil || kind != "view" {
		t.Fatal("mixed physical source retained", kind, e)
	}
	for _, cat := range []string{"text", "image"} {
		p, e := r.Query(ctx, "viewer", historyQuery{ModelCategory: cat})
		if e != nil || p.Total != 40 {
			t.Fatalf("large migration %s %+v %v", cat, p, e)
		}
	}
	value, e := r.Get(ctx, "viewer", "own", "[TEST] large000")
	if e != nil || value["request"] != body || value["pointsConsumed"] != "0.000000000" {
		t.Fatal("large migration body/snapshot mismatch", e)
	}
	out, e := writeHistoryExport(ctx, r, "viewer", historyQuery{ModelCategory: "image"}, "json", t.TempDir(), nil, func() int64 { return 256 << 10 })
	if e != nil || out.Count != 40 {
		t.Fatalf("large export %+v %v", out, e)
	}
}

func TestCategoryHistoryReplayAndTombstonesKeepPhysicalLeaf(t *testing.T) {
	ctx := context.Background()
	h, e := openChatHistory(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer h.db.Close()
	m, e := newHistoryManager(t.TempDir(), h)
	if e != nil {
		t.Fatal(e)
	}
	defer m.store.Close()
	m.migrating = true
	value := map[string]any{"id": "[TEST] image", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": "image", "request": "[TEST] recoverable body", "pointsConsumed": nil}
	if e = m.commit(ctx, "viewer", "others", "record", "[TEST] image", value); e != nil {
		t.Fatal(e)
	}
	if e = m.commit(ctx, "viewer", "others", "delete", "[TEST] image", map[string]any{}); e != nil {
		t.Fatal(e)
	}
	if e = m.commit(ctx, "viewer", "others", "record", "[TEST] image", value); e != nil {
		t.Fatal(e)
	}
	target, e := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if e != nil {
		t.Fatal(e)
	}
	defer target.Close()
	if e = m.replay(ctx, target, 0); e != nil {
		t.Fatal(e)
	}
	if _, e = target.Get(ctx, "viewer", "others", "[TEST] image"); e == nil {
		t.Fatal("replay resurrected deletion")
	}
	s := target.(*sqlHistoryRepository)
	var count int
	if e = s.db.QueryRow("SELECT count(*) FROM nn_history_image_records").Scan(&count); e != nil || count != 1 {
		t.Fatalf("duplicate/misclassified replay %d %v", count, e)
	}
	if e = m.replay(ctx, target, 0); e != nil {
		t.Fatal(e)
	}
}

func TestHistoryCategoryAuthoritativeTypeBeatsStaleProjection(t *testing.T) {
	ctx := context.Background()
	c := defaultHistoryDatabase(t.TempDir())
	r, e := openSQLHistoryTable(ctx, c, "nn_history_records")
	if e != nil {
		t.Fatal(e)
	}
	s := r.(*sqlHistoryRepository)
	value := map[string]any{"id": "[TEST] authority", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelType": "image", "request": "[TEST]"}
	if e = s.Save(ctx, "viewer", "own", value); e != nil {
		t.Fatal(e)
	}
	if _, e = s.db.Exec("UPDATE nn_history_records SET model_category='text'"); e != nil {
		t.Fatal(e)
	}
	s.Close()
	r, e = openSQLHistory(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	p, e := r.Query(ctx, "viewer", historyQuery{ModelCategory: "image"})
	if e != nil || p.Total != 1 {
		t.Fatalf("trusted type lost %+v %v", p, e)
	}
}

func TestHistoryCategoryVerificationUsesPrimaryKeysAndFastReopen(t *testing.T) {
	ctx := context.Background()
	c := defaultHistoryDatabase(t.TempDir())
	r, e := openSQLHistoryTable(ctx, c, "nn_history_records")
	if e != nil {
		t.Fatal(e)
	}
	old := r.(*sqlHistoryRepository)
	tx, e := old.db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	statement, e := tx.Prepare("INSERT INTO nn_history_records(" + historyRecordColumns + ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if e != nil {
		t.Fatal(e)
	}
	body := strings.Repeat("[TEST] synthetic body only ", 400)
	for i := 0; i < 4868; i++ {
		cat := "text"
		if i%2 == 0 {
			cat = "image"
		}
		raw := fmt.Sprintf(`{"id":"%05d","modelCategory":%q,"request":%q,"pointsConsumed":null}`, i, cat, body)
		meta := fmt.Sprintf(`{"id":"%05d","modelCategory":%q,"pointsConsumed":null}`, i, cat)
		if _, e = statement.Exec("viewer", "own", fmt.Sprintf("%05d", i), time.Now().UTC().Format(time.RFC3339Nano), fmt.Sprint(i), "", "", raw, meta, "", 0, "model", "", "", "model", "", "", cat, 2, 0); e != nil {
			t.Fatal(e)
		}
	}
	statement.Close()
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	old.Close()
	started := time.Now()
	r, e = openSQLHistory(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	first := time.Since(started)
	s := r.(*sqlHistoryRepository)
	// Recreate an empty source shape solely to inspect the same final verification
	// SQL plan; never run it on the already-cut-over compatibility union view.
	if _, e = s.db.Exec("DROP VIEW nn_history_records;CREATE TABLE nn_history_records AS SELECT * FROM nn_history_text_records WHERE 0"); e != nil {
		t.Fatal(e)
	}
	if _, e = s.db.Exec("CREATE UNIQUE INDEX test_source_pk ON nn_history_records(viewer,scope,id)"); e != nil {
		t.Fatal(e)
	}
	rows, e := s.db.Query("EXPLAIN QUERY PLAN "+historyCategoryBatchSQL(true), "viewer", "own", "00064")
	if e != nil {
		t.Fatal(e)
	}
	batchPlan := ""
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if e = rows.Scan(&id, &parent, &unused, &detail); e != nil {
			t.Fatal(e)
		}
		batchPlan += detail + "\n"
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(batchPlan, "SEARCH r USING INDEX test_source_pk") || strings.Contains(batchPlan, "TEMP B-TREE") {
		t.Fatalf("migration batch must seek past committed primary keys: %s", batchPlan)
	}
	for _, cat := range []string{"text", "image"} {
		rows, e := s.db.Query("EXPLAIN QUERY PLAN " + historyCategoryVerificationSQL(cat))
		if e != nil {
			t.Fatal(e)
		}
		plan := ""
		for rows.Next() {
			var a, b, c int
			var d string
			if e = rows.Scan(&a, &b, &c, &d); e != nil {
				t.Fatal(e)
			}
			plan += d + "\n"
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			t.Fatal(e)
		}
		if strings.Contains(plan, "MATERIALIZE") || strings.Contains(plan, "AUTOMATIC") || strings.Contains(plan, "TEMP B-TREE") || !strings.Contains(plan, "SEARCH t USING INDEX") || !strings.Contains(plan, "SEARCH r USING INDEX") {
			t.Fatalf("body verification must point-read PKs: %s", plan)
		}
	}
	if _, e = s.db.Exec("DROP TABLE nn_history_records;CREATE VIEW nn_history_records AS SELECT " + historyRecordColumns + " FROM nn_history_text_records UNION ALL SELECT " + historyRecordColumns + " FROM nn_history_image_records"); e != nil {
		t.Fatal(e)
	}
	s.Close()
	started = time.Now()
	r, e = openSQLHistory(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Close()
	reopen := time.Since(started)
	page, e := r.Query(ctx, "viewer", historyQuery{})
	if e != nil || page.Total != 4868 {
		t.Fatalf("identities %+v %v", page, e)
	}
	t.Logf("synthetic rows4868 bodyBytes%d firstOpen=%s completedReopen=%s", len(body), first, reopen)
	if first > 30*time.Second || reopen > 5*time.Second {
		t.Fatalf("migration/reopen exceeded bounded fixture budget first=%s reopen=%s", first, reopen)
	}
}

func TestHistoryCategoryFinalVerificationRejectsMissingIdentityAndChangedBody(t *testing.T) {
	ctx := context.Background()
	c := defaultHistoryDatabase(t.TempDir())
	r, e := openSQLHistoryTable(ctx, c, "nn_history_records")
	if e != nil {
		t.Fatal(e)
	}
	s := r.(*sqlHistoryRepository)
	defer s.Close()
	for _, cat := range []string{"text", "image"} {
		leaf, e := openSQLHistoryTable(ctx, c, "nn_history_"+cat+"_records")
		if e != nil {
			t.Fatal(e)
		}
		leaf.Close()
		v := map[string]any{"id": cat, "modelCategory": cat, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "[TEST]", "pointsConsumed": nil}
		if e = s.Save(ctx, "viewer", "own", v); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = s.db.Exec("CREATE TABLE nn_history_record_locations(viewer TEXT,scope TEXT,id TEXT,model_category TEXT,PRIMARY KEY(viewer,scope,id));INSERT INTO nn_history_text_records SELECT * FROM nn_history_records WHERE model_category='text';INSERT INTO nn_history_image_records SELECT * FROM nn_history_records WHERE model_category='image';INSERT INTO nn_history_record_locations SELECT viewer,scope,id,model_category FROM nn_history_records"); e != nil {
		t.Fatal(e)
	}
	if e = s.verifyCategoryMigration(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e = s.db.Exec("DELETE FROM nn_history_record_locations WHERE id='image'"); e != nil {
		t.Fatal(e)
	}
	if e = s.verifyCategoryMigration(ctx); e == nil {
		t.Fatal("missing identity accepted")
	}
	if _, e = s.db.Exec("INSERT INTO nn_history_record_locations VALUES('viewer','own','image','image');UPDATE nn_history_image_records SET payload='{}'"); e != nil {
		t.Fatal(e)
	}
	if e = s.verifyCategoryMigration(ctx); e == nil {
		t.Fatal("changed image body accepted")
	}
	if _, e = s.db.Exec("UPDATE nn_history_image_records SET payload=(SELECT payload FROM nn_history_records WHERE id='image');UPDATE nn_history_text_records SET metadata='{}'"); e != nil {
		t.Fatal(e)
	}
	if e = s.verifyCategoryMigration(ctx); e == nil {
		t.Fatal("changed text metadata accepted")
	}
}

func TestSpeechReportingKeepsMultimodalChatInText(t *testing.T) {
	ctx := context.Background()
	repo, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	for _, kind := range []string{"asr", "llm", "image"} {
		category := "text"
		if kind == "image" {
			category = "image"
		}
		v := map[string]any{"id": kind, "modelType": kind, "modelCategory": category, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "attachments": []any{map[string]any{"type": "audio"}}}
		if err := repo.Save(ctx, "viewer", "own", v); err != nil {
			t.Fatal(err)
		}
	}
	for category, kind := range map[string]string{"audio": "asr", "text": "llm", "image": "image"} {
		result, err := repo.Query(ctx, "viewer", historyQuery{Scope: "own", ModelCategory: category})
		if err != nil || len(result.Items) != 1 || result.Items[0]["modelType"] != kind {
			t.Fatalf("%s: %v %v", category, result.Items, err)
		}
	}
}
