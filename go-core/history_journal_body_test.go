package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"modernc.org/sqlite"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestJournalPublicationPinsBodyWhileConcurrentCollectionWaits(t *testing.T) {
	ctx := context.Background()
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	name := fmt.Sprintf("nn_test_body_publish_%d", time.Now().UnixNano())
	err := sqlite.RegisterScalarFunction(name, 0, func(_ *sqlite.FunctionContext, _ []driver.Value) (driver.Value, error) {
		close(entered)
		<-release
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	if _, err = h.db.Exec(historyManagerSchema); err != nil {
		t.Fatal(err)
	}
	if _, err = h.db.Exec("CREATE TRIGGER pause_body BEFORE INSERT ON text_exchanges BEGIN SELECT " + name + "(); END"); err != nil {
		t.Fatal(err)
	}
	saved := make(chan error, 1)
	go func() {
		saved <- h.save("[TEST] cold", map[string]any{"id": "[TEST] publish", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": strings.Repeat("[TEST] body", 10000)}, false)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer never reached durable blocks")
	}
	collected := make(chan error, 1)
	go func() { collected <- h.collectJournalBodies(ctx, 100) }()
	select {
	case err := <-collected:
		t.Fatalf("collector crossed publication lock: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err = <-saved; err != nil {
		t.Fatal(err)
	}
	if err = <-collected; err != nil {
		t.Fatal(err)
	}
	if _, err = h.get("[TEST] cold", "[TEST] publish"); err != nil {
		t.Fatal("published body removed", err)
	}
}

func TestJournalDuplicateCaptureKeepsOriginalBody(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	value := map[string]any{"id": "[TEST] duplicate", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "original"}
	if err = h.save("viewer", value, false); err != nil {
		t.Fatal(err)
	}
	if err = h.save("viewer", value, false); err != nil {
		t.Fatal(err)
	}
	value["request"] = "changed"
	if err = h.save("viewer", value, false); err == nil {
		t.Fatal("conflicting duplicate accepted")
	}
	got, err := h.get("viewer", "[TEST] duplicate")
	if err != nil || got["request"] != "original" {
		t.Fatal("original body lost", err)
	}
}

func TestJournalMigrationKeepsNonRecordEventsAndRelocatesPendingOnlyBody(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	if _, err = h.db.Exec(historyManagerSchema); err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"id": "[TEST] pending", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": strings.Repeat("[TEST] pending only", 3000), "response": "[TEST] response"}
	raw, _ := json.Marshal(value)
	if _, err = h.db.Exec("INSERT INTO history_pending(viewer,scope,kind,id,payload) VALUES('viewer','others','record',?,?),('viewer','others','delete',?,'{}')", value["id"], string(raw), value["id"]); err != nil {
		t.Fatal(err)
	}
	if _, err = h.migrateJournalBodies(ctx, 10); err != nil {
		t.Fatal(err)
	}
	var header, deleted string
	if err = h.db.QueryRow("SELECT payload FROM history_pending WHERE kind='record'").Scan(&header); err != nil {
		t.Fatal(err)
	}
	if err = h.db.QueryRow("SELECT payload FROM history_pending WHERE kind='delete'").Scan(&deleted); err != nil || deleted != "{}" {
		t.Fatal("non-record changed", err)
	}
	directory := t.TempDir()
	if err = migrateJournalBodyDatabase(h, directory); err != nil {
		t.Fatal(err)
	}
	db, store, err := openJournalBodies(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := store.Unpack(ctx, "viewer", header)
	if err != nil || got["request"] != value["request"] {
		t.Fatal("pending-only relocation", err)
	}
}

func TestJournalCollectionRotatesPastLiveReferences(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	if _, err = h.db.Exec(historyManagerSchema); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 105; i++ {
		id := fmt.Sprintf("[TEST] %03d", i)
		value := map[string]any{"id": id, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "body" + id}
		if err = h.save("viewer", value, false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = h.db.Exec("DELETE FROM exchanges WHERE id='[TEST] 104'"); err != nil {
		t.Fatal(err)
	}
	if err = h.collectJournalBodies(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if err = h.collectJournalBodies(ctx, 100); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM nn_history_text_body_refs WHERE id='[TEST] 104'").Scan(&count); err != nil || count != 0 {
		t.Fatal("orphan starved", count, err)
	}
}

func TestDisabledCommitDoesNotPublishBodies(t *testing.T) {
	ctx := context.Background()
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
	m.config.Engine = "off"
	if err = m.commitLocked(ctx, "viewer", "own", "record", "[TEST] off", map[string]any{"request": "must not be stored"}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM nn_history_text_body_blocks").Scan(&count); err != nil || count != 0 {
		t.Fatal("disabled stored body", count, err)
	}
}

func TestJournalCompressesAndSharesBodyWithSQLiteRepository(t *testing.T) {
	home := t.TempDir()
	directory := filepath.Join(home, ".newnoa", "sqlite")
	h, err := openChatHistoryAt(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	repo, err := openHistoryRepository(context.Background(), defaultHistoryDatabase(home))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	request := `{"input":[{"role":"user","content":"` + strings.Repeat("[TEST] repeat context 中文/", 40000) + `"}]}`
	response := "data: {\"delta\":\"[TEST] answer\"}\n\ndata: [DONE]\n\n"
	value := map[string]any{"id": "[TEST] body", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "[TEST] model", "agent": "[TEST] agent", "storageId": "[TEST] session", "request": request, "response": response}
	if err = h.save("[TEST] viewer", value, false); err != nil {
		t.Fatal(err)
	}
	if err = repo.Save(context.Background(), "[TEST] viewer", "own", value); err != nil {
		t.Fatal(err)
	}
	var header string
	if err = h.db.QueryRow("SELECT payload FROM exchanges").Scan(&header); err != nil {
		t.Fatal(err)
	}
	if !json.Valid([]byte(header)) || len(header) > len(request)/10 || strings.Contains(header, "repeat context") {
		t.Fatalf("journal still holds full body: %d", len(header))
	}
	got, err := h.get("[TEST] viewer", "[TEST] body")
	if err != nil || got["request"] != request || got["response"] != response {
		t.Fatalf("journal restore: %v", err)
	}
	sqlRepo := repo.(*sqlHistoryRepository)
	var blocks, sources int
	if err = sqlRepo.db.QueryRow("SELECT count(*) FROM nn_history_text_body_blocks").Scan(&blocks); err != nil {
		t.Fatal(err)
	}
	if err = sqlRepo.db.QueryRow("SELECT count(DISTINCT source) FROM nn_history_text_body_refs").Scan(&sources); err != nil || sources != 2 {
		t.Fatalf("shared sources=%d err=%v", sources, err)
	}
	if err = repo.Save(context.Background(), "[TEST] viewer", "own", map[string]any{"id": "[TEST] second", "createdAt": value["createdAt"], "request": request, "response": response}); err != nil {
		t.Fatal(err)
	}
	var after int
	if err = sqlRepo.db.QueryRow("SELECT count(*) FROM nn_history_text_body_blocks").Scan(&after); err != nil || after != blocks {
		t.Fatalf("shared body stored again before=%d after=%d err=%v", blocks, after, err)
	}
}

func TestJournalCompressedBodiesSurviveRestart(t *testing.T) {
	directory := t.TempDir()
	h, err := openChatHistoryAt(directory)
	if err != nil {
		t.Fatal(err)
	}
	request := strings.Repeat("[TEST] keep exact UTF8/escape\\\n中文", 20000)
	value := map[string]any{"id": "[TEST] restart", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "[TEST] model", "agent": "[TEST] agent", "request": request, "response": "[TEST] response"}
	if err = h.save("[TEST] viewer", value, true); err != nil {
		t.Fatal(err)
	}
	if err = h.db.Close(); err != nil {
		t.Fatal(err)
	}
	h, err = openChatHistoryAt(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	got, err := h.get("[TEST] viewer", "[TEST] restart")
	if err != nil || got["request"] != request {
		t.Fatalf("restart body not restored: %v", err)
	}
}

func TestJournalDeletedBodiesRetireMetadataBeforeUnpinning(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	if _, err = h.db.Exec(historyManagerSchema); err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"id": "[TEST] deleted", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": strings.Repeat("[TEST] body", 1000), "response": "[TEST] response", "pointsConsumed": "0.000000", "pointsStatus": "known"}
	if err = h.save("viewer", value, true); err != nil {
		t.Fatal(err)
	}
	header, err := h.bodies.Pack(ctx, "pending", "viewer", "own", historyText(value["id"]), value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.db.Exec("INSERT INTO history_pending(viewer,scope,kind,id,payload) VALUES('viewer','own','record',?,?)", value["id"], header); err != nil {
		t.Fatal(err)
	}
	if _, err = h.db.Exec("INSERT INTO private_usage_outbox(request_id,node_id,payload) VALUES('request','node','{\"billing\":true}')"); err != nil {
		t.Fatal(err)
	}
	if _, err = h.db.Exec("UPDATE exchanges SET deleted_at=CURRENT_TIMESTAMP,pending=0 WHERE account='viewer'"); err != nil {
		t.Fatal(err)
	}
	if err = h.collectJournalBodies(ctx, 100); err != nil {
		t.Fatal(err)
	}
	var raw string
	var tombstone, pending int
	if err = h.db.QueryRow("SELECT payload,deleted_at IS NOT NULL,pending FROM exchanges").Scan(&raw, &tombstone, &pending); err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err = json.Unmarshal([]byte(raw), &metadata); err != nil {
		t.Fatal(err)
	}
	if tombstone != 1 || pending != 0 || metadata["pointsStatus"] != "known" || metadata["pointsConsumed"] != "0.000000" || metadata["id"] != value["id"] {
		t.Fatal("tombstone metadata changed")
	}
	for _, key := range []string{"request", "response", historyBodyMarker} {
		if _, ok := metadata[key]; ok {
			t.Fatal("deleted body survived", key)
		}
	}
	var refs int
	if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM (SELECT * FROM nn_history_text_body_refs UNION ALL SELECT * FROM nn_history_image_body_refs) WHERE source='journal'").Scan(&refs); err != nil || refs != 0 {
		t.Fatal("deleted journal body remains pinned", refs, err)
	}
	got, err := h.decodeBody(ctx, "viewer", header)
	if err != nil || got["request"] != value["request"] {
		t.Fatal("independent pending body lost", err)
	}
	for i := 0; i < 2; i++ {
		if _, err = h.db.Exec("DELETE FROM history_body_migration"); err != nil && i != 0 {
			t.Fatal(err)
		}
		if _, err = h.migrateJournalBodies(ctx, 32); err != nil {
			t.Fatal(err)
		}
	}
	if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM (SELECT * FROM nn_history_text_body_refs UNION ALL SELECT * FROM nn_history_image_body_refs) WHERE source='journal'").Scan(&refs); err != nil || refs != 0 {
		t.Fatal("migration repinned deleted body", refs, err)
	}
	var outbox int
	if err = h.db.QueryRow("SELECT count(*) FROM private_usage_outbox WHERE payload='{\"billing\":true}'").Scan(&outbox); err != nil || outbox != 1 {
		t.Fatal("usage outbox changed", err)
	}
	if _, err = h.db.Exec("DELETE FROM history_pending"); err != nil {
		t.Fatal(err)
	}
	if err = h.collectJournalBodies(ctx, 100); err != nil {
		t.Fatal(err)
	}
	var blocks int
	if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM (SELECT * FROM nn_history_text_body_blocks UNION ALL SELECT * FROM nn_history_image_body_blocks)").Scan(&blocks); err != nil || blocks != 0 {
		t.Fatal("unreferenced deleted blocks retained", blocks, err)
	}
}

func TestJournalMigrationClearsDeletedLegacyPlaintextWithoutBlocks(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	raw := `{"id":"[TEST] old","request":"private body","response":"answer","pointsStatus":"known","pointsConsumed":"0"}`
	if _, err = h.db.Exec("INSERT INTO exchanges(account,id,created,model,agent,payload,deleted_at) VALUES('viewer','old',?,'m','a',?,CURRENT_TIMESTAMP)", time.Now().UTC().Format(time.RFC3339Nano), raw); err != nil {
		t.Fatal(err)
	}
	if _, err = h.migrateJournalBodies(ctx, 32); err != nil {
		t.Fatal(err)
	}
	var payload string
	if err = h.db.QueryRow("SELECT payload FROM exchanges").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(payload, "private body") || strings.Contains(payload, "answer") || historyBodyIsPacked(payload) {
		t.Fatal("deleted legacy body compressed instead of retired")
	}
	var blocks int
	if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM (SELECT * FROM nn_history_text_body_blocks UNION ALL SELECT * FROM nn_history_image_body_blocks)").Scan(&blocks); err != nil || blocks != 0 {
		t.Fatal("deleted legacy body allocated blocks", blocks, err)
	}
}

func TestJournalDeleteRetiresAcrossConfiguredEngines(t *testing.T) {
	for _, engine := range []string{"sqlite", "mysql", "postgres", "mongodb"} {
		t.Run(engine, func(t *testing.T) {
			ctx := context.Background()
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
			value := map[string]any{"id": "[TEST] deletion", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "private body", "response": "answer", "pointsStatus": "known"}
			if err = h.save("viewer", value, false); err != nil {
				t.Fatal(err)
			}
			if err = m.store.Save(ctx, "viewer", "own", value); err != nil {
				t.Fatal(err)
			}
			// The selected engine must not control retirement in the separate durable journal.
			m.config.Engine = engine
			if err = m.commit(ctx, "viewer", "own", "delete", historyText(value["id"]), map[string]any{}); err != nil {
				t.Fatal(err)
			}
			var payload string
			if err = h.db.QueryRow("SELECT payload FROM exchanges WHERE account='viewer'").Scan(&payload); err != nil {
				t.Fatal(err)
			}
			if historyBodyIsPacked(payload) || strings.Contains(payload, "private body") || strings.Contains(payload, "answer") {
				t.Fatal("delete left durable body")
			}
			var refs int
			if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM (SELECT * FROM nn_history_text_body_refs UNION ALL SELECT * FROM nn_history_image_body_refs) WHERE source='journal'").Scan(&refs); err != nil || refs != 0 {
				t.Fatal("delete retained journal references", refs, err)
			}
			if err = h.save("viewer", value, false); err != nil {
				t.Fatal("duplicate capture no longer idempotent", err)
			}
			if _, err = h.get("viewer", historyText(value["id"])); err == nil {
				t.Fatal("duplicate capture resurrected tombstone")
			}
			if err = h.collectJournalBodies(ctx, 100); err != nil {
				t.Fatal(err)
			}
			var blocks int
			if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM (SELECT * FROM nn_history_text_body_blocks UNION ALL SELECT * FROM nn_history_image_body_blocks)").Scan(&blocks); err != nil || blocks != 0 {
				t.Fatal("deleted body blocks remain", blocks, err)
			}
		})
	}
}

func TestJournalRetirementFailureKeepsDurableReferences(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	if _, err = h.db.Exec(historyManagerSchema); err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"id": "[TEST] failure", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "keep recoverable"}
	if err = h.save("viewer", value, false); err != nil {
		t.Fatal(err)
	}
	if _, err = h.db.Exec("UPDATE exchanges SET deleted_at=CURRENT_TIMESTAMP; CREATE TRIGGER prevent_retirement BEFORE UPDATE OF payload ON text_exchanges BEGIN SELECT RAISE(ABORT,'[TEST] retry'); END"); err != nil {
		t.Fatal(err)
	}
	if err = h.collectJournalBodies(ctx, 100); err == nil {
		t.Fatal("failed durable retirement reported success")
	}
	var raw string
	if err = h.db.QueryRow("SELECT payload FROM exchanges").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	got, err := h.decodeBody(ctx, "viewer", raw)
	if err != nil || got["request"] != value["request"] {
		t.Fatal("failed retirement released body", err)
	}
	if _, err = h.db.Exec("DROP TRIGGER prevent_retirement"); err != nil {
		t.Fatal(err)
	}
	if err = h.collectJournalBodies(ctx, 100); err != nil {
		t.Fatal("retirement retry failed", err)
	}
	var refs int
	if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM (SELECT * FROM nn_history_text_body_refs UNION ALL SELECT * FROM nn_history_image_body_refs)").Scan(&refs); err != nil || refs != 0 {
		t.Fatal("retry retained references", refs, err)
	}
}

func TestCategoryJournalRetirementKeepsPendingEntitiesAfterRestart(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	h, err := openChatHistoryAt(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if h != nil {
			h.db.Close()
		}
	}()
	if _, err = h.db.Exec(historyManagerSchema); err != nil {
		t.Fatal(err)
	}
	if err = initializeCategoryPending(h.db.DB); err != nil {
		t.Fatal(err)
	}
	for _, category := range []string{"text", "image"} {
		value := map[string]any{"id": category, "modelCategory": category, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "[TEST] shared request", "response": "[TEST] shared response", "pointsStatus": "known", "pointsConsumed": "0"}
		if err = h.save("viewer", value, false); err != nil {
			t.Fatal(err)
		}
		header, err := h.encodeBody(ctx, "pending", "viewer", "own", category, value)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = queueEncodedHistoryEvent(ctx, h.db.DB, "viewer", "own", "record", category, value, header); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = h.db.Exec("UPDATE exchanges SET deleted_at=CURRENT_TIMESTAMP,pending=0"); err != nil {
		t.Fatal(err)
	}
	if err = h.collectJournalBodies(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if err = h.db.Close(); err != nil {
		t.Fatal(err)
	}
	h = nil
	h, err = openChatHistoryAt(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, category := range []string{"text", "image"} {
		var raw string
		if err = h.db.QueryRow("SELECT payload FROM exchanges WHERE id=?", category).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var metadata map[string]any
		if err = json.Unmarshal([]byte(raw), &metadata); err != nil || historyBodyIsPacked(raw) || metadata["modelCategory"] != category || metadata["pointsStatus"] != "known" {
			t.Fatal("category tombstone metadata changed", err)
		}
		var refs int
		if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM nn_history_" + category + "_body_refs WHERE source='journal'").Scan(&refs); err != nil || refs != 0 {
			t.Fatal("category journal refs leaked", refs, err)
		}
		var seq int64
		var ref string
		if err = h.db.QueryRow("SELECT seq,payload FROM history_pending WHERE id=?", category).Scan(&seq, &ref); err != nil {
			t.Fatal(err)
		}
		entity, err := resolveHistoryEvent(ctx, h.db.DB, seq, ref)
		if err != nil {
			t.Fatal(err)
		}
		got, err := h.decodeBody(ctx, "viewer", entity)
		if err != nil || got["request"] != "[TEST] shared request" || got["modelCategory"] != category {
			t.Fatal("categorized pending recovery lost", err)
		}
	}
	for {
		n, err := h.migrateJournalBodies(ctx, 2)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	if _, err = h.db.Exec("DELETE FROM history_pending"); err != nil {
		t.Fatal(err)
	}
	if err = h.collectJournalBodies(ctx, 100); err != nil {
		t.Fatal(err)
	}
	for _, category := range []string{"text", "image"} {
		var count int
		if err = h.db.bodyDB.QueryRow("SELECT count(*) FROM nn_history_" + category + "_body_blocks").Scan(&count); err != nil || count != 0 {
			t.Fatal("retired category blocks remained", category, count, err)
		}
	}
}
