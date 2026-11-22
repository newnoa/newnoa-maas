package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Replays actual Java/NAS wire archives through Core's production sync and SQLite paths.
func TestHistoryNASActualWireSQLiteImport(t *testing.T) {
	path := os.Getenv("HISTORY_NAS_SYNC_WIRE_FIXTURE")
	if path == "" {
		t.Skip("requires private actual Java/NAS history wire fixture")
	}
	if !filepath.IsAbs(path) {
		t.Fatal("wire fixture must use an absolute private file path")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != filepath.Clean(path) {
		t.Fatal("wire fixture must exist without symbolic links")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 32<<20 {
		t.Fatal("wire fixture must be a bounded private regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("cannot open private wire fixture")
	}
	raw, err := io.ReadAll(io.LimitReader(file, (32<<20)+1))
	closeErr := file.Close()
	if err != nil || closeErr != nil || len(raw) > 32<<20 {
		t.Fatal("cannot read bounded wire fixture")
	}
	var fixture struct {
		SchemaVersion int             `json:"schemaVersion"`
		Snapshot      json.RawMessage `json:"snapshot"`
		Batch         json.RawMessage `json:"batch"`
		Blocks        struct {
			SchemaVersion int              `json:"schemaVersion"`
			Blocks        []historyArchive `json:"blocks"`
		} `json:"blocks"`
		Expected struct {
			ID                string `json:"id"`
			Request, Response struct {
				SHA256   string `json:"sha256"`
				RawBytes int    `json:"rawBytes"`
			}
		} `json:"expected"`
	}
	if json.Unmarshal(raw, &fixture) != nil || fixture.SchemaVersion != 1 || fixture.Expected.ID == "" || fixture.Blocks.SchemaVersion != 1 {
		t.Fatal("invalid private wire fixture envelope")
	}
	for _, field := range []struct {
		hash string
		size int
	}{{fixture.Expected.Request.SHA256, fixture.Expected.Request.RawBytes}, {fixture.Expected.Response.SHA256, fixture.Expected.Response.RawBytes}} {
		if !historyBodyValidHash(field.hash) || field.size < 0 {
			t.Fatal("invalid expected field digest")
		}
	}
	var batch struct {
		SchemaVersion int `json:"schemaVersion"`
		Items         []struct {
			ID, Kind string
			Metadata map[string]any `json:"metadata"`
		}
		Done bool `json:"done"`
	}
	if json.Unmarshal(fixture.Batch, &batch) != nil || batch.SchemaVersion != 1 || !batch.Done {
		t.Fatal("wire fixture must contain one complete actual batch")
	}
	account, scope := "", ""
	for _, item := range batch.Items {
		if item.ID == fixture.Expected.ID && item.Kind == "record" {
			scope = historyText(item.Metadata["scope"])
			if scope == "own" {
				account = historyText(item.Metadata["callerAccountId"])
			} else if scope == "others" {
				account = historyText(item.Metadata["publisherAccountId"])
			}
		}
	}
	if !nodePattern.MatchString(account) || scope == "" {
		t.Fatal("expected record missing authenticated scope metadata")
	}
	blocks := map[string]historyArchive{}
	for _, block := range fixture.Blocks.Blocks {
		if block.Codec != "zstd-v1" && block.Codec != "gzip-v1" || !historyBodyValidHash(block.ID) || block.SHA256 != block.ID {
			t.Fatal("fixture contains unsupported actual content block")
		}
		if _, exists := blocks[block.ID]; exists {
			t.Fatal("fixture duplicates a content block")
		}
		blocks[block.ID] = block
	}
	if len(blocks) == 0 {
		t.Fatal("fixture has no actual archived blocks")
	}
	var starts, batches, blockRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/me/history/settings":
			json.NewEncoder(w).Encode(map[string]any{"canSyncOwn": true, "canSyncOthers": true, "cloudEnabled": true})
		case "/api/v1/me/history/sync/start":
			starts.Add(1)
			w.Write(fixture.Snapshot)
		case "/api/v1/me/history/sync/batch":
			batches.Add(1)
			w.Write(fixture.Batch)
		case "/api/v1/me/history/sync/blocks":
			var input struct {
				IDs []string `json:"ids"`
			}
			if json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&input) != nil || len(input.IDs) != 1 {
				w.WriteHeader(400)
				return
			}
			block, ok := blocks[input.IDs[0]]
			if !ok {
				w.WriteHeader(404)
				return
			}
			blockRequests.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": fixture.Blocks.SchemaVersion, "blocks": []historyArchive{block}})
		default:
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	home := t.TempDir()
	h, err := openChatHistoryAt(defaultHistoryDatabase(home).SQLiteDirectory)
	if err != nil {
		t.Fatal("cannot open isolated SQLite journal")
	}
	defer h.db.Close()
	m, err := newHistoryManager(home, h)
	if err != nil {
		t.Fatal("cannot open isolated SQLite history store")
	}
	defer m.store.Close()
	c := newCore(home, server.URL)
	c.control = server.URL
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	verify := func() {
		t.Helper()
		value, err := m.store.Get(ctx, account, scope, fixture.Expected.ID)
		if err != nil {
			t.Fatal("actual Java/NAS wire record unavailable after production import")
		}
		for _, field := range []struct {
			name, hash string
			size       int
		}{{"request", fixture.Expected.Request.SHA256, fixture.Expected.Request.RawBytes}, {"response", fixture.Expected.Response.SHA256, fixture.Expected.Response.RawBytes}} {
			body, ok := value[field.name].(string)
			if !ok || len(body) != field.size || historyHash([]byte(body)) != field.hash {
				t.Fatal("actual wire field digest or length differs", field.name)
			}
		}
	}
	if err = c.syncHistory(ctx, m, account, "[TEST] fixture session", scope, ""); err != nil {
		t.Fatal("production Core sync rejected actual Java/NAS wire archive")
	}
	verify()
	sqlStore, ok := m.store.(*sqlHistoryRepository)
	if !ok {
		t.Fatal("wire import did not use real SQLite history repository")
	}
	var firstBlocks, firstRefs int
	if err = sqlStore.db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT * FROM nn_history_text_body_blocks UNION ALL SELECT * FROM nn_history_image_body_blocks)").Scan(&firstBlocks); err != nil || firstBlocks == 0 {
		t.Fatal("import did not persist compressed blocks")
	}
	if err = sqlStore.db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT * FROM nn_history_text_body_refs UNION ALL SELECT * FROM nn_history_image_body_refs)").Scan(&firstRefs); err != nil || firstRefs == 0 {
		t.Fatal("import did not persist body references")
	}
	var storedHeader, search string
	if err = sqlStore.db.QueryRowContext(ctx, "SELECT payload,search_text FROM nn_history_records WHERE viewer=? AND scope=? AND id=?", account, scope, fixture.Expected.ID).Scan(&storedHeader, &search); err != nil || !historyBodyIsPacked(storedHeader) || search != "" {
		t.Fatal("actual wire import retained plaintext payload/projection")
	}
	firstRequests := blockRequests.Load()
	if firstRequests == 0 {
		t.Fatal("actual wire content blocks bypassed production fetch/decode")
	}
	// Repeat the same actual snapshot from its original cursor; immutable records and blocks must deduplicate.
	if _, err = h.db.ExecContext(ctx, "DELETE FROM history_sync_cursors"); err != nil {
		t.Fatal("cannot reset isolated fixture cursor")
	}
	if err = c.syncHistory(ctx, m, account, "[TEST] fixture session", scope, ""); err != nil {
		t.Fatal("production repeat import rejected actual wire archive")
	}
	verify()
	var repeatedBlocks, repeatedRefs int
	if err = sqlStore.db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT * FROM nn_history_text_body_blocks UNION ALL SELECT * FROM nn_history_image_body_blocks)").Scan(&repeatedBlocks); err != nil {
		t.Fatal("cannot count repeated imported blocks")
	}
	if err = sqlStore.db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT * FROM nn_history_text_body_refs UNION ALL SELECT * FROM nn_history_image_body_refs)").Scan(&repeatedRefs); err != nil {
		t.Fatal("cannot count repeated imported references")
	}
	if repeatedBlocks != firstBlocks || repeatedRefs != firstRefs || blockRequests.Load() != firstRequests {
		t.Fatal("repeated actual wire import did not deduplicate SQLite blocks/cache")
	}
	if starts.Load() != 2 || batches.Load() != 2 {
		t.Fatal("fixture did not exercise complete production synchronization twice")
	}
	t.Logf("actual Java/NAS wire verified request_bytes=%d response_bytes=%d fetched_blocks=%d SQLite_blocks=%d SQLite_refs=%d repeated_import_deduplicated=true", fixture.Expected.Request.RawBytes, fixture.Expected.Response.RawBytes, firstRequests, firstBlocks, firstRefs)
}
