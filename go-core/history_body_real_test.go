package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// This destructive compaction test accepts only the task's explicitly prepared offline copy.
func TestHistoryRealSQLiteOfflineMigration(t *testing.T) {
	home := os.Getenv("HISTORY_REAL_SQLITE_HOME")
	if home == "" {
		t.Skip("requires an explicitly prepared offline SQLite copy")
	}
	home, err := filepath.Abs(home)
	if err != nil {
		t.Fatal("invalid copied home")
	}
	suffix := filepath.Join(".local", "history-body-compression", "local-migration-home")
	if !strings.HasSuffix(home, string(filepath.Separator)+suffix) {
		t.Fatal("offline test requires the dedicated local-migration-home copy")
	}
	resolved, err := filepath.EvalSymlinks(home)
	if err != nil || resolved != home {
		t.Fatal("copied home must exist without symbolic links")
	}
	directory := filepath.Join(home, ".newnoa", "sqlite")
	originalHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal("cannot establish original home")
	}
	assertSafe := func() {
		t.Helper()
		resolved, err := filepath.EvalSymlinks(directory)
		if err != nil || resolved != directory {
			t.Fatal("copied SQLite directory must exist without symbolic links")
		}
		for _, name := range []string{"chat-history.sqlite", "history.sqlite"} {
			for _, extension := range []string{"", "-wal", "-shm"} {
				info, err := os.Lstat(filepath.Join(directory, name+extension))
				if os.IsNotExist(err) {
					continue
				}
				if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
					t.Fatal("unsafe copied database file")
				}
				for _, sourceDirectory := range []string{filepath.Join(originalHome, ".newnoa", "sqlite"), filepath.Join(originalHome, ".newnoa")} {
					sourceInfo, sourceErr := os.Stat(filepath.Join(sourceDirectory, name+extension))
					if sourceErr == nil && os.SameFile(info, sourceInfo) {
						t.Fatal("offline copy aliases the original database")
					}
				}
			}
		}
	}
	assertSafe()
	// A fresh report prevents an interrupted run from being mistaken for success.
	compatibilityOnly := os.Getenv("HISTORY_REAL_SQLITE_COMPAT_ONLY") == "1"
	reportName := "local-migration-result.json"
	if compatibilityOnly {
		reportName = "local-category-compatibility-result.json"
	}
	reportPath := filepath.Join(filepath.Dir(home), reportName)
	report := map[string]any{"compatibilityOnly": compatibilityOnly, "status": "running", "startedAt": time.Now().UTC().Format(time.RFC3339), "originalDatabaseOpened": false}
	writeReport := func() {
		t.Helper()
		raw, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(reportPath, append(raw, '\n'), 0600); err != nil {
			t.Fatal("cannot write aggregate migration evidence")
		}
	}
	defer func() {
		if t.Failed() {
			report["status"] = "failed"
		}
		writeReport()
	}()
	fileBytes := func() map[string]int64 {
		t.Helper()
		out := map[string]int64{}
		for _, name := range []string{"chat-history.sqlite", "history.sqlite"} {
			for _, extension := range []string{"", "-wal", "-shm"} {
				info, err := os.Stat(filepath.Join(directory, name+extension))
				if os.IsNotExist(err) {
					continue
				}
				if err != nil {
					t.Fatal("cannot measure copied database")
				}
				out[name+extension] = info.Size()
			}
		}
		return out
	}
	sumBytes := func(values map[string]int64) int64 {
		var n int64
		for _, v := range values {
			n += v
		}
		return n
	}
	beforeFiles := fileBytes()
	report["beforeFilesBytes"] = beforeFiles
	report["beforeTotalBytes"] = sumBytes(beforeFiles)
	writeReport()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()
	config := defaultHistoryDatabase(home)
	if config.SQLiteDirectory != directory {
		t.Fatal("runtime redirected the offline SQLite path")
	}
	var h *chatHistory
	var s *sqlHistoryRepository
	closeStores := func() error {
		var errs []error
		if s != nil {
			errs = append(errs, s.Close())
			s = nil
		}
		if h != nil {
			errs = append(errs, h.db.Close())
			h = nil
		}
		return errors.Join(errs...)
	}
	defer closeStores()
	openStores := func() {
		t.Helper()
		var err error
		h, err = openChatHistoryAt(directory)
		if err != nil {
			t.Fatal("cannot open copied journal")
		}
		repository, err := openSQLHistory(ctx, config)
		if err != nil {
			t.Fatal("cannot open copied query database")
		}
		s = repository.(*sqlHistoryRepository)
	}
	openStores()
	type identity struct{ table, viewer, scope, id string }
	type bodyHash struct {
		Hash              [32]byte
		Size              int
		Present, IsString bool
	}
	type fingerprint [2]bodyHash
	capture := func() (map[identity]fingerprint, int64, map[string]int) {
		t.Helper()
		values := map[identity]fingerprint{}
		var total int64
		counts := map[string]int{}
		tables := []struct {
			name, query string
			db          *sql.DB
			decode      func(context.Context, string, string) (map[string]any, error)
		}{
			{"records", "SELECT l.rowid,r.viewer,r.scope,r.id,r.payload FROM nn_history_records r JOIN nn_history_record_locations l ON l.viewer=r.viewer AND l.scope=r.scope AND l.id=r.id WHERE l.rowid>? AND r.deleted=0 ORDER BY l.rowid LIMIT 1", s.db, s.unpack},
			{"journal", "SELECT rowid,account,'own',id,payload FROM exchanges WHERE rowid>? AND deleted_at IS NULL ORDER BY rowid LIMIT 1", h.db.DB, h.decodeBody},
		}
		var pending int
		if err := h.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='history_pending'").Scan(&pending); err != nil {
			t.Fatal("cannot inspect copied pending schema")
		}
		if pending != 0 {
			tables = append(tables, struct {
				name, query string
				db          *sql.DB
				decode      func(context.Context, string, string) (map[string]any, error)
			}{"pending", "SELECT seq,viewer,scope,CAST(seq AS TEXT),payload FROM history_pending WHERE seq>? AND kind='record' ORDER BY seq LIMIT 1", h.db.DB, h.decodeBody})
		}
		for _, table := range tables {
			var after int64
			for {
				var next int64
				var viewer, scope, id, raw string
				err := table.db.QueryRowContext(ctx, table.query, after).Scan(&next, &viewer, &scope, &id, &raw)
				if err == sql.ErrNoRows {
					break
				}
				if err != nil {
					t.Fatal("cannot scan copied history bodies")
				}
				if table.name == "pending" {
					var e error
					raw, e = resolveHistoryEvent(ctx, h.db.DB, next, raw)
					if e != nil {
						t.Fatal("cannot resolve copied categorized pending event")
					}
				}
				value, err := table.decode(ctx, viewer, raw)
				if err != nil {
					t.Fatal("copied history body failed verification")
				}
				var fp fingerprint
				for i, field := range []string{"request", "response"} {
					v, present := value[field]
					body, isString := v.(string)
					fp[i] = bodyHash{Hash: sha256.Sum256([]byte(body)), Size: len(body), Present: present, IsString: isString}
					total += int64(len(body))
				}
				values[identity{table.name, viewer, scope, id}] = fp
				counts[table.name]++
				after = next
			}
		}
		return values, total, counts
	}
	report["phase"] = "baseline-body-hashes"
	writeReport()
	expected, logicalBytes, counts := capture()
	if len(expected) == 0 {
		t.Fatal("offline fixture contains no history bodies")
	}
	report["recordCounts"] = counts
	report["logicalBodyBytes"] = logicalBytes
	report["verifiedBeforeRecords"] = len(expected)
	writeReport()
	verify := func() {
		t.Helper()
		actual, _, _ := capture()
		if len(actual) != len(expected) {
			t.Fatal("record count changed during offline migration")
		}
		for key, value := range expected {
			if actual[key] != value {
				t.Fatal("body SHA, byte length or field presence changed during offline migration")
			}
		}
	}
	started := time.Now()
	sqlMigrated, journalMigrated := 0, 0
	report["phase"] = "sql-body-migration"
	writeReport()
	for {
		n, err := s.migrateBodiesBatch(ctx, 32)
		if err != nil {
			t.Fatal("SQL migration failed on offline copy")
		}
		sqlMigrated += n
		if n == 0 {
			break
		}
	}
	report["phase"] = "journal-body-migration"
	writeReport()
	for {
		n, err := h.migrateJournalBodies(ctx, 32)
		if err != nil {
			t.Fatal("journal migration failed on offline copy")
		}
		journalMigrated += n
		if n == 0 {
			break
		}
	}
	if err = s.collectBodies(ctx); err != nil {
		t.Fatal("copied query body collection failed")
	}
	report["phase"] = "verify-migrated-body-hashes"
	writeReport()
	verify()
	report["sqlMigratedRecords"] = sqlMigrated
	report["journalMigratedRecords"] = journalMigrated
	report["migrationElapsedMs"] = time.Since(started).Milliseconds()
	report["verifiedAfterMigrationRecords"] = len(expected)
	var blockCount, blockRawBytes, blockStoredBytes, referenceCount, orphanCount, plainSQL, plainJournal int64
	if err = s.db.QueryRowContext(ctx, "SELECT count(*),COALESCE(SUM(raw_size),0),COALESCE(SUM(length(data)),0) FROM (SELECT * FROM nn_history_text_body_blocks UNION ALL SELECT * FROM nn_history_image_body_blocks)").Scan(&blockCount, &blockRawBytes, &blockStoredBytes); err != nil {
		t.Fatal("cannot measure copied block store")
	}
	if err = s.db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT * FROM nn_history_text_body_refs UNION ALL SELECT * FROM nn_history_image_body_refs)").Scan(&referenceCount); err != nil {
		t.Fatal("cannot measure copied references")
	}
	if err = s.db.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM nn_history_text_body_blocks b WHERE NOT EXISTS (SELECT 1 FROM nn_history_text_body_refs r WHERE r.viewer=b.viewer AND r.hash=b.hash))+(SELECT count(*) FROM nn_history_image_body_blocks b WHERE NOT EXISTS (SELECT 1 FROM nn_history_image_body_refs r WHERE r.viewer=b.viewer AND r.hash=b.hash))").Scan(&orphanCount); err != nil || orphanCount != 0 {
		t.Fatal("unreferenced blocks remain after copied query collection")
	}
	if err = s.db.QueryRowContext(ctx, "SELECT count(*) FROM nn_history_records WHERE deleted=0 AND (json_type(payload,'$._newnoaBody') IS NULL OR search_text<>'')").Scan(&plainSQL); err != nil || plainSQL != 0 {
		t.Fatal("query plaintext or search projection remains")
	}
	if err = h.db.QueryRowContext(ctx, "SELECT count(*) FROM exchanges WHERE deleted_at IS NULL AND json_type(payload,'$._newnoaBody') IS NULL").Scan(&plainJournal); err != nil || plainJournal != 0 {
		t.Fatal("journal plaintext remains")
	}
	// The primary key and verified loader enforce one physical block per account/hash.
	var duplicates int64
	if err = s.db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT model_category,viewer,hash FROM (SELECT 'text' AS model_category,viewer,hash FROM nn_history_text_body_blocks UNION ALL SELECT 'image',viewer,hash FROM nn_history_image_body_blocks) GROUP BY model_category,viewer,hash HAVING count(*)>1)").Scan(&duplicates); err != nil || duplicates != 0 {
		t.Fatal("duplicate physical content blocks")
	}
	referenceSources := map[string]int64{}
	sourceRows, err := s.db.QueryContext(ctx, "SELECT source,count(*) FROM (SELECT * FROM nn_history_text_body_refs UNION ALL SELECT * FROM nn_history_image_body_refs) GROUP BY source")
	if err != nil {
		t.Fatal("cannot count shared reference sources")
	}
	for sourceRows.Next() {
		var source string
		var count int64
		if err = sourceRows.Scan(&source, &count); err != nil {
			sourceRows.Close()
			t.Fatal("cannot scan shared reference sources")
		}
		referenceSources[source] = count
	}
	if err = sourceRows.Err(); err != nil {
		sourceRows.Close()
		t.Fatal("cannot scan shared reference sources")
	}
	if err = sourceRows.Close(); err != nil {
		t.Fatal("cannot close shared reference scan")
	}
	var journalVisibleBlocks int64
	if err = h.db.bodyDB.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT * FROM nn_history_text_body_blocks UNION ALL SELECT * FROM nn_history_image_body_blocks)").Scan(&journalVisibleBlocks); err != nil || journalVisibleBlocks != blockCount {
		t.Fatal("journal and default query store do not share body blocks")
	}
	report["referenceSources"] = referenceSources
	report["journalVisibleSharedBlocks"] = journalVisibleBlocks
	report["blockCount"] = blockCount
	report["uniqueBlockRawBytes"] = blockRawBytes
	report["compressedBlockBytes"] = blockStoredBytes
	report["referenceCount"] = referenceCount
	report["duplicateBlocks"] = duplicates
	report["orphanBlocks"] = orphanCount
	report["preVacuumFilesBytes"] = fileBytes()
	writeReport()
	if err = closeStores(); err != nil {
		t.Fatal("cannot close copied stores before compaction")
	}
	assertSafe()
	report["phase"] = "offline-vacuum-integrity"
	writeReport()
	for _, name := range []string{"chat-history.sqlite", "history.sqlite"} {
		db, err := sql.Open("sqlite", filepath.Join(directory, name))
		if err != nil {
			t.Fatal("cannot open offline compaction database")
		}
		if _, err = db.ExecContext(ctx, "PRAGMA busy_timeout=5000; PRAGMA wal_checkpoint(TRUNCATE); VACUUM;"); err != nil {
			db.Close()
			t.Fatal("offline SQLite compaction failed")
		}
		var integrity string
		err = db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity)
		closeErr := db.Close()
		if err != nil || closeErr != nil || integrity != "ok" {
			t.Fatal("offline compacted SQLite integrity check failed")
		}
	}
	afterFiles := fileBytes()
	report["afterFilesBytes"] = afterFiles
	report["afterTotalBytes"] = sumBytes(afterFiles)
	report["reclaimedBytes"] = sumBytes(beforeFiles) - sumBytes(afterFiles)
	report["phase"] = "reopen-body-verification"
	writeReport()
	openStores()
	verify()
	report["verifiedAfterReopenRecords"] = len(expected)
	if err = closeStores(); err != nil {
		t.Fatal("cannot close reopened copied stores")
	}
	if !compatibilityOnly && sumBytes(afterFiles) >= sumBytes(beforeFiles) {
		t.Fatal("offline migration did not reduce actual SQLite file bytes")
	}
	report["phase"] = "complete"
	report["status"] = "passed"
	report["finishedAt"] = time.Now().UTC().Format(time.RFC3339)
	writeReport()
	t.Logf("offline SQLite migration verified_records=%d blocks=%d original_file_bytes=%d compacted_file_bytes=%d reclaimed_bytes=%d", len(expected), blockCount, sumBytes(beforeFiles), sumBytes(afterFiles), sumBytes(beforeFiles)-sumBytes(afterFiles))
}
