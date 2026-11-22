package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSQLHistoryCompressedBodiesSharedAndIsolated(t *testing.T) {
	ctx := context.Background()
	r, err := openSQLHistory(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.(*sqlHistoryRepository)
	request := strings.Repeat("用户问题 Alpha %_ 跨块文字\n", 30000)
	for _, id := range []string{"first", "second"} {
		v := map[string]any{"id": id, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": request, "response": "data: 保留\r\n\n", "requestId": "original"}
		if err = s.Save(ctx, "owner", "own", v); err != nil {
			t.Fatal(err)
		}
	}
	var raw, search string
	if err = s.db.QueryRow("SELECT payload,search_text FROM nn_history_text_records WHERE id='first'").Scan(&raw, &search); err != nil {
		t.Fatal(err)
	}
	var header map[string]any
	if json.Unmarshal([]byte(raw), &header) != nil || header["_newnoaBody"] == nil || header["requestId"] != "original" {
		t.Fatal("new history payload must be compact valid JSON retaining metadata")
	}
	if strings.Contains(raw, request[:500]) || search != "" {
		t.Fatal("plaintext or search duplicate persisted")
	}
	var blocks int
	if err = s.db.QueryRow("SELECT count(*) FROM nn_history_text_body_blocks").Scan(&blocks); err != nil || blocks == 0 {
		t.Fatal(blocks, err)
	}
	if err = s.Save(ctx, "stranger", "own", map[string]any{"id": "third", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": request, "response": "data: 保留\r\n\n"}); err != nil {
		t.Fatal(err)
	}
	var isolated int
	if err = s.db.QueryRow("SELECT count(*) FROM nn_history_text_body_blocks").Scan(&isolated); err != nil || isolated != blocks*2 {
		t.Fatal("blocks must be account-scoped", blocks, isolated, err)
	}
	if err = s.Delete(ctx, "owner", "own", "first"); err != nil {
		t.Fatal(err)
	}
	value, err := s.Get(ctx, "owner", "own", "second")
	if err != nil || value["request"] != request || value["response"] != "data: 保留\r\n\n" {
		t.Fatal("shared body damaged", err)
	}
	if _, err = s.Get(ctx, "stranger", "own", "second"); err == nil {
		t.Fatal("cross-account record visible")
	}
}

func TestSQLHistoryBodySearchPaginatesWithFullTotal(t *testing.T) {
	ctx := context.Background()
	r, err := openSQLHistory(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	now := time.Now().UTC()
	body := strings.Repeat("甲", 87381) + "MiXeD %_界" + strings.Repeat("乙", 100)
	for i := 0; i < 105; i++ {
		id := fmt.Sprintf("%03d", i)
		text := "absent"
		if i%2 == 0 {
			text = body
		}
		if err = r.Save(ctx, "owner", "own", map[string]any{"id": id, "createdAt": now.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano), "request": text, "response": "保留", "model": "model", "keyName": "Team"}); err != nil {
			t.Fatal(err)
		}
	}
	q := historyQuery{Scope: "own", Q: "mixed %_界", Limit: 7, KeyName: "team"}
	result, err := r.Query(ctx, "owner", q)
	if err != nil || result.Total != 53 || len(result.Items) != 7 || result.NextCursor == "" {
		t.Fatal("first page", result.Total, len(result.Items), err)
	}
	if _, exists := result.Items[0]["request"]; exists {
		t.Fatal("metadata query leaked body")
	}
	if result.Items[0]["id"] != "104" {
		t.Fatal("sort order", result.Items[0]["id"])
	}
	q.Before = result.NextCursor
	q.IncludeContent = true
	second, err := r.Query(ctx, "owner", q)
	if err != nil || second.Total != 53 || len(second.Items) != 7 || second.Items[0]["id"] != "090" || second.Items[0]["request"] != body {
		t.Fatal("second page", second.Total, len(second.Items), err)
	}
}

func TestSQLHistoryBodyMigrationAndCorruptionPreservesPlaintext(t *testing.T) {
	ctx := context.Background()
	config := defaultHistoryDatabase(t.TempDir())
	r, err := openSQLHistory(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	s := r.(*sqlHistoryRepository)
	value := map[string]any{"id": "legacy", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": strings.Repeat("保留原文", 10000), "response": "answer"}
	if err = s.Save(ctx, "owner", "own", value); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(value)
	if _, err = s.db.Exec("UPDATE nn_history_text_records SET payload=?,search_text=?,body_version=0", string(raw), strings.ToLower(historyText(value["request"]))); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE nn_history_text_body_blocks SET data=?", []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	if err = s.migrateBodies(ctx); err == nil {
		t.Fatal("corrupt reused block accepted")
	}
	var after string
	var version int
	if err = s.db.QueryRow("SELECT payload,body_version FROM nn_history_text_records").Scan(&after, &version); err != nil || after != string(raw) || version != 0 {
		t.Fatal("failed migration lost plaintext", err)
	}
	if _, err = s.db.Exec("DELETE FROM nn_history_text_body_refs"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("DELETE FROM nn_history_text_body_blocks"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = openSQLHistory(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s = r.(*sqlHistoryRepository)
	if err = s.migrateBodies(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "owner", "own", "legacy")
	if err != nil || got["request"] != value["request"] {
		t.Fatal("migrated body changed", err)
	}
	var rawSearch string
	if err = s.db.QueryRow("SELECT payload,body_version,search_text FROM nn_history_text_records").Scan(&after, &version, &rawSearch); err != nil {
		t.Fatal(err)
	}
	if !historyBodyIsPacked(after) || version != 1 || rawSearch != "" {
		t.Fatal("legacy plaintext not removed")
	}
	if err = s.migrateBodies(ctx); err != nil {
		t.Fatal("migration not idempotent", err)
	}
}

func TestSQLHistoryDuplicateSaveAndRollbackKeepReferences(t *testing.T) {
	ctx := context.Background()
	r, err := openSQLHistory(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	s := r.(*sqlHistoryRepository)
	original := map[string]any{"id": "immutable", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": strings.Repeat("original", 10000)}
	if err = s.Save(ctx, "owner", "own", original); err != nil {
		t.Fatal(err)
	}
	conflicting := map[string]any{"id": "immutable", "createdAt": original["createdAt"], "request": "conflicting replacement"}
	if err = s.Save(ctx, "owner", "own", conflicting); err != nil {
		t.Fatal(err)
	}
	if err = s.collectBodies(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "owner", "own", "immutable")
	if err != nil || got["request"] != original["request"] {
		t.Fatal("conflicting save replaced original refs", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.bodies.PackTx(ctx, tx, "records", "owner", "own", "immutable", conflicting); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = s.collectBodies(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = s.Get(ctx, "owner", "own", "immutable")
	if err != nil || got["request"] != original["request"] {
		t.Fatal("rolled back write damaged refs", err)
	}
	header, err := s.bodies.Pack(ctx, "journal", "owner", "own", "immutable", original)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Delete(ctx, "owner", "own", "immutable"); err != nil {
		t.Fatal(err)
	}
	got, err = s.bodies.Unpack(ctx, "owner", header)
	if err != nil || got["request"] != original["request"] {
		t.Fatal("query delete removed journal shared blocks", err)
	}
	if err = s.bodies.DeleteRefs(ctx, "journal", "owner", "own", "immutable"); err != nil {
		t.Fatal(err)
	}
	if err = s.bodies.Collect(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = s.db.QueryRow("SELECT count(*) FROM nn_history_text_body_blocks").Scan(&count); err != nil || count != 0 {
		t.Fatal("orphan block remained", count, err)
	}
}

func TestSQLHistoryCompressedBodyEngineMatrix(t *testing.T) {
	password := os.Getenv("HISTORY_DATABASE_PASSWORD")
	if password == "" {
		t.Skip("requires authorized dedicated history sandbox")
	}
	for _, target := range []struct {
		name, engine string
		port         int
	}{{"mysql8", "mysql", 3308}, {"mysql9", "mysql", 3309}, {"mysql26", "mysql", 3326}, {"postgres16", "postgres", 5416}, {"postgres17", "postgres", 5417}, {"postgres18", "postgres", 5418}} {
		t.Run(target.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			config := historySandboxDatabase(t, defaultHistoryDatabase(t.TempDir()))
			config.Engine = target.engine
			config.Host = "127.0.0.1"
			config.Port = target.port
			config.Username = "newnoa"
			config.Password = password
			r, err := openSQLHistory(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			s := r.(*sqlHistoryRepository)
			viewer := "[TEST]-body-" + uuid.NewString()
			defer func() {
				for _, table := range []string{"nn_history_text_body_refs", "nn_history_text_body_blocks", "nn_history_text_records", "nn_history_candidates"} {
					if _, err := s.db.ExecContext(context.Background(), s.bind("DELETE FROM "+table+" WHERE viewer=?"), viewer); err != nil {
						t.Error("cleanup", err)
					}
				}
			}()
			request := strings.Repeat("跨库相同 context \r\n%_ ", 60000) + "NEEDLE跨块"
			now := time.Now().UTC().Format(time.RFC3339Nano)
			for _, id := range []string{"first", "second"} {
				if err = s.Save(ctx, viewer, "own", map[string]any{"id": id, "createdAt": now, "request": request, "response": "SSE\r\n\r\n"}); err != nil {
					t.Fatal(err)
				}
			}
			var count int
			var storedBytes int64
			if err = s.db.QueryRowContext(ctx, s.bind("SELECT count(*),SUM(OCTET_LENGTH(data)) FROM nn_history_text_body_blocks WHERE viewer=?"), viewer).Scan(&count, &storedBytes); err != nil {
				t.Fatal(err)
			}
			if count < 1 || storedBytes >= int64(len(request))/10 {
				t.Fatal("unexpected compressed storage", count, storedBytes)
			}
			result, err := s.Query(ctx, viewer, historyQuery{Scope: "own", Q: "needle跨块", Limit: 1, IncludeContent: true})
			if err != nil || result.Total != 2 || len(result.Items) != 1 || result.Items[0]["request"] != request || result.NextCursor == "" {
				t.Fatal("search/expansion", result.Total, err)
			}
			if err = s.Delete(ctx, viewer, "own", "first"); err != nil {
				t.Fatal(err)
			}
			got, err := s.Get(ctx, viewer, "own", "second")
			if err != nil || got["request"] != request {
				t.Fatal("shared survivor", err)
			}
			raw, _ := json.Marshal(got)
			if _, err = s.db.ExecContext(ctx, s.bind("UPDATE nn_history_text_records SET payload=?,search_text=?,body_version=0 WHERE viewer=? AND id=?"), string(raw), strings.ToLower(request), viewer, "second"); err != nil {
				t.Fatal(err)
			}
			n, err := s.migrateBodiesBatch(ctx, 1)
			if err != nil || n != 1 {
				t.Fatal("legacy batch migration", n, err)
			}
			got, err = s.Get(ctx, viewer, "own", "second")
			if err != nil || got["request"] != request {
				t.Fatal("migrated content changed", err)
			}
			var payload, search string
			if err = s.db.QueryRowContext(ctx, s.bind("SELECT payload,search_text FROM nn_history_text_records WHERE viewer=? AND id=?"), viewer, "second").Scan(&payload, &search); err != nil || !historyBodyIsPacked(payload) || search != "" {
				t.Fatal("plaintext survived migration", err)
			}
			t.Logf("%s blocks=%d compressed_bytes=%d original_body_bytes=%d; sharing/search/delete/migration verified", target.name, count, storedBytes, len(request))
		})
	}
}
