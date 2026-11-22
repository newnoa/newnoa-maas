package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestHistoryCategoryQueriesUseProjectedIndex(t *testing.T) {
	ctx := context.Background()
	repo, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	store := repo.(*sqlHistoryRepository)
	leaf := store.leaves["image"]
	for i := 0; i < 40; i++ {
		kind := "text"
		if i%10 == 0 {
			kind = "image"
		}
		if err = store.Save(ctx, "[TEST] viewer", "own", map[string]any{"id": fmt.Sprintf("[TEST] %03d", i), "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": kind, "model": "[TEST] model", "request": strings.Repeat("large unrelated text ", 4096)}); err != nil {
			t.Fatal(err)
		}
	}
	// Drop/recreate on the same physical leaf connection used by explicit queries.
	if _, err = leaf.db.Exec("DROP INDEX IF EXISTS nn_history_image_records_filter_model_category"); err != nil {
		t.Fatal(err)
	}
	if err = leaf.initializeFilters(ctx); err != nil {
		t.Fatal(err)
	}
	q := historyQuery{Scope: "own", ModelCategory: "image", Limit: 2}
	where, args, err := store.where(ctx, "[TEST] viewer", q, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"SELECT count(*) FROM nn_history_records WHERE " + where, "SELECT metadata,created,id FROM nn_history_records WHERE " + where + " ORDER BY created DESC,id DESC LIMIT 3"} {
		rows, e := store.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+leaf.categorySQL(query), args...)
		if e != nil {
			t.Fatal(e)
		}
		plan := ""
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if e = rows.Scan(&id, &parent, &unused, &detail); e != nil {
				t.Fatal(e)
			}
			plan += detail + "\n"
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			t.Fatal(e)
		}
		if strings.HasPrefix(query, "SELECT count") && !strings.Contains(plan, "COVERING INDEX") {
			t.Fatalf("count must not visit large payload pages: %s", plan)
		}
		if !strings.Contains(plan, "nn_history_image_records_filter_model_category") || !strings.Contains(plan, "model_category=?") || strings.Contains(plan, "TEMP B-TREE") {
			t.Fatalf("category query must avoid unrelated record pages and sorting: %s", plan)
		}
	}
	page, err := store.Query(ctx, "[TEST] viewer", q)
	if err != nil || page.Total != 4 || len(page.Items) != 2 || page.NextCursor == "" {
		t.Fatalf("category pagination: %+v %v", page, err)
	}
	q.Before = page.NextCursor
	next, err := store.Query(ctx, "[TEST] viewer", q)
	if err != nil || next.Total != 4 || len(next.Items) != 2 || next.Items[0]["id"] == page.Items[0]["id"] {
		t.Fatalf("category next page: %+v %v", next, err)
	}
}

func TestHistoryCategoryFilterPlansAvoidPayloadPages(t *testing.T) {
	ctx := context.Background()
	repo, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	store := repo.(*sqlHistoryRepository)
	leaf := store.leaves["image"]
	for i := 0; i < 40; i++ {
		if err = store.Save(ctx, "[TEST] viewer", "own", map[string]any{"id": fmt.Sprintf("[TEST] facet %03d", i), "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": "text", "model": "[TEST] model", "keyId": fmt.Sprintf("[TEST] key %02d", i%3), "keyName": "[TEST] key", "request": strings.Repeat("large unrelated payload ", 4096)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"keys", "models"} {
		id, name, prefix := "key_id", "key_label", "key_prefix"
		if kind == "models" {
			id = "CASE WHEN deployment_id<>'' THEN deployment_id ELSE model_id END"
			name = "model_label"
			prefix = "''"
		}
		query := "SELECT " + id + " AS filter_id,MAX(" + name + ") AS filter_name,MAX(" + prefix + ") AS filter_prefix FROM nn_history_records WHERE viewer=? AND scope=? AND deleted=0 AND " + id + "<>'' AND model_category=? GROUP BY " + id + " ORDER BY filter_id LIMIT ?"
		rows, e := store.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+leaf.categorySQL(query), "[TEST] viewer", "own", "image", 51)
		if e != nil {
			t.Fatal(e)
		}
		plan := ""
		for rows.Next() {
			var a, b, c int
			var detail string
			if e = rows.Scan(&a, &b, &c, &detail); e != nil {
				t.Fatal(e)
			}
			plan += detail + "\n"
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(plan, "COVERING INDEX nn_history_image_records_category_"+kind) || !strings.Contains(plan, "model_category=?") {
			t.Fatalf("%s category options must avoid unrelated payload pages: %s", kind, plan)
		}
		if kind == "keys" && strings.Contains(plan, "TEMP B-TREE") {
			t.Fatalf("key category options should retain index ordering: %s", plan)
		}
		result, e := store.Filters(ctx, "[TEST] viewer", historyFilterQuery{ModelCategory: "image", Kind: kind})
		if e != nil || len(result.Items) != 0 {
			t.Fatalf("empty image options %s: %+v %v", kind, result, e)
		}
		result, e = store.Filters(ctx, "[TEST] viewer", historyFilterQuery{ModelCategory: "text", Kind: kind, Limit: 2})
		if e != nil || len(result.Items) == 0 {
			t.Fatalf("text options %s: %+v %v", kind, result, e)
		}
		if kind == "keys" {
			if len(result.Items) != 2 || result.NextCursor == "" {
				t.Fatalf("key options pagination: %+v", result)
			}
			next, e := store.Filters(ctx, "[TEST] viewer", historyFilterQuery{ModelCategory: "text", Kind: kind, Before: result.NextCursor, Limit: 2})
			if e != nil || len(next.Items) != 1 || next.Items[0].ID == result.Items[0].ID {
				t.Fatalf("key next options: %+v %v", next, e)
			}
		}
	}
}
