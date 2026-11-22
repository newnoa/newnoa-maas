package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testHistoryExactFilters(t *testing.T, store historyRepository, viewer string) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 125; i++ {
		value := map[string]any{"id": fmt.Sprintf("facet-%03d", i), "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "model-a", "deploymentId": "deploy-a", "modelName": "Deleted model", "keyId": "key-a", "keyName": "Revoked key", "keyPrefix": "sk-old", "request": "body"}
		if i == 0 {
			value["deploymentId"] = "deploy-b"
			value["keyId"] = "key-b"
		}
		if i == 1 {
			value["deploymentId"] = "deploy-c"
		}
		if i == 2 {
			value["modelName"] = "Archived model name"
		}
		if err := store.Save(ctx, viewer, "own", value); err != nil {
			t.Fatal(err)
		}
	}
	for _, identity := range []struct{ viewer, scope string }{{viewer + "-other", "own"}, {viewer, "others"}} {
		if err := store.Save(ctx, identity.viewer, identity.scope, map[string]any{"id": "private", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "deploymentId": "secret", "keyId": "secret", "modelName": "Deleted model", "keyName": "Revoked key"}); err != nil {
			t.Fatal(err)
		}
		defer store.Delete(ctx, identity.viewer, identity.scope, "private")
	}
	var q historyQuery
	if err := json.Unmarshal([]byte(`{"model":"deploy-b","keyId":"key-b"}`), &q); err != nil {
		t.Fatal(err)
	}
	result, err := store.Query(ctx, viewer, q)
	if err != nil || result.Total != 1 || result.Items[0]["id"] != "facet-000" {
		t.Fatalf("exact IDs ignored: %+v %v", result, err)
	}
	q.Model = "model-a"
	q.KeyID = "key-b"
	result, err = store.Query(ctx, viewer, q)
	if err != nil || result.Total != 1 {
		t.Fatalf("exact underlying model ID: %+v %v", result, err)
	}
	for _, format := range []string{"json", "csv"} {
		exported, err := writeHistoryExport(ctx, store, viewer, q, format, t.TempDir(), func(int) {}, nil)
		if err != nil || exported.Count != 1 {
			t.Fatalf("filtered %s export: %+v %v", format, exported, err)
		}
		if _, err = os.Stat(exported.Path); err != nil {
			t.Fatal(err)
		}
	}
	renamed, err := store.Filters(ctx, viewer, historyFilterQuery{Kind: "models", KeyID: "key-a", Q: "Archived model name"})
	if err != nil || len(renamed.Items) != 1 || renamed.Items[0].ID != "deploy-a" {
		t.Fatalf("historical name search: %+v %v", renamed, err)
	}
	keys, err := store.Filters(ctx, viewer, historyFilterQuery{Kind: "keys", Q: "revoked", Limit: 1})
	if err != nil || len(keys.Items) != 1 || keys.Items[0].ID != "key-a" || keys.Items[0].Prefix != "sk-old" || keys.NextCursor == "" {
		t.Fatalf("key candidates: %+v %v", keys, err)
	}
	byPrefix, err := store.Filters(ctx, viewer, historyFilterQuery{Kind: "keys", Q: "sk-old"})
	if err != nil || len(byPrefix.Items) != 2 {
		t.Fatalf("key prefix search: %+v %v", byPrefix, err)
	}
	next, err := store.Filters(ctx, viewer, historyFilterQuery{Kind: "keys", Q: "revoked", Limit: 1, Before: keys.NextCursor})
	if err != nil || len(next.Items) != 1 || next.Items[0].ID != "key-b" || next.NextCursor != "" {
		t.Fatalf("key candidate next: %+v %v", next, err)
	}
	models, err := store.Filters(ctx, viewer, historyFilterQuery{Kind: "models", KeyID: "key-a", Limit: 1})
	if err != nil || len(models.Items) != 1 || models.Items[0].ID != "deploy-a" || models.Items[0].Name != "Deleted model" || models.NextCursor == "" {
		t.Fatalf("cascade models: %+v %v", models, err)
	}
	models, err = store.Filters(ctx, viewer, historyFilterQuery{Kind: "models", KeyID: "key-a", Limit: 1, Before: models.NextCursor})
	if err != nil || len(models.Items) != 1 || models.Items[0].ID != "deploy-c" || models.NextCursor != "" {
		t.Fatalf("cascade models next: %+v %v", models, err)
	}
	models, err = store.Filters(ctx, viewer, historyFilterQuery{Kind: "models", KeyID: "key-b", Limit: 100})
	if err != nil || len(models.Items) != 1 || models.Items[0].ID != "deploy-b" {
		t.Fatalf("cross-key leak: %+v %v", models, err)
	}
}
func TestHistoryExactFiltersAndCascadingCandidates(t *testing.T) {
	store, err := openHistoryRepository(context.Background(), defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	testHistoryExactFilters(t, store, "viewer")
}

func TestHistoryLegacyFiltersDuringMigration(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	store, err := openLegacyHistoryRepository(h)
	if err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"id": "legacy", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "old-model", "keyId": "old-key", "keyName": "Old key"}
	if err = store.Save(context.Background(), "viewer", "own", value); err != nil {
		t.Fatal(err)
	}
	got, err := store.Query(context.Background(), "viewer", historyQuery{Model: "old-model", KeyID: "old-key"})
	if err != nil || got.Total != 1 {
		t.Fatalf("legacy exact filter: %+v %v", got, err)
	}
	filters, err := store.Filters(context.Background(), "viewer", historyFilterQuery{Kind: "models", KeyID: "old-key"})
	if err != nil || len(filters.Items) != 1 || filters.Items[0].ID != "old-model" {
		t.Fatalf("legacy facets: %+v %v", filters, err)
	}
}

// Missing projections simulate a record written before indexed filters were introduced.
func testHistoryFilterBackfill(t *testing.T, store historyRepository, viewer string) {
	t.Helper()
	ctx := context.Background()
	value := map[string]any{"id": "old-projections", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "old-model", "keyId": "old-key"}
	if err := store.Save(ctx, viewer, "own", value); err != nil {
		t.Fatal(err)
	}
	if err := store.Enrich(ctx, viewer, "own", "old-projections", map[string]any{"deploymentId": "old-deployment", "modelName": "Deleted legacy model", "keyName": "Revoked legacy key", "keyPrefix": "sk-old"}); err != nil {
		t.Fatal(err)
	}
	switch s := store.(type) {
	case *sqlHistoryRepository:
		if _, err := s.db.ExecContext(ctx, s.bind("UPDATE nn_history_records SET model_id='',deployment_id='',key_id='',filter_version=0 WHERE viewer=? AND id=?"), viewer, "old-projections"); err != nil {
			t.Fatal(err)
		}
		if err := s.initializeFilters(ctx); err != nil {
			t.Fatal(err)
		}
	case *mongoHistoryRepository:
		if _, err := s.leaves["text"].records.UpdateOne(ctx, bson.M{"viewer": viewer, "id": "old-projections"}, bson.M{"$unset": bson.M{"filter_version": "", "model_id": "", "deployment_id": "", "key_id": ""}}); err != nil {
			t.Fatal(err)
		}
		if err := s.initializeFilters(ctx); err != nil {
			t.Fatal(err)
		}
	}
	page, err := store.Query(ctx, viewer, historyQuery{Model: "old-deployment", KeyID: "old-key"})
	if err != nil || page.Total != 1 || page.Items[0]["modelName"] != "Deleted legacy model" {
		t.Fatalf("backfill lost snapshots: %+v %v", page, err)
	}
	facets, err := store.Filters(ctx, viewer, historyFilterQuery{Kind: "models", KeyID: "old-key"})
	if err != nil || len(facets.Items) != 1 || facets.Items[0].ID != "old-deployment" || facets.Items[0].Name != "Deleted legacy model" {
		t.Fatalf("backfill lost facets: %+v %v", facets, err)
	}
}
func TestHistoryFiltersBackfillPreservesEnrichedSnapshots(t *testing.T) {
	store, err := openHistoryRepository(context.Background(), defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	testHistoryFilterBackfill(t, store, "viewer")
}

func TestHistoryFiltersRejectInvalidBoundsAndCrossCascadeCursor(t *testing.T) {
	for _, q := range []historyFilterQuery{
		{Kind: "unknown"}, {Kind: "keys", Limit: 101}, {Kind: "keys", Limit: -1}, {Kind: "keys", KeyID: "key-a"}, {Kind: "models", Scope: "all"}, {Kind: "models", Before: "bad"}, {Kind: "models", KeyID: "key-b", Before: historyCursor("models/key-a", "model")},
	} {
		if err := q.validate(); err == nil {
			t.Fatalf("accepted invalid filter %+v", q)
		}
	}
}

func TestHistoryFiltersUpgradeExistingSQLiteSchema(t *testing.T) {
	ctx := context.Background()
	config := defaultHistoryDatabase(t.TempDir())
	if err := historyDirectory(config.SQLiteDirectory); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(config.SQLiteDirectory, "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE nn_history_records (viewer TEXT NOT NULL,scope VARCHAR(10) NOT NULL,id TEXT NOT NULL,created VARCHAR(40) NOT NULL,storage_id TEXT NOT NULL,caller_id TEXT NOT NULL,key_name TEXT NOT NULL,payload TEXT NOT NULL,metadata TEXT NOT NULL,search_text TEXT NOT NULL,deleted INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(viewer,scope,id))`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	stamp := time.Now().UTC()
	value := `{"id":"old","createdAt":"` + stamp.Format(time.RFC3339Nano) + `","model":"ModelCase","keyId":"KeyCase","keyName":"Retired key","modelName":"Deleted model"}`
	_, err = db.Exec("INSERT INTO nn_history_records(viewer,scope,id,created,storage_id,caller_id,key_name,payload,metadata,search_text) VALUES(?,?,?,?,?,?,?,?,?,?)", "viewer", "own", "old", stamp.Format("2006-01-02T15:04:05.000000000Z"), "old", "", "retired key", value, value, "")
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	store, err := openHistoryRepository(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	page, err := store.Query(ctx, "viewer", historyQuery{Model: "ModelCase", KeyID: "KeyCase"})
	if err != nil || page.Total != 1 {
		t.Fatal(page, err)
	}
	page, err = store.Query(ctx, "viewer", historyQuery{Model: "modelcase", KeyID: "KeyCase"})
	if err != nil || page.Total != 0 {
		t.Fatalf("IDs became case insensitive: %+v %v", page, err)
	}
	facets, err := store.Filters(ctx, "viewer", historyFilterQuery{Kind: "models", KeyID: "KeyCase"})
	if err != nil || len(facets.Items) != 1 || facets.Items[0].Name != "Deleted model" {
		t.Fatal(facets, err)
	}
}
