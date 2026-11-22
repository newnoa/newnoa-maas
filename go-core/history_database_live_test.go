package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

func historySandboxDatabaseName(name, fallback string) (string, error) {
	if name == "" {
		return fallback, nil
	}
	if !regexp.MustCompile(`^test_core_live_[a-z0-9_]{1,40}$`).MatchString(name) {
		return "", fmt.Errorf("history sandbox database requires an isolated test_core_live_ name")
	}
	return name, nil
}

func historySandboxDatabase(t *testing.T, config historyDatabaseConfig) historyDatabaseConfig {
	t.Helper()
	name, err := historySandboxDatabaseName(os.Getenv("HISTORY_TEST_DATABASE_NAME"), config.Database)
	if err != nil {
		t.Fatal(err)
	}
	config.Database = name
	return config
}

func TestHistorySandboxDatabaseName(t *testing.T) {
	for _, name := range []string{"test_core_live_mongo8_123", "test_core_live_pg_456"} {
		actual, err := historySandboxDatabaseName(name, "newnoa_history")
		if err != nil || actual != name {
			t.Fatalf("valid isolated database: %q %v", actual, err)
		}
	}
	for _, name := range []string{"newnoa_history", "test_core_live_", "test_core_live_x;DROP DATABASE x", "test_core_live_../x", "test_core_live_" + strings.Repeat("x", 50)} {
		if _, err := historySandboxDatabaseName(name, "newnoa_history"); err == nil {
			t.Fatalf("unsafe database accepted: %q", name)
		}
	}
	if actual, err := historySandboxDatabaseName("", "newnoa_history"); err != nil || actual != "newnoa_history" {
		t.Fatal("legacy fixture default changed")
	}
}

func TestHistoryDatabaseMatrix(t *testing.T) {
	password := os.Getenv("HISTORY_DATABASE_PASSWORD")
	if password == "" {
		t.Skip("requires authorized dedicated history sandbox")
	}
	for _, target := range []struct {
		name, engine string
		port         int
	}{{"mysql8", "mysql", 3308}, {"mysql9", "mysql", 3309}, {"mysql26", "mysql", 3326}, {"postgres16", "postgres", 5416}, {"postgres17", "postgres", 5417}, {"postgres18", "postgres", 5418}, {"mongo8", "mongodb", 27018}, {"mongo9", "mongodb", 27019}} {
		t.Run(target.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			config := historySandboxDatabase(t, defaultHistoryDatabase(t.TempDir()))
			config.Engine = target.engine
			config.Host = "127.0.0.1"
			config.Port = target.port
			config.Username = "newnoa"
			config.Password = password
			store, err := openHistoryRepository(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			viewer := "[TEST]-history-" + uuid.NewString()
			defer func() {
				switch s := store.(type) {
				case *sqlHistoryRepository:
					for _, table := range []string{"nn_history_text_records", "nn_history_image_records", "nn_history_record_locations", "nn_history_candidates"} {
						if _, err := s.db.ExecContext(context.Background(), s.bind("DELETE FROM "+table+" WHERE viewer=? OR viewer=?"), viewer, viewer+"-other"); err != nil {
							t.Error("cleanup", err)
						}
					}
				case *mongoHistoryRepository:
					for _, collection := range []string{"nn_history_text_records", "nn_history_image_records", "nn_history_text_chunks", "nn_history_image_chunks", "nn_history_record_locations", "nn_history_candidates"} {
						if _, err := s.client.Database(config.Database).Collection(collection).DeleteMany(context.Background(), bson.M{"viewer": bson.M{"$in": bson.A{viewer, viewer + "-other"}}}); err != nil {
							t.Error("cleanup", err)
						}
					}
				}
			}()
			testHistoryExactFilters(t, store, viewer)
			backfill := store
			if sqlStore, ok := store.(*sqlHistoryRepository); ok {
				// Projection corruption belongs to a physical leaf; the union view is read-only.
				backfill = sqlStore.leaves["text"]
			}
			testHistoryFilterBackfill(t, backfill, viewer)
			record := map[string]any{"id": "unicode", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "中文多行\n literal [a-z] %_", "response": "ok", "keyName": "[TEST] 名称密钥", "callerAccountId": "caller", "callerEmail": "shared@example.test"}
			if err = store.Save(ctx, viewer, "others", record); err != nil {
				t.Fatal(err)
			}
			if err = store.Save(ctx, viewer, "others", record); err != nil {
				t.Fatal(err)
			}
			result, err := store.Query(ctx, viewer, historyQuery{Scope: "others", Q: "[a-z] %_", CallerID: "caller", KeyName: "名称", Limit: 10})
			if err != nil || result.Total != 1 || len(result.Items) != 1 {
				t.Fatal("literal search", result, err)
			}
			if err = store.Candidate(ctx, viewer, "shared", "shared@example.test", true); err != nil {
				t.Fatal(err)
			}
			accounts, err := store.Accounts(ctx, viewer, "shared", 20)
			if err != nil || len(accounts) != 1 {
				t.Fatal("candidates", accounts, err)
			}
			large := strings.Repeat("[TEST] large record 中文 ", 900000)
			record["id"] = "large"
			record["request"] = large
			if err = store.Save(ctx, viewer, "own", record); err != nil {
				t.Fatal("large body", err)
			}
			got, err := store.Get(ctx, viewer, "own", "large")
			if err != nil || got["request"] != large {
				t.Fatal("large body roundtrip", err)
			}
			record["id"] = "boundary"
			record["request"] = strings.Repeat("x", (256<<10)-3) + "NEEDLE跨块"
			if err = store.Save(ctx, viewer, "own", record); err != nil {
				t.Fatal(err)
			}
			boundary, err := store.Query(ctx, viewer, historyQuery{Scope: "own", Q: "NEEDLE跨块", Limit: 10})
			if err != nil || boundary.Total != 1 {
				t.Fatal("chunk boundary search", boundary, err)
			}
			if err = store.Delete(ctx, viewer, "own", "large"); err != nil {
				t.Fatal(err)
			}
			if _, err = store.Get(ctx, viewer, "own", "large"); err == nil {
				t.Fatal("deleted record remained visible")
			}
			t.Logf("version %s, exact model/key filters, local key/model cascade, historical prefix/name search, enriched metadata backfill, scoped CSV/JSON exports, literal Unicode search and %d-byte body roundtrip", store.Version(), len(large))
		})
	}
}

func TestHistoryMigrationMatrix(t *testing.T) {
	password := os.Getenv("HISTORY_DATABASE_PASSWORD")
	if password == "" {
		t.Skip("requires authorized dedicated history sandbox")
	}
	for _, target := range []struct {
		name, engine string
		port         int
	}{{"mysql8", "mysql", 3308}, {"mysql9", "mysql", 3309}, {"mysql26", "mysql", 3326}, {"postgres16", "postgres", 5416}, {"postgres17", "postgres", 5417}, {"postgres18", "postgres", 5418}, {"mongo8", "mongodb", 27018}, {"mongo9", "mongodb", 27019}} {
		t.Run(target.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
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
			viewer := "[TEST]-migration-" + uuid.NewString()
			record := map[string]any{"id": "shared-id", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "[TEST] migration", "request": "original Unicode 中文", "response": "[TEST] answer"}
			for _, scope := range []string{"own", "others"} {
				if err = m.commit(ctx, viewer, scope, "record", "shared-id", record); err != nil {
					t.Fatal(err)
				}
			}
			if err = m.commit(ctx, viewer, "others", "candidate", "candidate", map[string]any{"email": "[TEST]-migration@example.test", "active": true}); err != nil {
				t.Fatal(err)
			}
			config := historySandboxDatabase(t, m.config)
			config.Engine = target.engine
			config.Host = "127.0.0.1"
			config.Port = target.port
			config.Username = "newnoa"
			config.Password = password
			destination, err := openHistoryRepository(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			cleanup := func() {
				switch s := destination.(type) {
				case *sqlHistoryRepository:
					for _, table := range []string{"nn_history_text_records", "nn_history_image_records", "nn_history_record_locations", "nn_history_candidates"} {
						if _, e := s.db.ExecContext(context.Background(), s.bind("DELETE FROM "+table+" WHERE viewer=?"), viewer); e != nil {
							t.Error("cleanup", e)
						}
					}
				case *mongoHistoryRepository:
					for _, collection := range []string{"nn_history_text_records", "nn_history_image_records", "nn_history_text_chunks", "nn_history_image_chunks", "nn_history_record_locations", "nn_history_candidates"} {
						if _, e := s.client.Database(config.Database).Collection(collection).DeleteMany(context.Background(), bson.M{"viewer": viewer}); e != nil {
							t.Error("cleanup", e)
						}
					}
				}
			}
			defer destination.Close()
			defer cleanup()
			wrong := map[string]any{}
			for key, value := range record {
				wrong[key] = value
			}
			wrong["request"] = "[TEST] mismatch"
			if err = destination.Save(ctx, viewer, "own", wrong); err != nil {
				t.Fatal(err)
			}
			if err = m.migrate(ctx, config); err == nil {
				t.Fatal("mismatched target accepted")
			}
			if m.config.Engine != "sqlite" {
				t.Fatal("failure changed active engine")
			}
			original, err := m.store.Get(ctx, viewer, "own", "shared-id")
			if err != nil || original["request"] != record["request"] {
				t.Fatal("failure damaged source", original, err)
			}
			cleanup()
			if err = m.migrate(ctx, config); err != nil {
				t.Fatal("verified migration", err)
			}
			if m.config.Engine != target.engine {
				t.Fatal("cutover missing")
			}
			for _, scope := range []string{"own", "others"} {
				got, err := m.store.Get(ctx, viewer, scope, "shared-id")
				if err != nil || got["request"] != record["request"] {
					t.Fatal("scope lost", scope, got, err)
				}
			}
			accounts, err := m.store.Accounts(ctx, viewer, "", 10)
			if err != nil || len(accounts) != 1 {
				t.Fatal("candidates lost", accounts, err)
			}
			if err = m.commit(ctx, viewer, "own", "record", "after-cutover", map[string]any{"id": "after-cutover", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "[TEST] capture after configuration"}); err != nil {
				t.Fatal(err)
			}
			got, err := m.store.Get(ctx, viewer, "own", "after-cutover")
			if err != nil || got["request"] != "[TEST] capture after configuration" {
				t.Fatal(got, err)
			}
			t.Log("verified mismatch failure preserves SQLite; real cutover, scopes, candidates and post-cutover capture", m.store.Version())
		})
	}
}
