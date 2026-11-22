package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHistoryDatabaseConfigTextLengths(t *testing.T) {
	fields := []struct {
		name  string
		limit int
		set   func(*historyDatabaseConfig, string)
	}{
		{"host", 253, func(c *historyDatabaseConfig, value string) { c.Host = value }},
		{"database", 63, func(c *historyDatabaseConfig, value string) { c.Database = value }},
		{"username", 128, func(c *historyDatabaseConfig, value string) { c.Username = value }},
		{"password", 256, func(c *historyDatabaseConfig, value string) { c.Password = value }},
		{"authDatabase", 128, func(c *historyDatabaseConfig, value string) { c.AuthDatabase = value }},
		{"sqliteDirectory", 4096, func(c *historyDatabaseConfig, value string) { c.SQLiteDirectory = "/" + value[1:] }},
		{"exportDirectory", 4096, func(c *historyDatabaseConfig, value string) { c.ExportDirectory = "/" + value[1:] }},
	}
	for _, field := range fields {
		t.Run(field.name, func(t *testing.T) {
			config := historyDatabaseConfig{Engine: "postgres", RetentionDays: 7, Host: "localhost", Port: 5432, Database: "history", Username: "user"}
			field.set(&config, strings.Repeat("x", field.limit))
			if err := config.validate(); err != nil {
				t.Fatalf("boundary rejected: %v", err)
			}
			field.set(&config, strings.Repeat("x", field.limit+1))
			if err := config.validate(); err == nil {
				t.Fatal("oversized field accepted")
			}
			config.Engine = "off"
			if err := config.validate(); err == nil {
				t.Fatal("disabled storage bypassed config bound")
			}
		})
	}
}

func TestHistoryDatabaseUnicodeCredentialsUseCharacterLimits(t *testing.T) {
	config := historyDatabaseConfig{Engine: "postgres", RetentionDays: 7, Host: "localhost", Port: 5432, Database: "history", Username: strings.Repeat("用", 128), Password: strings.Repeat("密", 256)}
	if err := config.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryDatabaseRetentionAndPaths(t *testing.T) {
	query := historyQuery{Scope: "own", From: "2026-11-12T08:00:00+08:00", To: "2026-11-12T01:00:00Z"}
	if err := query.validate(); err != nil {
		t.Fatal("date range compares instants", err)
	}
	for _, engine := range []string{"sqlite", "mongodb", "postgres", "mysql"} {
		for _, days := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 30, 90, 180, 365} {
			config := historyDatabaseConfig{Engine: engine, RetentionDays: days, SQLiteDirectory: t.TempDir(), Host: "localhost", Port: 1234, Database: "history", Username: "test"}
			err := config.validate()
			if engine == "sqlite" && (days < 1 || days > 7) || engine != "sqlite" && days > 0 && days < 30 && days != 7 {
				if err == nil {
					t.Fatalf("%s accepted %d", engine, days)
				}
			} else if err != nil {
				t.Fatal(engine, days, err)
			}
		}
	}
	root := t.TempDir()
	c := defaultHistoryDatabase(root)
	if c.SQLiteDirectory != filepath.Join(root, ".newnoa", "sqlite") || c.ExportDirectory != filepath.Join(root, ".newnoa", "exports") {
		t.Fatalf("wrong defaults: %+v", c)
	}
}

func TestHistoryDatabaseQueryIsLocalScopedAndBeyondFirstPage(t *testing.T) {
	ctx := context.Background()
	config := defaultHistoryDatabase(t.TempDir())
	store, err := openHistoryRepository(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	start := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 65; i++ {
		record := map[string]any{"id": fmt.Sprintf("request-%03d", i), "createdAt": start.Add(time.Duration(i) * time.Second).Format(time.RFC3339Nano), "model": "qwen", "keyName": "团队密钥", "request": "hello"}
		if i == 2 {
			record["request"] = "稀有内容 %_"
		}
		if err = store.Save(ctx, "viewer", "own", record); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.Save(ctx, "other", "own", map[string]any{"id": "secret", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "稀有内容 %_"}); err != nil {
		t.Fatal(err)
	}
	result, err := store.Query(ctx, "viewer", historyQuery{Scope: "own", Q: "稀有内容 %_", KeyName: "团队", Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || len(result.Items) != 1 || result.Items[0]["id"] != "request-002" {
		t.Fatalf("not local full query: %+v", result)
	}
	result, err = store.Query(ctx, "viewer", historyQuery{Scope: "own", Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 50 || result.NextCursor == "" || result.Total != 65 {
		t.Fatalf("wrong firstpage: %+v", result)
	}
	next, err := store.Query(ctx, "viewer", historyQuery{Scope: "own", Before: result.NextCursor, Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Items) != 15 {
		t.Fatalf("wrong nextpage: %d", len(next.Items))
	}
}

func TestHistoryDatabaseCandidateUnionAndViewIsolation(t *testing.T) {
	ctx := context.Background()
	store, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	own := map[string]any{"id": "same", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "own"}
	other := map[string]any{"id": "same", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "other", "callerAccountId": "caller", "callerEmail": "called@example.test"}
	if err = store.Save(ctx, "viewer", "own", own); err != nil {
		t.Fatal(err)
	}
	if err = store.Save(ctx, "viewer", "others", other); err != nil {
		t.Fatal(err)
	}
	for _, email := range []string{"called@example.test", "shared@example.test"} {
		if err = store.Candidate(ctx, "viewer", email, email, true); err != nil {
			t.Fatal(err)
		}
	}
	accounts, err := store.Accounts(ctx, "viewer", "shared", 20)
	if err != nil || len(accounts) != 1 {
		t.Fatal(accounts, err)
	}
	result, err := store.Query(ctx, "viewer", historyQuery{Scope: "others", CallerID: "caller", Limit: 20})
	if err != nil || len(result.Items) != 1 || result.Items[0]["contentSummary"] != "other" {
		t.Fatal(result, err)
	}
	detail, err := store.Get(ctx, "viewer", "others", "same")
	if err != nil || detail["request"] != "other" {
		t.Fatal(detail, err)
	}
}
