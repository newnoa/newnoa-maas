package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHistoryUserExportOmitsBillingDiagnosticsWithoutChangingEvidence(t *testing.T) {
	ctx := context.Background()
	store, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	diagnostics := map[string]any{
		"pointsStatus": "estimated", "settlementStatus": "pending", "settlementReason": "IMAGE_USAGE_INCOMPLETE",
		"usageSource": "estimated", "billingPhase": "provisional", "usageSnapshot": map[string]any{"estimator": "reference-o200k-v1"},
	}
	value := map[string]any{
		"id": "[TEST] user-export", "createdAt": time.Now().UTC().Format(time.RFC3339Nano),
		"pointsConsumed": "0.040769123", "inputTokens": 123, "cacheWriteTokens": nil,
		"request": `{"billingPhase":"provisional","text":"user supplied"}`, "response": "estimated usage is the user's quoted text",
		"imageMetadata": map[string]any{"size": "1024x1024", "details": []any{diagnostics}},
	}
	for key, field := range diagnostics {
		value[key] = field
	}
	if err = store.Save(ctx, "viewer", "own", value); err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"json", "csv"} {
		result, err := writeHistoryExport(ctx, store, "viewer", historyQuery{Scope: "own"}, format, t.TempDir(), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(result.Path)
		if err != nil {
			t.Fatal(err)
		}
		var exported map[string]any
		if format == "json" {
			var rows []map[string]any
			if err = json.Unmarshal(raw, &rows); err != nil || len(rows) != 1 {
				t.Fatal(err, string(raw))
			}
			exported = rows[0]
		} else {
			rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(string(raw), "\ufeff"))).ReadAll()
			if err != nil || len(rows) != 2 {
				t.Fatal(err, rows)
			}
			exported = map[string]any{}
			for index, field := range rows[0] {
				exported[field] = rows[1][index]
			}
		}
		for key := range diagnostics {
			if _, exists := exported[key]; exists {
				t.Fatalf("%s contains internal column %s", format, key)
			}
		}
		if exported["pointsConsumed"] != value["pointsConsumed"] || exported["request"] != value["request"] || exported["response"] != value["response"] {
			t.Fatal("changed amount or user content", format, exported)
		}
		metadata, _ := json.Marshal(exported["imageMetadata"])
		for key := range diagnostics {
			if strings.Contains(string(metadata), key) {
				t.Fatalf("%s nested metadata contains %s", format, key)
			}
		}
	}
	saved, err := store.Get(ctx, "viewer", "own", historyText(value["id"]))
	if err != nil || saved["billingPhase"] != "provisional" || saved["pointsConsumed"] != value["pointsConsumed"] || saved["cacheWriteTokens"] != nil {
		t.Fatal("export changed stored evidence", err, saved)
	}
}

func TestHistoryExportSegmentsMergeExactResultsAndCleanTemporaryFiles(t *testing.T) {
	ctx := context.Background()
	store, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, id := range []string{"a", "b", "c"} {
		if err = store.Save(ctx, "viewer", "own", map[string]any{"id": id, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "keyName": "team", "request": "=formula,\"中文\"\nsecond"}); err != nil {
			t.Fatal(err)
		}
	}
	if err = store.Save(ctx, "viewer", "others", map[string]any{"id": "secret", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "secret"}); err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"json", "csv"} {
		directory := t.TempDir()
		percent := 0
		result, err := writeHistoryExport(ctx, store, "viewer", historyQuery{Scope: "own", KeyName: "team"}, format, directory, func(p int) {
			if p < percent {
				t.Fatal("progress moved backwards")
			}
			percent = p
		}, func() int64 { return 64 })
		if err != nil {
			t.Fatal(err)
		}
		if result.Count != 3 || percent != 100 {
			t.Fatal(result, percent)
		}
		files, _ := os.ReadDir(directory)
		if len(files) != 1 {
			t.Fatalf("temporary files left: %v", files)
		}
		raw, err := os.ReadFile(result.Path)
		if err != nil {
			t.Fatal(err)
		}
		if format == "json" {
			var records []map[string]any
			if err = json.Unmarshal(raw, &records); err != nil || len(records) != 3 {
				t.Fatal(string(raw), err)
			}
		} else {
			rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(string(raw), "\ufeff"))).ReadAll()
			if err != nil || len(rows) != 4 {
				t.Fatal(rows, err)
			}
			if !strings.Contains(string(raw), "'=formula") {
				t.Fatal("formula not escaped")
			}
		}
	}
}
func TestHistoryExportCancellationRemovesSegmentsAndNeverPublishesPartialFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Save(ctx, "viewer", "own", map[string]any{"id": "a", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "hello"}); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	_, err = writeHistoryExport(ctx, store, "viewer", historyQuery{Scope: "own"}, "json", directory, func(p int) {
		if p >= 49 {
			cancel()
		}
	}, func() int64 { return 1 })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(directory, "*"))
	if len(files) != 0 {
		t.Fatal(files)
	}
	hidden, _ := filepath.Glob(filepath.Join(directory, ".newnoa*"))
	if len(hidden) != 0 {
		t.Fatal(hidden)
	}
}
