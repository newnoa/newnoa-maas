package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHistoryCapturePersistsKnownZeroAndUnknownUsage(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	historyPolicyFixture(t, h, "local")
	for _, tc := range []struct {
		name, body string
		status     int
		hasUsage   bool
	}{
		{"private-zero", `{"choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":0,"cache_write_tokens":0}}}`, 200, true},
		{"failed", `{"error":{"message":"[TEST] denied"}}`, 403, false},
		{"unknown", `{"choices":[{"finish_reason":"stop"}]}`, 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Newnoa-Request", "00000000-0000-0000-0000-000000000123")
				w.Header().Set("X-Newnoa-Path", "private-http")
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			})
			h.capture(next, tc.name, "[TEST] key").ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"[TEST] private"}`)))
			row, err := h.get(tc.name, "00000000-0000-0000-0000-000000000123")
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"inputTokens", "outputTokens", "cacheWriteTokens", "cachedInputTokens"} {
				if tc.hasUsage {
					if row[key] != float64(0) {
						t.Fatalf("real zero lost %s=%v", key, row[key])
					}
				} else if row[key] != nil {
					t.Fatalf("unknown fabricated as zero %s=%v", key, row[key])
				}
			}
			if row["pointsStatus"] != "pending" || row["pointsConsumed"] != nil {
				t.Fatal("locally invented API settlement", row)
			}
		})
	}
}

func TestHistoryTrustedMetadataRefreshAndExportConfigure(t *testing.T) {
	account := "00000000-0000-0000-0000-000000000001"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(map[string]any{"id": account}) }))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	configureTestHistorySQLite(t, c.home, account)
	c.control = server.URL
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.shutdown(); m.store.Close(); h.db.Close() }()
	value := map[string]any{"id": "[TEST] record", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "[TEST] immutable", "pointsStatus": "pending"}
	if err = m.commit(context.Background(), account, "own", "record", "[TEST] record", value); err != nil {
		t.Fatal(err)
	}
	metadata := map[string]any{"id": "[TEST] record", "createdAt": value["createdAt"], "inputTokens": 0, "outputTokens": 4, "cacheWriteTokens": 0, "cachedInputTokens": 0, "pointsConsumed": "0.000000", "pointsStatus": "known", "pointsMultiplier": "0.750000", "effectivePointsMultiplier": "1.500000", "longContextApplied": true, "longContextPricing": true, "longContextThreshold": 272000, "request": "must not replace body"}
	if err = m.commit(context.Background(), account, "own", "record", "[TEST] record", metadata); err != nil {
		t.Fatal(err)
	}
	record, err := m.store.Get(context.Background(), account, "own", "[TEST] record")
	if err != nil {
		t.Fatal(err)
	}
	if record["pointsConsumed"] != "0.000000" || record["pointsStatus"] != "known" || record["inputTokens"] != float64(0) || record["outputTokens"] != float64(4) || record["request"] != "[TEST] immutable" || record["effectivePointsMultiplier"] != "1.500000" || record["longContextApplied"] != true || record["longContextThreshold"] != float64(272000) {
		t.Fatal("trusted snapshot/body merge failed", record)
	}
	for _, format := range []string{"json", "csv"} {
		exported, err := writeHistoryExport(context.Background(), m.store, account, historyQuery{Scope: "own"}, format, t.TempDir(), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(exported.Path)
		if err != nil || !strings.Contains(string(raw), "0.000000") || strings.Contains(string(raw), "pointsStatus") {
			t.Fatal("settled zero lost in export", string(raw), err)
		}
	}
	originalStore, originalDirectory := m.store, m.config.SQLiteDirectory
	directory := filepath.Join(c.home, "separate-exports")
	raw, _ := json.Marshal(map[string]any{"session": "[TEST] session", "exportDirectory": directory})
	result, err := c.historyCommand("history.export.configure", raw)
	if err != nil {
		t.Fatal(err)
	}
	if m.store != originalStore || m.config.SQLiteDirectory != originalDirectory || m.config.ExportDirectory != directory || m.busy != "" || result.(map[string]any)["exportDirectory"] != directory {
		t.Fatal("export setting started migration", result)
	}
	config := m.config
	config.ExportDirectory = ""
	config.SQLiteDirectory = filepath.Join(c.home, "new-selected-history")
	input := map[string]any{}
	raw, _ = json.Marshal(config)
	json.Unmarshal(raw, &input)
	delete(input, "exportDirectory")
	input["session"] = "[TEST] session"
	raw, _ = json.Marshal(input)
	if _, err = c.historyCommand("history.database.test", raw); err != nil {
		t.Fatal("omitted export directory invalidated database test", err)
	}
	result, err = c.historyCommand("history.database.configure", raw)
	if err != nil {
		t.Fatal(err)
	}
	jobID := result.(map[string]any)["jobId"].(string)
	m.mu.Lock()
	done := m.jobs[jobID].done
	m.mu.Unlock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("database migration did not complete")
	}
	m.mu.Lock()
	job := *m.jobs[jobID]
	current := m.config
	m.mu.Unlock()
	if job.State != "complete" || current.ExportDirectory != directory {
		t.Fatal("database configure overwrote independently saved export directory", job, current)
	}
}

func TestHistoryUsageRecoveredFromExistingResponse(t *testing.T) {
	home := t.TempDir()
	account := "00000000-0000-0000-0000-000000000001"
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err := newHistoryManager(home, h)
	if err != nil {
		t.Fatal(err)
	}
	seedExpiredHistory(t, m, account, "[TEST] old-version", time.Now().UTC())
	response := `{"choices":[{"finish_reason":"stop"}],"usage":{"prompt_tokens":9,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":0}}}`
	s := m.store.(*sqlHistoryRepository)
	if _, err = s.db.Exec("UPDATE nn_history_records SET payload=json_set(payload,'$.response',?,'$.protocol','/v1/chat/completions'),metadata=json_remove(metadata,'$.request')", response); err != nil {
		t.Fatal(err)
	}
	// Simulate an already imported account whose selected database contains pre-upgrade rows.
	if _, err = h.db.Exec("DELETE FROM exchanges; INSERT INTO history_meta(key,value) VALUES(?,'1')", "legacy-imported:"+account); err != nil {
		t.Fatal(err)
	}
	m.store.Close()
	configureTestHistorySQLite(t, home, account)
	m, err = newAccountHistoryManager(home, h, account, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()
	result, err := m.store.Query(context.Background(), account, historyQuery{Scope: "own", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0]["inputTokens"] != float64(9) || result.Items[0]["outputTokens"] != float64(0) || result.Items[0]["cacheWriteTokens"] != nil || result.Items[0]["pointsStatus"] != "unavailable" {
		t.Fatal("recoverable old snapshot missing or fabricated", result)
	}
}
