package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestHistorySQLiteStoresOneBodyCopyAndKeepsIndependentDetail(t *testing.T) {
	ctx := context.Background()
	store, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sqlStore := store.(*sqlHistoryRepository)
	request := `{"model":"MODEL","messages":[{"role":"user","content":"ÉCOLE ΩМОДЕЛЬ ` + strings.Repeat("context/", 1<<17) + `"}],"tools":[{"arguments":"{\"path\":\"C:\\\\work/file\",\"pattern\":\"%_!\"}"}]}`
	response := "data: {\"delta\":\"Reply/结果\"}\n\ndata: [DONE]\n\n"
	for _, id := range []string{"first", "second"} {
		value := map[string]any{"id": id, "storageId": "shared-session", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "MODEL", "modelName": "Display Model", "request": request, "response": response}
		if err = store.Save(ctx, "viewer", "own", value); err != nil {
			t.Fatal(err)
		}
		var payload, metadata, search string
		if err = sqlStore.db.QueryRow("SELECT payload,metadata,search_text FROM nn_history_records WHERE id=?", id).Scan(&payload, &metadata, &search); err != nil {
			t.Fatal(err)
		}
		if search != "" || len(payload)+len(metadata)+len(search) >= len(request)*3/2 {
			t.Fatalf("body duplicated in SQLite: request=%d payload=%d metadata=%d search=%d", len(request), len(payload), len(metadata), len(search))
		}
		t.Logf("id=%s request=%d payload=%d metadata=%d search=%d bytes", id, len(request), len(payload), len(metadata), len(search))
		got, getErr := store.Get(ctx, "viewer", "own", id)
		if getErr != nil || got["id"] != id || got["request"] != request || got["response"] != response || !json.Valid([]byte(got["request"].(string))) {
			t.Fatalf("independent detail lost JSON/context/stream fidelity: id=%s err=%v", id, getErr)
		}
	}
	for _, query := range []string{"CONTEXT/", `C:\\\\work/file`, "%_!", "reply/结果", "display model", "école", "ωмодель"} {
		result, queryErr := store.Query(ctx, "viewer", historyQuery{Q: query})
		if queryErr != nil || result.Total != 2 {
			t.Fatalf("decoded content search %q: count=%d err=%v", query, result.Total, queryErr)
		}
	}
}

func TestHistorySQLiteBodySearchPreservesUnicodeAndStoredProjection(t *testing.T) {
	ctx := context.Background()
	store, err := openHistoryRepository(ctx, defaultHistoryDatabase(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Save(ctx, "viewer", "own", map[string]any{"id": "unicode", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "ÉCOLE ΩМОДЕЛЬ"}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"école", "ωмодель"} {
		result, queryErr := store.Query(ctx, "viewer", historyQuery{Q: query})
		if queryErr != nil || result.Total != 1 {
			t.Fatalf("Unicode case search %q: count=%d err=%v", query, result.Total, queryErr)
		}
	}
	if _, err = store.(*sqlHistoryRepository).db.Exec("UPDATE nn_history_records SET search_text=? WHERE id=?", "stored projection", "unicode"); err != nil {
		t.Fatal(err)
	}
	result, err := store.Query(ctx, "viewer", historyQuery{Q: "stored projection"})
	if err != nil || result.Total != 1 {
		t.Fatalf("existing stored search projection ignored: count=%d err=%v", result.Total, err)
	}
}
