package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/klauspost/compress/zstd"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestHistorySyncCommitBeforeCursorAndCorruption(t *testing.T) {
	account := "00000000-0000-0000-0000-000000000001"
	snapshot := "00000000-0000-0000-0000-000000000099"
	body := []byte("同步中文 request")
	response := []byte("response")
	corrupt := true
	archive := func(raw []byte, codec string) historyArchive {
		encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		compressed := encoder.EncodeAll(raw, nil)
		encoder.Close()
		return historyArchive{ID: historyHash(raw), Codec: codec, RawBytes: len(raw), CompressedBytes: len(compressed), SHA256: historyHash(raw), BodyBase64: base64.StdEncoding.EncodeToString(compressed)}
	}
	requestBlock := archive(body, "zstd-v1")
	responseBlock := archive(response, "zstd-v1")
	manifest := map[string]any{"schemaVersion": 1, "record": map[string]any{"id": "r", "requestId": "r", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m"}, "fields": map[string]any{"request": map[string]any{"rawBytes": len(body), "sha256": historyHash(body), "blocks": []string{requestBlock.ID}}, "response": map[string]any{"rawBytes": len(response), "sha256": historyHash(response), "blocks": []string{responseBlock.ID}}}}
	raw, _ := json.Marshal(manifest)
	itemArchive := archive(raw, "zstd-json-manifest-v1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/me/history/settings":
			json.NewEncoder(w).Encode(map[string]any{"canSyncOwn": true, "cloudEnabled": true, "canSyncOthers": true})
		case "/api/v1/me/history/sync/start":
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "snapshot": snapshot, "after": "0", "upper": "2", "totalRecords": 1, "totalCandidates": 0})
		case "/api/v1/me/history/sync/batch":
			raw, _ := json.Marshal(itemArchive)
			var item map[string]any
			json.Unmarshal(raw, &item)
			item["id"] = "r"
			item["kind"] = "record"
			item["sequence"] = "1"
			item["metadata"] = map[string]any{"scope": "own", "callerAccountId": account, "keyName": "[TEST] trusted key"}
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "items": []any{item}, "nextCursor": "1", "done": true})
		case "/api/v1/me/history/sync/blocks":
			var input struct {
				IDs []string `json:"ids"`
			}
			json.NewDecoder(r.Body).Decode(&input)
			block := requestBlock
			if input.IDs[0] == responseBlock.ID {
				block = responseBlock
			}
			if corrupt {
				block.BodyBase64 = base64.StdEncoding.EncodeToString([]byte("corrupt"))
				block.CompressedBytes = 7
			}
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "blocks": []historyArchive{block}})
		default:
			t.Error("unexpected cloud request", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	configureTestHistorySQLite(t, c.home, account)
	c.control = server.URL
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()

	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()
	if err = m.commit(context.Background(), account, "own", "record", "r", map[string]any{"id": "r", "requestId": "r", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "request": string(body), "response": string(response)}); err != nil {
		t.Fatal(err)
	}
	if err = c.syncHistory(context.Background(), m, account, "session", "own", "job"); err == nil {
		t.Fatal("corrupt block accepted")
	}
	var count int
	if err = h.db.QueryRow("SELECT COUNT(*) FROM history_sync_cursors").Scan(&count); err != nil || count != 0 {
		t.Fatal("advanced corrupt cursor", count, err)
	}
	corrupt = false
	if err = c.syncHistory(context.Background(), m, account, "session", "own", "job"); err != nil {
		t.Fatal(err)
	}
	record, err := m.store.Get(context.Background(), account, "own", "r")
	if err != nil || record["request"] != string(body) || record["keyName"] != "[TEST] trusted key" {
		t.Fatal(record, err)
	}
	var cursor string
	if err = h.db.QueryRow("SELECT cursor FROM history_sync_cursors WHERE viewer=? AND scope='own'", account).Scan(&cursor); err != nil || cursor != "2" {
		t.Fatal(cursor, err)
	}
	if _, err = m.store.Get(context.Background(), account, "others", "r"); err == nil {
		t.Fatal("scope leaked")
	}
}

func TestHistoryCandidatePaginationBeyondFirstPage(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err := newHistoryManager(t.TempDir(), h)
	if err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()
	for i := 0; i < 205; i++ {
		id := strconv.Itoa(i)
		if err = m.store.Candidate(context.Background(), "viewer", id, strings.Repeat("a", 3)+id+"@example.test", true); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	before := ""
	for {
		page, err := m.store.Accounts(context.Background(), "viewer", "", 50, before)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range page {
			id := historyText(item["id"])
			if seen[id] {
				t.Fatal("duplicate", id)
			}
			seen[id] = true
		}
		if len(page) < 50 {
			break
		}
		last := page[len(page)-1]
		before = historyCursor(historyText(last["email"]), historyText(last["id"]))
	}
	if len(seen) != 205 {
		t.Fatal(len(seen))
	}
}

func TestHistoryOthersIndependentOwnSwitchAndScopedTombstones(t *testing.T) {
	account := "00000000-0000-0000-0000-000000000001"
	starts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/me/history/settings":
			json.NewEncoder(w).Encode(map[string]any{"cloudEnabled": false, "canSyncOwn": false, "canSyncOthers": true})
		case "/api/v1/me/history/sync/start":
			starts++
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "snapshot": "00000000-0000-0000-0000-000000000099", "after": "0", "upper": "1", "totalRecords": 1})
		case "/api/v1/me/history/sync/batch":
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "items": []any{map[string]any{"kind": "delete", "id": "r", "sequence": "1", "metadata": map[string]any{"scope": "others"}}}, "nextCursor": "1", "done": true})
		default:
			t.Fatal("unexpected path", r.URL.Path)
		}
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	configureTestHistorySQLite(t, c.home, account)
	c.control = server.URL
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()
	record := map[string]any{"id": "r", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "request": "same id independent scope"}
	for _, scope := range []string{"own", "others"} {
		if err = m.commit(context.Background(), account, scope, "record", "r", record); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.syncHistory(context.Background(), m, account, "session", "own", "job"); err == nil {
		t.Fatal("own off ignored")
	}
	if starts != 0 {
		t.Fatal("own off started snapshot")
	}
	if err = c.syncHistory(context.Background(), m, account, "session", "others", "job"); err != nil {
		t.Fatal(err)
	}
	if _, err = m.store.Get(context.Background(), account, "others", "r"); err == nil {
		t.Fatal("cloud tombstone ignored")
	}
	if _, err = m.store.Get(context.Background(), account, "own", "r"); err != nil {
		t.Fatal("others delete removed own", err)
	}
	if err = m.commit(context.Background(), account, "others", "record", "r", record); err != nil {
		t.Fatal("replayed tombstoned record stalled sync", err)
	}
	if _, err = m.store.Get(context.Background(), account, "others", "r"); err == nil {
		t.Fatal("local tombstone resurrected")
	}
}

func TestHistoryCaptureBeforeRequestAllocationStaysLocal(t *testing.T) {
	account := "00000000-0000-0000-0000-000000000001"
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	historyPolicyFixture(t, h, "cloud")
	handler := h.capture(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		w.Write([]byte("no authorized request allocated"))
	}), account, "key")
	request := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"m","input":"hello"}`))
	request.Header.Set("Authorization", "Bearer test")
	handler.ServeHTTP(httptest.NewRecorder(), request)
	var pending, count int
	if err = h.db.QueryRow("SELECT count(*),COALESCE(sum(pending),0) FROM exchanges WHERE account=?", account).Scan(&count, &pending); err != nil || count != 1 || pending != 0 {
		t.Fatal("unallocated request entered cloud queue", count, pending, err)
	}
}

func TestHistoryPausedUploadKeepsPendingForRetry(t *testing.T) {
	account := "00000000-0000-0000-0000-000000000001"
	ack := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/me/history/settings":
			json.NewEncoder(w).Encode(map[string]any{"cloudEnabled": true, "canSyncOwn": true})
		case "/api/v1/me/history":
			json.NewEncoder(w).Encode(map[string]any{"saved": ack, "paused": !ack})
		case "/api/v1/me/history/sync/start":
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "snapshot": "00000000-0000-0000-0000-000000000099", "after": "0", "upper": "0", "totalRecords": 0})
		case "/api/v1/me/history/sync/batch":
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "items": []any{}, "nextCursor": "0", "done": true})
		default:
			t.Error("unexpected route", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	configureTestHistorySQLite(t, c.home, account)
	c.control = server.URL
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	defer m.store.Close()
	record := map[string]any{"id": "00000000-0000-0000-0000-000000000010", "requestId": "00000000-0000-0000-0000-000000000010", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "m", "agent": "a", "request": "retained pending"}
	if err = h.save(account, record, true); err != nil {
		t.Fatal(err)
	}
	if err = c.syncHistory(context.Background(), m, account, "session", "own", "job"); err == nil {
		t.Fatal("paused receipt accepted")
	}
	var pending int
	if err = h.db.QueryRow("SELECT pending FROM exchanges WHERE account=? AND id=?", account, record["id"]).Scan(&pending); err != nil || pending != 1 {
		t.Fatal("paused upload lost pending", pending, err)
	}
	ack = true
	if err = c.syncHistory(context.Background(), m, account, "session", "own", "job"); err != nil {
		t.Fatal(err)
	}
	if err = h.db.QueryRow("SELECT pending FROM exchanges WHERE account=? AND id=?", account, record["id"]).Scan(&pending); err != nil || pending != 0 {
		t.Fatal("acknowledged upload remained pending", pending, err)
	}
}
