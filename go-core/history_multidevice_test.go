package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// The API owns cloud deduplication; this fixture exercises each real Core's independent journal and protocol.
func TestHistorySyncIndependentDevicesConcurrentRetryAndRestart(t *testing.T) {
	const account = "00000000-0000-0000-0000-000000000001"
	const snapshot = "00000000-0000-0000-0000-000000000099"
	ctx := context.Background()
	archive := func(raw []byte, codec string) historyArchive {
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
		compressed := encoder.EncodeAll(raw, nil)
		encoder.Close()
		return historyArchive{ID: historyHash(raw), Codec: codec, RawBytes: len(raw), CompressedBytes: len(compressed), SHA256: historyHash(raw), BodyBase64: base64.StdEncoding.EncodeToString(compressed)}
	}
	records := []map[string]any{}
	items := []map[string]any{}
	blocks := map[string]historyArchive{}
	secondBlock := ""
	for i, id := range []string{"00000000-0000-0000-0000-000000000010", "00000000-0000-0000-0000-000000000011"} {
		record := map[string]any{"id": id, "requestId": id, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "same-model", "agent": "test", "request": fmt.Sprintf("shared request %d 中文", i), "response": fmt.Sprintf("shared response %d", i)}
		records = append(records, record)
		fields := map[string]any{}
		for _, field := range []string{"request", "response"} {
			raw := []byte(historyText(record[field]))
			block := archive(raw, "zstd-v1")
			blocks[block.ID] = block
			if i == 1 && field == "request" {
				secondBlock = block.ID
			}
			fields[field] = map[string]any{"rawBytes": len(raw), "sha256": block.ID, "blocks": []string{block.ID}}
		}
		header := historyRecordMetadata(record)
		raw, err := json.Marshal(map[string]any{"schemaVersion": 1, "record": header, "fields": fields})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(archive(raw, "zstd-json-manifest-v1"))
		if err != nil {
			t.Fatal(err)
		}
		var item map[string]any
		if err = json.Unmarshal(encoded, &item); err != nil {
			t.Fatal(err)
		}
		item["id"], item["kind"], item["sequence"] = id, "record", strconv.Itoa(i+1)
		item["metadata"] = map[string]any{"scope": "own", "callerAccountId": account, "deploymentId": "deployment", "keyId": "historical-key", "keyName": "Revoked key"}
		items = append(items, item)
	}
	var mu sync.Mutex
	starts := map[string][]string{}
	// One content-addressed object represents both uploads of the same globally allocated request ID.
	cloudBodies := map[string]string{}
	uploads := map[string]int{}
	bothUploaded := make(chan struct{})
	var failB atomic.Bool
	failB.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		device := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		switch r.URL.Path {
		case "/api/v1/client/requests/00000000-0000-0000-0000-000000000010/consumption", "/api/v1/client/requests/00000000-0000-0000-0000-000000000011/consumption":
			// This fixture verifies archive retry independence while consumption metadata is unavailable.
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/api/v1/me":
			json.NewEncoder(w).Encode(map[string]any{"id": account})
		case "/api/v1/me/history/settings":
			json.NewEncoder(w).Encode(map[string]any{"cloudEnabled": true, "canSyncOwn": true})
		case "/api/v1/me/history":
			var input struct {
				Record  map[string]any `json:"record"`
				Archive struct {
					SHA256 string `json:"sha256"`
				} `json:"archive"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			id := historyText(input.Record["requestId"])
			mu.Lock()
			uploads[device]++
			if hash, exists := cloudBodies[id]; exists && hash != input.Archive.SHA256 {
				t.Error("same request ID changed body")
			}
			cloudBodies[id] = input.Archive.SHA256
			if uploads["device-a"] == 1 && uploads["device-b"] == 1 {
				close(bothUploaded)
			}
			mu.Unlock()
			select {
			case <-bothUploaded:
			case <-r.Context().Done():
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"saved": true})
		case "/api/v1/me/history/sync/start":
			var input struct {
				Cursor string `json:"cursor"`
				Scope  string `json:"scope"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			mu.Lock()
			starts[device] = append(starts[device], input.Cursor)
			mu.Unlock()
			if input.Scope != "own" {
				t.Error("unexpected scope", input.Scope)
			}
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "snapshot": snapshot, "after": input.Cursor, "upper": "2", "totalRecords": 2})
		case "/api/v1/me/history/sync/batch":
			after, err := strconv.Atoi(r.URL.Query().Get("after"))
			if err != nil || after < 0 || after > 2 {
				t.Error("invalid after")
				w.WriteHeader(400)
				return
			}
			page := []map[string]any{}
			next := after
			if after < 2 {
				page = append(page, items[after])
				next++
			}
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "items": page, "nextCursor": strconv.Itoa(next), "done": next == 2})
		case "/api/v1/me/history/sync/blocks":
			var input struct {
				IDs []string `json:"ids"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil || len(input.IDs) != 1 {
				t.Error("invalid block request")
				w.WriteHeader(400)
				return
			}
			if device == "device-b" && input.IDs[0] == secondBlock && failB.Load() {
				w.WriteHeader(503)
				return
			}
			block, ok := blocks[input.IDs[0]]
			if !ok {
				t.Error("unknown block")
				w.WriteHeader(404)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "blocks": []historyArchive{block}})
		default:
			t.Error("unexpected cloud route", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	type device struct {
		c             *core
		h             *chatHistory
		m             *historyManager
		home, session string
	}
	openDevice := func(home, session string) *device {
		c := newCore(home, server.URL)
		c.control = server.URL
		h, err := c.historyStore()
		if err != nil {
			t.Fatal(err)
		}
		m, err := h.manager.forAccount(account)
		if err != nil {
			h.db.Close()
			t.Fatal(err)
		}
		return &device{c: c, h: h, m: m, home: home, session: session}
	}
	closeDevice := func(d *device) {
		if d.c.hostedCancel != nil {
			d.c.hostedCancel()
			<-d.c.hostedDone
		}
		d.m.store.Close()
		d.h.db.Close()
	}
	homeA, homeB := t.TempDir(), t.TempDir()
	configureTestHistorySQLite(t, homeA, account)
	configureTestHistorySQLite(t, homeB, account)
	a := openDevice(homeA, "device-a")
	defer closeDevice(a)
	b := openDevice(homeB, "device-b")
	defer func() { closeDevice(b) }()
	for _, d := range []*device{a, b} {
		if err := d.h.save(account, records[1], true); err != nil {
			t.Fatal(err)
		}
	}
	// Distinct local configuration is intentionally persisted outside the cloud sync protocol.
	b.m.config.ExportDirectory = filepath.Join(b.home, "my-exports")
	b.m.config.RetentionDays = 30 // Simulate a pre-upgrade SQLite configuration on device B.
	raw, err := json.Marshal(b.m.config)
	if err != nil {
		t.Fatal(err)
	}
	if err = atomicFile(b.m.configPath(), raw); err != nil {
		t.Fatal(err)
	}
	start := func(d *device) string {
		raw, _ := json.Marshal(map[string]any{"session": d.session, "scope": "own"})
		result, err := d.c.historyCommand("history.sync", raw)
		if err != nil {
			t.Fatal(err)
		}
		return historyText(result.(map[string]any)["jobId"])
	}
	wait := func(d *device, id string) historyJob {
		d.m.mu.Lock()
		job := d.m.jobs[id]
		done := job.done
		d.m.mu.Unlock()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("sync did not complete", d.session)
		}
		raw, _ := json.Marshal(map[string]any{"session": d.session, "scope": "own"})
		value, err := d.c.historyCommand("history.sync.status", raw)
		if err != nil {
			t.Fatal(err)
		}
		return value.(historyJob)
	}
	idA, idB := start(a), start(b)
	if idA == idB {
		t.Fatal("devices shared job ID")
	}
	jobA, jobB := wait(a, idA), wait(b, idB)
	if jobA.State != "complete" || jobA.Percent != 100 || jobB.State != "failed" || jobB.Percent >= 100 {
		t.Fatalf("device jobs coupled: A=%+v B=%+v", jobA, jobB)
	}
	cursor := func(d *device) string {
		var value string
		if err := d.h.db.QueryRow("SELECT cursor FROM history_sync_cursors WHERE viewer=? AND scope='own' AND engine=?", account, d.m.cursorEngine()).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	if cursor(a) != "2" || cursor(b) != "1" {
		t.Fatal("device cursors coupled", cursor(a), cursor(b))
	}
	for _, d := range []*device{a, b} {
		var pending int
		if err := d.h.db.QueryRow("SELECT pending FROM exchanges WHERE account=? AND id=?", account, records[1]["id"]).Scan(&pending); err != nil || pending != 0 {
			t.Fatal("device receipt not local", pending, err)
		}
	}
	closeDevice(b)
	b = openDevice(b.home, b.session)
	if b.m.config.RetentionDays != 7 || b.m.config.ExportDirectory != filepath.Join(b.home, "my-exports") || a.m.config.RetentionDays != 7 {
		t.Fatal("local configuration crossed devices")
	}
	b.m.mu.Lock()
	persisted := b.m.jobs[idB]
	b.m.mu.Unlock()
	if persisted == nil || persisted.State != "failed" {
		t.Fatal("device B lost persisted failed job")
	}
	failB.Store(false)
	retry := start(b)
	if retry == idB {
		t.Fatal("retry reused job ID")
	}
	if job := wait(b, retry); job.State != "complete" || job.Percent != 100 {
		t.Fatal(job)
	}
	if cursor(a) != "2" || cursor(b) != "2" {
		t.Fatal("retry did not independently resume")
	}
	if job := wait(a, start(a)); job.State != "complete" {
		t.Fatal("repeat sync", job)
	}
	for _, d := range []*device{a, b} {
		page, err := d.m.store.Query(ctx, account, historyQuery{Scope: "own"})
		if err != nil || page.Total != 2 {
			t.Fatal("duplicate/missing local request", d.session, page, err)
		}
		for _, expected := range records {
			got, err := d.m.store.Get(ctx, account, "own", historyText(expected["id"]))
			if err != nil || got["request"] != expected["request"] || got["response"] != expected["response"] {
				t.Fatal("body changed", d.session, got, err)
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(cloudBodies) != 1 || uploads["device-a"] != 1 || uploads["device-b"] != 1 {
		t.Fatal("upload replay/dedup contract", cloudBodies, uploads)
	}
	if fmt.Sprint(starts["device-a"]) != "[0 2]" || fmt.Sprint(starts["device-b"]) != "[0 1]" {
		t.Fatal("device B skipped after A acknowledgement", starts)
	}
	if _, err = os.Stat(a.m.privatePath("history-sync-own.json")); err != nil {
		t.Fatal(err)
	}
	if a.m.privatePath("history-sync-own.json") == b.m.privatePath("history-sync-own.json") {
		t.Fatal("job paths shared")
	}
}
