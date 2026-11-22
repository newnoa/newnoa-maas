package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPublishedPackedJournalUpgradesCategoryBodies(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(directory, "chat-history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE exchanges(account TEXT NOT NULL,id TEXT NOT NULL,created TEXT NOT NULL,model TEXT NOT NULL,agent TEXT NOT NULL,payload TEXT NOT NULL,pending INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(account,id));` + historyManagerSchema); err != nil {
		t.Fatal(err)
	}
	bodyDB, err := sql.Open("sqlite", filepath.Join(directory, "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := newSQLHistoryBodyStore(ctx, bodyDB, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	snapshots := map[string]any{"textInput": "0.123456789", "imageOutput": nil}
	for _, cat := range []string{"text", "image"} {
		v := map[string]any{"id": cat, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": cat, "request": strings.Repeat("[TEST] published v27 body", 3000), "pointsMultipliers": snapshots, "pointsMultiplier": "1.123456789", "pointsConsumed": nil}
		header, e := legacy.Pack(ctx, "journal", "viewer", "own", cat, v)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.Exec("INSERT INTO exchanges(account,id,created,model,agent,payload) VALUES(?,?,?,?,?,?)", "viewer", cat, v["createdAt"], "m", "a", header); e != nil {
			t.Fatal(e)
		}
		v["id"] = cat + "pending"
		header, e = legacy.Pack(ctx, "pending", "viewer", "others", cat+"pending", v)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.Exec("INSERT INTO history_pending(viewer,scope,kind,id,payload) VALUES('viewer','others','record',?,?)", v["id"], header); e != nil {
			t.Fatal(e)
		}
	}
	db.Close()
	bodyDB.Close()
	for attempt := 0; attempt < 2; attempt++ {
		h, e := openChatHistoryAt(directory)
		if e != nil {
			t.Fatal(e)
		}
		for _, cat := range []string{"text", "image"} {
			got, e := h.get("viewer", cat)
			if e != nil || !reflect.DeepEqual(got["pointsMultipliers"], snapshots) {
				t.Fatal("published journal", got, e)
			}
			var raw string
			if e = h.db.QueryRow("SELECT payload FROM history_pending WHERE id=?", cat+"pending").Scan(&raw); e != nil {
				t.Fatal(e)
			}
			got, e = h.decodeBody(ctx, "viewer", raw)
			if e != nil || !reflect.DeepEqual(got["pointsMultipliers"], snapshots) {
				t.Fatal("published pending", got, e)
			}
		}
		h.db.Close()
	}
}

func TestCategoryReferencePendingOnlyBodyRelocates(t *testing.T) {
	ctx := context.Background()
	h, err := openChatHistoryAt(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	if _, err = h.db.Exec(historyManagerSchema); err != nil {
		t.Fatal(err)
	}
	if err = initializeCategoryPending(h.db.DB); err != nil {
		t.Fatal(err)
	}
	snapshots := map[string]any{"textInput": "0.123456789", "future": nil}
	for _, cat := range []string{"text", "image"} {
		v := map[string]any{"id": cat, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": cat, "request": strings.Repeat("[TEST] pending only", 3000), "pointsMultipliers": snapshots, "pointsConsumed": nil}
		header, e := h.encodeBody(ctx, "pending", "viewer", "others", cat, v)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = queueEncodedHistoryEvent(ctx, h.db.DB, "viewer", "others", "record", cat, v, header); e != nil {
			t.Fatal(e)
		}
	}
	directory := t.TempDir()
	if err = migrateJournalBodyDatabase(h, directory); err != nil {
		t.Fatal(err)
	}
	targetDB, target, err := openJournalBodies(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer targetDB.Close()
	for _, cat := range []string{"text", "image"} {
		var seq int64
		var ref string
		if err = h.db.QueryRow("SELECT seq,payload FROM history_pending WHERE id=?", cat).Scan(&seq, &ref); err != nil {
			t.Fatal(err)
		}
		raw, e := resolveHistoryEvent(ctx, h.db.DB, seq, ref)
		if e != nil {
			t.Fatal(e)
		}
		got, e := target.Unpack(ctx, "viewer", raw)
		if e != nil || !reflect.DeepEqual(got["pointsMultipliers"], snapshots) || got["pointsConsumed"] != nil {
			t.Fatal("pending relocation", got, e)
		}
		if _, e = h.decodeBody(ctx, "viewer", raw); e != nil {
			t.Fatal("source lost", e)
		}
	}
	targetDB.Close()
	targetPath := filepath.Join(directory, "chat-history.sqlite")
	if _, err = h.db.Exec("VACUUM INTO '" + strings.ReplaceAll(targetPath, "'", "''") + "'"); err != nil {
		t.Fatal(err)
	}
	relocated, err := openChatHistoryAt(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer relocated.db.Close()
	for _, cat := range []string{"text", "image"} {
		var seq int64
		var ref string
		if err = relocated.db.QueryRow("SELECT seq,payload FROM history_pending WHERE id=?", cat).Scan(&seq, &ref); err != nil {
			t.Fatal(err)
		}
		raw, e := resolveHistoryEvent(ctx, relocated.db.DB, seq, ref)
		if e != nil {
			t.Fatal(e)
		}
		got, e := relocated.decodeBody(ctx, "viewer", raw)
		if e != nil || !reflect.DeepEqual(got["pointsMultipliers"], snapshots) {
			t.Fatal("relocated journal restart", got, e)
		}
	}

}
