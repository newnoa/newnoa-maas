package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCategoryBodiesKeepPhysicalLeavesAndBillingSnapshots(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	h, err := openChatHistoryAt(defaultHistoryDatabase(home).SQLiteDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	repo, err := openSQLHistory(ctx, defaultHistoryDatabase(home))
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()
	s := repo.(*sqlHistoryRepository)
	if _, err = h.db.Exec(historyManagerSchema); err != nil {
		t.Fatal(err)
	}
	if err = initializeCategoryPending(h.db.DB); err != nil {
		t.Fatal(err)
	}
	snapshot := map[string]any{"textInput": "0.123456789", "imageOutput": "1.000000001", "future": nil}
	for _, cat := range []string{"text", "image"} {
		v := map[string]any{"id": cat, "model": "[TEST] model", "agent": "[TEST] agent", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": cat, "request": strings.Repeat("[TEST] same exact body 中文", 30000), "pointsMultipliers": snapshot, "pointsMultiplier": "0.750000001", "pointsConsumed": nil, "imageUsage": map[string]any{"textInputTokens": 0, "imageInputTokens": nil}}
		if err = h.save("viewer", v, false); err != nil {
			t.Fatal(err)
		}
		if err = s.Save(ctx, "viewer", "own", v); err != nil {
			t.Fatal(err)
		}
		for _, read := range []func() (map[string]any, error){func() (map[string]any, error) { return h.get("viewer", cat) }, func() (map[string]any, error) { return s.Get(ctx, "viewer", "own", cat) }} {
			got, e := read()
			if e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(got["pointsMultipliers"], snapshot) || got["pointsMultiplier"] != v["pointsMultiplier"] || got["pointsConsumed"] != nil || got["request"] != v["request"] {
				t.Fatalf("%s snapshot/body changed", cat)
			}
		}
		encoded, e := h.encodeBody(ctx, "pending", "viewer", "own", cat, v)
		if e != nil {
			t.Fatal(e)
		}
		seq, e := queueEncodedHistoryEvent(ctx, h.db.DB, "viewer", "own", "record", cat, v, encoded)
		if e != nil {
			t.Fatal(e)
		}
		var ref string
		if e = h.db.QueryRow("SELECT payload FROM history_pending WHERE seq=?", seq).Scan(&ref); e != nil {
			t.Fatal(e)
		}
		pending, e := resolveHistoryEvent(ctx, h.db.DB, seq, ref)
		if e != nil {
			t.Fatal(e)
		}
		decoded, e := h.decodeBody(ctx, "viewer", pending)
		if e != nil || !reflect.DeepEqual(decoded["pointsMultipliers"], snapshot) || decoded["request"] != v["request"] {
			t.Fatal("pending compressed snapshot", e)
		}
		if _, e = h.db.Exec("DELETE FROM history_pending WHERE seq=?", seq); e != nil {
			t.Fatal(e)
		}
		if e = h.collectJournalBodies(ctx, 100); e != nil {
			t.Fatal(e)
		}
		var count, sources int
		if err = s.db.QueryRow("SELECT count(*) FROM nn_history_" + cat + "_body_blocks").Scan(&count); err != nil || count == 0 {
			t.Fatal(cat, count, err)
		}
		if err = s.db.QueryRow("SELECT count(DISTINCT source) FROM nn_history_" + cat + "_body_refs").Scan(&sources); err != nil || sources != 2 {
			t.Fatal(cat, sources, err)
		}
	}
	var shared int
	if err = s.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('nn_history_body_blocks','nn_history_body_refs')").Scan(&shared); err != nil || shared != 0 {
		t.Fatal("mixed physical body tables", shared, err)
	}
	if err = s.Delete(ctx, "viewer", "own", "image"); err != nil {
		t.Fatal(err)
	}
	if err = h.collectJournalBodies(ctx, 100); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "viewer", "own", "text")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(got["pointsMultipliers"])
	if !strings.Contains(string(raw), "0.123456789") {
		t.Fatal("decimal snapshot lost")
	}
	if _, err = s.Get(ctx, "viewer", "own", "image"); err == nil {
		t.Fatal("deleted image resurrected")
	}
	if _, err = h.get("viewer", "image"); err != nil {
		t.Fatal("repository deletion removed journal image body", err)
	}
}
