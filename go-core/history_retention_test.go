package main

import (
	"testing"
	"time"
)

func TestHistoryRetentionTombstonesAndDoesNotResurrect(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	value := map[string]any{"id": "old", "createdAt": time.Now().Add(-8 * 24 * time.Hour).UTC().Format(time.RFC3339Nano), "model": "model", "agent": "agent"}
	if err = h.save("owner", value, true); err != nil {
		t.Fatal(err)
	}
	if err = h.expire("owner", 7); err != nil {
		t.Fatal(err)
	}
	rows, err := h.list("owner", "", 10)
	if err != nil || len(rows) != 0 {
		t.Fatal(rows, err)
	}
	if _, err = h.get("owner", "old"); err == nil {
		t.Fatal("expired detail visible")
	}
	if err = h.expire("owner", 365); err != nil {
		t.Fatal(err)
	}
	if err = h.save("owner", value, true); err != nil {
		t.Fatal(err)
	}
	rows, err = h.list("owner", "", 10)
	if err != nil || len(rows) != 0 {
		t.Fatal("tombstone resurrected", rows, err)
	}
	var count int
	if err = h.db.QueryRow("SELECT count(*) FROM exchanges WHERE account='owner' AND deleted_at IS NOT NULL").Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}
