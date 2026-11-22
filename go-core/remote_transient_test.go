package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

func TestRemoteImageReceiptIsTransientAndBounded(t *testing.T) {
	m, a, _ := remoteReadFixture(t)
	revision := m.snapshot()["revision"]
	for i := 0; i < 12; i++ {
		id := randomToken()
		if _, err := m.command(remoteEnvelope(id, "attachment.read", a.SessionID, map[string]any{"attachmentId": a.ID, "offset": 0})); err != nil {
			t.Fatal(err)
		}
		r := waitRemote(t, m, id)
		if r.Status != "providerAccepted" || r.Result["content"] == "" {
			t.Fatalf("read failed: %+v", r)
		}
		m.mu.Lock()
		count := 0
		for _, operation := range m.operations {
			if operation.Transient {
				count++
			}
		}
		m.mu.Unlock()
		if count > 8 {
			t.Fatal("unbounded read cache")
		}
	}
	if m.snapshot()["revision"] != revision {
		t.Fatal("image preview invalidated its own snapshot")
	}
	journal, err := os.ReadFile(m.path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := json.Marshal(m.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{journal, snapshot} {
		if bytes.Contains(raw, []byte(`"content"`)) || bytes.Contains(raw, []byte(`"offset"`)) {
			t.Fatal("image chunks leaked to durable or public projection")
		}
	}
	m.mu.Lock()
	for _, r := range m.operations {
		if r.Transient {
			r.created = time.Now().Add(-61 * time.Second)
		}
	}
	m.pruneTransientLocked()
	for _, r := range m.operations {
		if r.Transient {
			t.Error("expired image receipt retained")
		}
	}
	m.mu.Unlock()
}

func TestRemoteExpiredQueuedReadDoesNotExecuteOrPanic(t *testing.T) {
	m, a, _ := remoteReadFixture(t)
	id := randomToken()
	c := remoteEnvelope(id, "attachment.read", a.SessionID, map[string]any{"attachmentId": a.ID, "offset": 0})
	r := &remoteReceipt{OperationID: id, Status: "accepted", SessionID: a.SessionID, Transient: true, created: time.Now().Add(-61 * time.Second)}
	m.mu.Lock()
	m.operations[id] = r
	m.pruneTransientLocked()
	m.mu.Unlock()
	m.perform(remoteJob{command: c, receipt: r, payload: remotePayload{AttachmentID: a.ID}})
	if r.Status != "accepted" || r.Result != nil {
		t.Fatal("expired read executed")
	}
}

func TestRemoteUnchangedSnapshotHasStableAttachmentOrder(t *testing.T) {
	m, a, _ := remoteReadFixture(t)
	m.mu.Lock()
	for _, id := range []string{"second", "third", "fourth"} {
		copied := *a
		copied.ID = id
		m.attachments[id] = &copied
	}
	m.mu.Unlock()
	want, _ := json.Marshal(m.snapshot())
	for i := 0; i < 50; i++ {
		got, _ := json.Marshal(m.snapshot())
		if !bytes.Equal(want, got) {
			t.Fatal("unchanged snapshot is not deterministic")
		}
	}
}

func TestRemoteAuthorizationInvalidationAdvancesRevision(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	m.checkAuthorization = func(string, string) error { return errors.New("changed") }
	before := m.snapshot()["revision"].(float64)
	if _, err := m.command(remoteEnvelope("changed-executor", "session.create", "", map[string]any{"agent": "codex", "cwd": cwd})); err == nil {
		t.Fatal("changed executor accepted")
	}
	if m.snapshot()["revision"].(float64) <= before {
		t.Fatal("authorization invalidation did not advance revision")
	}
}

func TestRemoteImageReceiptExpiresWithoutFurtherCommands(t *testing.T) {
	m, _, _ := remoteFixture(t)
	m.mu.Lock()
	m.operations["expired-read"] = &remoteReceipt{OperationID: "expired-read", Status: "providerAccepted", Transient: true, created: time.Now().Add(-61 * time.Second), Result: map[string]any{"content": "private-image"}}
	m.mu.Unlock()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		cached := m.operations["expired-read"] != nil
		m.mu.Unlock()
		if !cached {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("idle runtime retained expired image content")
}
