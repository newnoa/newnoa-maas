package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestOpenHistoryDoesNotScanLegacyConsumption(t *testing.T) {
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	value, _ := json.Marshal(map[string]any{"requestId": "00000000-0000-0000-0000-000000000123", "pointsStatus": "pending"})
	_, err = h.db.Exec("INSERT INTO exchanges(account,id,created,model,agent,payload) VALUES('viewer','legacy',?,'[TEST] model','[TEST] agent',?)", time.Now().UTC().Format(time.RFC3339Nano), string(value))
	if err != nil {
		h.db.Close()
		t.Fatal(err)
	}
	h.db.Close()
	h, err = openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	var count int
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&count); err != nil || count != 0 {
		t.Fatal("opening the journal synchronously backfilled legacy JSON", count, err)
	}
}

func TestConsumptionBackfillResumesCommittedBatches(t *testing.T) {
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { h.db.Close() }()
	if _, err = newHistoryCoordinator(home, h); err != nil {
		t.Fatal(err)
	}
	count := 2*historyConsumptionBackfillBatch + 1
	for i := 1; i <= count; i++ {
		value, _ := json.Marshal(map[string]any{"requestId": fmt.Sprintf("00000000-0000-0000-0000-%012d", i), "pointsStatus": "pending"})
		if _, err = h.db.Exec("INSERT INTO exchanges(account,id,created,model,agent,payload) VALUES('viewer',?,?,'[TEST] model','[TEST] agent',?)", fmt.Sprint(i), time.Now().UTC().Format(time.RFC3339Nano), string(value)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = h.db.Exec(fmt.Sprintf("CREATE TRIGGER interrupt_backfill BEFORE INSERT ON history_consumption_pending WHEN NEW.id='%d' BEGIN SELECT RAISE(ABORT,'[TEST] interrupted'); END", historyConsumptionBackfillBatch+2)); err != nil {
		t.Fatal(err)
	}
	if done, err := h.backfillConsumption(context.Background()); err != nil || done {
		t.Fatal(done, err)
	}
	if _, err = h.backfillConsumption(context.Background()); err == nil {
		t.Fatal("expected interrupted second batch")
	}
	var pending, cursor int
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&pending); err != nil || pending != historyConsumptionBackfillBatch {
		t.Fatal("failed batch was partly committed", pending, err)
	}
	if err = h.db.QueryRow("SELECT value FROM history_meta WHERE key='consumption-backfill-v1'").Scan(&cursor); err != nil || cursor != historyConsumptionBackfillBatch {
		t.Fatal("cursor advanced past failed batch", cursor, err)
	}
	if _, err = h.db.Exec("DROP TRIGGER interrupt_backfill; UPDATE history_consumption_pending SET attempts=7 WHERE id='1'"); err != nil {
		t.Fatal(err)
	}
	h.db.Close()
	h, err = openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	for {
		done, err := h.backfillConsumption(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&pending); err != nil || pending != count {
		t.Fatal("restart lost legacy consumption", pending, err)
	}
	if err = h.db.QueryRow("SELECT attempts FROM history_consumption_pending WHERE id='1'").Scan(&pending); err != nil || pending != 7 {
		t.Fatal("restart reset existing retries", pending, err)
	}
	if _, err = h.db.Exec("DELETE FROM history_consumption_pending WHERE id='1'"); err != nil {
		t.Fatal(err)
	}
	if done, err := h.backfillConsumption(context.Background()); err != nil || !done {
		t.Fatal(done, err)
	}
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&pending); err != nil || pending != count-1 {
		t.Fatal("completed migration rescanned old payloads", pending, err)
	}
}

func TestConsumptionBackfillCancellationAndNewCapture(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	if _, err = newHistoryCoordinator(t.TempDir(), h); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = h.backfillConsumption(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation did not stop backfill", err)
	}
	if done, err := h.backfillConsumption(context.Background()); err != nil || !done {
		t.Fatal(done, err)
	}
	value := map[string]any{"id": "new", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "requestId": "00000000-0000-0000-0000-000000000123", "model": "[TEST] model", "agent": "[TEST] agent", "pointsStatus": "known", "pointsConsumed": "0.000000"}
	if err = h.save("viewer", value, false); err != nil {
		t.Fatal(err)
	}
	var pending int
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&pending); err != nil || pending != 1 {
		t.Fatal("fresh capture depended on the completed legacy scan", pending, err)
	}
}

func TestHostedSyncStartsWhileHistoryMaintenanceIsBlocked(t *testing.T) {
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	h.manager, err = newHistoryCoordinator(home, h)
	if err != nil {
		t.Fatal(err)
	}
	c := newCore(home, "http://127.0.0.1:1")
	h.manager.mu.Lock()
	started := make(chan struct{})
	go func() { c.startHostedSync(h); close(started) }()
	defer func() {
		h.manager.mu.Unlock()
		<-started
		c.hostedCancel()
		<-c.hostedDone
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Core startup waited for history maintenance")
	}
}

func waitHistoryBackfill(t *testing.T, h *chatHistory) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		var cursor int64
		if h.db.QueryRow("SELECT value FROM history_meta WHERE key='consumption-backfill-v1'").Scan(&cursor) == nil && cursor == -1 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("background history recovery did not finish")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestCaptureAndConsumptionQueueCommitTogether(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	if _, err = h.db.Exec("CREATE TRIGGER reject_consumption BEFORE INSERT ON history_consumption_pending BEGIN SELECT RAISE(ABORT,'[TEST] unavailable'); END"); err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"id": "capture", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "requestId": "00000000-0000-0000-0000-000000000123", "model": "[TEST] model", "agent": "[TEST] agent", "pointsStatus": "pending"}
	if err = h.save("viewer", value, false); err == nil {
		t.Fatal("queue write failure was hidden")
	}
	var count int
	if err = h.db.QueryRow("SELECT count(*) FROM exchanges").Scan(&count); err != nil || count != 0 {
		t.Fatal("capture committed without durable consumption recovery", count, err)
	}
	if _, err = h.db.Exec("DROP TRIGGER reject_consumption"); err != nil {
		t.Fatal(err)
	}
	if err = h.save("viewer", value, false); err != nil {
		t.Fatal(err)
	}
	if err = h.db.QueryRow("SELECT count(*) FROM exchanges e JOIN history_consumption_pending p ON e.account=p.account AND e.id=p.id").Scan(&count); err != nil || count != 1 {
		t.Fatal("capture and queue were not committed together", count, err)
	}
}
