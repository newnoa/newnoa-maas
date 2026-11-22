package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func remoteSnapshotSession(t *testing.T, snapshot map[string]any, id string) map[string]any {
	t.Helper()
	for _, entry := range snapshot["sessions"].([]any) {
		row := entry.(map[string]any)
		if row["id"] == id {
			return row
		}
	}
	t.Fatal("session missing from projection")
	return nil
}

func TestRemoteSnapshotBoundsHistoryWithoutChangingDurableSession(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	m.mu.Lock()
	s := m.sessions[sid]
	s.Messages = []remoteMessage{}
	for i := 0; i < 4; i++ {
		s.Messages = append(s.Messages, remoteMessage{ID: fmt.Sprint(i), Role: "assistant", Text: strings.Repeat(fmt.Sprint(i), 300000), CreatedAt: remoteNow()})
	}
	s.State = "running"
	s.RunID = "run-projection"
	s.AttemptID = "attempt-projection"
	m.changedLocked(s, "", "history.restored", nil)
	if err := m.saveLocked(); err != nil {
		m.mu.Unlock()
		t.Fatal(err)
	}
	m.mu.Unlock()
	snapshot := m.snapshot()
	raw, err := json.Marshal(snapshot)
	if err != nil || len(raw) > 512*1024 {
		t.Fatalf("projection not bounded: bytes=%d truncated=%v error=%v", len(raw), snapshot["truncated"], err)
	}
	row := remoteSnapshotSession(t, snapshot, sid)
	messages := row["messages"].([]any)
	if len(messages) != 4 || messages[0].(map[string]any)["id"] != "0" || messages[3].(map[string]any)["id"] != "3" {
		t.Fatal("truncation discarded newest messages")
	}
	for _, message := range messages {
		value := message.(map[string]any)
		if value["truncated"] != true || len(value["text"].(string)) != 2048 || value["textBytes"] != float64(300000) {
			t.Fatal("shortened message not marked truncated")
		}
	}
	if row["runId"] != "run-projection" || row["attemptId"] != "attempt-projection" {
		t.Fatal("history limit changed run identity")
	}
	m.mu.Lock()
	unchanged := len(s.Messages) == 4 && len(s.Messages[0].Text) == 300000
	m.mu.Unlock()
	if !unchanged {
		t.Fatal("snapshot truncation mutated authoritative history")
	}
	journal, err := os.ReadFile(m.path)
	if err != nil {
		t.Fatal(err)
	}
	var stored remoteJournal
	if err = json.Unmarshal(journal, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Sessions[sid].Messages) != 4 || len(stored.Sessions[sid].Messages[0].Text) != 300000 {
		t.Fatal("public truncation destroyed durable history")
	}
}

func TestRemoteSnapshotReturnsIndependentIdentityAndRecentOperationWindow(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	m.mu.Lock()
	m.osHome = "/operating-system/home"
	m.sessions[sid].OwnedProcess = remoteOwnedProcess{PID: 123, Identity: "private-owner", Executable: "private-binary"}
	m.sessions[sid].Approvals = []*remoteApproval{{ID: "public-approval", NativeID: "private-native", Generation: "private-generation", State: "open", Options: []string{"decline"}}}
	for i := 0; i < 70; i++ {
		id := fmt.Sprintf("op-%02d", i)
		m.operations[id] = &remoteReceipt{OperationID: id, Status: "providerAccepted", created: time.Unix(int64(i+1), 0)}
	}
	// Keep the actual create receipt out of this historic window assertion.
	delete(m.operations, "create-1")
	m.operations["image-content"] = &remoteReceipt{OperationID: "image-content", Status: "providerAccepted", Transient: true, created: time.Now(), Result: map[string]any{"content": "private-image-payload"}}
	m.mu.Unlock()
	snapshot := m.snapshot()
	row := remoteSnapshotSession(t, snapshot, sid)
	if snapshot["sessionLimit"] != float64(8) {
		t.Fatal("snapshot did not report the host session quota")
	}
	if snapshot["hostId"] != m.hostID || snapshot["hostEpoch"] != m.epoch || snapshot["home"] != "/operating-system/home" || snapshot["cursor"] != snapshot["revision"] {
		t.Fatal("snapshot lost host epoch or OS home identity")
	}
	if row["attached"] != true || row["needsResume"] != false {
		t.Fatal("attached native session incorrectly requires resume")
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"private-owner", "private-binary", "private-native", "private-generation", "private-image-payload"} {
		if bytes.Contains(raw, []byte(private)) {
			t.Fatal("snapshot exposed private runtime authority or image bytes")
		}
	}
	operations := snapshot["operations"].([]any)
	if len(operations) != 64 || operations[0].(map[string]any)["operationId"] != "op-69" || operations[63].(map[string]any)["operationId"] != "op-06" {
		t.Fatal("snapshot did not retain newest bounded operation window")
	}
	row["state"] = "forged"
	row["approvals"].([]any)[0].(map[string]any)["state"] = "forged"
	if again := remoteSnapshotSession(t, m.snapshot(), sid); again["state"] == "forged" || again["approvals"].([]any)[0].(map[string]any)["state"] == "forged" {
		t.Fatal("snapshot consumer mutated authoritative runtime")
	}
	m.mu.Lock()
	delete(m.providers, sid)
	m.sessions[sid].OwnedProcess = remoteOwnedProcess{}
	m.sessions[sid].State = "stopped"
	m.mu.Unlock()
	row = remoteSnapshotSession(t, m.snapshot(), sid)
	if row["attached"] != false || row["needsResume"] != true {
		t.Fatal("retained detached native session did not require explicit resume")
	}
}

func TestRemoteSnapshotStreamingOutputIsBoundedAndApprovalsInvalidateAtTerminalState(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	p.emit(remoteProviderEvent{Type: "state", State: "running", RunID: "run-stream"})
	p.emit(remoteProviderEvent{Type: "output", Text: strings.Repeat("a", 65530)})
	p.emit(remoteProviderEvent{Type: "output", Text: strings.Repeat("b", 20)})
	p.emit(remoteProviderEvent{Type: "output", Text: "discard-after-limit"})
	row := remoteSnapshotSession(t, m.snapshot(), sid)
	messages := row["messages"].([]any)
	message := messages[0].(map[string]any)
	m.mu.Lock()
	durableText := m.sessions[sid].Messages[0].Text
	m.mu.Unlock()
	if len(messages) != 1 || len(message["text"].(string)) != 2048 || !strings.HasSuffix(durableText, "bbbbbb") ||
		len(durableText) != 65536 || message["truncated"] != true {
		t.Fatal("streaming output was not merged and bounded")
	}
	p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: "first", Options: []string{"decline"}}})
	p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: "second", Options: []string{"decline"}}})
	p.emit(remoteProviderEvent{Type: "approvalInvalidated", Text: "first"})
	row = remoteSnapshotSession(t, m.snapshot(), sid)
	if row["state"] != "awaitingApproval" {
		t.Fatal("invalidating one approval lost another open approval")
	}
	p.emit(remoteProviderEvent{Type: "state", State: "idle"})
	row = remoteSnapshotSession(t, m.snapshot(), sid)
	for _, approval := range row["approvals"].([]any) {
		if approval.(map[string]any)["state"] != "invalidated" {
			t.Fatal("terminal native state left an open approval")
		}
	}
	for i := 0; i < 20; i++ {
		p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: fmt.Sprint(i), Options: []string{"decline"}}})
		p.emit(remoteProviderEvent{Type: "state", State: "idle"})
	}
	row = remoteSnapshotSession(t, m.snapshot(), sid)
	if len(row["approvals"].([]any)) > 17 {
		t.Fatal("closed approvals grew without bound")
	}
}

func TestRemoteSnapshotShutdownClearsImagesAndRejectsNewCommands(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	id := remotePrepareImage(t, m, sid, "image/png", 10)
	r := remoteUploadCommand(t, m, "attachment.upload", sid, map[string]any{"attachmentId": id, "offset": 0, "content": "AQID"})
	if r.Status != "providerAccepted" {
		t.Fatal(r)
	}
	m.mu.Lock()
	path := m.attachments[id].Path
	m.mu.Unlock()
	p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: "pending", Options: []string{"decline"}}})
	if !m.identityValid() {
		t.Fatal("current operating system identity rejected")
	}
	m.mu.Lock()
	uid := m.uid
	m.uid = "different-os-user"
	m.mu.Unlock()
	if m.identityValid() {
		t.Fatal("changed operating system identity accepted")
	}
	m.mu.Lock()
	m.uid = uid
	m.mu.Unlock()
	if err := m.shutdown(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("shutdown retained uploaded content", err)
	}
	snapshot := m.snapshot()
	row := remoteSnapshotSession(t, snapshot, sid)
	if snapshot["enabled"] != false || row["attached"] != false || row["state"] != "stopped" || len(snapshot["attachments"].([]any)) != 0 {
		t.Fatal("shutdown projection still appears live")
	}
	if row["approvals"].([]any)[0].(map[string]any)["state"] != "invalidated" {
		t.Fatal("shutdown retained approval authority")
	}
	if _, err := m.command(remoteEnvelope("after-shutdown", "message.send", sid, map[string]any{"text": "must not execute"})); err == nil {
		t.Fatal("closed manager admitted a command")
	}
}
