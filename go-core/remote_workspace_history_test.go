package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func TestRemoteWorkspaceHistoryAcrossCreatesAndRestart(t *testing.T) {
	c, m, p, api := remoteRPCFixture(t)
	var first, activity string
	for i := 0; i < 20; i++ {
		if m.snapshot()["canCreateSession"] != true {
			t.Fatalf("create %d capacity", i)
		}
		id := fmt.Sprintf("create-history-%d", i)
		cmd := remoteEnvelope(id, "session.create", "", map[string]any{"agent": "codex", "cwd": m.home})
		r, err := m.command(cmd)
		if err != nil || waitRemote(t, m, id).Status != "providerAccepted" {
			t.Fatalf("create %d: %v", i, err)
		}
		if i == 0 {
			first = r.SessionID
		}
		send := fmt.Sprintf("history-turn-%d", i)
		if _, err = m.command(remoteEnvelope(send, "message.send", r.SessionID, map[string]any{"text": fmt.Sprintf("question %d?", i)})); err != nil {
			t.Fatal(err)
		}
		if waitRemote(t, m, send).Status != "providerAccepted" {
			t.Fatal("send failed")
		}
		p.emit(remoteProviderEvent{Type: "output", Text: fmt.Sprintf("answer %d.", i)})
		p.emit(remoteProviderEvent{Type: "state", State: "idle"})
		if i == 0 {
			activity = remoteSnapshotSession(t, m.snapshot(), first)["updatedAt"].(string)
		}
		if len(m.snapshot()["sessions"].([]any)) > 8 {
			t.Fatal("active limit exceeded")
		}
	}
	check := func() {
		t.Helper()
		out, err := callRemoteRPC(t, c, "remote.snapshot", nil)
		if err != nil {
			t.Fatal(err)
		}
		snap := out.(map[string]any)
		archive := snap["historySessions"].([]any)
		if len(archive) != 12 || len(snap["sessions"].([]any)) != 8 {
			t.Fatalf("unexpected history: %d", len(archive))
		}
		row := archive[11].(map[string]any)
		if row["id"] != first || row["cwd"] != m.home || row["agent"] != "codex" || row["title"] != "question 0?" || row["updatedAt"] != activity {
			t.Fatal("lost history identity")
		}
		if row["messageCount"] != float64(2) {
			t.Fatal("lost history message count")
		}
		if _, exists := row["messages"]; exists {
			t.Fatal("history directory contains message bodies")
		}
		if _, err := c.remote.command(remoteEnvelope("archived-write", "session.resume", first, map[string]any{})); err == nil {
			t.Fatal("archived session accepted command")
		}
	}
	check()
	if err := c.stopRemote(); err != nil {
		t.Fatal(err)
	}
	check()
	api.mu.Lock()
	api.owner = remoteRPCOtherOwner
	api.mu.Unlock()
	out, err := callRemoteRPC(t, c, "remote.snapshot", nil)
	if err != nil || len(out.(map[string]any)["historySessions"].([]any)) != 0 {
		t.Fatal("other account saw history", err)
	}
}

func TestRemoteWorkspaceHistoryPagesAndSnapshotTranscriptBoundaries(t *testing.T) {
	c, m, _, _ := remoteRPCFixture(t)
	archived := &remoteSession{ID: "history-page", Cwd: m.home, State: "stopped", Agent: "codex", UpdatedAt: remoteNow()}
	for i := 0; i < 25; i++ {
		archived.Messages = append(archived.Messages, remoteMessage{ID: fmt.Sprint(i), Role: "user", Text: fmt.Sprintf("message %d", i), CreatedAt: remoteNow()})
	}
	m.history = []*remoteSession{archived}
	running := &remoteSession{ID: "running-page", Cwd: m.home, State: "running", Agent: "codex", UpdatedAt: remoteNow()}
	for i := 0; i < 25; i++ {
		running.Messages = append(running.Messages, remoteMessage{ID: fmt.Sprint(i), Role: "user", Text: fmt.Sprintf("message %d", i), CreatedAt: remoteNow()})
	}
	m.sessions[running.ID] = running
	snapshot, err := callRemoteRPC(t, c, "remote.snapshot", nil)
	if err != nil {
		t.Fatal(err)
	}
	row := snapshot.(map[string]any)["historySessions"].([]any)[0].(map[string]any)
	if _, exists := row["messages"]; exists {
		t.Fatal("archived session list contains conversation messages")
	}
	if row["messageCount"] != float64(25) || row["detailAvailable"] != true {
		t.Fatal("archive summary omitted message metadata")
	}
	runningRow := remoteSnapshotSession(t, snapshot.(map[string]any), running.ID)
	runningMessages := runningRow["messages"].([]any)
	if fmt.Sprint(runningRow["messageCount"]) != "25" || fmt.Sprint(runningRow["messageStart"]) != "5" ||
		len(runningMessages) != 20 || runningMessages[0].(map[string]any)["id"] != "5" {
		t.Fatalf("running snapshot did not contain only the latest 20 messages: row=%v count=%d", runningRow, len(runningMessages))
	}
	read := func(id string, payload map[string]any) map[string]any {
		t.Helper()
		command := remoteEnvelope(id, "session.history.read", archived.ID, payload)
		command.ExpectedHostEpoch = m.epoch
		result, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": command})
		if err != nil {
			t.Fatal(err)
		}
		return result.(remoteReceipt).Result
	}
	latest := read("latest-page", map[string]any{"limit": 20})
	if latest["start"] != 5 || latest["end"] != 25 || latest["total"] != 25 || latest["hasOlder"] != true {
		t.Fatal("invalid latest page metadata", latest)
	}
	latestMessages := latest["messages"].([]remoteMessage)
	if len(latestMessages) != 20 || latestMessages[0].ID != "5" || latestMessages[19].ID != "24" {
		t.Fatal("latest page did not return the newest 20 messages")
	}
	older := read("older-page", map[string]any{"before": 5, "limit": 20})
	olderMessages := older["messages"].([]remoteMessage)
	if older["start"] != 0 || older["end"] != 5 || older["hasOlder"] != false || len(olderMessages) != 5 || olderMessages[0].ID != "0" {
		t.Fatal("invalid older page", older)
	}
}

func TestRemoteWorkspaceHistoryProtectsBusyAndFailedCleanup(t *testing.T) {
	m, p, home := remoteFixture(t)
	for i := 0; i < 8; i++ {
		id := fmt.Sprint("busy-", i)
		r, err := m.command(remoteEnvelope(id, "session.create", "", map[string]any{"agent": "codex", "cwd": home}))
		if err != nil {
			t.Fatal(err)
		}
		waitRemote(t, m, id)
		send := id + "-send"
		if _, err = m.command(remoteEnvelope(send, "message.send", r.SessionID, map[string]any{"text": "pending"})); err != nil {
			t.Fatal(err)
		}
		waitRemote(t, m, send)
	}
	create := remoteEnvelope("ninth", "session.create", "", map[string]any{"agent": "codex", "cwd": home})
	if m.snapshot()["canCreateSession"] != false {
		t.Fatal("all busy advertised capacity")
	}
	if _, err := m.command(create); err == nil || err.Error() != remoteSessionLimitError {
		t.Fatal("lost quota guard", err)
	}
	p.emit(remoteProviderEvent{Type: "state", State: "idle"})
	if _, err := m.commandWithAuthorization(create, func(context.Context) error { return fmt.Errorf("revoked") }); err == nil {
		t.Fatal("revoked retirement accepted")
	}
	p.closeErr = fmt.Errorf("cleanup failed")
	if _, err := m.command(create); err == nil {
		t.Fatal("lost session after cleanup failure")
	}
	p.closeErr = nil
	snap := m.snapshot()
	if len(snap["sessions"].([]any)) != 8 || len(snap["historySessions"].([]any)) != 0 {
		t.Fatal("failed retirement displaced session")
	}
}

func TestRemoteWorkspaceHistoryProjectionBudget(t *testing.T) {
	m, _, _ := remoteFixture(t)
	for i := 0; i < 80; i++ {
		s := &remoteSession{ID: fmt.Sprint(i), Cwd: m.home, State: "stopped", Agent: "codex", UpdatedAt: remoteNow()}
		for j := 0; j < 16; j++ {
			s.Messages = append(s.Messages, remoteMessage{ID: fmt.Sprint(j), Role: []string{"user", "assistant"}[j%2], Text: strings.Repeat("你<&", 4000)})
		}
		m.archiveSessionLocked(s)
	}
	snap := m.snapshot()
	raw, err := json.Marshal(snap)
	if err != nil || len(raw) > 512*1024 {
		t.Fatalf("snapshot %d: %v", len(raw), err)
	}
	rows := snap["historySessions"].([]any)
	if len(rows) != remoteHistoryLimit || len(rows) <= 10 {
		t.Fatal("directory size", len(rows))
	}
	for _, entry := range rows {
		row := entry.(map[string]any)
		if _, exists := row["messages"]; exists || row["messageCount"] != float64(16) {
			t.Fatal("directory projection contains conversation messages")
		}
	}
}

func TestRemoteWorkspaceHistoryDetailRoundTripAndBounds(t *testing.T) {
	c, m, _, api := remoteRPCFixture(t)
	messages := []remoteMessage{{ID: "prompt", Role: "user", Text: "original opening question", CreatedAt: remoteNow(), Attachments: []string{"image-id"}}}
	for i := 1; i < 256; i++ {
		messages = append(messages, remoteMessage{ID: fmt.Sprint(i), Role: "assistant", Text: strings.Repeat("正文<>\n\"\\", 110), CreatedAt: remoteNow()})
	}
	old := time.Now().Add(-8 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)
	sid := "11111111-1111-4111-8111-111111111110"
	m.sessions[sid] = &remoteSession{ID: sid, NativeID: "owner-native-handle", Agent: "codex", Cwd: m.home, State: "stopped", Messages: messages, UpdatedAt: old}
	if err := c.stopRemote(); err != nil {
		t.Fatal(err)
	}
	out, err := callRemoteRPC(t, c, "remote.snapshot", nil)
	if err != nil {
		t.Fatal(err)
	}
	snap := out.(map[string]any)
	row := snap["historySessions"].([]any)[0].(map[string]any)
	if row["detailAvailable"] != true || row["nativeId"] != nil || row["messageCount"] != float64(256) {
		t.Fatal("archive metadata")
	}
	m = c.remote
	archive := m.history[0]
	if archive.NativeID != "owner-native-handle" || len(archive.Messages) != 256 {
		t.Fatal("legacy session lost full native history")
	}
	read := func(id, session, epoch string, payload any) (remoteReceipt, error) {
		cmd := remoteEnvelope(id, "session.history.read", session, payload)
		cmd.ExpectedHostEpoch = epoch
		val, err := callRemoteRPC(t, c, "remote.command", map[string]any{"command": cmd})
		if err != nil {
			return remoteReceipt{}, err
		}
		return val.(remoteReceipt), nil
	}
	fileBefore, err := os.ReadFile(m.path)
	if err != nil {
		t.Fatal(err)
	}
	var assembled strings.Builder
	for {
		offset := assembled.Len()
		r, err := read(fmt.Sprint("page-", offset), sid, m.epoch, map[string]any{"offset": offset})
		if err != nil {
			t.Fatal(err)
		}
		encoded, _ := json.Marshal(r)
		page := r.Result
		chunk := page["content"].(string)
		if r.Status != "providerAccepted" || r.SessionID != sid || page["sessionId"] != sid || page["offset"] != offset || len(encoded) > 32768 || len(chunk) > 4096 || len(chunk) == 0 || !utf8.ValidString(chunk) {
			t.Fatalf("invalid receipt at %d", offset)
		}
		assembled.WriteString(chunk)
		if page["nextOffset"] != assembled.Len() {
			t.Fatal("offset drift")
		}
		if page["total"] == assembled.Len() {
			break
		}
	}
	want, _ := json.Marshal(messages)
	if assembled.String() != string(want) {
		t.Fatal("transcript differs from durable input")
	}
	before := m.snapshot()["revision"]
	for i, p := range []any{map[string]any{"offset": -1}, map[string]any{"offset": len(want) + 1}, map[string]any{"offset": strings.Index(string(want), "正") + 1}, map[string]any{"offset": 0, "text": "extra"}, map[string]any{}} {
		if _, err := read(fmt.Sprint("bad-", i), sid, m.epoch, p); err == nil {
			t.Fatal("accepted invalid history payload")
		}
	}
	if _, err := read("wrong-epoch", sid, "stale", map[string]int{"offset": 0}); err == nil {
		t.Fatal("wrong epoch allowed")
	}
	if m.snapshot()["revision"] != before {
		t.Fatal("read mutated revision")
	}
	fileAfter, err := os.ReadFile(m.path)
	if err != nil || !bytes.Equal(fileBefore, fileAfter) {
		t.Fatal("history read wrote journal", err)
	}
	api.mu.Lock()
	api.owner = remoteRPCOtherOwner
	api.mu.Unlock()
	if _, err := callRemoteRPC(t, c, "remote.snapshot", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := read("wrong-owner", sid, c.remote.epoch, map[string]int{"offset": 0}); err == nil {
		t.Fatal("cross-owner read allowed")
	}
}

func TestRemoteWorkspaceHistoryEmptyImageAndActiveControlBudget(t *testing.T) {
	m, _, home := remoteFixture(t)
	for i := 0; i < 2; i++ {
		s := &remoteSession{ID: fmt.Sprint(i), Cwd: home, Agent: "codex", State: "stopped", UpdatedAt: remoteNow()}
		if i == 1 {
			s.Messages = []remoteMessage{{ID: "image", Role: "user", Attachments: []string{"upload-id"}}}
		}
		m.archiveSessionLocked(s)
	}
	id := createRemote(t, m, home)
	m.mu.Lock()
	m.sessions[id].Approvals = []*remoteApproval{{ID: "decision", State: "open", Options: []string{"decline"}, Data: json.RawMessage(`"` + strings.Repeat("x", 440000) + `"`)}}
	m.mu.Unlock()
	snap := m.snapshot()
	entries := snap["historySessions"].([]any)
	if len(entries) != 2 || entries[1].(map[string]any)["messageCount"] != float64(0) {
		t.Fatal("lost empty archive")
	}
	if entries[0].(map[string]any)["messageCount"] != float64(1) {
		t.Fatal("lost message count")
	}
	approval := remoteSnapshotSession(t, snap, id)["approvals"].([]any)[0].(map[string]any)
	if len(approval["data"].(string)) != 440000 || len(snap["operations"].([]any)) == 0 {
		t.Fatal("lost active control data")
	}
	raw, _ := json.Marshal(snap)
	if len(raw) > remoteSnapshotLimit {
		t.Fatal("snapshot exceeds limit")
	}
}

func TestRemoteWorkspaceHistoryJournalQuotaRollback(t *testing.T) {
	m, _, home := remoteFixture(t)
	var first string
	for i := 0; i < 8; i++ {
		id := fmt.Sprint("quota-", i)
		r, err := m.command(remoteEnvelope(id, "session.create", "", map[string]any{"agent": "codex", "cwd": home}))
		if err != nil {
			t.Fatal(err)
		}
		waitRemote(t, m, id)
		if i == 0 {
			first = r.SessionID
		}
	}
	old, err := os.ReadFile(m.path)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an accumulated transcript at the journal boundary without a real provider.
	m.sessions[first].Messages = []remoteMessage{{Role: "user", Text: strings.Repeat("x", remoteMaxJournalBytes)}}
	_, err = m.command(remoteEnvelope("quota-refused", "session.create", "", map[string]any{"agent": "codex", "cwd": home}))
	if err == nil || !strings.Contains(err.Error(), "storage quota") {
		t.Fatal("oversized archive admitted", err)
	}
	saved, e := os.ReadFile(m.path)
	if e != nil || !bytes.Equal(saved, old) || len(m.sessions) != 8 || len(m.history) != 0 || len(m.sessions[first].Messages[0].Text) != remoteMaxJournalBytes {
		t.Fatal("failed save lost session or changed journal", e)
	}
	m.sessions[first].Messages = nil
}

func TestRemoteWorkspaceHistoryConcurrentCreateAndPendingWork(t *testing.T) {
	m, _, home := remoteFixture(t)
	var ids []string
	for i := 0; i < 8; i++ {
		id := fmt.Sprint("init-", i)
		r, err := m.command(remoteEnvelope(id, "session.create", "", map[string]any{"agent": "codex", "cwd": home}))
		if err != nil {
			t.Fatal(err)
		}
		waitRemote(t, m, id)
		ids = append(ids, r.SessionID)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	_, err := m.commandWithAuthorization(remoteEnvelope("pending", "run.stop", ids[0], map[string]any{}), func(context.Context) error { close(entered); <-release; return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		id := fmt.Sprint("same-time-", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.command(remoteEnvelope(id, "session.create", "", map[string]any{"agent": "codex", "cwd": home}))
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	close(release)
	waitRemote(t, m, "pending")
	waitRemote(t, m, "same-time-0")
	waitRemote(t, m, "same-time-1")
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sessions) != 8 || len(m.history) != 2 || m.sessions[ids[0]] == nil {
		t.Fatal("in-flight session displaced or slot count drifted")
	}
}

func TestRemoteWorkspaceHistoryRelayRechecksGrant(t *testing.T) {
	for _, mode := range []string{"valid", "revoked", "different-session"} {
		t.Run(mode, func(t *testing.T) {
			m, _, _ := remoteFixture(t)
			s := &remoteSession{ID: "archive", Agent: "codex", State: "stopped", Cwd: m.home, UpdatedAt: remoteNow(), Messages: []remoteMessage{{ID: "old", Role: "user", Text: "Question?"}}}
			m.archiveSessionLocked(s)
			c := remoteEnvelope("relay-history", "session.history.read", s.ID, map[string]int{"offset": 0})
			c.ExpectedHostEpoch = m.epoch
			var verifications atomic.Int32
			received := make(chan remoteReceipt, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if serveRelayFixtureWS(w, r, m.hostID, true) {
					return
				}
				switch filepath.Base(r.URL.Path) {
				case "poll":
					json.NewEncoder(w).Encode(map[string]any{"commands": []any{map[string]any{"command": c, "grant": "fixture-grant", "commandDigest": remoteDigest(c)}}})
				case "verify-grant":
					var request map[string]string
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request["operationId"] != c.OperationID || request["commandDigest"] != remoteDigest(c) || request["grant"] != "fixture-grant" || r.Header.Get("X-NewNoa-Host-Epoch") != m.epoch {
						t.Error("bad grant binding")
					}
					n := verifications.Add(1)
					verified := c
					if n == 2 && mode == "different-session" {
						verified.SessionID = "other"
					}
					json.NewEncoder(w).Encode(map[string]any{"valid": n == 1 || mode != "revoked", "command": verified, "scope": c.Type, "hostEpoch": m.epoch})
				case "publish":
					var body struct{ Receipts []remoteReceipt }
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					for _, r := range body.Receipts {
						select {
						case received <- r:
						default:
						}
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			host := newCore(m.home, server.URL)
			host.control = server.URL
			m.session = "fixture-session"
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); host.remoteRelay(ctx, m) }()
			defer func() { cancel(); <-done }()
			select {
			case receipt := <-received:
				if verifications.Load() != 2 || receipt.SessionID != s.ID {
					t.Fatal("missing read-time grant or session binding")
				}
				if mode == "valid" {
					if receipt.Status != "providerAccepted" || !strings.Contains(receipt.Result["content"].(string), "Question?") {
						t.Fatal("granted transcript missing")
					}
				} else if receipt.Status != "rejected" || receipt.Result != nil {
					t.Fatal("unverified grant returned content")
				}
			case <-time.After(4 * time.Second):
				t.Fatal("history receipt not published")
			}
		})
	}
}

func TestRemoteWorkspaceHistoryBecomingBusyDuringGrantCheck(t *testing.T) {
	m, p, home := remoteFixture(t)
	for i := 0; i < 8; i++ {
		id := fmt.Sprint("before-grant-", i)
		_, err := m.command(remoteEnvelope(id, "session.create", "", map[string]any{"agent": "codex", "cwd": home}))
		if err != nil {
			t.Fatal(err)
		}
		waitRemote(t, m, id)
		if i < 7 {
			p.emit(remoteProviderEvent{Type: "state", State: "running"})
		}
	}
	_, err := m.commandWithAuthorization(remoteEnvelope("after-grant", "session.create", "", map[string]any{"agent": "codex", "cwd": home}), func(context.Context) error { p.emit(remoteProviderEvent{Type: "state", State: "running"}); return nil })
	if err == nil || len(m.history) != 0 || m.snapshot()["canCreateSession"] != false {
		t.Fatal("session retired while starting a turn")
	}
}

func TestHistoryPageEmptyWireAndFrozenOversizeReceipt(t *testing.T) {
	m, _, _ := remoteFixture(t)
	s := &remoteSession{ID: "frozen-transcript", State: "running"}
	m.sessions[s.ID] = s
	n := 0
	read := func(sid string, params map[string]any) (remoteReceipt, error) {
		n++
		c := remoteEnvelope(fmt.Sprintf("page-%d", n), "session.history.read", sid, params)
		c.ExpectedHostEpoch = m.epoch
		return m.command(c)
	}
	empty, err := read(s.ID, map[string]any{"limit": 20})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(empty.Result["messages"])
	if err != nil || string(wire) != "[]" {
		t.Fatalf("empty transcript encoded as %s (%v)", wire, err)
	}
	s.Messages = []remoteMessage{{ID: "one", Text: strings.Repeat("字\\\n", 10000), Role: "assistant"}}
	expected, err := json.Marshal(s.Messages)
	if err != nil {
		t.Fatal(err)
	}
	first, err := read(s.ID, map[string]any{"limit": 20})
	if err != nil {
		t.Fatal(err)
	}
	page, ok := first.Result["pageId"].(string)
	if !ok || page == "" {
		t.Fatal("oversize page has no pinned snapshot")
	}
	s.Messages[0].Text = "updated while reading"
	s.Messages = append(s.Messages, remoteMessage{ID: "two", Text: "later"})
	next := func(offset int) map[string]any {
		return map[string]any{"limit": 20, "before": 1, "pageId": page, "offset": offset}
	}
	if _, err := read("another-session", next(4096)); err == nil {
		t.Fatal("continued a snapshot on a different session")
	}
	var assembled []byte
	for part := first; ; {
		b, err := json.Marshal(part)
		if err != nil || len(b) > 32*1024 {
			t.Fatalf("receipt exceeds API budget: %d, %v", len(b), err)
		}
		r := part.Result
		content, ok := r["content"].(string)
		if !ok || !utf8.ValidString(content) || len(content) > 4096 || r["offset"] != len(assembled) || r["total"] != 1 || r["end"] != 1 || r["totalBytes"] != len(expected) {
			t.Fatal("incorrect frozen page metadata or chunk")
		}
		assembled = append(assembled, content...)
		if r["nextOffset"] != len(assembled) || len(content) == 0 {
			t.Fatal("chunk did not advance")
		}
		if len(assembled) == len(expected) {
			break
		}
		part, err = read(s.ID, next(len(assembled)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(assembled, expected) {
		t.Fatal("frozen transcript changed during delivery")
	}
}

func TestRemoteHistoryBoundsContinuationsAndEvictsOldSnapshots(t *testing.T) {
	const id = "transcript"
	session := &remoteSession{ID: id, Messages: []remoteMessage{{ID: "one", Role: "assistant", Text: strings.Repeat("汉", 20000)}}}
	m := &remoteManager{
		epoch: "7", sessions: map[string]*remoteSession{id: session},
		operations: map[string]*remoteReceipt{},
	}
	c := remoteCommandEnvelope{ExpectedHostEpoch: "7", SessionID: id}
	limit, before, offset := 10, 1, 1
	var first string
	for i := range 9 {
		c.OperationID = fmt.Sprintf("frozen-%d", i)
		r, err := m.historyPageReceiptLocked(c, c.OperationID, remoteHistoryPageRequest{Limit: &limit})
		if err != nil {
			t.Fatal(err)
		}
		pageID := r.Result["pageId"].(string)
		if i == 0 {
			first = pageID
			m.historyPages[pageID].lastRead = time.Unix(1, 0)
		}
	}
	if len(m.historyPages) != 8 {
		t.Fatalf("retained %d pages, wanted 8", len(m.historyPages))
	}
	if m.historyPages[first] != nil {
		t.Fatal("oldest page retained")
	}
	latest := m.operations["frozen-8"].Result["pageId"].(string)
	page := m.historyPages[latest]
	for offset < len(page.data) && utf8.RuneStart(page.data[offset]) {
		offset++
	}
	for _, tc := range []struct {
		name string
		c    remoteCommandEnvelope
		req  remoteHistoryPageRequest
		want string
	}{
		{"invalid limit", c, remoteHistoryPageRequest{Limit: new(21)}, "Invalid history page"},
		{"partial continuation", c, remoteHistoryPageRequest{Limit: &limit, PageID: &latest}, "Invalid history continuation"},
		{"invalid UTF-8 offset", c, remoteHistoryPageRequest{Limit: &limit, PageID: &latest, Before: &before, Offset: &offset}, "Invalid history page offset"},
		{"evicted snapshot", c, remoteHistoryPageRequest{Limit: &limit, PageID: &first, Before: &before, Offset: &offset}, remoteHistoryPageExpired},
		{"missing session", remoteCommandEnvelope{ExpectedHostEpoch: "7", SessionID: "missing"}, remoteHistoryPageRequest{Limit: &limit}, "Remote session not found"},
		{"past transcript end", c, remoteHistoryPageRequest{Limit: &limit, Before: new(2)}, "Invalid history page offset"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := m.historyPageReceiptLocked(tc.c, tc.name, tc.req); err == nil || err.Error() != tc.want {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
	session.Messages[0].Text = strings.Repeat("a", 8*1024*1024)
	if _, err := m.historyPageReceiptLocked(c, "oversize", remoteHistoryPageRequest{Limit: &limit}); err == nil || err.Error() != "History page too large; request fewer messages" {
		t.Fatal("oversize transcript was accepted", err)
	}
}
