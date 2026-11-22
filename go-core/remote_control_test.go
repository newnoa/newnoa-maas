package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type remoteTestProvider struct {
	mu                            sync.Mutex
	sends, steers, stops, answers int
	emit                          func(remoteProviderEvent)
	steerErr                      error
	closeErr                      error
}

func (p *remoteTestProvider) StartTurn(context.Context, remoteProviderInput) error {
	p.mu.Lock()
	p.sends++
	p.mu.Unlock()
	p.emit(remoteProviderEvent{Type: "state", State: "running", RunID: "native-run"})
	return nil
}
func (p *remoteTestProvider) Steer(context.Context, remoteProviderInput) error {
	p.mu.Lock()
	p.steers++
	p.mu.Unlock()
	return p.steerErr
}
func (p *remoteTestProvider) Stop(context.Context) error {
	p.mu.Lock()
	p.stops++
	p.mu.Unlock()
	p.emit(remoteProviderEvent{Type: "state", State: "stopped"})
	return nil
}
func (p *remoteTestProvider) Respond(context.Context, remoteProviderReply) error {
	p.mu.Lock()
	p.answers++
	p.mu.Unlock()
	return nil
}
func (p *remoteTestProvider) Close() error { p.mu.Lock(); p.stops++; p.mu.Unlock(); return p.closeErr }

func remoteFixture(t *testing.T) (*remoteManager, *remoteTestProvider, string) {
	t.Helper()
	home := t.TempDir()
	m, err := newRemoteManager(home, "11111111-1111-4111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	p := &remoteTestProvider{}
	m.start = func(_ context.Context, _, _, _ string, emit func(remoteProviderEvent)) (remoteProvider, error) {
		p.emit = emit
		emit(remoteProviderEvent{Type: "session", NativeID: "native-session", Model: "confirmed-model"})
		return p, nil
	}
	m.executors["codex"] = &remoteExecutor{Agent: "codex", Supported: true, Authorized: true, State: "ready", Capabilities: map[string]bool{"steer": true, "image": true, "resume": true, "modelChange": false}}
	t.Cleanup(func() { m.shutdown() })
	return m, p, home
}
func remoteEnvelope(id, typ, sid string, payload any) remoteCommandEnvelope {
	raw, _ := json.Marshal(payload)
	return remoteCommandEnvelope{ProtocolVersion: 1, OperationID: id, Type: typ, SessionID: sid, Payload: raw}
}
func waitRemote(t *testing.T, m *remoteManager, id string) remoteReceipt {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		r := m.operations[id]
		if r != nil && r.Status != "accepted" {
			out := *r
			m.mu.Unlock()
			return out
		}
		m.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("operation %s never settled", id)
	return remoteReceipt{}
}
func createRemote(t *testing.T, m *remoteManager, cwd string) string {
	t.Helper()
	r, err := m.command(remoteEnvelope("create-1", "session.create", "", map[string]any{"agent": "codex", "cwd": cwd}))
	if err != nil {
		t.Fatal(err)
	}
	if final := waitRemote(t, m, "create-1"); final.Status != "providerAccepted" {
		t.Fatalf("create: %+v", final)
	}
	return r.SessionID
}
func TestRemoteCommandIdempotencyAndPayloadConflict(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	command := remoteEnvelope("message-1", "message.send", sid, map[string]any{"text": "hello"})
	if _, err := m.command(command); err != nil {
		t.Fatal(err)
	}
	waitRemote(t, m, "message-1")
	if _, err := m.command(command); err != nil {
		t.Fatal(err)
	}
	command.Payload = json.RawMessage(`{"text":"different"}`)
	if _, err := m.command(command); err == nil {
		t.Fatal("operation payload reuse accepted")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sends != 1 {
		t.Fatalf("native sends %d", p.sends)
	}
}
func TestRemoteBusyLockSteerAndStaleRun(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	_, err := m.command(remoteEnvelope("send", "message.send", sid, map[string]any{"text": "first"}))
	if err != nil {
		t.Fatal(err)
	}
	waitRemote(t, m, "send")
	_, err = m.command(remoteEnvelope("steer", "message.send", sid, map[string]any{"text": "adjust now"}))
	if err != nil {
		t.Fatal(err)
	}
	waitRemote(t, m, "steer")
	m.mu.Lock()
	busy := m.providerBusyLocked("codex")
	m.mu.Unlock()
	if !busy {
		t.Fatal("active provider not locked")
	}
	stale := remoteEnvelope("stop-stale", "run.stop", sid, map[string]any{})
	stale.RunID = "another-run"
	if _, err = m.command(stale); err == nil {
		t.Fatal("stale run accepted")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sends != 1 || p.steers != 1 {
		t.Fatalf("send/steer %d/%d", p.sends, p.steers)
	}
}
func TestRemoteApprovalOnlyFirstAnswerAndLateEventsFenced(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: "approval-1", Kind: "tool", Title: "Run tool", Options: []string{"accept", "decline"}}})
	m.mu.Lock()
	approvalID := m.sessions[sid].Approvals[0].ID
	m.mu.Unlock()
	first := remoteEnvelope("approve-1", "approval.respond", sid, map[string]any{"approvalId": approvalID, "decision": "accept"})
	if _, err := m.command(first); err != nil {
		t.Fatal(err)
	}
	if _, err := m.command(remoteEnvelope("approve-2", "approval.respond", sid, map[string]any{"approvalId": approvalID, "decision": "accept"})); err == nil {
		t.Fatal("second approval accepted")
	}
	waitRemote(t, m, "approve-1")
	m.shutdown()
	p.emit(remoteProviderEvent{Type: "output", Text: "late output"})
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, v := range m.sessions[sid].Messages {
		if strings.Contains(v.Text, "late output") {
			t.Fatal("late provider callback escaped fence")
		}
	}
}
func TestRemoteJournalRecoveryMarksUnknownAndPrivate(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	if _, err := m.command(remoteEnvelope("send-recover", "message.send", sid, map[string]any{"text": "work"})); err != nil {
		t.Fatal(err)
	}
	waitRemote(t, m, "send-recover")
	m.cancel()
	m.mu.Lock()
	m.closed = true
	m.providers = map[string]remoteProvider{}
	m.mu.Unlock()
	if m.lock != nil {
		m.lock.Close()
		m.lock = nil
	}
	recovered, err := newRemoteManager(cwd, "11111111-1111-4111-8111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.shutdown()
	recovered.mu.Lock()
	state := recovered.sessions[sid].State
	// The simulated old CLI does not exist. Mark it stopped only for fixture cleanup.
	recovered.sessions[sid].State = "stopped"
	recovered.mu.Unlock()
	if state != "unknown" {
		t.Fatalf("recovered running state %q", state)
	}
	info, err := os.Stat(m.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRemoteJournalPrivacy(m.path, m.uid, info); err != nil {
		t.Fatal(err)
	}
	other, err := newRemoteManager(cwd, "22222222-2222-4222-8222-222222222222")
	if err != nil {
		t.Fatal(err)
	}
	defer other.shutdown()
	if len(other.sessions) != 0 {
		t.Fatal("cross-account state leaked")
	}
}
func TestRemoteRejectsInvalidDirectoryAndUnsupportedExecutor(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	for _, payload := range []map[string]any{{"agent": "codex", "cwd": filepath.Join(cwd, "missing")}, {"agent": "claude-desktop", "cwd": cwd}, {"agent": "codex", "cwd": cwd, "executable": "sh"}} {
		if _, err := m.command(remoteEnvelope("invalid-"+randomToken(), "session.create", "", payload)); err == nil {
			t.Fatalf("accepted invalid create %+v", payload)
		}
	}
}
func TestRemoteAttachmentBoundedAndOwned(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	for _, payload := range []map[string]any{{"name": "../../evil", "mime": "image/png", "size": 1}, {"name": "a.png", "mime": "text/plain", "size": 1}, {"name": "a.png", "mime": "image/png", "size": remoteMaxImageBytes + 1}} {
		if _, err := m.command(remoteEnvelope("bad-image-"+randomToken(), "attachment.prepare", sid, payload)); err == nil {
			t.Fatal("invalid attachment accepted")
		}
	}
}
func TestRemoteEnvelopeRejectsAuthorityOverrides(t *testing.T) {
	for _, raw := range []string{`{"protocolVersion":1,"operationId":"x","type":"session.create","payload":{},"ownerId":"victim"}`, `{"protocolVersion":2,"operationId":"x","type":"shell","payload":{}}`, `{"protocolVersion":1,"operationId":"x","type":"session.create","payload":{}} {}`} {
		if _, err := parseRemoteCommand(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted unsafe envelope %s", raw)
		}
	}
}

func TestRemoteCleanupFailureRemainsVisibleUntilRetrySucceeds(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	_ = createRemote(t, m, cwd)
	p.closeErr = errors.New("owned process remains")
	if m.shutdown() == nil {
		t.Fatal("cleanup failure hidden")
	}
	if m.shutdown() == nil {
		t.Fatal("second cleanup falsely succeeded")
	}
	p.closeErr = nil
	if err := m.shutdown(); err != nil {
		t.Fatal(err)
	}
}
func TestRemoteShutdownClearsVerifiedProcessRecord(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	id := createRemote(t, m, cwd)
	m.mu.Lock()
	m.sessions[id].OwnedProcess = remoteOwnedProcess{PID: 12345, Identity: "owned-fixture"}
	m.sessions[id].OwnedGateway = &remoteOwnedProcess{PID: 12346, Identity: "owned-gateway-fixture"}
	m.mu.Unlock()
	p.closeErr = errors.New("fixture cleanup incomplete")
	if err := m.shutdown(); err == nil {
		t.Fatal("failed cleanup reported success")
	}
	m.mu.Lock()
	preserved := m.sessions[id].OwnedProcess.PID == 12345 && m.sessions[id].OwnedGateway != nil
	m.mu.Unlock()
	if !preserved {
		t.Fatal("failed cleanup dropped process ownership")
	}
	p.closeErr = nil
	for i := range 2 {
		if err := m.shutdown(); err != nil {
			t.Fatalf("shutdown %d: %v", i+1, err)
		}
	}
}

func TestRemoteQueuedSendBlocksModelChangeBeforeNativeStarts(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	m.modelChange = func(context.Context, *remoteSession, remotePayload) error { return nil }
	m.mu.Lock()
	m.sessions[sid].State = "starting"
	m.mu.Unlock()
	if _, err := m.command(remoteEnvelope("switch-too-soon", "model.change", sid, map[string]any{"keyId": "x", "modelId": "y"})); err == nil {
		t.Fatal("model changed while send starting")
	}
}

func TestRemoteProjectionRedactsCredentialsButPreservesApprovalQuestions(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	_ = createRemote(t, m, cwd)
	p.emit(remoteProviderEvent{Type: "output", Text: "key sk-1234567890abcdefghijklmnop"})
	p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: "q", Title: "Confirm", Options: []string{"accept"}, Data: json.RawMessage(`{"apiKey":"secret-credential","questions":[{"id":"q","question":"Continue?"}]}`)}})
	raw, _ := json.Marshal(m.snapshot())
	if strings.Contains(string(raw), "secret-credential") || strings.Contains(string(raw), "sk-1234567890abcdefghijklmnop") || strings.Contains(string(raw), "apiKey") {
		t.Fatal("control projection leaked credential")
	}
	if !strings.Contains(string(raw), "Continue?") {
		t.Fatal("approval context lost")
	}
}
func TestRemoteQueuedAuthorizationIsRecheckedBeforeNativeEffect(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	_, err := m.commandWithAuthorization(remoteEnvelope("revoked-send", "message.send", sid, map[string]any{"text": "must not execute"}), func(context.Context) error { return errors.New("authorization expired") })
	if err != nil {
		t.Fatal(err)
	}
	r := waitRemote(t, m, "revoked-send")
	if r.Status != "rejected" {
		t.Fatal(r)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sends != 0 {
		t.Fatal("revoked queued input reached native provider")
	}
}

func TestRemoteExclusiveOwnerLease(t *testing.T) {
	m, _, home := remoteFixture(t)
	second, err := newRemoteManager(home, m.owner)
	if err == nil {
		second.shutdown()
		t.Fatal("second Core acquired same account writer")
	}
}

func TestRemoteNativeApprovalReuseCannotResolveAnotherAttempt(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	open := func() {
		p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: "native-reused", Options: []string{"accept"}}})
	}
	open()
	m.mu.Lock()
	first := m.sessions[sid].Approvals[0].ID
	m.sessions[sid].Approvals[0].State = "invalidated"
	m.mu.Unlock()
	open()
	m.mu.Lock()
	second := m.sessions[sid].Approvals[1].ID
	m.mu.Unlock()
	if first == second {
		t.Fatal("public approval identity reused across callbacks")
	}
	if _, err := m.command(remoteEnvelope("new-approval", "approval.respond", sid, map[string]any{"approvalId": second, "decision": "accept"})); err != nil {
		t.Fatal(err)
	}
	waitRemote(t, m, "new-approval")
}

func TestRemoteFailedIntentDoesNotConsumeSessionApprovalOrSwitch(t *testing.T) {
	for _, kind := range []string{"session.create", "approval.respond", "model.change"} {
		t.Run(kind, func(t *testing.T) {
			m, p, cwd := remoteFixture(t)
			sid := ""
			payload := map[string]any{"agent": "codex", "cwd": cwd}
			if kind != "session.create" {
				sid = createRemote(t, m, cwd)
			}
			if kind == "approval.respond" {
				p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: "native", Options: []string{"accept"}}})
				m.mu.Lock()
				payload = map[string]any{"approvalId": m.sessions[sid].Approvals[0].ID, "decision": "accept"}
				m.mu.Unlock()
			}
			if kind == "model.change" {
				m.modelChange = func(context.Context, *remoteSession, remotePayload) error { return nil }
				payload = map[string]any{"keyId": "key", "modelId": "model"}
			}
			directory := filepath.Dir(m.path)
			if err := os.Chmod(directory, 0500); err != nil {
				t.Fatal(err)
			}
			_, err := m.command(remoteEnvelope("cannot-save", kind, sid, payload))
			_ = os.Chmod(directory, 0700)
			if err == nil {
				t.Fatal("writefailure not reported")
			}
			m.mu.Lock()
			defer m.mu.Unlock()
			if kind == "session.create" && len(m.sessions) != 0 {
				t.Fatal("phantom session afterfailedintent")
			}
			if kind == "approval.respond" && m.sessions[sid].Approvals[0].State != "open" {
				t.Fatal("approvalconsumed byfailedintent")
			}
			if kind == "model.change" && (m.switching["codex"] || m.sessions[sid].State != "idle") {
				t.Fatal("modelfailure leftbusylock")
			}
		})
	}
}

func TestRemoteSnapshotOmitsOwnedProcessAndStaleApprovalInternals(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: "native-sensitive-id", Options: []string{"accept"}}})
	m.mu.Lock()
	m.sessions[sid].OwnedProcess = remoteOwnedProcess{PID: 123, Identity: "private-process-birth", Executable: "private-executable"}
	m.mu.Unlock()
	raw, _ := json.Marshal(m.snapshot())
	if strings.Contains(string(raw), "private-process-birth") || strings.Contains(string(raw), "private-executable") || strings.Contains(string(raw), "native-sensitive-id") {
		t.Fatal("private native authority leaked in cloudprojection")
	}
}

func TestRemoteStopDoesNotReplaceNativeTerminalState(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	if _, err := m.command(remoteEnvelope("send", "message.send", sid, map[string]any{"text": "hello"})); err != nil {
		t.Fatal(err)
	}
	waitRemote(t, m, "send")
	// The queued stop arrives after the provider reported turn completion.
	if _, err := m.commandWithAuthorization(remoteEnvelope("stop", "run.stop", sid, map[string]any{}), func(context.Context) error {
		p.emit(remoteProviderEvent{Type: "state", State: "idle"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := waitRemote(t, m, "stop")
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.Status != "providerAccepted" || m.sessions[sid].State != "idle" || p.stops != 0 {
		t.Fatalf("late stop modified an already completed turn: %+v %s stops=%d", r, m.sessions[sid].State, p.stops)
	}
}

func TestRemoteModelFenceSurvivesNativeMetadataButNotSupersedingModel(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	m.modelChange = func(context.Context, *remoteSession, remotePayload) error {
		p.emit(remoteProviderEvent{Type: "state", State: "idle"})
		return nil
	}
	if _, err := m.command(remoteEnvelope("change", "model.change", sid, map[string]any{"keyId": "key", "modelId": "model"})); err != nil {
		t.Fatal(err)
	}
	changed := waitRemote(t, m, "change")
	revision, ok := changed.Result["modelRevision"].(uint64)
	if changed.Status != "providerAccepted" || !ok || revision == 0 {
		t.Fatalf("missing model revision: %+v", changed)
	}
	p.emit(remoteProviderEvent{Type: "model", Model: "reported-model"})
	m.mu.Lock()
	updated := m.sessions[sid].Revision > revision
	m.mu.Unlock()
	if !updated {
		t.Fatal("provider metadata must advance the snapshot revision")
	}
	if _, err := m.command(remoteEnvelope("wrong-model", "message.send", sid, map[string]any{"text": "must not send", "modelRevision": revision + 1})); err == nil {
		t.Fatal("incorrect model revision was accepted")
	}
	if _, err := m.command(remoteEnvelope("send-next", "message.send", sid, map[string]any{"text": "next", "modelRevision": revision})); err != nil {
		t.Fatal(err)
	}
	if r := waitRemote(t, m, "send-next"); r.Status != "providerAccepted" {
		t.Fatal(r)
	}
}

func TestRemoteClaudeStopKeepsNativeSessionForExplicitResume(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	id := createRemote(t, m, cwd)
	m.mu.Lock()
	m.sessions[id].Agent = "claude-cli"
	m.sessions[id].State = "running"
	m.mu.Unlock()
	if _, err := m.command(remoteEnvelope("stop-claude", "run.stop", id, map[string]any{})); err != nil {
		t.Fatal(err)
	}
	r := waitRemote(t, m, "stop-claude")
	m.mu.Lock()
	defer m.mu.Unlock()
	if s := m.sessions[id]; r.Status != "providerAccepted" || s.State != "stopped" || s.NativeID == "" || m.providers[id] != nil || p.stops != 2 {
		t.Fatalf("interrupt did not close and preserve the original session: %+v %s stops=%d", r, s.State, p.stops)
	}
}

func TestRemoteProviderOutputRedactsCredentialsAcrossDeltas(t *testing.T) {
	m, _, _ := remoteFixture(t)
	const id = "stream-fixture"
	const generation = "fixture-generation"
	const fragment = "nn_stream_fixture_token"
	const credential = fragment + "_only"
	m.mu.Lock()
	m.generations[id] = generation
	m.sessions[id] = &remoteSession{ID: id, State: "running", CreatedAt: remoteNow(), UpdatedAt: remoteNow()}
	m.mu.Unlock()

	check := func(stage string, raw []byte, want string) {
		t.Helper()
		if strings.Contains(string(raw), fragment) || !strings.Contains(string(raw), want) {
			t.Fatalf("%s did not preserve safe output while withholding the credential", stage)
		}
	}
	readJournal := func() []byte {
		t.Helper()
		raw, err := os.ReadFile(m.path)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	readSnapshot := func() []byte {
		t.Helper()
		raw, err := json.Marshal(m.snapshot())
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	m.providerEvent(id, generation, remoteProviderEvent{Type: "output", Text: "before " + fragment})
	check("live snapshot while output is pending", readSnapshot(), "before ")
	check("journal while output is pending", readJournal(), "before ")
	m.providerEvent(id, generation, remoteProviderEvent{Type: "output", Text: credential[len(fragment):] + " after Bear"})
	m.providerEvent(id, generation, remoteProviderEvent{Type: "output", Text: "er synthetic-local-reference done"})
	check("live snapshot", readSnapshot(), "before [redacted] after [redacted] done")
	m.providerEvent(id, generation, remoteProviderEvent{Type: "state", State: "idle"})

	m.mu.Lock()
	limit := 10
	receipt, err := m.historyPageReceiptLocked(remoteCommandEnvelope{ExpectedHostEpoch: m.epoch, OperationID: "read-fixture", SessionID: id}, "read-fixture", remoteHistoryPageRequest{Limit: &limit})
	m.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(receipt.Result)
	if err != nil {
		t.Fatal(err)
	}
	check("history receipt", raw, "before [redacted] after [redacted] done")
	check("journal", readJournal(), "before [redacted] after [redacted] done")
}

func TestRemoteOutputRedactionPreservesSafeTextAtEveryBoundary(t *testing.T) {
	for _, input := range []string{
		"中文正常文本 and harmless words",
		"open nn_synthetic_local_reference close",
		"open bEaReR \t synthetic-local-fixture close",
		"open PASSWORD=fixture-only&next",
		"open api_key=fixture-only\" ending",
		"open nn_syntheticReferenceBearer another-token-reference end",
		"open nref_synthetic_local_reference",
		"t",
	} {
		want := remoteRedact(input)
		for split := 0; split <= len(input); split++ {
			var session remoteSession
			session.appendNativeOutput(input[:split], false)
			if len(session.Messages) > 0 && !strings.HasPrefix(want, session.Messages[0].Text) {
				t.Fatalf("split %d published output inconsistent with complete redaction", split)
			}
			session.appendNativeOutput(input[split:], false)
			session.appendNativeOutput("", true)
			if len(session.Messages) != 1 || session.Messages[0].Text != want {
				t.Fatalf("split %d did not preserve the expected redacted text", split)
			}
		}
	}
}

func TestRemoteOutputDropsAnUnboundedCredentialTail(t *testing.T) {
	var session remoteSession
	session.appendNativeOutput("safe text ", false)
	session.appendNativeOutput("token="+strings.Repeat("x", remoteOutputLimit), false)
	session.appendNativeOutput("continued synthetic content", false)
	session.appendNativeOutput("", true)
	if len(session.Messages) != 1 || session.Messages[0].Text != "safe text " || !session.Messages[0].Truncated {
		t.Fatal("oversized pending output must be withheld and marked truncated")
	}
	if session.outputTail != "" || session.outputTruncated {
		t.Fatal("terminal event left pending stream state")
	}
	session.Messages = append(session.Messages, remoteMessage{Role: "user", Text: "next turn"})
	session.appendNativeOutput("next reply", true)
	if got := session.Messages[len(session.Messages)-1].Text; got != "next reply" {
		t.Fatal("previous truncation suppressed next turn")
	}
}

func TestRemoteLoadRedactsPreviouslyStoredAssistantOutput(t *testing.T) {
	m, _, _ := remoteFixture(t)
	const credential = "nref_legacy_fixture_reference_only"
	m.mu.Lock()
	m.sessions["legacy-live"] = &remoteSession{ID: "legacy-live", State: "running", UpdatedAt: remoteNow(), Messages: []remoteMessage{
		{Role: "assistant", Text: "one " + credential},
		{Role: "user", Text: "user message"},
	}}
	m.history = []*remoteSession{{ID: "legacy-archive", Messages: []remoteMessage{{Role: "assistant", Text: "two " + credential}}}}
	err := m.saveLocked()
	m.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	reloaded := &remoteManager{
		owner: m.owner, home: m.home, path: m.path, uid: m.uid,
		sessions: map[string]*remoteSession{}, operations: map[string]*remoteReceipt{},
	}
	if err := reloaded.load(); err != nil {
		t.Fatal(err)
	}
	live := reloaded.sessions["legacy-live"].Messages
	if live[0].Text != "one [redacted]" || live[1].Text != "user message" || reloaded.history[0].Messages[0].Text != "two [redacted]" {
		t.Fatal("loading legacy state did not sanitize assistant output")
	}
}

func TestRemoteOutputKeepsOnlyCompleteUTF8WithinLimit(t *testing.T) {
	var session remoteSession
	head := strings.Repeat("x", remoteOutputLimit-1)
	session.appendNativeOutput(head+"字", false)
	if message := session.Messages[0]; message.Text != head || !message.Truncated {
		t.Fatal("output limit split a UTF-8 code point or lost the truncation marker")
	}
}
