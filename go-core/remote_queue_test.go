package main

import (
	"context"
	"testing"
	"time"
)

func TestRemoteDiscardedQueuedImageDoesNotReachProvider(t *testing.T) {
	m, a, _ := remoteReadFixture(t)
	if _, err := m.command(remoteEnvelope("running-before-discard", "message.send", a.SessionID, map[string]any{"text": "work"})); err != nil {
		t.Fatal(err)
	}
	waitRemote(t, m, "running-before-discard")
	entered, release := make(chan struct{}), make(chan struct{})
	_, err := m.commandWithAuthorization(remoteEnvelope("discard-image", "attachment.discard", a.SessionID, map[string]any{"attachmentId": a.ID}), func(context.Context) error { close(entered); <-release; return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	if _, err = m.command(remoteEnvelope("send-discarded-image", "message.send", a.SessionID, map[string]any{"text": "image", "attachments": []string{a.ID}})); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if r := waitRemote(t, m, "send-discarded-image"); r.Status != "rejected" {
		t.Fatalf("discarded image accepted: %+v", r)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sessions[a.SessionID].State != "running" {
		t.Fatal("pre-execution rejection lost current run state")
	}
}

func TestRemoteQueuedSteerNeverBecomesNewTurn(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	_, err := m.command(remoteEnvelope("first-turn", "message.send", sid, map[string]any{"text": "work"}))
	if err != nil {
		t.Fatal(err)
	}
	waitRemote(t, m, "first-turn")
	entered := make(chan struct{})
	release := make(chan struct{})
	_, err = m.commandWithAuthorization(remoteEnvelope("late-steer", "message.send", sid, map[string]any{"text": "adjust"}), func(context.Context) error { close(entered); <-release; return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	p.emit(remoteProviderEvent{Type: "state", State: "idle"})
	close(release)
	r := waitRemote(t, m, "late-steer")
	if r.Status != "rejected" {
		t.Fatal(r)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sends != 1 || p.steers != 0 {
		t.Fatalf("late steer turned into native input %d/%d", p.sends, p.steers)
	}
}
func TestRemoteApprovalResolutionTargetsOnlyNativeRequest(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	for _, id := range []string{"a", "b"} {
		p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: id, Options: []string{"accept"}}})
	}
	p.emit(remoteProviderEvent{Type: "approvalResolved", Text: "a"})
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[sid]
	if s.Approvals[0].State != "resolved" || s.Approvals[1].State != "open" || s.State != "awaitingApproval" {
		t.Fatal("unrelated approval was consumed")
	}
}
func TestRemoteSecondQueuedResumeRejected(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	m.mu.Lock()
	delete(m.providers, sid)
	m.sessions[sid].State = "stopped"
	m.mu.Unlock()
	entered := make(chan struct{})
	release := make(chan struct{})
	m.start = func(_ context.Context, _, _, _ string, emit func(remoteProviderEvent)) (remoteProvider, error) {
		close(entered)
		<-release
		p.emit = emit
		return p, nil
	}
	if _, err := m.command(remoteEnvelope("resume-a", "session.resume", sid, map[string]any{})); err != nil {
		t.Fatal(err)
	}
	<-entered
	if _, err := m.command(remoteEnvelope("resume-b", "session.resume", sid, map[string]any{})); err == nil {
		t.Fatal("duplicate pending resume admitted")
	}
	close(release)
	waitRemote(t, m, "resume-a")
}
func TestRemoteAttachmentQuotaCheckedWhenQueuedPrepareExecutes(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	entered := make(chan struct{})
	release := make(chan struct{})
	_, err := m.commandWithAuthorization(remoteEnvelope("prepare-full", "attachment.prepare", sid, map[string]any{"name": "a.png", "mime": "image/png", "size": 10}), func(context.Context) error { close(entered); <-release; return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	m.mu.Lock()
	for i := 0; i < 32; i++ {
		m.attachments[randomToken()] = &remoteAttachment{ID: randomToken(), CreatedAt: time.Now()}
	}
	m.mu.Unlock()
	close(release)
	r := waitRemote(t, m, "prepare-full")
	if r.Status != "rejected" {
		t.Fatal("queued quota bypass", r)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.attachments) != 32 {
		t.Fatal("attachment count exceeds bound")
	}
}
