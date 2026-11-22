package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNativeClaudeExplicitResultIdentityAcknowledges(t *testing.T) {
	for _, field := range []string{"user_message_uuid", "user_message_uuids"} {
		t.Run(field, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			p, message, finished, _ := remoteAckFixture(t, ctx)
			id := message["uuid"].(string)
			r := map[string]any{"type": "result", "session_id": p.nativeID, "uuid": "result-own-id", "is_error": false}
			if field == "user_message_uuid" {
				r[field] = id
			} else {
				r[field] = []string{id}
			}
			remoteAckMessage(p, r)
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal("explicit identity did not acknowledge", err)
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatal("exact result correlation ignored")
			}
		})
	}
}

type nativeBlockedWriter struct {
	entered, release chan struct{}
	once             sync.Once
	mu               sync.Mutex
	data             bytes.Buffer
}

func (w *nativeBlockedWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.data.Write(b)
}
func (w *nativeBlockedWriter) Close() error { return nil }
func TestNativeApprovalReusedIDSurvivesOldWrite(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	m.mu.Lock()
	gen := m.generations[sid]
	m.mu.Unlock()
	p, _ := remoteProtocolParser("claude")
	w := &nativeBlockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	p.stdin = w
	register := func(path string) {
		raw := []byte(`{"type":"control_request","request_id":"reused","request":{"subtype":"can_use_tool","tool_name":"Read","input":{"path":"` + path + `"}}}`)
		var msg remoteNativeMessage
		_ = json.Unmarshal(raw, &msg)
		msg.Raw = raw
		p.claudeMessage(msg)
		m.providerEvent(sid, gen, <-p.events)
	}
	register("first")
	done := make(chan error, 1)
	go func() {
		done <- p.Respond(context.Background(), remoteProviderReply{RequestID: "reused", Decision: "allow"})
	}()
	<-w.entered
	register("second")
	close(w.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	m.providerEvent(sid, gen, <-p.events)
	p.mu.Lock()
	_, pending := p.approvals["reused"]
	p.mu.Unlock()
	if !pending {
		t.Fatal("old response deleted replacement native approval")
	}
	m.mu.Lock()
	state := m.sessions[sid].Approvals[1].State
	m.mu.Unlock()
	if state != "open" {
		t.Fatal("old response resolved replacement public approval")
	}
	if err := p.Respond(context.Background(), remoteProviderReply{RequestID: "reused", Decision: "deny"}); err != nil {
		t.Fatal("replacement no longer answerable", err)
	}
}

func TestNativeApprovalCancellationWinsBeforeWrite(t *testing.T) {
	p, sink := remoteProtocolParser("claude")
	p.approvals["approval"] = remoteNativeMessage{Type: "control_request", RequestID: "approval", Request: json.RawMessage(`{"subtype":"can_use_tool","tool_name":"Read","input":{"path":"fixture"}}`)}
	p.writeMu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- p.Respond(context.Background(), remoteProviderReply{RequestID: "approval", Decision: "allow"})
	}()
	// Acquire replyMu after Respond owns it, so it has passed initial lookup and is
	// waiting behind writeMu. No sleep-based interleaving is required by the fix.
	deadline := time.Now().Add(time.Second)
	for {
		p.mu.Lock()
		claimed := p.approvalClaims["approval"] != nil
		p.mu.Unlock()
		if claimed {
			break
		}
		if time.Now().After(deadline) {
			p.writeMu.Unlock()
			t.Fatal("response did not enter")
		}
		time.Sleep(time.Millisecond)
	}
	p.claudeMessage(remoteNativeMessage{Type: "control_cancel_request", RequestID: "approval", Raw: json.RawMessage(`{"type":"control_cancel_request","request_id":"approval"}`)})
	p.writeMu.Unlock()
	if err := <-done; err == nil {
		t.Fatal("cancelled approval was sent")
	}
	if strings.Contains(sink.String(), "control_response") {
		t.Fatal("cancelled approval crossed writer")
	}
}

type remoteShortAckProvider struct{ *remoteNativeProvider }

func (p remoteShortAckProvider) StartTurn(ctx context.Context, in remoteProviderInput) error {
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	return p.remoteNativeProvider.StartTurn(short, in)
}

func TestNativeManagerUnknownThenExactLateClaudeReceipt(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	sink := &remoteAckSink{sent: make(chan map[string]any, 4)}
	p := &remoteNativeProvider{agent: "claude", nativeID: "native-session", pending: map[string]chan remoteNativeMessage{}, approvals: map[string]remoteNativeMessage{}, events: make(chan remoteProviderEvent, 32), done: make(chan struct{}), stdin: sink, cancel: func() {}}
	m.mu.Lock()
	generation := m.generations[sid]
	m.providers[sid] = remoteShortAckProvider{p}
	m.mu.Unlock()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case e := <-p.events:
				m.providerEvent(sid, generation, e)
			case <-stop:
				return
			}
		}
	})
	t.Cleanup(func() { close(stop); wg.Wait(); close(p.done) })
	if _, err := m.command(remoteEnvelope("original-send", "message.send", sid, map[string]any{"text": "synthetic"})); err != nil {
		t.Fatal(err)
	}
	message := <-sink.sent
	if r := waitRemote(t, m, "original-send"); r.Status != "unknown" {
		t.Fatalf("written timeout falsely settled %s", r.Status)
	}
	remoteAckMessage(p, map[string]any{"type": "result", "session_id": p.nativeID, "uuid": "generic-result", "is_error": false})
	m.mu.Lock()
	m.operations["unrelated-send"] = &remoteReceipt{OperationID: "unrelated-send", SessionID: sid, Status: "unknown"}
	m.mu.Unlock()
	remoteAckMessage(p, map[string]any{"type": "user", "session_id": p.nativeID, "uuid": message["uuid"]})
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		status := m.operations["original-send"].Status
		other := m.operations["unrelated-send"].Status
		m.mu.Unlock()
		if other != "unknown" {
			t.Fatal("late ack consumed another operation")
		}
		if status == "providerAccepted" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("late exact ack never settled original")
		}
		time.Sleep(time.Millisecond)
	}
	// The already observed terminal must not be reopened by a late replay.
	p.mu.Lock()
	run := p.runID
	p.mu.Unlock()
	if run != "" {
		t.Fatal("late replay reopened ended turn")
	}
}

func TestNativeManagerMalformedCodexReplyThenLateExactSuccess(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	sink := &remoteAckSink{sent: make(chan map[string]any, 4)}
	p := &remoteNativeProvider{agent: "codex", nativeID: "native-session", pending: map[string]chan remoteNativeMessage{}, approvals: map[string]remoteNativeMessage{}, completedRuns: map[string]bool{}, events: make(chan remoteProviderEvent, 32), done: make(chan struct{}), stdin: sink, cancel: func() {}}
	m.mu.Lock()
	generation := m.generations[sid]
	m.providers[sid] = remoteShortAckProvider{p}
	m.mu.Unlock()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case e := <-p.events:
				m.providerEvent(sid, generation, e)
			case <-stop:
				return
			}
		}
	})
	t.Cleanup(func() { close(stop); wg.Wait(); close(p.done) })
	if _, err := m.command(remoteEnvelope("codex-malformed", "message.send", sid, map[string]any{"text": "synthetic"})); err != nil {
		t.Fatal(err)
	}
	msg := <-sink.sent
	id, _ := json.Marshal(msg["id"])
	p.codexMessage(remoteNativeMessage{ID: id, Result: json.RawMessage(`{}`)})
	if r := waitRemote(t, m, "codex-malformed"); r.Status != "unknown" {
		t.Fatal("malformed postwrite response falsely rejected", r.Status)
	}
	m.providerEvent(sid, "old-generation", remoteProviderEvent{Type: "dispatch", OperationID: "codex-malformed", Delivery: "providerAccepted"})
	m.mu.Lock()
	status := m.operations["codex-malformed"].Status
	m.mu.Unlock()
	if status != "unknown" {
		t.Fatal("old generation settled receipt")
	}
	p.codexMessage(remoteNativeMessage{ID: id, Result: json.RawMessage(`{"turn":{"id":"recovered-run"}}`)})
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		status := m.operations["codex-malformed"].Status
		m.mu.Unlock()
		if status == "providerAccepted" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("late valid RPC did not settle original")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNativeDispatchBoundsExpiryAndDuplicateAck(t *testing.T) {
	p, _ := remoteProtocolParser("claude")
	p.mu.Lock()
	for i := range remoteMaxNativeDispatches {
		if err := p.armDispatchLocked(fmt.Sprint(i), "original", "user"); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.armDispatchLocked("overflow", "other", "user"); err == nil {
		t.Fatal("unknown dispatch silently evicted")
	}
	// A native reply can arrive near the end of the API-authorized execution.
	p.pruneDispatchesLocked(time.Now().Add(maximumExecutionTimeout))
	if len(p.dispatches) != remoteMaxNativeDispatches {
		t.Fatal("native operation identities expired during permitted inference")
	}
	old := p.dispatches["0"]
	old.expires = time.Now().Add(-time.Second)
	p.dispatches["0"] = old
	if p.settleDispatchLocked("0", "providerAccepted") {
		t.Fatal("expired identity accepted")
	}
	if err := p.armDispatchLocked("new", "new-operation", "user"); err != nil {
		t.Fatal("expiry did not free bounded slot")
	}
	if !p.settleDispatchLocked("new", "providerAccepted") || p.settleDispatchLocked("new", "providerAccepted") {
		t.Fatal("ack not exactly once")
	}
	p.mu.Unlock()
}

type remoteUnwrittenProvider struct{ *remoteTestProvider }

func (p remoteUnwrittenProvider) StartTurn(context.Context, remoteProviderInput) error {
	return context.Canceled
}
func TestNativeManagerProvablyUnwrittenStaysRejectedIdle(t *testing.T) {
	m, p, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	m.mu.Lock()
	m.providers[sid] = remoteUnwrittenProvider{p}
	m.mu.Unlock()
	if _, err := m.command(remoteEnvelope("unwritten-send", "message.send", sid, map[string]any{"text": "synthetic"})); err != nil {
		t.Fatal(err)
	}
	if r := waitRemote(t, m, "unwritten-send"); r.Status != "rejected" {
		t.Fatal("unwritten request became unknown")
	}
	m.mu.Lock()
	state := m.sessions[sid].State
	m.mu.Unlock()
	if state != "idle" {
		t.Fatal("unwritten request changed idle to", state)
	}
}

func TestNativeCodexLateRPCIdentityAndClientID(t *testing.T) {
	p, _ := remoteProtocolParser("codex")
	p.runID = ""
	sink := &remoteAckSink{sent: make(chan map[string]any, 4)}
	p.stdin = sink
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- p.StartTurn(ctx, remoteProviderInput{OperationID: "codex-original", Text: "synthetic"})
	}()
	msg := <-sink.sent
	params := msg["params"].(map[string]any)
	if params["clientUserMessageId"] != "codex-original" {
		t.Fatal("stable client message identity omitted")
	}
	if err := <-done; !isRemoteDispatchUnknown(err) {
		t.Fatal("written codex timeout not unknown", err)
	}
	remoteProtocolCodexMessage(p, `"foreign"`, "", `{}`)
	remoteProtocolCodexMessage(p, `"`+msg["id"].(string)+`"`, "", `{}`)
	// A response needs the original RPC ID and a structurally valid turn/start result.
	for len(p.events) > 0 {
		if e := <-p.events; e.Type == "dispatch" {
			t.Fatal("uncorrelated or malformed reply accepted")
		}
	}
	id, _ := json.Marshal(msg["id"])
	p.codexMessage(remoteNativeMessage{ID: id, Result: json.RawMessage(`{"turn":{"id":"late-run"}}`)})
	found := false
	for len(p.events) > 0 {
		e := <-p.events
		if e.Type == "dispatch" {
			if e.OperationID != "codex-original" || e.Delivery != "providerAccepted" {
				t.Fatal("wrong codex operation")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("late RPC was forgotten")
	}
}
