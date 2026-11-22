package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestRemoteACPTurnWaitsForCorrelatedNativeEvidenceAndTerminal(t *testing.T) {
	p := &remoteNativeProvider{nativeID: "native-a", runID: "turn-1", events: make(chan remoteProviderEvent, 16)}
	p.acp = &remoteACPState{requestID: "7", operationID: "send-1", accepted: make(chan error, 1), done: make(chan string, 1)}
	feed := func(raw string) {
		var m remoteNativeMessage
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatal(err)
		}
		p.acpMessage(m)
	}
	feed(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"other","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"wrong session"}}}}`)
	feed(`{"jsonrpc":"2.0","id":8,"result":{"stopReason":"end_turn"}}`)
	select {
	case <-p.events:
		t.Fatal("unrelated event changed the turn")
	default:
	}
	turn := p.acp
	feed(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"native-a","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"reply"}}}}`)
	want := []struct{ kind, value string }{{"dispatch", "providerAccepted"}, {"output", "reply"}}
	for _, w := range want {
		select {
		case e := <-p.events:
			if e.Type != w.kind || (e.Delivery != w.value && e.Text != w.value) {
				t.Fatalf("event: %+v", e)
			}
		case <-time.After(time.Second):
			t.Fatal("missing event", w.kind)
		}
	}
	if err := <-turn.accepted; err != nil {
		t.Fatal(err)
	}
	if p.runID != "turn-1" {
		t.Fatal("streaming update settled turn early")
	}
	feed(`{"jsonrpc":"2.0","id":7,"result":{"stopReason":"cancelled"}}`)
	if state := <-turn.done; state != "stopped" {
		t.Fatalf("terminal: %s", state)
	}
	e := <-p.events
	if e.State != "stopped" || p.runID != "" {
		t.Fatalf("did not stop: %+v", e)
	}
}

func TestRemoteACPRefusalIsNotAcceptedAndFailsTurn(t *testing.T) {
	p := &remoteNativeProvider{nativeID: "native-a", runID: "turn-1", events: make(chan remoteProviderEvent, 16)}
	turn := &remoteACPState{requestID: "1", operationID: "send-1", accepted: make(chan error, 1), done: make(chan string, 1)}
	p.acp = turn
	p.acpMessage(remoteNativeMessage{ID: json.RawMessage(`1`), Result: json.RawMessage(`{"stopReason":"refusal"}`)})
	if <-turn.accepted == nil {
		t.Fatal("refusal must not acknowledge delivery")
	}
	if state := <-turn.done; state != "failed" {
		t.Fatalf("terminal: %s", state)
	}
	e := <-p.events
	if e.Type != "error" || e.State != "failed" {
		t.Fatalf("missing native rejection: %+v", e)
	}
}

func TestRemoteACPInitializeResumesWithoutReplayingAndRejectsReplacement(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			remoteProtocolExecutable(t, "openclaw", map[bool]string{false: "ok", true: "missing"}[replacement])
			binary, err := exec.LookPath("openclaw")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			events := make(chan remoteProviderEvent, 8)
			p, err := startRemoteProviderCommand(ctx, "openclaw", t.TempDir(), "original", func(e remoteProviderEvent) { events <- e }, binary, []string{"fixture-acp"}, nil)
			if replacement {
				if err == nil {
					_ = p.Close()
					t.Fatal("resumed a missing session")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if err = p.StartTurn(ctx, remoteProviderInput{Text: "test turn", OperationID: "send"}); err != nil {
				t.Fatal(err)
			}
			see := false
			for !see {
				select {
				case e := <-events:
					if e.Type == "output" {
						if e.Text != "fixture reply" {
							t.Fatalf("unexpected text %q", e.Text)
						}
						see = true
					}
				case <-ctx.Done():
					t.Fatal("missing output")
				}
			}
		})
	}
}

func TestRemoteOwnedGatewayDoesNotExposeProcessRecord(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	id := createRemote(t, m, cwd)
	m.mu.Lock()
	m.sessions[id].OwnedGateway = &remoteOwnedProcess{PID: 2, Identity: "private-owner"}
	m.mu.Unlock()
	if raw, _ := json.Marshal(m.snapshot()); strings.Contains(string(raw), "private-owner") || strings.Contains(string(raw), "ownedGateway") {
		t.Fatal("Gateway process metadata leaked")
	}
	m.mu.Lock()
	m.sessions[id].OwnedGateway = nil
	m.mu.Unlock()
}

func TestRemoteACPProbeAcknowledgmentUsesProbeDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	wait, finish := remoteACPAcknowledgmentContext(ctx, true)
	defer finish()
	a, _ := ctx.Deadline()
	b, _ := wait.Deadline()
	if !a.Equal(b) {
		t.Fatal("native startup allowance incorrectly truncated authorization probe")
	}
	normal, finishNormal := remoteACPAcknowledgmentContext(ctx, false)
	defer finishNormal()
	c, _ := normal.Deadline()
	if !c.Before(a) {
		t.Fatal("normal dispatch held the command worker for the whole turn")
	}
}

func TestRemoteOpenClawEmptyTerminalKeepsReceiptButFailsTurn(t *testing.T) {
	for _, reason := range []string{"end_turn", "max_tokens", "max_turn_requests", "cancelled"} {
		t.Run(reason, func(t *testing.T) {
			p := &remoteNativeProvider{agent: "openclaw", nativeID: "abc", runID: "r", events: make(chan remoteProviderEvent, 8)}
			turn := &remoteACPState{requestID: "4", operationID: "sent", accepted: make(chan error, 1), done: make(chan string, 1)}
			p.acp = turn
			// Native typed transcript replay is not a reply to the current prompt.
			p.acpMessage(remoteNativeMessage{Method: "session/update", Params: json.RawMessage(`{"sessionId":"abc","update":{"sessionUpdate":"user_message_chunk","content":{"type":"text","text":"prompt"}}}`)})
			p.acpMessage(remoteNativeMessage{ID: json.RawMessage(`4`), Result: json.RawMessage(fmt.Sprintf(`{"stopReason":%q}`, reason))})
			if err := <-turn.accepted; err != nil {
				t.Fatalf("correlated native terminal proves delivery: %v", err)
			}
			if e := <-p.events; e.Type != "dispatch" || e.OperationID != "sent" || e.Delivery != "providerAccepted" {
				t.Fatalf("wrong delivery: %+v", e)
			}
			if reason == "cancelled" {
				if e := <-p.events; e.State != "stopped" || <-turn.done != "stopped" {
					t.Fatalf("cancellation changed: %+v", e)
				}
			} else if e := <-p.events; e.Type != "error" || e.State != "failed" || e.Error == "" || <-turn.done != "failed" {
				t.Fatalf("native end without text was presented as successful: %+v", e)
			}
			if p.runID != "" || p.acp != nil {
				t.Fatal("terminal kept the provider busy")
			}
		})
	}
}
