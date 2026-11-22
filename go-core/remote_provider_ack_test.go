package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestClaudeAcknowledgmentUsesOperationDeadline(t *testing.T) {
	for _, budget := range []time.Duration{30 * time.Second, 45 * time.Second, time.Second} {
		t.Run(budget.String(), func(t *testing.T) {
			parent, stop := context.WithTimeout(context.Background(), budget)
			defer stop()
			ack, cancel := claudeAcknowledgmentContext(parent)
			defer cancel()
			want, _ := parent.Deadline()
			got, bounded := ack.Deadline()
			if !bounded || !got.Equal(want) {
				t.Fatalf("ack deadline changed operation budget: got=%v want=%v", got, want)
			}
			stop()
			if !errors.Is(ack.Err(), context.Canceled) {
				t.Fatal("operation cancellation was not retained", ack.Err())
			}
		})
	}
}

func TestClaudeAcknowledgmentWithoutDeadlineIsBounded(t *testing.T) {
	before := time.Now()
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	ack, cancel := claudeAcknowledgmentContext(parent)
	defer cancel()
	deadline, bounded := ack.Deadline()
	if !bounded || deadline.Before(before.Add(45*time.Second)) || deadline.After(time.Now().Add(45*time.Second)) {
		t.Fatal("unbounded or incorrect fallback acknowledgment deadline", deadline)
	}
	stop()
	if !errors.Is(ack.Err(), context.Canceled) {
		t.Fatal("fallback ignored parent cancellation", ack.Err())
	}
}

// An in-process stdio sink exposes only the request just written by StartTurn.
// No native CLI, executable shim or model endpoint is used by these tests.
type remoteAckSink struct{ sent chan map[string]any }

func (s *remoteAckSink) Write(raw []byte) (int, error) {
	var message map[string]any
	if err := json.Unmarshal(raw, &message); err != nil {
		return 0, err
	}
	s.sent <- message
	return len(raw), nil
}
func (*remoteAckSink) Close() error { return nil }

func remoteAckFixture(t *testing.T, ctx context.Context) (*remoteNativeProvider, map[string]any, <-chan error, <-chan struct{}) {
	t.Helper()
	sink := &remoteAckSink{sent: make(chan map[string]any, 1)}
	canceled := make(chan struct{}, 1)
	p := &remoteNativeProvider{agent: "claude", nativeID: "fixture-session", pending: map[string]chan remoteNativeMessage{}, approvals: map[string]remoteNativeMessage{}, events: make(chan remoteProviderEvent, 16), done: make(chan struct{}), stdin: sink, cancel: func() {
		select {
		case canceled <- struct{}{}:
		default:
		}
	}}
	finished := make(chan error, 1)
	go func() { finished <- p.StartTurn(ctx, remoteProviderInput{Text: "offline acknowledgment fixture"}) }()
	select {
	case message := <-sink.sent:
		return p, message, finished, canceled
	case <-time.After(time.Second):
		t.Fatal("StartTurn did not write native input")
		return nil, nil, nil, nil
	}
}

func remoteAckMessage(p *remoteNativeProvider, message map[string]any) {
	raw, _ := json.Marshal(message)
	p.claudeMessage(remoteNativeMessage{Type: message["type"].(string), Raw: raw})
}

func remoteAckPending(t *testing.T, p *remoteNativeProvider, id string, want bool) {
	t.Helper()
	p.mu.Lock()
	_, got := p.pending[id]
	p.mu.Unlock()
	if got != want {
		t.Fatalf("pending acknowledgment exists=%v want=%v", got, want)
	}
}

func TestClaudeAcknowledgmentWaitsForMatchingReplayAfterOutputAndTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p, message, finished, _ := remoteAckFixture(t, ctx)
	id := message["uuid"].(string)
	remoteAckMessage(p, map[string]any{"type": "user", "uuid": "wrong-uuid", "session_id": p.nativeID})
	remoteAckMessage(p, map[string]any{"type": "user", "uuid": id, "session_id": p.nativeID, "parent_tool_use_id": "child-tool"})
	remoteAckMessage(p, map[string]any{"type": "stream_event", "session_id": p.nativeID, "event": map[string]any{"delta": map[string]any{"text": "NEWNOA_REMOTE_OK"}}})
	remoteAckMessage(p, map[string]any{"type": "result", "session_id": p.nativeID, "is_error": false})
	select {
	case err := <-finished:
		t.Fatal("output, terminal, wrong UUID or child replay incorrectly acknowledged input", err)
	default:
	}
	remoteAckPending(t, p, id, true)
	// The separate deadline tests cover >15s operation budgets without sleeping.
	// Replay remains mandatory even after output and a terminal result arrive.
	remoteAckMessage(p, map[string]any{"type": "user", "uuid": id, "session_id": p.nativeID})
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("matching late replay was not acknowledged")
	}
	remoteAckPending(t, p, id, false)
}

func TestClaudeAcknowledgmentCancellationDeadlineAndProcessExit(t *testing.T) {
	for _, mode := range []string{"cancel", "no-ack-expiry", "process-exit", "wrong-session"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			p, message, finished, canceled := remoteAckFixture(t, ctx)
			id := message["uuid"].(string)
			want := context.Canceled
			switch mode {
			case "cancel":
				cancel()
			case "no-ack-expiry":
				remoteAckMessage(p, map[string]any{"type": "user", "uuid": "wrong-uuid", "session_id": p.nativeID})
				want = context.DeadlineExceeded
			case "process-exit":
				close(p.done)
			case "wrong-session":
				remoteAckMessage(p, map[string]any{"type": "user", "uuid": id, "session_id": "other-session"})
				select {
				case <-canceled:
				default:
					t.Fatal("wrong session did not cancel native process")
				}
				remoteAckPending(t, p, id, true)
				close(p.done)
			}
			select {
			case err := <-finished:
				if mode == "process-exit" || mode == "wrong-session" {
					if err == nil || !strings.Contains(err.Error(), "closed before message acknowledgment") {
						t.Fatal("native exit was accepted as acknowledgment", err)
					}
				} else if !errors.Is(err, want) || !strings.Contains(err.Error(), "native message acknowledgment:") {
					t.Fatal("operation boundary was not preserved", err)
				}
			case <-time.After(time.Second):
				t.Fatal("acknowledgment wait hung past its operation boundary")
			}
			remoteAckPending(t, p, id, false)
		})
	}
}

var _ io.WriteCloser = (*remoteAckSink)(nil)
