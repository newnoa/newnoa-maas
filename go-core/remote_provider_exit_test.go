package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestRemoteProviderReportsExitedAfterNativeProcessTerminates(t *testing.T) {
	if os.Getenv("NEWNOA_REMOTE_EXIT_FAKE") == "1" {
		decoder, encoder := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
		for {
			var message struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			if decoder.Decode(&message) != nil {
				return
			}
			switch message.Method {
			case "initialize":
				_ = encoder.Encode(map[string]any{"id": message.ID, "result": map[string]any{}})
			case "thread/start":
				_ = encoder.Encode(map[string]any{"id": message.ID, "result": map[string]any{"thread": map[string]any{"id": "exit-native-session"}}})
			case "test/exit":
				os.Exit(0)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events := make(chan remoteProviderEvent, 32)
	provider, err := startRemoteProviderCommand(ctx, "codex", t.TempDir(), "", func(event remoteProviderEvent) { events <- event }, os.Args[0], []string{"-test.run=^TestRemoteProviderReportsExitedAfterNativeProcessTerminates$"}, []string{"NEWNOA_REMOTE_EXIT_FAKE=1"})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	native := provider.(*remoteNativeProvider)
	if err = native.write(map[string]any{"method": "test/exit"}); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case event := <-events:
			if event.Type == "error" {
				t.Fatal("native termination was reported as an ordinary turn error")
			}
			if event.Type == "exited" {
				if native.command.ProcessState == nil || !native.command.ProcessState.Exited() {
					t.Fatal("exit event preceded native process termination")
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("native termination never produced an exited event")
		}
	}
}

type remoteExitTestProvider struct {
	*remoteTestProvider
	entered    chan struct{}
	release    chan struct{}
	closeError error
}

func (p *remoteExitTestProvider) Close() error {
	close(p.entered)
	<-p.release
	return p.closeError
}

func remoteExitFixture(t *testing.T) (*remoteManager, *remoteExitTestProvider, string, string) {
	t.Helper()
	m, original, cwd := remoteFixture(t)
	id := createRemote(t, m, cwd)
	p := &remoteExitTestProvider{remoteTestProvider: original, entered: make(chan struct{}), release: make(chan struct{})}
	m.mu.Lock()
	m.providers[id] = p
	m.sessions[id].OwnedProcess = remoteOwnedProcess{PID: 12345, Identity: "retained-owner", Executable: "native"}
	generation := m.generations[id]
	m.mu.Unlock()
	t.Cleanup(func() {
		select {
		case <-p.release:
		default:
			close(p.release)
		}
		m.mu.Lock()
		delete(m.providers, id)
		m.sessions[id].OwnedProcess = remoteOwnedProcess{}
		m.sessions[id].State = "stopped"
		m.mu.Unlock()
	})
	return m, p, id, generation
}

func remoteWaitExitCleanup(t *testing.T, p *remoteExitTestProvider) {
	t.Helper()
	select {
	case <-p.entered:
	case <-time.After(time.Second):
		t.Fatal("exited provider cleanup was never started")
	}
}

func TestRemoteExitedProviderDetachesOnlyAfterCleanupAndCanResume(t *testing.T) {
	m, p, id, generation := remoteExitFixture(t)
	m.mu.Lock()
	m.sessions[id].Approvals = []*remoteApproval{{ID: "pending", State: "open"}}
	revision := m.revision
	m.mu.Unlock()
	m.providerEvent(id, generation, remoteProviderEvent{Type: "exited", Error: "native CLI exited unexpectedly"})
	remoteWaitExitCleanup(t, p)
	m.mu.Lock()
	retained := m.providers[id] == p && m.sessions[id].OwnedProcess.PID != 0
	invalidated := m.sessions[id].Approvals[0].State == "invalidated" && m.revision > revision
	m.mu.Unlock()
	if !retained {
		t.Fatal("ownership was discarded before cleanup finished")
	}
	if !invalidated {
		t.Fatal("native exit left an approval open or changed state without a revision")
	}
	close(p.release)
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		detached := m.providers[id] == nil
		state, owned, message := m.sessions[id].State, m.sessions[id].OwnedProcess, m.sessions[id].Error
		m.mu.Unlock()
		if detached {
			if state != "failed" || owned.PID != 0 || !strings.Contains(strings.ToLower(message), "resume") {
				t.Fatalf("exit recovery projection: %s %+v %q", state, owned, message)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("successfully cleaned provider remained attached")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := m.command(remoteEnvelope("resume-after-exit", "session.resume", id, map[string]any{})); err != nil {
		t.Fatal(err)
	}
	if receipt := waitRemote(t, m, "resume-after-exit"); receipt.Status != "providerAccepted" {
		t.Fatal(receipt)
	}
}

func TestRemoteExitedProviderCleanupFailurePreservesOwnership(t *testing.T) {
	m, p, id, generation := remoteExitFixture(t)
	p.closeError = errors.New("owned descendant cleanup failed")
	m.providerEvent(id, generation, remoteProviderEvent{Type: "exited"})
	remoteWaitExitCleanup(t, p)
	close(p.release)
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		s := m.sessions[id]
		finished := strings.Contains(s.Error, "cleanup failed")
		retained := s.State == "unknown" && m.providers[id] == p && s.OwnedProcess.PID == 12345
		m.mu.Unlock()
		if finished && !retained {
			t.Fatal("cleanup failure lost ownership fence")
		}
		if finished {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("provider cleanup failure was not surfaced")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRemoteExitedProviderLateCleanupCannotDetachReplacement(t *testing.T) {
	for _, changeGeneration := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			m, p, id, generation := remoteExitFixture(t)
			m.providerEvent(id, generation, remoteProviderEvent{Type: "exited"})
			remoteWaitExitCleanup(t, p)
			replacement := &remoteTestProvider{}
			m.mu.Lock()
			if changeGeneration {
				m.generations[id] = "replacement-generation"
			}
			m.providers[id] = replacement
			m.sessions[id].State = "idle"
			m.sessions[id].OwnedProcess = remoteOwnedProcess{PID: 54321, Identity: "replacement-owner"}
			m.mu.Unlock()
			close(p.release)
			synctest.Wait()
			m.mu.Lock()
			defer m.mu.Unlock()
			if m.providers[id] != replacement || m.sessions[id].State != "idle" || m.sessions[id].OwnedProcess.PID != 54321 {
				t.Fatal("stale exit cleanup changed replacement ownership")
			}
		})
	}
}

func TestRemoteOrdinaryTurnErrorKeepsProviderAttached(t *testing.T) {
	m, p, id, generation := remoteExitFixture(t)
	m.providerEvent(id, generation, remoteProviderEvent{Type: "error", Error: "turn failed"})
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.providers[id] != p || m.sessions[id].State != "failed" {
		t.Fatal("turn error changed provider ownership")
	}
	select {
	case <-p.entered:
		t.Fatal("ordinary turn error triggered process cleanup")
	default:
	}
}

func TestRemoteExitPersistenceFailureKeepsRecoveryOwnership(t *testing.T) {
	m, p, id, generation := remoteExitFixture(t)
	directory := filepath.Dir(m.path)
	if err := os.Chmod(directory, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(directory, 0700) })
	m.providerEvent(id, generation, remoteProviderEvent{Type: "exited"})
	remoteWaitExitCleanup(t, p)
	close(p.release)
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		finished := m.failure == "Remote exit cleanup persistence failed"
		retained := m.providers[id] == p && m.sessions[id].OwnedProcess.PID == 12345 && m.sessions[id].State == "unknown"
		m.mu.Unlock()
		if finished {
			if !retained {
				t.Fatal("persistence failure discarded the ownership needed to retry cleanup")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("exit cleanup persistence failure was not surfaced")
		}
		time.Sleep(time.Millisecond)
	}
}
