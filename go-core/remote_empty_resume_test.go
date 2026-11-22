package main

import (
	"context"
	"testing"
)

func TestRemoteResumeOnlyRecreatesNeverUsedNativeSessions(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli", "pi-agent", "hermes-agent"} {
		for _, history := range []string{"empty", "message", "run", "attempt"} {
			t.Run(agent+"/"+history, func(t *testing.T) {
				m, provider, cwd := remoteFixture(t)
				id := createRemote(t, m, cwd)
				m.mu.Lock()
				s := m.sessions[id]
				s.Agent, s.State, s.NativeID = agent, "failed", "original-native"
				s.Error = "Previous startup failed"
				m.executors[agent] = &remoteExecutor{Agent: agent, Supported: true, Authorized: true, State: "ready", Capabilities: map[string]bool{"resume": true}}
				delete(m.providers, id)
				switch history {
				case "message":
					s.Messages = []remoteMessage{{Role: "user", Text: "keep my conversation"}}
				case "run":
					s.RunID = "prior-run"
				case "attempt":
					s.AttemptID = "prior-attempt"
				}
				m.mu.Unlock()
				chosen := make(chan string, 1)
				m.start = func(_ context.Context, _, _, native string, emit func(remoteProviderEvent)) (remoteProvider, error) {
					chosen <- native
					provider.emit = emit
					if native == "" {
						native = "new-empty-native"
					}
					emit(remoteProviderEvent{Type: "session", NativeID: native})
					return provider, nil
				}
				if _, err := m.command(remoteEnvelope("resume-empty", "session.resume", id, map[string]any{})); err != nil {
					t.Fatal(err)
				}
				if receipt := waitRemote(t, m, "resume-empty"); receipt.Status != "providerAccepted" || receipt.SessionID != id {
					t.Fatalf("resume failed: %+v", receipt)
				}
				m.mu.Lock()
				staleError := m.sessions[id].Error
				m.mu.Unlock()
				if staleError != "" {
					t.Fatalf("successful recovery retained obsolete error: %q", staleError)
				}
				want := "original-native"
				if history == "empty" && agent != "hermes-agent" {
					want = ""
				}
				if got := <-chosen; got != want {
					t.Fatalf("native resume=%q want=%q", got, want)
				}
			})
		}
	}
}
