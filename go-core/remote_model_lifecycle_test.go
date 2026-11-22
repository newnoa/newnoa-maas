package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRemoteModelRestoresSameNativeSessionAndReportsOnlyConfirmedModel(t *testing.T) {
	c, m, target, old, p, _, _ := remoteModelFixture(t, "")
	target.Messages = []remoteMessage{{ID: "prior", Role: "user", Text: "keep context"}}
	var resumed *remoteTestProvider
	m.start = func(_ context.Context, agent, cwd, native string, emit func(remoteProviderEvent)) (remoteProvider, error) {
		if agent != target.Agent || cwd != target.Cwd || native != target.NativeID {
			t.Error("model switch lost native context")
		}
		resumed = &remoteTestProvider{emit: emit}
		return resumed, nil
	}
	if err := c.changeRemoteModel(context.Background(), m, target, p); err != nil {
		t.Fatal(err)
	}
	if target.Source != "newnoa" || target.Model != "" || target.State != "idle" || m.providers[target.ID] != resumed || len(target.Messages) != 1 {
		t.Fatal("switch fabricated a model or lost session state")
	}
	old.mu.Lock()
	stops := old.stops
	old.mu.Unlock()
	if stops != 1 {
		t.Fatal("old provider was not stopped exactly once")
	}
	if _, err := os.Stat(c.configPath()); err != nil {
		t.Fatal("authorized configuration not installed", err)
	}
	resumed.emit(remoteProviderEvent{Type: "session", NativeID: target.NativeID, Model: "provider-confirmed"})
	if target.Model != "provider-confirmed" {
		t.Fatal("native actual model was not observed")
	}
}

func TestRemoteModelRejectsInvalidOrUnavailableRecovery(t *testing.T) {
	for _, kind := range []string{"key", "model", "session", "account", "native", "provider", "closed", "proxy", "preview", "close"} {
		t.Run(kind, func(t *testing.T) {
			c, m, target, old, p, _, starts := remoteModelFixture(t, "")
			switch kind {
			case "key":
				p.KeyID = "invalid"
			case "model":
				p.ModelID = "invalid"
			case "session":
				m.session = ""
			case "account":
				m.owner = "other-account"
			case "native":
				target.NativeID = ""
			case "provider":
				delete(m.providers, target.ID)
			case "closed":
				m.closed = true
				defer func() { m.closed = false }()
			case "proxy":
				c.server = nil
			case "preview":
				if err := atomicFile(c.configPath(), []byte("model = [")); err != nil {
					t.Fatal(err)
				}
			case "close":
				old.closeErr = errors.New("cannot verify old process cleanup")
			}
			if err := c.changeRemoteModel(context.Background(), m, target, p); err == nil {
				t.Fatal("invalid model recovery accepted")
			}
			if *starts != 0 {
				t.Fatal("invalid recovery started a new provider")
			}
		})
	}
}

type remoteModelCloseMutation struct {
	*remoteTestProvider
	mutate func()
}

func (p *remoteModelCloseMutation) Close() error { p.mutate(); return p.remoteTestProvider.Close() }

func TestRemoteModelPreservesExternalConfigChangedDuringRestart(t *testing.T) {
	c, m, target, old, p, _, _ := remoteModelFixture(t, "")
	external := []byte("model = \"external-change\"\n")
	m.providers[target.ID] = &remoteModelCloseMutation{remoteTestProvider: old, mutate: func() {
		if err := atomicFile(c.configPath(), external); err != nil {
			t.Error(err)
		}
	}}
	if err := c.changeRemoteModel(context.Background(), m, target, p); err == nil {
		t.Fatal("concurrent config edit was overwritten")
	}
	got, err := os.ReadFile(c.configPath())
	if err != nil || string(got) != string(external) {
		t.Fatal("external config was not preserved")
	}
	if m.providers[target.ID] != nil {
		t.Fatal("closed provider remained attached after config conflict")
	}
}

func TestRemoteModelOwnershipPersistenceFailureRetainsFence(t *testing.T) {
	c, m, target, old, p, _, _ := remoteModelFixture(t, "")
	originalPath := m.path
	m.providers[target.ID] = &remoteModelCloseMutation{remoteTestProvider: old, mutate: func() { m.path = filepath.Join(t.TempDir(), "outside-home", "state.json") }}
	if err := c.changeRemoteModel(context.Background(), m, target, p); err == nil {
		t.Fatal("unpersisted ownership was discarded")
	}
	if m.providers[target.ID] == nil || target.State != "unknown" {
		t.Fatal("persistence failure lost recovery fence")
	}
	m.path = originalPath
}

func TestRemoteModelDoesNotAttachProviderThatExitedDuringRestore(t *testing.T) {
	c, m, target, _, p, _, _ := remoteModelFixture(t, "")
	var replacement *remoteTestProvider
	m.start = func(_ context.Context, _, _, _ string, emit func(remoteProviderEvent)) (remoteProvider, error) {
		replacement = &remoteTestProvider{}
		emit(remoteProviderEvent{Type: "exited"})
		return replacement, nil
	}
	if err := c.changeRemoteModel(context.Background(), m, target, p); err == nil {
		t.Fatal("exited replacement was attached")
	}
	if m.providers[target.ID] != nil || replacement.stops == 0 {
		t.Fatal("failed replacement was not cleaned")
	}
}

func TestRemoteModelFailedReplacementIsDetachedForExplicitRecovery(t *testing.T) {
	c, m, target, _, p, _, _ := remoteModelFixture(t, "")
	m.start = func(context.Context, string, string, string, func(remoteProviderEvent)) (remoteProvider, error) {
		return nil, errors.New("replacement startup failed")
	}
	if err := c.changeRemoteModel(context.Background(), m, target, p); err == nil {
		t.Fatal("failed replacement reported success")
	}
	m.mu.Lock()
	attached := m.providers[target.ID] != nil
	owned := target.OwnedProcess
	m.mu.Unlock()
	if attached || owned.PID != 0 {
		t.Fatal("closed old provider prevents explicit recovery")
	}
}

func TestRemoteProcessBirthRecordRejectsStaleAndMissingOwners(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	m.mu.Lock()
	generation := m.generations[sid]
	m.mu.Unlock()
	ctx := m.providerContext(context.Background(), sid, generation)
	owned := remoteOwnedProcess{PID: 1234, Identity: "test-only-record"}
	if err := remoteRecordOwnedProcess(ctx, owned); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	got := m.sessions[sid].OwnedProcess
	m.generations[sid] = "replacement"
	m.mu.Unlock()
	if got.PID != owned.PID {
		t.Fatal("owned process was not persisted before handshake")
	}
	if err := remoteRecordOwnedProcess(ctx, owned); err == nil {
		t.Fatal("stale generation overwrote ownership")
	}
	m.mu.Lock()
	m.generations[sid] = generation
	original := m.sessions[sid]
	delete(m.sessions, sid)
	m.mu.Unlock()
	if err := remoteRecordOwnedProcess(ctx, owned); err == nil {
		t.Fatal("removed session accepted ownership")
	}
	m.mu.Lock()
	m.sessions[sid] = original
	original.OwnedProcess = remoteOwnedProcess{}
	m.mu.Unlock()
}

func TestRemoteModelRestartsAnUnusedCodexDraftWithoutNativeHistory(t *testing.T) {
	c, m, target, _, p, _, _ := remoteModelFixture(t, "")
	sessionID := target.ID
	m.start = func(_ context.Context, agent, cwd, native string, emit func(remoteProviderEvent)) (remoteProvider, error) {
		if native != "" {
			return nil, errors.New("no rollout found for unused native thread")
		}
		emit(remoteProviderEvent{Type: "session", NativeID: "new-empty-draft"})
		return &remoteTestProvider{emit: emit}, nil
	}
	if err := c.changeRemoteModel(context.Background(), m, target, p); err != nil {
		t.Fatal(err)
	}
	if target.ID != sessionID || target.NativeID != "new-empty-draft" || target.State != "idle" {
		t.Fatalf("draft did not recover under its existing mobile session ID: %+v", target)
	}
}
