package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRemoteValidationRejectsMalformedEnvelopeAndPayload(t *testing.T) {
	for _, raw := range []string{
		`{`, `null`, `{"protocolVersion":1,"operationId":"","type":"message.send","payload":{}}`,
		`{"protocolVersion":1,"operationId":"x","type":"execute","payload":{}}`,
		`{"protocolVersion":1,"operationId":"x","type":"message.send","payload":{},"secret":"override"}`,
		`{"protocolVersion":1,"operationId":"` + strings.Repeat("a", 101) + `","type":"message.send","payload":{}}`,
		`{"protocolVersion":1,"operationId":"x","type":"message.send","payload":{"text":"` + strings.Repeat("a", 32768) + `"}}`,
	} {
		if _, err := parseRemoteCommand(json.RawMessage(raw)); err == nil {
			t.Fatal("unsafe envelope accepted")
		}
	}
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	command := remoteEnvelope("unknown-payload", "message.send", sid, map[string]any{"text": "hello", "executable": "sh"})
	if _, err := m.command(command); err == nil {
		t.Fatal("unknown payload field accepted")
	}
	command = remoteEnvelope("wrong-payload-type", "message.send", sid, map[string]any{"text": 42})
	if _, err := m.command(command); err == nil {
		t.Fatal("wrong payload type accepted")
	}
}

func TestRemoteValidationRejectedCommandsHaveNoIntentOrNativeEffect(t *testing.T) {
	type mutate func(*remoteManager, *remoteSession, *remoteCommandEnvelope)
	for _, test := range []struct {
		name, kind string
		payload    map[string]any
		mutate     mutate
	}{
		{"closed", "message.send", map[string]any{"text": "hello"}, func(m *remoteManager, _ *remoteSession, _ *remoteCommandEnvelope) { m.closed = true }},
		{"epoch", "message.send", map[string]any{"text": "hello"}, func(_ *remoteManager, _ *remoteSession, c *remoteCommandEnvelope) {
			c.ExpectedHostEpoch = "previous-epoch"
		}},
		{"missingSession", "message.send", map[string]any{"text": "hello"}, func(_ *remoteManager, _ *remoteSession, c *remoteCommandEnvelope) { c.SessionID = "missing" }},
		{"switching", "message.send", map[string]any{"text": "hello"}, func(m *remoteManager, _ *remoteSession, _ *remoteCommandEnvelope) { m.switching["codex"] = true }},
		{"attempt", "message.send", map[string]any{"text": "hello"}, func(_ *remoteManager, _ *remoteSession, c *remoteCommandEnvelope) { c.AttemptID = "old-attempt" }},
		{"empty", "message.send", map[string]any{"text": " \t\n"}, nil},
		{"textBytes", "message.send", map[string]any{"text": strings.Repeat("a", 16385)}, nil},
		{"attachmentCount", "message.send", map[string]any{"attachments": []string{"1", "2", "3", "4", "5"}}, nil},
		{"notReady", "message.send", map[string]any{"text": "hello"}, func(_ *remoteManager, s *remoteSession, _ *remoteCommandEnvelope) { s.State = "awaitingApproval" }},
		{"unsupportedSteer", "message.send", map[string]any{"text": "hello"}, func(_ *remoteManager, s *remoteSession, _ *remoteCommandEnvelope) {
			s.State = "running"
			s.Capabilities["steer"] = false
		}},
		{"detached", "message.send", map[string]any{"text": "hello"}, func(m *remoteManager, s *remoteSession, _ *remoteCommandEnvelope) { delete(m.providers, s.ID) }},
		{"unknownAttachment", "message.send", map[string]any{"attachments": []string{"missing"}}, nil},
		{"foreignAttachment", "message.send", map[string]any{"attachments": []string{"foreign"}}, func(m *remoteManager, _ *remoteSession, _ *remoteCommandEnvelope) {
			m.attachments["foreign"] = &remoteAttachment{ID: "foreign", SessionID: "another", Ready: true}
		}},
		{"resumeRevision", "session.resume", map[string]any{}, func(_ *remoteManager, s *remoteSession, c *remoteCommandEnvelope) {
			revision := s.Revision + 1
			c.ExpectedRevision = &revision
		}},
		{"resumePending", "session.resume", map[string]any{}, func(_ *remoteManager, s *remoteSession, _ *remoteCommandEnvelope) { s.State = "recovering" }},
		{"resumeUnauthorized", "session.resume", map[string]any{}, func(m *remoteManager, _ *remoteSession, _ *remoteCommandEnvelope) {
			m.executors["codex"].Authorized = false
		}},
		{"resumeNoNative", "session.resume", map[string]any{}, func(_ *remoteManager, s *remoteSession, _ *remoteCommandEnvelope) { s.NativeID = "" }},
		{"resumeUnsupported", "session.resume", map[string]any{}, func(_ *remoteManager, s *remoteSession, _ *remoteCommandEnvelope) { s.Capabilities["resume"] = false }},
		{"resumeAttached", "session.resume", map[string]any{}, nil},
		{"resumeUnverifiedOwner", "session.resume", map[string]any{}, func(m *remoteManager, s *remoteSession, _ *remoteCommandEnvelope) {
			s.State = "unknown"
			delete(m.providers, s.ID)
		}},
		{"stopNoOwner", "run.stop", map[string]any{}, func(m *remoteManager, s *remoteSession, _ *remoteCommandEnvelope) {
			s.State = "running"
			delete(m.providers, s.ID)
		}},
		{"missingApproval", "approval.respond", map[string]any{"approvalId": "missing", "decision": "accept"}, nil},
		{"modelMissingSelection", "model.change", map[string]any{}, nil},
		{"sendAfterAnotherSessionRevision", "message.send", map[string]any{"text": "must not send"}, func(_ *remoteManager, s *remoteSession, c *remoteCommandEnvelope) {
			revision := s.Revision + 1
			c.ExpectedRevision = &revision
		}},
		{"modelRevision", "model.change", map[string]any{"keyId": "key", "modelId": "model"}, func(_ *remoteManager, s *remoteSession, c *remoteCommandEnvelope) {
			revision := s.Revision + 1
			c.ExpectedRevision = &revision
		}},
		{"imageUnsupported", "attachment.prepare", map[string]any{"name": "a.png", "mime": "image/png", "size": 1}, func(_ *remoteManager, s *remoteSession, _ *remoteCommandEnvelope) { s.Capabilities["image"] = false }},
		{"imageName", "attachment.prepare", map[string]any{"name": strings.Repeat("a", 121), "mime": "image/png", "size": 1}, nil},
		{"imageEmpty", "attachment.prepare", map[string]any{"name": "a.png", "mime": "image/png", "size": 0}, nil},
		{"imageMime", "attachment.prepare", map[string]any{"name": "a.svg", "mime": "image/svg+xml", "size": 1}, nil},
		{"imageQuota", "attachment.prepare", map[string]any{"name": "a.png", "mime": "image/png", "size": 1}, func(m *remoteManager, s *remoteSession, _ *remoteCommandEnvelope) {
			for i := 0; i < 32; i++ {
				id := fmt.Sprint(i)
				m.attachments[id] = &remoteAttachment{ID: id, SessionID: s.ID}
			}
		}},
		{"readMissing", "attachment.read", map[string]any{"attachmentId": "missing"}, nil},
		{"uploadMissing", "attachment.upload", map[string]any{"attachmentId": "missing", "content": "AA=="}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, p, cwd := remoteFixture(t)
			sid := createRemote(t, m, cwd)
			command := remoteEnvelope("must-reject", ""+test.kind, sid, test.payload)
			m.mu.Lock()
			if test.mutate != nil {
				test.mutate(m, m.sessions[sid], &command)
			}
			revision := m.revision
			state := m.sessions[sid].State
			m.mu.Unlock()
			if _, err := m.command(command); err == nil {
				t.Fatal("invalid command admitted")
			}
			m.mu.Lock()
			stored := m.operations[command.OperationID]
			unchanged := m.revision == revision && m.sessions[sid].State == state
			m.mu.Unlock()
			if stored != nil || !unchanged {
				t.Fatal("rejected command changed intent or session state")
			}
			p.mu.Lock()
			effects := p.sends + p.steers + p.answers
			p.mu.Unlock()
			if effects != 0 {
				t.Fatal("rejected command reached native provider")
			}
		})
	}
}

func TestRemoteValidationCreateRequiresAuthorizedExecutorAndDirectory(t *testing.T) {
	for _, scenario := range []string{"noAuthorization", "sessionQuota", "emptyDirectory", "createWithText", "regularFile"} {
		t.Run(scenario, func(t *testing.T) {
			m, _, cwd := remoteFixture(t)
			payload := map[string]any{"agent": "codex", "cwd": cwd}
			m.mu.Lock()
			switch scenario {
			case "noAuthorization":
				m.executors["codex"].Authorized = false
			case "sessionQuota":
				for i := 0; i < 8; i++ {
					id := fmt.Sprint(i)
					m.sessions[id] = &remoteSession{ID: id, State: "stopped"}
				}
			case "emptyDirectory":
				payload["cwd"] = ""
			case "createWithText":
				payload["text"] = "do work"
			case "regularFile":
				path := filepath.Join(cwd, "file")
				if err := os.WriteFile(path, []byte("file"), 0600); err != nil {
					m.mu.Unlock()
					t.Fatal(err)
				}
				payload["cwd"] = path
			}
			before := len(m.sessions)
			m.mu.Unlock()
			if _, err := m.command(remoteEnvelope("invalid-create", "session.create", "", payload)); err == nil {
				t.Fatal("invalid create admitted")
			}
			m.mu.Lock()
			defer m.mu.Unlock()
			if len(m.sessions) != before || m.operations["invalid-create"] != nil {
				t.Fatal("failed create consumed a session")
			}
		})
	}
}

func TestRemoteValidationApprovalChoiceAndGenerationAreBoundToLiveRequest(t *testing.T) {
	for _, scenario := range []string{"choice", "resolved", "generation", "authority"} {
		t.Run(scenario, func(t *testing.T) {
			m, p, cwd := remoteFixture(t)
			sid := createRemote(t, m, cwd)
			p.emit(remoteProviderEvent{Type: "approval", Approval: &remoteProviderApproval{ID: "native-question", Options: []string{"accept", "decline"}}})
			m.mu.Lock()
			a := m.sessions[sid].Approvals[0]
			id := a.ID
			if scenario == "resolved" {
				a.State = "resolved"
			}
			if scenario == "generation" {
				a.Generation = "old-generation"
			}
			m.mu.Unlock()
			decision := "accept"
			if scenario == "choice" {
				decision = "acceptForSession"
			}
			command := remoteEnvelope("respond", "approval.respond", sid, map[string]any{"approvalId": id, "decision": decision})
			var validate func(context.Context) error
			if scenario == "authority" {
				validate = func(context.Context) error { return errors.New("revoked") }
			}
			_, err := m.commandWithAuthorization(command, validate)
			if scenario == "choice" || scenario == "resolved" {
				if err == nil {
					t.Fatal("invalid approval admitted")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if r := waitRemote(t, m, "respond"); r.Status != "rejected" {
					t.Fatal("stale approval reached provider", r)
				}
				m.mu.Lock()
				invalidated := a.State == "invalidated"
				m.mu.Unlock()
				if !invalidated {
					t.Fatal("rejected native approval remained usable")
				}
			}
			p.mu.Lock()
			answers := p.answers
			p.mu.Unlock()
			if answers != 0 {
				t.Fatal("invalid approval was delivered")
			}
		})
	}
}

func TestRemoteValidationQueueBackpressureDoesNotConsumeOperationID(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	entered, release := make(chan struct{}), make(chan struct{})
	_, err := m.commandWithAuthorization(remoteEnvelope("blocked-prepare", "attachment.prepare", sid, map[string]any{"name": "a.png", "mime": "image/png", "size": 1}), func(context.Context) error { close(entered); <-release; return nil })
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	ids := []string{}
	for i := 0; i < 32; i++ {
		id := fmt.Sprintf("queued-%d", i)
		if _, err := m.command(remoteEnvelope(id, "attachment.prepare", sid, map[string]any{"name": "a.png", "mime": "image/png", "size": 1})); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	retry := remoteEnvelope("retry-after-busy", "attachment.discard", sid, map[string]any{"attachmentId": "missing"})
	if _, err := m.command(retry); err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatal("full queue accepted more work", err)
	}
	m.mu.Lock()
	consumed := m.operations[retry.OperationID] != nil
	m.mu.Unlock()
	if consumed {
		t.Fatal("backpressure consumed operation identity")
	}
	close(release)
	waitRemote(t, m, "blocked-prepare")
	for _, id := range ids {
		waitRemote(t, m, id)
	}
}

func TestRemoteValidationNativeLaunchFailureIsVisibleAndRetryNeedsNewIntent(t *testing.T) {
	m, _, cwd := remoteFixture(t)
	m.start = func(context.Context, string, string, string, func(remoteProviderEvent)) (remoteProvider, error) {
		return nil, errors.New("native unavailable")
	}
	command := remoteEnvelope("launch-failure", "session.create", "", map[string]any{"agent": "codex", "cwd": cwd})
	accepted, err := m.command(command)
	if err != nil {
		t.Fatal(err)
	}
	r := waitRemote(t, m, command.OperationID)
	if r.Status != "rejected" || r.Error != remotePublicFailure {
		t.Fatal(r)
	}
	replayed, err := m.command(command)
	if err != nil || replayed.SessionID != accepted.SessionID || replayed.Status != "rejected" {
		t.Fatal("failed native intent was executed again", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sessions[accepted.SessionID].State != "failed" || m.providers[accepted.SessionID] != nil {
		t.Fatal("failed launch appeared attached or ready")
	}
}
