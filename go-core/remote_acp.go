package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type remoteACPState struct {
	requestID, operationID string
	accepted               chan error
	done                   chan string
	acknowledged, hasText  bool
}

func remoteACPAcknowledgmentContext(ctx context.Context, probe bool) (context.Context, context.CancelFunc) {
	if probe {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, 8*time.Second)
}

func (p *remoteNativeProvider) startACPTurn(ctx context.Context, input remoteProviderInput) error {
	if len(input.Images) != 0 || strings.HasPrefix(strings.TrimSpace(input.Text), "/") {
		return errors.New("Native slash commands and image input are unavailable for this remote executor")
	}
	p.mu.Lock()
	p.seq++
	turn := &remoteACPState{requestID: fmt.Sprint(p.seq), operationID: input.OperationID, accepted: make(chan error, 1), done: make(chan string, 1)}
	sid := p.nativeID
	p.acp, p.runID = turn, uuid.NewString()
	p.mu.Unlock()
	attempted, err := p.writeGuarded(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(turn.requestID), "method": "session/prompt", "params": map[string]any{"sessionId": sid, "prompt": []map[string]string{{"type": "text", "text": input.Text}}}}, func() error { return remoteCheckAuthority(ctx) })
	if err != nil {
		if !attempted {
			p.mu.Lock()
			if p.acp == turn {
				p.acp = nil
				p.runID = ""
			}
			p.mu.Unlock()
		}
		return remoteDispatchOutcome(err, attempted)
	}
	wait, cancel := remoteACPAcknowledgmentContext(ctx, p.probe)
	defer cancel()
	select {
	case err := <-turn.accepted:
		return err
	case <-wait.Done():
		return remoteDispatchOutcome(errors.New("ACP turn acknowledgment is pending"), true)
	case <-p.done:
		return remoteDispatchOutcome(errors.New("ACP executor disconnected before turn acknowledgment"), true)
	}
}

func (p *remoteNativeProvider) stopACPTurn(ctx context.Context) error {
	p.mu.Lock()
	turn, sid := p.acp, p.nativeID
	p.mu.Unlock()
	if turn == nil {
		return errors.New("No active ACP turn")
	}
	if _, err := p.writeGuarded(map[string]any{"jsonrpc": "2.0", "method": "session/cancel", "params": map[string]string{"sessionId": sid}}, func() error { return remoteCheckAuthority(ctx) }); err != nil {
		return err
	}
	select {
	case state := <-turn.done:
		if state != "stopped" && !(p.agent == "hermes" && state == "idle") {
			return fmt.Errorf("ACP executor ended as %s without confirming cancellation", state)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return errors.New("ACP executor disconnected before cancellation was confirmed")
	}
}

func (p *remoteNativeProvider) acknowledgeACPLocked(turn *remoteACPState) {
	if turn.acknowledged {
		return
	}
	turn.acknowledged = true
	turn.accepted <- nil
	if turn.operationID != "" {
		p.eventLocked(remoteProviderEvent{Type: "dispatch", OperationID: turn.operationID, Delivery: "providerAccepted"})
	}
}

func (p *remoteNativeProvider) acpMessage(m remoteNativeMessage) {
	id := remoteMessageID(m.ID)
	p.mu.Lock()
	defer p.mu.Unlock()
	turn := p.acp
	if m.Method == "" && id != "" {
		if turn != nil && turn.requestID == id {
			p.finishACPLocked(turn, m)
			return
		}
		if ch := p.pending[id]; ch != nil {
			select {
			case ch <- m:
			default:
			}
		}
		return
	}
	if m.Method == "session/request_permission" && id != "" && id != "null" {
		// Permission requests are denied until this executor has a verified mapping to live Core approvals.
		go func() {
			if err := p.write(map[string]any{"jsonrpc": "2.0", "id": m.ID, "result": map[string]any{"outcome": map[string]string{"outcome": "cancelled"}}}); err != nil {
				p.cancel()
			}
		}()
		return
	}
	if turn == nil || m.Method != "session/update" || id != "" {
		return
	}
	var payload struct {
		SessionID string `json:"sessionId"`
		Update    struct {
			Kind    string `json:"sessionUpdate"`
			Content struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"update"`
	}
	if json.Unmarshal(m.Params, &payload) != nil || payload.SessionID != p.nativeID {
		return
	}
	if payload.Update.Kind == "agent_message_chunk" && payload.Update.Content.Type == "text" && payload.Update.Content.Text != "" {
		turn.hasText = turn.hasText || strings.TrimSpace(payload.Update.Content.Text) != ""
		p.acknowledgeACPLocked(turn)
		p.eventLocked(remoteProviderEvent{Type: "output", Text: payload.Update.Content.Text})
	}
}

func (p *remoteNativeProvider) finishACPLocked(turn *remoteACPState, m remoteNativeMessage) {
	var terminal struct {
		StopReason string `json:"stopReason"`
	}
	state := "failed"
	if (len(m.Error) == 0 || string(m.Error) == "null") && json.Unmarshal(m.Result, &terminal) == nil {
		switch terminal.StopReason {
		case "end_turn", "max_tokens", "max_turn_requests":
			state = "idle"
		case "cancelled":
			state = "stopped"
		}
	}
	if state == "failed" {
		err := errors.New("Native ACP executor rejected or failed the turn")
		if !turn.acknowledged {
			turn.accepted <- err
		}
		p.eventLocked(remoteProviderEvent{Type: "error", State: state, Error: err.Error()})
	} else {
		p.acknowledgeACPLocked(turn)
		if p.agent == "openclaw" && state == "idle" && !turn.hasText {
			// OpenClaw can end a turn after an upstream error without forwarding an error frame.
			state = "failed"
			p.eventLocked(remoteProviderEvent{Type: "error", State: state, Error: "OpenClaw ended the turn without a reply"})
		} else {
			p.eventLocked(remoteProviderEvent{Type: "state", State: state})
		}
	}
	turn.done <- state
	p.acp = nil
	p.runID = ""
}

func remoteACPAgent(agent string) bool {
	return agent == "openclaw" || agent == "hermes" || agent == "opencode"
}

func (p *remoteNativeProvider) initializeACP(ctx context.Context, cwd, nativeID string) error {
	raw, err := p.rpc(ctx, "initialize", map[string]any{"protocolVersion": 1, "clientCapabilities": map[string]any{}})
	if err != nil {
		return err
	}
	var init struct {
		Protocol int `json:"protocolVersion"`
	}
	if json.Unmarshal(raw, &init) != nil || init.Protocol != 1 {
		return errors.New("Unsupported ACP protocol version")
	}
	method := "session/new"
	params := map[string]any{"cwd": cwd, "mcpServers": []any{}}
	if nativeID != "" {
		method = "session/resume"
		params["sessionId"] = nativeID
		if p.agent == "hermes" || p.agent == "opencode" {
			method = "session/load"
		}
		if p.agent == "openclaw" {
			// The CLI's default session/new maps ACP ID to acp-bridge:<ID>,
			// while a fresh bridge's session/resume does not infer that mapping.
			params["_meta"] = map[string]any{"sessionKey": "acp-bridge:" + nativeID, "requireExistingSession": true}
		}
	}
	raw, err = p.rpc(ctx, method, params)
	if err != nil {
		return err
	}
	var session struct {
		SessionID string `json:"sessionId"`
		Models    struct {
			Current string `json:"currentModelId"`
		} `json:"models"`
	}
	if len(raw) == 0 || string(raw) == "null" || json.Unmarshal(raw, &session) != nil || len(session.SessionID) > 160 || (nativeID == "" && session.SessionID == "") || (nativeID != "" && session.SessionID != "" && nativeID != session.SessionID) {
		return errors.New("ACP did not confirm the requested native session")
	}
	if session.SessionID == "" {
		session.SessionID = nativeID
	}
	p.mu.Lock()
	p.nativeID = session.SessionID
	if session.Models.Current != "" {
		p.eventLocked(remoteProviderEvent{Type: "session", Model: session.Models.Current})
	}
	p.mu.Unlock()
	return nil
}
