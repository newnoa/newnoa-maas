package main

import (
	"encoding/json"
	"errors"
	"time"
)

// Inspired by Orca 31b23b84's bounded replay waiters and exact dispatch echoes.
// This is an independent Go implementation; a write is never an acceptance.
const remoteMaxNativeDispatches = 128
const remoteNativeDispatchRetention = inferenceWrapperTimeout

type remoteNativeDispatch struct {
	operationID, method string
	expires             time.Time
}
type remoteApprovalClaim struct{ held bool }
type remotePiWaiter struct {
	command string
	reply   chan remoteNativeMessage
}

func (p *remoteNativeProvider) newApprovalInstanceLocked(id string) uint64 {
	p.approvalSequence++
	if p.approvalInstances == nil {
		p.approvalInstances = map[string]uint64{}
	}
	p.approvalInstances[id] = p.approvalSequence
	return p.approvalSequence
}

type remoteDispatchUnknown struct{ cause error }

func (e *remoteDispatchUnknown) Error() string { return e.cause.Error() }
func (e *remoteDispatchUnknown) Unwrap() error { return e.cause }
func remoteDispatchOutcome(err error, attempted bool) error {
	if err == nil || !attempted {
		return err
	}
	return &remoteDispatchUnknown{cause: err}
}
func isRemoteDispatchUnknown(err error) bool {
	var unknown *remoteDispatchUnknown
	return errors.As(err, &unknown)
}

func (p *remoteNativeProvider) armDispatchLocked(id, operationID, method string) error {
	p.pruneDispatchesLocked(time.Now())
	if len(p.dispatches) >= remoteMaxNativeDispatches {
		return errors.New("native dispatch receipt capacity reached")
	}
	if p.dispatches == nil {
		p.dispatches = map[string]remoteNativeDispatch{}
	}
	p.dispatches[id] = remoteNativeDispatch{operationID: operationID, method: method, expires: time.Now().Add(remoteNativeDispatchRetention)}
	return nil
}
func (p *remoteNativeProvider) pruneDispatchesLocked(now time.Time) {
	for id, d := range p.dispatches {
		if !now.Before(d.expires) {
			delete(p.dispatches, id)
		}
	}
}
func (p *remoteNativeProvider) forgetDispatch(id string) {
	p.mu.Lock()
	delete(p.dispatches, id)
	p.mu.Unlock()
}

// Keep timed-out identities until an exact reply arrives. Never join by content,
// queue order, output or an uncorrelated terminal, and never evict a live identity.
func (p *remoteNativeProvider) settleDispatchLocked(id, delivery string) bool {
	p.pruneDispatchesLocked(time.Now())
	d, ok := p.dispatches[id]
	if !ok {
		return false
	}
	delete(p.dispatches, id)
	if d.operationID != "" {
		p.eventLocked(remoteProviderEvent{Type: "dispatch", OperationID: d.operationID, Delivery: delivery})
	}
	return true
}

func (p *remoteNativeProvider) claudeDispatchAck(data map[string]any, result bool) {
	ids := []string{}
	if !result {
		if id, ok := data["uuid"].(string); ok {
			ids = append(ids, id)
		}
	}
	if id, ok := data["user_message_uuid"].(string); ok {
		ids = append(ids, id)
	}
	if result {
		if list, ok := data["user_message_uuids"].([]any); ok {
			for _, value := range list {
				if id, ok := value.(string); ok {
					ids = append(ids, id)
				}
			}
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if data["session_id"] != p.nativeID {
		return
	}
	for _, id := range ids {
		if !p.settleDispatchLocked(id, "providerAccepted") {
			continue
		}
		if !result && p.runID == id {
			p.eventLocked(remoteProviderEvent{Type: "state", State: "running", RunID: id})
		}
		if ch := p.pending[id]; ch != nil {
			select {
			case ch <- remoteNativeMessage{}:
			default:
			}
		}
	}
}

func (p *remoteNativeProvider) codexDispatchReplyLocked(id string, m remoteNativeMessage) {
	p.pruneDispatchesLocked(time.Now())
	d, ok := p.dispatches[id]
	if !ok {
		return
	}
	if len(m.Error) > 0 && string(m.Error) != "null" {
		p.settleDispatchLocked(id, "rejected")
		return
	}
	if d.method == "turn/start" {
		var reply struct {
			Turn struct {
				ID string `json:"id"`
			} `json:"turn"`
		}
		if json.Unmarshal(m.Result, &reply) != nil || reply.Turn.ID == "" {
			return
		}
		if p.runID == "" && !p.completedRuns[reply.Turn.ID] {
			p.runID = reply.Turn.ID
			p.eventLocked(remoteProviderEvent{Type: "state", State: "running", RunID: reply.Turn.ID})
		}
	}
	p.settleDispatchLocked(id, "providerAccepted")
}
