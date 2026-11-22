package main

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
)

func remoteReceiptFingerprint(r remoteReceipt) string {
	raw, _ := json.Marshal(publicRemoteReceipt(r))
	return hash(raw)
}
func (c *core) publishChangedRemoteState(ctx context.Context, m *remoteManager, session string, previous *uint64) error {
	m.mu.Lock()
	if m.relayPublished == nil {
		m.relayPublished = map[string]string{}
	}
	receipts := []remoteReceipt{}
	for id := range m.relayOperations {
		if r := m.operations[id]; r != nil && m.relayPublished[id] != remoteReceiptFingerprint(*r) {
			receipts = append(receipts, publicRemoteReceipt(*r))
		}
	}
	revision := m.revision
	if previous != nil && revision == *previous && len(receipts) == 0 {
		m.mu.Unlock()
		return nil
	}
	sort.Slice(receipts, func(i, j int) bool { return receipts[i].created.Before(receipts[j].created) })
	more := len(receipts) > 32
	if more {
		receipts = receipts[:32]
	}
	snapshot := m.snapshotLocked()
	m.mu.Unlock()
	path := "/me/remote-hosts/" + m.hostID
	err := c.remoteHTTP(ctx, "POST", path+"/publish", session, m, map[string]any{"snapshot": snapshot, "receipts": receipts}, nil)
	if err != nil {
		var rejected *remoteHTTPError
		pruned := false
		if errors.As(err, &rejected) && rejected.Status == 404 {
			for _, r := range receipts {
				lookup := c.remoteHTTP(ctx, "GET", path+"/operations/"+r.OperationID, session, nil, nil, nil)
				var gone *remoteHTTPError
				if errors.As(lookup, &gone) && gone.Status == 404 {
					m.mu.Lock()
					pruned = true
					delete(m.relayOperations, r.OperationID)
					delete(m.relayPublished, r.OperationID)
					m.mu.Unlock()
				}
			}
		}
		m.mu.Lock()
		m.setFailureLocked("Remote state synchronization failed; local execution state remains authoritative")
		m.mu.Unlock()
		if pruned {
			return errors.New("Expired remote receipt removed; resynchronize")
		}
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if previous != nil {
		*previous = revision
	}
	if more {
		m.signalChangesLocked()
	}
	for _, r := range receipts {
		current := m.operations[r.OperationID]
		fingerprint := remoteReceiptFingerprint(r)
		if current == nil {
			delete(m.relayOperations, r.OperationID)
			continue
		}
		if remoteReceiptFingerprint(*current) == fingerprint && r.Status != "accepted" && r.Status != "unknown" {
			delete(m.relayOperations, r.OperationID)
			delete(m.relayPublished, r.OperationID)
		} else {
			m.relayPublished[r.OperationID] = fingerprint
		}
	}
	if m.failure == "Remote state synchronization failed; local execution state remains authoritative" {
		m.setFailureLocked("")
	}
	return nil
}
