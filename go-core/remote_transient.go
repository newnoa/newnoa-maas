package main

import (
	"errors"
	"time"
)

func (m *remoteManager) isTransientType(kind string) bool {
	return kind == "attachment.read" || kind == "session.history.read"
}
func (m *remoteManager) cleanTransient() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			m.pruneTransientLocked()
			m.mu.Unlock()
		}
	}
}
func (m *remoteManager) pruneTransientLocked() {
	cutoff := time.Now().Add(-60 * time.Second)
	for id, page := range m.historyPages {
		if page.lastRead.Before(cutoff) {
			delete(m.historyPages, id)
		}
	}
	for id, r := range m.operations {
		if r.Transient && r.created.Before(cutoff) {
			delete(m.operations, id)
			delete(m.relayOperations, id)
			delete(m.relayPublished, id)
		}
	}
}
func (m *remoteManager) retainedOperationsLocked() int {
	n := 0
	for _, r := range m.operations {
		if !r.Transient {
			n++
		}
	}
	return n
}
func (m *remoteManager) reserveTransientLocked() error {
	count := 0
	oldest := ""
	var created time.Time
	for id, r := range m.operations {
		if !r.Transient {
			continue
		}
		count++
		if r.Status != "accepted" && (!m.relayOperations[id]) && (oldest == "" || r.created.Before(created)) {
			oldest = id
			created = r.created
		}
	}
	if count < 8 {
		return nil
	}
	if oldest != "" {
		delete(m.operations, oldest)
		delete(m.relayPublished, oldest)
		return nil
	}
	return errors.New("Too many active remote reads; retry after a previous read completes")
}
