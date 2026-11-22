package main

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
)

func (m *historyManager) privatePath(name string) string {
	if m.owner == "" {
		return filepath.Join(m.home, ".newnoa", name)
	}
	return filepath.Join(m.home, ".newnoa", "history-accounts", m.owner, name)
}
func (m *historyManager) forAccount(account string) (*historyManager, error) {
	if !m.coordinator {
		return m, nil
	}
	if !nodePattern.MatchString(account) {
		return nil, errors.New("invalid history account")
	}
	if child := m.readyAccount(account); child != nil {
		return child, nil
	}
	m.childMu.Lock()
	defer m.childMu.Unlock()
	m.childrenMu.RLock()
	closing := m.closing
	m.childrenMu.RUnlock()
	if closing {
		return nil, errors.New("history manager is closing")
	}
	if child := m.readyAccount(account); child != nil {
		return child, nil
	}
	child, err := newAccountHistoryManager(m.home, m.journal, account, m.historyRuntime, false)
	if err != nil {
		return nil, err
	}
	m.childrenMu.Lock()
	m.children[account] = child
	m.childrenMu.Unlock()
	return child, nil
}
func (m *historyManager) readyAccount(account string) *historyManager {
	if !m.coordinator {
		return m
	}
	m.childrenMu.RLock()
	defer m.childrenMu.RUnlock()
	return m.children[account]
}
func (m *historyManager) initializeAsync(account string) {
	if !m.coordinator || !nodePattern.MatchString(account) {
		return
	}
	m.childrenMu.Lock()
	if m.closing || m.children[account] != nil || m.initializing[account] {
		m.childrenMu.Unlock()
		return
	}
	m.initializing[account] = true
	m.initWG.Add(1)
	m.childrenMu.Unlock()
	go func() {
		defer m.initWG.Done()
		m.forAccount(account)
		m.childrenMu.Lock()
		m.initializing[account] = false
		m.childrenMu.Unlock()
	}()
}
func (m *historyManager) drainPending(ctx context.Context) error {
	for {
		more, err := m.drainOnePending(ctx)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
}
func (m *historyManager) drainOnePending(ctx context.Context) (bool, error) {
	var seq int64
	var viewer, scope, kind, id, raw string
	err := m.journal.db.QueryRowContext(ctx, "SELECT seq,viewer,scope,kind,id,payload FROM history_pending WHERE (?='' OR viewer=?) ORDER BY seq LIMIT 1", m.owner, m.owner).Scan(&seq, &viewer, &scope, &kind, &id, &raw)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	raw, err = resolveHistoryEvent(ctx, m.journal.db.DB, seq, raw)
	if err != nil {
		return false, err
	}
	value, decodeErr := m.journal.decodeBody(ctx, viewer, raw)
	if decodeErr != nil {
		return false, decodeErr
	}

	if err = m.apply(ctx, viewer, scope, kind, id, value); err != nil {
		return false, err
	}
	if _, err = m.journal.db.ExecContext(ctx, "DELETE FROM history_pending WHERE seq=?", seq); err != nil {
		return false, err
	}
	return true, nil
}
func (m *historyManager) flushPending(ctx context.Context) {
	for {
		m.mu.Lock()
		if m.busy != "" || m.config.Engine == "off" {
			m.mu.Unlock()
			return
		}
		more, err := m.drainOnePending(ctx)
		m.mu.Unlock()
		if err != nil || !more {
			return
		}
	}
}
