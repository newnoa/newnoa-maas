package main

import "context"

// An unavailable selected database is explicit; capture still reaches the durable journal.
type unavailableHistoryRepository struct{ err error }

func (s *unavailableHistoryRepository) Save(context.Context, string, string, map[string]any) error {
	return s.err
}
func (s *unavailableHistoryRepository) Query(context.Context, string, historyQuery) (historyQueryResult, error) {
	return historyQueryResult{}, s.err
}
func (s *unavailableHistoryRepository) Get(context.Context, string, string, string) (map[string]any, error) {
	return nil, s.err
}
func (s *unavailableHistoryRepository) Delete(context.Context, string, string, string) error {
	return s.err
}
func (s *unavailableHistoryRepository) Candidate(context.Context, string, string, string, bool) error {
	return s.err
}
func (s *unavailableHistoryRepository) Accounts(context.Context, string, string, int, ...string) ([]map[string]any, error) {
	return nil, s.err
}
func (s *unavailableHistoryRepository) Expire(context.Context, string, int) error { return s.err }
func (s *unavailableHistoryRepository) Version() string                           { return "unavailable" }
func (s *unavailableHistoryRepository) Close() error                              { return nil }
func (m *historyManager) reconnect(ctx context.Context) {
	m.mu.Lock()
	_, unavailable := m.store.(*unavailableHistoryRepository)
	config := m.config
	busy := m.busy != ""
	m.mu.Unlock()
	if !unavailable || busy {
		return
	}
	next, err := openHistoryRepository(ctx, config)
	if err != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, unavailable = m.store.(*unavailableHistoryRepository); !unavailable || m.busy != "" {
		next.Close()
		return
	}
	m.store = next
	if err = m.restore(ctx); err != nil {
		next.Close()
		m.store = &unavailableHistoryRepository{err: err}
	}
}

func (s *unavailableHistoryRepository) CandidateStates(context.Context, string, string, int) ([]map[string]any, error) {
	return nil, s.err
}
