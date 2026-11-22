package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
)

var errHistoryDisabled = errors.New("history recording is disabled")

// Disabled storage keeps existing databases intact and accepts no new records.
type disabledHistoryRepository struct{ unavailableHistoryRepository }

func (*disabledHistoryRepository) Query(context.Context, string, historyQuery) (historyQueryResult, error) {
	return historyQueryResult{Items: []map[string]any{}, Database: "off"}, nil
}
func (*disabledHistoryRepository) Filters(context.Context, string, historyFilterQuery) (historyFilterResult, error) {
	return historyFilterResult{Items: []historyFilterItem{}}, nil
}
func (*disabledHistoryRepository) Get(context.Context, string, string, string) (map[string]any, error) {
	return nil, errors.New("history recording is disabled")
}
func (*disabledHistoryRepository) Accounts(context.Context, string, string, int, ...string) ([]map[string]any, error) {
	return []map[string]any{}, nil
}
func (*disabledHistoryRepository) CandidateStates(context.Context, string, string, int) ([]map[string]any, error) {
	return []map[string]any{}, nil
}
func (*disabledHistoryRepository) Version() string                           { return "off" }
func (*disabledHistoryRepository) Expire(context.Context, string, int) error { return nil }

func (m *historyManager) recordingDisabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.config.Engine == "off"
}

func (m *historyManager) accountRecordingDisabled(account string) (bool, error) {
	if child := m.readyAccount(account); child != nil {
		return child.recordingDisabled(), nil
	}
	if !nodePattern.MatchString(account) {
		return false, errors.New("invalid history account")
	}
	child := &historyManager{home: m.home, owner: account}
	raw, err := os.ReadFile(child.configPath())
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	var config historyDatabaseConfig
	if err = json.Unmarshal(raw, &config); err != nil {
		return false, err
	}
	return config.Engine == "off", nil
}

func (m *historyManager) disableRecording() (any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.busy != "" {
		return nil, errors.New("history synchronization, migration or export already running")
	}
	config := m.config
	if config.Engine != "off" {
		config.ResumeEngine = config.Engine
	}
	config.Engine = "off"
	if err := m.switchRecording(config); err != nil {
		return nil, err
	}
	m.migration = ""
	m.notifyChange(m.owner, historySettingsChanged|historyRecordsChanged)
	return map[string]any{"state": "complete"}, nil
}

// Caller holds m.mu; switching off never copies or deletes the original records.
func (m *historyManager) switchRecording(config historyDatabaseConfig) error {
	next, err := openHistoryRepository(context.Background(), config)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(config)
	if err == nil {
		err = atomicFile(m.configPath(), raw)
	}
	if err != nil {
		next.Close()
		return err
	}
	previous := m.store
	m.store, m.config = next, config
	previous.Close()
	return nil
}
