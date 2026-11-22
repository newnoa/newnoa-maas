package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"
)

func (c *core) historyCommand(method string, raw json.RawMessage) (any, error) {
	var input struct {
		historyQuery
		Kind            string        `json:"kind"`
		Session         string        `json:"session"`
		ID              string        `json:"id"`
		JobID           string        `json:"jobId"`
		Field           string        `json:"field"`
		Offset          int           `json:"offset"`
		Enabled         *bool         `json:"enabled"`
		Mode            string        `json:"mode"`
		Format          string        `json:"format"`
		Filters         *historyQuery `json:"filters"`
		ExportDirectory string        `json:"exportDirectory"`
	}
	if json.Unmarshal(raw, &input) != nil {
		return nil, errors.New("invalid history command")
	}
	if err := input.historyQuery.validate(); err != nil {
		return nil, err
	}
	account, err := c.historyIdentity(input.Session)
	if err != nil {
		return nil, err
	}
	h, err := c.historyStore()
	if err != nil {
		return nil, err
	}
	m, err := h.manager.forAccount(account)
	if err != nil {
		return nil, err
	}
	// Viewing persisted H5 history can be the first authenticated operation after a Core restart.
	select {
	case h.consumptionWork <- historyConsumptionWork{account: account, authorization: "Bearer " + input.Session}:
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	switch method {
	case "history.database.status":
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.masked(), nil
	case "history.database.test", "history.database.configure":
		var config historyDatabaseConfig
		if json.Unmarshal(raw, &config) != nil {
			return nil, errors.New("invalid history database configuration")
		}
		if config.Engine == "off" {
			if method == "history.database.test" {
				return nil, errors.New("history recording is disabled")
			}
			return m.disableRecording()
		}
		m.mu.Lock()
		if config.ExportDirectory == "" {
			config.ExportDirectory = m.config.ExportDirectory
		}
		if config.Password == "" && m.config.sameConnection(config) {
			config.Password = m.config.Password
		}
		m.mu.Unlock()
		if err = config.validate(); err != nil {
			return nil, err
		}
		if err = historyDirectory(config.ExportDirectory); err != nil {
			return nil, err
		}
		if method == "history.database.test" {
			store, err := openHistoryRepository(ctx, config)
			if err != nil {
				return nil, err
			}
			defer store.Close()
			return map[string]any{"ok": true, "engine": config.Engine, "version": store.Version(), "canInitialize": true}, nil
		}
		jobCtx, id, err := m.start("migration", account)
		if err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.migration = id
		m.mu.Unlock()
		go func() { err := m.migrate(jobCtx, config); m.finish(id, err, historyExportResult{}) }()
		return map[string]any{"jobId": id, "state": "running", "percent": 0}, nil
	case "history.settings":
		enabled := m.enabled(account)
		m.mu.Lock()
		days := m.config.RetentionDays
		m.mu.Unlock()
		return map[string]any{"enabled": enabled, "mode": "local", "retentionDays": days}, nil
	case "history.configure":
		if input.Enabled == nil {
			return nil, errors.New("enabled is required")
		}
		if input.Mode != "" { // Legacy clients retain their explicit cloud settings operation.
			if _, err = c.legacyHistoryCommand(method, raw); err != nil {
				return nil, err
			}
		}
		_, err = h.db.ExecContext(ctx, "INSERT INTO history_local_settings(viewer,enabled) VALUES(?,?) ON CONFLICT(viewer) DO UPDATE SET enabled=excluded.enabled", account, *input.Enabled)
		return map[string]any{"enabled": *input.Enabled, "mode": "local"}, err
	case "history.query", "history.list", "history.conversation":
		if method == "history.conversation" && input.StorageID == "" {
			return nil, errors.New("invalid history conversation")
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.busy == "" {
			if err = m.store.Expire(ctx, account, m.config.RetentionDays); err != nil {
				return nil, err
			}
		}
		q := input.historyQuery
		if q.Before != "" {
			if _, _, err = parseHistoryCursor(q.Before); err != nil {
				if record, e := m.store.Get(ctx, account, q.Scope, q.Before); e == nil {
					q.Before = historyCursor(historyText(record["createdAt"]), historyText(record["id"]))
				} else {
					return nil, err
				}
			}
		}
		result, err := m.store.Query(ctx, account, q)
		if err != nil {
			return nil, err
		}
		if method == "history.list" {
			for _, meta := range result.Items {
				var pending bool
				h.db.QueryRowContext(ctx, "SELECT pending FROM exchanges WHERE account=? AND id=?", account, meta["id"]).Scan(&pending)
				meta["pending"] = pending
			}
			return result.Items, nil
		}
		return result, nil
	case "history.filters":
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.busy == "" {
			if err = m.store.Expire(ctx, account, m.config.RetentionDays); err != nil {
				return nil, err
			}
		}
		return m.store.Filters(ctx, account, historyFilterQuery{ModelCategory: input.ModelCategory, Scope: input.Scope, Kind: input.Kind, Q: input.Q, KeyID: input.KeyID, Before: input.Before, Limit: input.Limit})
	case "history.accounts":
		m.mu.Lock()
		defer m.mu.Unlock()
		items, err := m.store.Accounts(ctx, account, input.Q, input.Limit, input.Before)
		if err != nil {
			return nil, err
		}
		next := ""
		if len(items) == input.Limit {
			last := items[len(items)-1]
			next = historyCursor(historyText(last["email"]), historyText(last["id"]))
		}
		return map[string]any{"items": items, "nextCursor": next}, nil
	case "history.get", "history.content":
		m.mu.Lock()
		value, err := m.store.Get(ctx, account, input.Scope, input.ID)
		m.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if method == "history.get" {
			return historyRecordMetadata(value), nil
		}
		if input.Field != "request" && input.Field != "response" {
			return nil, errors.New("invalid history field")
		}
		content := historyText(value[input.Field])
		if input.Offset < 0 || input.Offset > len(content) || (input.Offset < len(content) && !utf8.RuneStart(content[input.Offset])) {
			return nil, errors.New("invalid history offset")
		}
		end := min(input.Offset+historyChunkSize, len(content))
		for end < len(content) && !utf8.RuneStart(content[end]) {
			end--
		}
		return map[string]any{"text": content[input.Offset:end], "nextOffset": end, "done": end == len(content)}, nil
	case "history.delete":
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.config.Engine == "off" {
			return nil, errHistoryDisabled
		}
		if err = m.commitLocked(ctx, account, input.Scope, "delete", input.ID, map[string]any{}); err != nil {
			return nil, err
		}
		_, err = h.db.ExecContext(ctx, "UPDATE exchanges SET deleted_at=COALESCE(deleted_at,CURRENT_TIMESTAMP),pending=0 WHERE account=? AND id=?", account, input.ID)
		return map[string]any{"deleted": err == nil}, err
	case "history.export":
		if m.recordingDisabled() {
			return nil, errHistoryDisabled
		}
		q := input.historyQuery
		if input.Filters != nil {
			q = *input.Filters
			q.Scope = input.Scope
		}
		if err = q.validate(); err != nil {
			return nil, err
		}
		if input.Format != "json" && input.Format != "csv" {
			return nil, errors.New("unsupported history export format")
		}
		jobCtx, id, err := m.start("export", account)
		if err != nil {
			return nil, err
		}
		m.mu.Lock()
		store, directory := m.store, m.config.ExportDirectory
		m.mu.Unlock()
		go func() {
			result, err := writeHistoryExport(context.WithValue(jobCtx, historyExportJobKey{}, id), store, account, q, input.Format, directory, func(p int) { m.progress(id, p) }, nil)
			m.finish(id, err, result)
		}()
		return map[string]any{"jobId": id, "state": "running", "percent": 0}, nil
	case "history.export.status":
		m.mu.Lock()
		defer m.mu.Unlock()
		job := m.jobs[input.JobID]
		if job == nil || job.owner != account || job.kind != "export" {
			return nil, errors.New("history export job not found")
		}
		return job.publicStatus(), nil
	case "history.export.cancel":
		m.mu.Lock()
		defer m.mu.Unlock()
		job := m.jobs[input.JobID]
		if job == nil || job.owner != account || job.kind != "export" {
			return nil, errors.New("history export job not found")
		}
		job.cancel()
		return map[string]any{"jobId": input.JobID}, nil
	case "history.export.directory":
		m.mu.Lock()
		defer m.mu.Unlock()
		return map[string]any{"path": m.config.ExportDirectory, "exportDirectory": m.config.ExportDirectory}, nil
	case "history.export.configure":
		if err = historyDirectory(input.ExportDirectory); err != nil {
			return nil, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.busy != "" {
			return nil, errors.New("history synchronization, migration or export already running")
		}
		config := m.config
		config.ExportDirectory = input.ExportDirectory
		encoded, err := json.Marshal(config)
		if err != nil {
			return nil, err
		}
		if err = m.preserveHistoryCursors(ctx, m.config, config); err != nil {
			return nil, err
		}
		if err = atomicFile(m.configPath(), encoded); err != nil {
			return nil, err
		}
		m.config = config
		m.notifyChange(account, historySettingsChanged)
		return map[string]any{"exportDirectory": config.ExportDirectory}, nil
	case "history.sync.status":
		m.mu.Lock()
		defer m.mu.Unlock()
		job := m.jobs[m.syncJobs[account+"/"+input.Scope]]
		if job == nil {
			return historyJob{State: "idle"}, nil
		}
		return job.publicStatus(), nil
	case "history.sync":
		if m.recordingDisabled() {
			return nil, errors.New("history recording is disabled")
		}
		jobCtx, id, err := m.start("sync", account, input.Scope)
		if err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.syncJobs[account+"/"+input.Scope] = id
		m.mu.Unlock()
		go func() {
			err := c.syncHistory(jobCtx, m, account, input.Session, input.Scope, id)
			m.finish(id, err, historyExportResult{})
		}()
		return map[string]any{"jobId": id, "state": "running", "percent": 0}, nil
	}
	return nil, errors.New("history method unavailable")
}
