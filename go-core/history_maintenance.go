package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const historyDeleteBatch = 256

func historyCutoff(now time.Time, days int) time.Time {
	if days < 1 || days > 7 {
		days = 7
	}
	return now.UTC().Add(-time.Duration(days) * 24 * time.Hour)
}
func historyRetentionTime(ctx context.Context) time.Time {
	if now, ok := ctx.Value(historyRetentionContextKey{}).(time.Time); ok {
		return now
	}
	return time.Now()
}
func expiredHistoryAtCutoff(value map[string]any, cutoff time.Time) bool {
	stamp, err := time.Parse(time.RFC3339Nano, historyText(value["createdAt"]))
	return err == nil && stamp.Before(cutoff)
}
func (c historyDatabaseConfig) expiresRecord(value map[string]any, now time.Time) bool {
	return c.Engine == "sqlite" && expiredHistoryAtCutoff(value, historyCutoff(now, c.RetentionDays))
}

// Delete in bounded transactions; usage outboxes never participate in history retention.
func deleteHistoryBatches(ctx context.Context, db *sql.DB, query string, args ...any) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		result, err := db.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count < historyDeleteBatch {
			return nil
		}
	}
}
func reclaimHistorySQLite(ctx context.Context, db *sql.DB) error {
	// Existing databases without auto_vacuum reuse freed pages; no blocking full VACUUM is scheduled.
	_, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE); PRAGMA incremental_vacuum(256);")
	return err
}
func (s *sqlHistoryRepository) expireSQLite(ctx context.Context, viewer string, cutoff time.Time) error {
	if s.leaves != nil {
		for _, leaf := range s.leaves {
			if e := leaf.expireSQLite(ctx, viewer, cutoff); e != nil {
				return e
			}
		}
		return nil
	}
	if err := deleteHistoryBatches(ctx, s.db, s.categorySQL("DELETE FROM nn_history_records WHERE rowid IN (SELECT rowid FROM nn_history_records WHERE viewer=? AND created<? LIMIT 256)"), viewer, cutoff.UTC().Format("2006-01-02T15:04:05.000000000Z")); err != nil {
		return err
	}
	if err := s.collectBodies(ctx); err != nil {
		return err
	}
	return reclaimHistorySQLite(ctx, s.db)
}
func (m *historyManager) maintenanceViewers(ctx context.Context) ([]string, error) {
	if m.owner != "" {
		return []string{m.owner}, nil
	}
	rows, err := m.journal.db.QueryContext(ctx, "SELECT viewer FROM history_viewers UNION SELECT account FROM exchanges UNION SELECT viewer FROM history_pending")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var viewers []string
	for rows.Next() {
		var viewer string
		if err = rows.Scan(&viewer); err != nil {
			return nil, err
		}
		viewers = append(viewers, viewer)
	}
	return viewers, rows.Err()
}
func (m *historyManager) maintain(ctx context.Context, now time.Time) error {
	if m.coordinator {
		if _, err := m.journal.db.ExecContext(ctx, `DELETE FROM proxy_compression_daily WHERE day < ?`, now.In(compressionZone).AddDate(0, 0, -6).Format("2006-01-02")); err != nil {
			return err
		}
		m.mu.Lock()
		busy := m.busy != ""
		m.mu.Unlock()
		if busy {
			return nil
		}
		viewers, err := m.maintenanceViewers(ctx)
		if err != nil {
			return err
		}
		var firstErr error
		for _, viewer := range viewers {
			if err = ctx.Err(); err != nil {
				return err
			}
			child, e := m.forAccount(viewer)
			if e != nil {
				if firstErr == nil {
					firstErr = e
				}
				continue
			}
			if e = child.maintain(ctx, now); e != nil {
				if firstErr == nil {
					firstErr = e
				}
			}
		}
		return firstErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.busy != "" || m.config.Engine != "sqlite" {
		return nil
	}
	return m.maintainSQLite(ctx, now)
}

// Caller holds m.mu and has selected a SQLite policy.
func (m *historyManager) maintainSQLite(ctx context.Context, now time.Time) error {
	viewers, err := m.maintenanceViewers(ctx)
	if err != nil {
		return err
	}
	for _, viewer := range viewers {
		if err = m.maintainViewer(ctx, viewer, now); err != nil {
			return err
		}
	}
	return nil
}
func (m *historyManager) maintainViewer(ctx context.Context, viewer string, now time.Time) error {
	cutoff := historyCutoff(now, m.config.RetentionDays)
	if s, ok := m.store.(*sqlHistoryRepository); ok {
		if err := s.expireSQLite(ctx, viewer, cutoff); err != nil {
			return err
		}
	}
	if err := m.deleteExpiredJournal(ctx, viewer, cutoff, false); err != nil {
		return err
	}
	if err := m.deleteExpiredJournal(ctx, viewer, cutoff, true); err != nil {
		return err
	}
	if err := deleteHistoryBatches(ctx, m.journal.db.DB, "DELETE FROM history_consumption_pending WHERE rowid IN (SELECT p.rowid FROM history_consumption_pending p WHERE p.account=? AND NOT EXISTS (SELECT 1 FROM exchanges e WHERE e.account=p.account AND e.id=p.id AND e.deleted_at IS NULL) LIMIT 256)", viewer); err != nil {
		return err
	}
	if err := reclaimHistorySQLite(ctx, m.journal.db.DB); err != nil {
		return err
	}
	return m.collectHistoryBlocks(viewer)
}
func (m *historyManager) deleteExpiredJournal(ctx context.Context, viewer string, cutoff time.Time, pending bool) error {
	table, identity, created, condition := "exchanges", "rowid", "created", "account=?"
	if pending {
		table, identity, created, condition = "history_pending", "seq", "json_extract(payload,'$.createdAt')", "viewer=? AND kind='record' AND json_valid(payload)"
	}
	var after int64
	for {
		rows, err := m.journal.db.QueryContext(ctx, "SELECT "+identity+","+created+" FROM "+table+" WHERE "+condition+" AND "+identity+">? ORDER BY "+identity+" LIMIT 256", viewer, after)
		if err != nil {
			return err
		}
		var expired []any
		count := 0
		for rows.Next() {
			var id int64
			var raw sql.NullString
			if err = rows.Scan(&id, &raw); err != nil {
				break
			}
			after = id
			count++
			stamp, e := time.Parse(time.RFC3339Nano, raw.String)
			if e == nil && stamp.Before(cutoff) {
				expired = append(expired, id)
			}
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return err
		}
		if len(expired) > 0 {
			placeholders := strings.TrimSuffix(strings.Repeat("?,", len(expired)), ",")
			if _, err = m.journal.db.ExecContext(ctx, "DELETE FROM "+table+" WHERE "+identity+" IN ("+placeholders+")", expired...); err != nil {
				return err
			}
		}
		if count < historyDeleteBatch {
			return nil
		}
	}
}
func (m *historyManager) collectHistoryBlocks(viewer string) error {
	if viewer == "" || filepath.Base(viewer) != viewer {
		return errors.New("invalid history cache account")
	}
	directory := filepath.Join(m.home, ".newnoa", "history-blocks", viewer)
	if _, err := os.Lstat(directory); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if err := historyDirectory(directory); err != nil {
		return err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		// Blocks are a retry optimization only, and no sync/export/migration can be active under this lock.
		if len(name) == 64 && strings.Trim(name, "0123456789abcdef") == "" && entry.Type().IsRegular() {
			if err = os.Remove(filepath.Join(directory, name)); err != nil {
				return err
			}
		}
	}
	return nil
}
func (m *historyManager) runMaintenance(ctx context.Context, ticks <-chan time.Time, started chan<- struct{}) {
	run := func(now time.Time) {
		if err := m.maintain(ctx, now); err != nil && ctx.Err() == nil {
			fmt.Fprintln(os.Stderr, "Call record maintenance failed")
		}
	}
	run(time.Now())
	if started != nil {
		close(started)
	}
	backfill := func() {
		if err := m.compressHistoryBodies(ctx); err != nil && ctx.Err() == nil {
			fmt.Fprintln(os.Stderr, "Call record body migration failed")
		}
		for ctx.Err() == nil {
			done, err := m.journal.backfillConsumption(ctx)
			if err != nil {
				if ctx.Err() == nil {
					fmt.Fprintln(os.Stderr, "Call record consumption recovery failed")
				}
				return
			}
			if done {
				return
			}
		}
	}
	backfill()
	for {
		select {
		case <-ctx.Done():
			return
		case now, ok := <-ticks:
			if !ok {
				return
			}
			run(now)
			backfill()
		}
	}
}

// Backfill only fields missing from the stored snapshot; explicit API null/zero values remain authoritative.
func (s *sqlHistoryRepository) recoverUsage(ctx context.Context, viewer string) error {
	for _, scope := range []string{"own", "others"} {
		after := ""
		for {
			var id, payload, raw string
			where, args, err := s.where(ctx, viewer, historyQuery{Scope: scope}, false)
			if err != nil {
				return err
			}
			args = append(args, after)
			err = s.db.QueryRowContext(ctx, s.bind("SELECT id,payload,metadata FROM nn_history_records WHERE "+where+" AND id>? AND metadata NOT LIKE '%\"pointsStatus\"%' ORDER BY id LIMIT 1"), args...).Scan(&id, &payload, &raw)
			if err == sql.ErrNoRows {
				break
			}
			if err != nil {
				return err
			}
			record, decodeErr := s.bodies.Unpack(ctx, viewer, payload)
			var metadata map[string]any
			if decodeErr != nil || json.Unmarshal([]byte(raw), &metadata) != nil {
				return errors.New("invalid history record")
			}
			mergeHistoryTrustedMetadata(record, metadata)
			recoverHistoryUsage(record)
			if err = s.Enrich(ctx, viewer, scope, id, record); err != nil {
				return err
			}
			after = id
		}
	}
	return nil
}

type historyRetentionContextKey struct{}
