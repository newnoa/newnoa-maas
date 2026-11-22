package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// Both journal and query records reference the same immutable SQLite body blocks.
type historyJournalDatabase struct {
	*sql.DB
	bodyDB *sql.DB
}

func (db *historyJournalDatabase) Close() error {
	return errors.Join(db.DB.Close(), db.bodyDB.Close())
}

func openJournalBodies(directory string) (*sql.DB, *sqlHistoryBodyStore, error) {
	path := filepath.Join(directory, "history.sqlite")
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return nil, nil, errors.New("unsafe history body database")
	} else if err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("PRAGMA busy_timeout=5000; PRAGMA secure_delete=ON; PRAGMA auto_vacuum=INCREMENTAL; PRAGMA journal_mode=WAL;"); err != nil {
		db.Close()
		return nil, nil, err
	}
	text, err := newSQLHistoryBodyStore(context.Background(), db, "sqlite", "text")
	var image *sqlHistoryBodyStore
	if err == nil {
		image, err = newSQLHistoryBodyStore(context.Background(), db, "sqlite", "image")
	}
	store := &sqlHistoryBodyStore{db: db, engine: "sqlite", categories: map[string]*sqlHistoryBodyStore{"text": text, "image": image}}
	if err == nil {
		err = os.Chmod(path, 0600)
	}
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	return db, store, nil
}

func (h *chatHistory) decodeBody(ctx context.Context, viewer, raw string) (map[string]any, error) {
	return h.bodies.Unpack(ctx, viewer, raw)
}

func (h *chatHistory) encodeBody(ctx context.Context, source, viewer, scope, id string, value map[string]any) (string, error) {
	// Same identity is immutable in body bytes; metadata enrichment must never unpin older content.
	query := "SELECT payload,deleted_at IS NOT NULL FROM exchanges WHERE account=? AND id=?"
	args := []any{viewer, id}
	if source == "pending" {
		query = "SELECT payload,0 FROM history_pending WHERE viewer=? AND scope=? AND id=? AND kind='record' ORDER BY seq DESC LIMIT 1"
		args = []any{viewer, scope, id}
	}
	var previous string
	var deleted bool
	err := h.db.QueryRowContext(ctx, query, args...).Scan(&previous, &deleted)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	if err == nil && deleted {
		return previous, nil
	}
	if err == nil {
		if source == "pending" {
			var ref struct {
				Category string `json:"categoryRef"`
			}
			if json.Unmarshal([]byte(previous), &ref) == nil && ref.Category != "" {
				var seq int64
				if e := h.db.QueryRowContext(ctx, "SELECT seq FROM history_pending WHERE viewer=? AND scope=? AND id=? AND kind='record' ORDER BY seq DESC LIMIT 1", viewer, scope, id).Scan(&seq); e != nil {
					return "", e
				}
				previous, err = resolveHistoryEvent(ctx, h.db.DB, seq, previous)
				if err != nil {
					return "", err
				}
			}
		}
		original, e := h.decodeBody(ctx, viewer, previous)
		if e != nil {
			return "", e
		}
		for _, field := range []string{"request", "response"} {
			if historyText(original[field]) != historyText(value[field]) {
				return "", errors.New("history body identity conflict")
			}
		}
	}
	return h.bodies.Pack(ctx, source, viewer, scope, id, value)
}

// Caller holds bodyMu: clear the durable header before releasing its cross-database pin.
func (h *chatHistory) retireDeletedJournalBodyLocked(ctx context.Context, viewer, id string) error {
	if _, err := h.db.ExecContext(ctx, "UPDATE exchanges SET payload=json_remove(payload,'$.request','$.response','$._newnoaBody') WHERE account=? AND id=? AND deleted_at IS NOT NULL AND json_valid(payload)", viewer, id); err != nil {
		return err
	}
	var active int
	if err := h.db.QueryRowContext(ctx, "SELECT count(*) FROM exchanges WHERE account=? AND id=? AND (deleted_at IS NULL OR NOT json_valid(payload))", viewer, id).Scan(&active); err != nil {
		return err
	}
	if active != 0 {
		return nil
	}
	return h.bodies.DeleteRefs(ctx, "journal", viewer, "own", id)
}

// Cursor commits follow verified body publication. Interrupted rows remain readable and retryable.
func (h *chatHistory) migrateJournalBodies(ctx context.Context, limit int) (int, error) {
	h.bodyMu.Lock()
	defer h.bodyMu.Unlock()
	if limit < 1 || limit > 256 {
		return 0, errors.New("invalid history body migration batch")
	}
	if _, err := h.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS history_body_migration (kind TEXT PRIMARY KEY,cursor INTEGER NOT NULL)"); err != nil {
		return 0, err
	}
	count := 0
	for _, kind := range []string{"exchanges", "history_pending"} {
		if kind == "history_pending" {
			var exists int
			if err := h.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='history_pending'").Scan(&exists); err != nil {
				return count, err
			}
			if exists == 0 {
				continue
			}
		}
		var after int64
		err := h.db.QueryRowContext(ctx, "SELECT cursor FROM history_body_migration WHERE kind=?", kind).Scan(&after)
		if err != nil && err != sql.ErrNoRows {
			return count, err
		}
		for count < limit {
			query := "SELECT rowid,account,'own',id,payload,deleted_at IS NOT NULL FROM exchanges WHERE rowid>? ORDER BY rowid LIMIT 1"
			source := "journal"
			if kind == "history_pending" {
				source = "pending"
				query = "SELECT seq,viewer,scope,id,payload,0 FROM history_pending WHERE seq>? AND kind='record' ORDER BY seq LIMIT 1"
			}
			var rowid int64
			var viewer, scope, id, raw string
			var deleted bool
			err = h.db.QueryRowContext(ctx, query, after).Scan(&rowid, &viewer, &scope, &id, &raw, &deleted)
			if err == sql.ErrNoRows {
				break
			}
			if err != nil {
				return count, err
			}
			if deleted {
				if err = h.retireDeletedJournalBodyLocked(ctx, viewer, id); err != nil {
					return count, err
				}
				if _, err = h.db.ExecContext(ctx, "INSERT INTO history_body_migration(kind,cursor) VALUES(?,?) ON CONFLICT(kind) DO UPDATE SET cursor=excluded.cursor", kind, rowid); err != nil {
					return count, err
				}
				after = rowid
				count++
				continue
			}
			originalRaw := raw
			if kind == "history_pending" {
				raw, err = resolveHistoryEvent(ctx, h.db.DB, rowid, raw)
				if err != nil {
					return count, err
				}

			}
			value, err := h.decodeBody(ctx, viewer, raw)
			if err != nil {
				return count, err
			}
			header, err := h.encodeBody(ctx, source, viewer, scope, id, value)
			if err != nil {
				return count, err
			}
			tx, err := h.db.BeginTx(ctx, nil)
			if err != nil {
				return count, err
			}
			if kind == "history_pending" && originalRaw != raw {
				_, err = tx.ExecContext(ctx, "UPDATE history_"+historyModelCategory(value)+"_pending_entities SET payload=? WHERE seq=? AND payload=?", header, rowid, raw)
			} else {
				_, err = tx.ExecContext(ctx, "UPDATE "+kind+" SET payload=? WHERE rowid=? AND payload=?", header, rowid, raw)
			}
			if err == nil {
				_, err = tx.ExecContext(ctx, "INSERT INTO history_body_migration(kind,cursor) VALUES(?,?) ON CONFLICT(kind) DO UPDATE SET cursor=excluded.cursor", kind, rowid)
			}
			if err != nil {
				tx.Rollback()
				return count, err
			}
			if err = tx.Commit(); err != nil {
				return count, err
			}
			after = rowid
			count++
		}
	}
	return count, nil
}

// Stale references are removed only after their journal rows have disappeared.
func (h *chatHistory) collectJournalBodies(ctx context.Context, limit int) error {
	for _, cat := range []string{"text", "image"} {
		if e := h.collectCategoryJournalBodies(ctx, limit, cat); e != nil {
			return e
		}
	}
	return nil
}
func (h *chatHistory) collectCategoryJournalBodies(ctx context.Context, limit int, category string) error {
	h.bodyMu.Lock()
	defer h.bodyMu.Unlock()
	if limit < 1 || limit > 256 {
		return errors.New("invalid history collection batch")
	}
	if _, err := h.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS history_body_gc_cursor (singleton INTEGER PRIMARY KEY,source TEXT NOT NULL,viewer TEXT NOT NULL,scope TEXT NOT NULL,id TEXT NOT NULL)"); err != nil {
		return err
	}
	var last [4]string
	err := h.db.QueryRowContext(ctx, "SELECT source,viewer,scope,id FROM history_body_gc_cursor WHERE singleton=?", map[string]int{"text": 1, "image": 2}[category]).Scan(&last[0], &last[1], &last[2], &last[3])
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	store := h.bodies.categories[category]
	rows, err := h.db.bodyDB.QueryContext(ctx, store.bind("SELECT source,viewer,scope,id FROM nn_history_body_refs WHERE source IN ('journal','pending') AND (source,viewer,scope,id)>(?,?,?,?) GROUP BY source,viewer,scope,id ORDER BY source,viewer,scope,id LIMIT ?"), last[0], last[1], last[2], last[3], limit)
	if err != nil {
		return err
	}
	type ref struct{ source, viewer, scope, id string }
	var refs []ref
	for rows.Next() {
		var r ref
		if err = rows.Scan(&r.source, &r.viewer, &r.scope, &r.id); err != nil {
			break
		}
		refs = append(refs, r)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	for _, r := range refs {
		if r.source == "journal" {
			if err = h.retireDeletedJournalBodyLocked(ctx, r.viewer, r.id); err != nil {
				return err
			}
		}
		query := "SELECT count(*) FROM exchanges WHERE account=? AND id=? AND (deleted_at IS NULL OR NOT json_valid(payload))"
		args := []any{r.viewer, r.id}
		if r.source == "pending" {
			query = "SELECT count(*) FROM history_pending WHERE viewer=? AND scope=? AND id=? AND kind='record'"
			args = []any{r.viewer, r.scope, r.id}
		}
		var count int
		if err = h.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			if err = store.DeleteRefs(ctx, r.source, r.viewer, r.scope, r.id); err != nil {
				return err
			}
		}
	}
	if len(refs) > 0 {
		r := refs[len(refs)-1]
		last = [4]string{r.source, r.viewer, r.scope, r.id}
	}
	if len(refs) < limit {
		last = [4]string{}
	}
	if _, err = h.db.ExecContext(ctx, "INSERT INTO history_body_gc_cursor(singleton,source,viewer,scope,id) VALUES(?,?,?,?,?) ON CONFLICT(singleton) DO UPDATE SET source=excluded.source,viewer=excluded.viewer,scope=excluded.scope,id=excluded.id", map[string]int{"text": 1, "image": 2}[category], last[0], last[1], last[2], last[3]); err != nil {
		return err
	}
	if err = store.Collect(ctx); err != nil {
		return err
	}
	return reclaimHistorySQLite(ctx, h.db.bodyDB)
}

func (h *chatHistory) decodePending(ctx context.Context, viewer, raw string) (string, error) {
	value, err := h.decodeBody(ctx, viewer, raw)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(value)
	return string(encoded), err
}

// Existing runtime serializes jobs with this mutex; each batch yields before the next one.
func (m *historyManager) compressHistoryBodies(ctx context.Context) error {
	if m.coordinator {
		m.childrenMu.RLock()
		children := make([]*historyManager, 0, len(m.children))
		for _, child := range m.children {
			children = append(children, child)
		}
		m.childrenMu.RUnlock()
		for _, child := range children {
			if err := child.compressHistoryBodies(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	for ctx.Err() == nil {
		m.mu.Lock()
		if m.busy != "" || m.config.Engine == "off" {
			m.mu.Unlock()
			return nil
		}
		count := 0
		switch store := m.store.(type) {
		case *sqlHistoryRepository:
			n, err := store.migrateBodiesBatch(ctx, 32)
			if err != nil {
				m.mu.Unlock()
				return err
			}
			count += n
			if err = store.collectBodies(ctx); err != nil {
				m.mu.Unlock()
				return err
			}
		case *mongoHistoryRepository:
			n, err := store.migrateBodies(ctx, 32)
			if err != nil {
				m.mu.Unlock()
				return err
			}
			count += n
			if _, err = store.collectBodies(ctx, 100); err != nil {
				m.mu.Unlock()
				return err
			}
		}
		n, err := m.journal.migrateJournalBodies(ctx, 32)
		count += n
		if err == nil {
			err = m.journal.collectJournalBodies(ctx, 100)
		}
		m.mu.Unlock()
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
	}
	return ctx.Err()
}

// Copy verified source blocks first; a failed journal cutover leaves the original pair intact.
func migrateJournalBodyDatabase(old *chatHistory, directory string) error {
	targetDB, target, err := openJournalBodies(directory)
	if err != nil {
		return err
	}
	defer targetDB.Close()
	var after int64
	for {
		var rowid int64
		var viewer, id, raw string
		err = old.db.QueryRow("SELECT rowid,account,id,payload FROM exchanges WHERE rowid>? ORDER BY rowid LIMIT 1", after).Scan(&rowid, &viewer, &id, &raw)
		if err == sql.ErrNoRows {
			break
		}
		if err != nil {
			return err
		}
		if historyBodyIsPacked(raw) {
			value, err := old.decodeBody(context.Background(), viewer, raw)
			if err != nil {
				return err
			}
			header, err := target.Pack(context.Background(), "journal", viewer, "own", id, value)
			if err != nil {
				return err
			}
			if header != raw {
				return errors.New("history journal body manifest changed during relocation")
			}
		}
		after = rowid
	}
	var exists int
	if err = old.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='history_pending'").Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return nil
	}
	after = 0
	for {
		var seq int64
		var viewer, scope, id, raw string
		err = old.db.QueryRow("SELECT seq,viewer,scope,id,payload FROM history_pending WHERE seq>? AND kind='record' ORDER BY seq LIMIT 1", after).Scan(&seq, &viewer, &scope, &id, &raw)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		raw, err = resolveHistoryEvent(context.Background(), old.db.DB, seq, raw)
		if err != nil {
			return err
		}
		if historyBodyIsPacked(raw) {
			value, e := old.decodeBody(context.Background(), viewer, raw)
			if e != nil {
				return e
			}
			header, e := target.Pack(context.Background(), "pending", viewer, scope, id, value)
			if e != nil {
				return e
			}
			if header != raw {
				return errors.New("pending body changed during relocation")
			}
		}
		after = seq
	}

}

// Upgrade published compressed journals before exposing category readers. The old
// blocks remain recovery material; only verified category manifests are published.
func (h *chatHistory) migrateLegacyJournalBodyCategories() error {
	ctx := context.Background()
	var exists int
	if err := h.db.bodyDB.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='nn_history_body_blocks'").Scan(&exists); err != nil || exists == 0 {
		return err
	}
	if _, err := h.db.bodyDB.Exec("CREATE TABLE IF NOT EXISTS nn_history_journal_body_category_migration(singleton INTEGER PRIMARY KEY)"); err != nil {
		return err
	}
	if err := h.db.bodyDB.QueryRow("SELECT count(*) FROM nn_history_journal_body_category_migration WHERE singleton=1").Scan(&exists); err != nil || exists != 0 {
		return err
	}
	legacy := &sqlHistoryBodyStore{db: h.db.bodyDB, engine: "sqlite", bind: func(q string) string { return q }}
	for _, kind := range []string{"journal", "pending"} {
		if kind == "pending" {
			if err := h.db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='history_pending'").Scan(&exists); err != nil {
				return err
			}
			if exists == 0 {
				continue
			}
		}
		var after int64
		for {
			query := "SELECT rowid,account,'own',id,payload FROM exchanges WHERE rowid>? ORDER BY rowid LIMIT 1"
			if kind == "pending" {
				query = "SELECT seq,viewer,scope,id,payload FROM history_pending WHERE seq>? AND kind='record' ORDER BY seq LIMIT 1"
			}
			var seq int64
			var viewer, scope, id, raw string
			err := h.db.QueryRow(query, after).Scan(&seq, &viewer, &scope, &id, &raw)
			if err == sql.ErrNoRows {
				break
			}
			if err != nil {
				return err
			}
			if kind == "pending" {
				raw, err = resolveHistoryEvent(ctx, h.db.DB, seq, raw)
				if err != nil {
					return err
				}
			}
			if historyBodyIsPacked(raw) {
				value, e := h.decodeBody(ctx, viewer, raw)
				if e != nil {
					value, e = legacy.Unpack(ctx, viewer, raw)
				}
				if e != nil {
					return e
				}
				header, e := h.bodies.Pack(ctx, kind, viewer, scope, id, value)
				if e != nil {
					return e
				}
				if header != raw {
					return errors.New("history body manifest changed during category upgrade")
				}
			}
			after = seq
		}
	}
	_, err := h.db.bodyDB.Exec("INSERT INTO nn_history_journal_body_category_migration(singleton) VALUES(1)")
	return err
}
