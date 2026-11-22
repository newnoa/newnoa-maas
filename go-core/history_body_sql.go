package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type sqlHistoryBodyStore struct {
	db         *sql.DB
	engine     string
	bind       func(string) string
	category   string
	categories map[string]*sqlHistoryBodyStore
}

func newSQLHistoryBodyStore(ctx context.Context, db *sql.DB, engine string, category ...string) (*sqlHistoryBodyStore, error) {
	s := &sqlHistoryBodyStore{db: db, engine: engine}
	if len(category) > 0 {
		s.category = category[0]
	}
	if engine == "sqlite" {
		if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
			return nil, err
		}
	}
	s.bind = func(q string) string {
		if s.category != "" {
			q = strings.ReplaceAll(q, "nn_history_body_", "nn_history_"+s.category+"_body_")
		}
		if engine != "postgres" {
			return q
		}
		var b strings.Builder
		n := 0
		for _, r := range q {
			if r == '?' {
				n++
				fmt.Fprintf(&b, "$%d", n)
			} else {
				b.WriteRune(r)
			}
		}
		return b.String()
	}
	blockType := "BLOB"
	if engine == "postgres" {
		blockType = "BYTEA"
	}
	if engine == "mysql" {
		blockType = "LONGBLOB"
	}
	keyType := "TEXT"
	if engine == "mysql" {
		keyType = "VARCHAR(200) COLLATE utf8mb4_bin"
	}
	stmts := []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS nn_history_body_blocks (viewer %s NOT NULL, hash VARCHAR(64) NOT NULL, raw_size INTEGER NOT NULL, data %s NOT NULL, PRIMARY KEY(viewer,hash))", keyType, blockType),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS nn_history_body_refs (source VARCHAR(16) NOT NULL, viewer %s NOT NULL, scope VARCHAR(16) NOT NULL, id %s NOT NULL, field VARCHAR(16) NOT NULL, ordinal INTEGER NOT NULL, hash VARCHAR(64) NOT NULL, raw_size INTEGER NOT NULL, PRIMARY KEY(source,viewer,scope,id,field,ordinal), FOREIGN KEY(viewer,hash) REFERENCES nn_history_body_blocks(viewer,hash))", keyType, keyType),
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, s.bind(stmt)); err != nil {
			return nil, err
		}
	}
	exists := "IF NOT EXISTS "
	if engine == "mysql" {
		exists = ""
	}
	if _, err := db.ExecContext(ctx, s.bind("CREATE INDEX "+exists+"nn_history_body_refs_hash ON nn_history_body_refs(viewer,hash)")); err != nil && !historyDuplicateIndex(err) {
		return nil, err
	}
	return s, nil
}
func (s *sqlHistoryBodyStore) Pack(ctx context.Context, source, viewer, scope, id string, value map[string]any) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	header, err := s.pack(ctx, tx, source, viewer, scope, id, value)
	if err != nil {
		return "", err
	}
	if err = tx.Commit(); err != nil {
		return "", err
	}
	return header, nil
}
func (s *sqlHistoryBodyStore) PackTx(ctx context.Context, tx *sql.Tx, source, viewer, scope, id string, value map[string]any) (string, error) {
	if tx == nil {
		return "", errors.New("history body transaction required")
	}
	return s.pack(ctx, tx, source, viewer, scope, id, value)
}
func (s *sqlHistoryBodyStore) pack(ctx context.Context, tx *sql.Tx, source, viewer, scope, id string, value map[string]any) (string, error) {
	if s.categories != nil {
		return s.categories[historyModelCategory(value)].pack(ctx, tx, source, viewer, scope, id, value)
	}
	header, blocks, err := historyBodyEncode(value)
	if err != nil {
		return "", err
	}
	exec := func(q string, args ...any) error {
		q = s.bind(q)
		if tx != nil {
			_, err := tx.ExecContext(ctx, q, args...)
			return err
		}
		_, err := s.db.ExecContext(ctx, q, args...)
		return err
	}
	if err = exec("DELETE FROM nn_history_body_refs WHERE source=? AND viewer=? AND scope=? AND id=?", source, viewer, scope, id); err != nil {
		return "", err
	}
	blockSQL := "INSERT INTO nn_history_body_blocks(viewer,hash,raw_size,data) VALUES(?,?,?,?) ON CONFLICT(viewer,hash) DO NOTHING"
	if s.engine == "mysql" {
		blockSQL = "INSERT INTO nn_history_body_blocks(viewer,hash,raw_size,data) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE hash=hash"
	}
	for _, block := range blocks {
		if err = exec(blockSQL, viewer, block.Hash, block.RawSize, block.Data); err != nil {
			return "", err
		}
	}
	if _, err = historyBodyDecode(header, func(hash string) (historyBodyBlock, error) {
		var b historyBodyBlock
		err := tx.QueryRowContext(ctx, s.bind("SELECT hash,raw_size,data FROM nn_history_body_blocks WHERE viewer=? AND hash=?"), viewer, hash).Scan(&b.Hash, &b.RawSize, &b.Data)
		return b, err
	}); err != nil {
		return "", err
	}
	var manifest struct {
		Fields map[string]struct {
			Parts []struct {
				Hash string `json:"sha256"`
				Size int    `json:"size"`
			} `json:"parts"`
		} `json:"fields"`
	}
	var record map[string]json.RawMessage
	if err = json.Unmarshal([]byte(header), &record); err != nil {
		return "", err
	}
	if err = json.Unmarshal(record[historyBodyMarker], &manifest); err != nil {
		return "", err
	}
	for field, entry := range manifest.Fields {
		for i, part := range entry.Parts {
			if err = exec("INSERT INTO nn_history_body_refs(source,viewer,scope,id,field,ordinal,hash,raw_size) VALUES(?,?,?,?,?,?,?,?)", source, viewer, scope, id, field, i, part.Hash, part.Size); err != nil {
				return "", err
			}
		}
	}
	return header, nil
}
func (s *sqlHistoryBodyStore) Unpack(ctx context.Context, viewer, header string) (map[string]any, error) {
	if s.categories != nil {
		var value map[string]any
		if e := json.Unmarshal([]byte(header), &value); e != nil {
			return nil, e
		}
		return s.categories[historyModelCategory(value)].Unpack(ctx, viewer, header)
	}
	return historyBodyDecode(header, func(hash string) (historyBodyBlock, error) {
		var b historyBodyBlock
		err := s.db.QueryRowContext(ctx, s.bind("SELECT hash,raw_size,data FROM nn_history_body_blocks WHERE viewer=? AND hash=?"), viewer, hash).Scan(&b.Hash, &b.RawSize, &b.Data)
		if err != nil {
			return b, errors.New("history body block unavailable")
		}
		return b, nil
	})
}
func (s *sqlHistoryBodyStore) DeleteRefs(ctx context.Context, source, viewer, scope, id string) error {
	if s.categories != nil {
		for _, store := range s.categories {
			if e := store.DeleteRefs(ctx, source, viewer, scope, id); e != nil {
				return e
			}
		}
		return nil
	}
	_, err := s.db.ExecContext(ctx, s.bind("DELETE FROM nn_history_body_refs WHERE source=? AND viewer=? AND scope=? AND id=?"), source, viewer, scope, id)
	return err
}
func (s *sqlHistoryBodyStore) DeleteRefsTx(ctx context.Context, tx *sql.Tx, source, viewer, scope, id string) error {
	_, err := tx.ExecContext(ctx, s.bind("DELETE FROM nn_history_body_refs WHERE source=? AND viewer=? AND scope=? AND id=?"), source, viewer, scope, id)
	return err
}
func (s *sqlHistoryBodyStore) Collect(ctx context.Context) error {
	if s.categories != nil {
		for _, store := range s.categories {
			if e := store.Collect(ctx); e != nil {
				return e
			}
		}
		return nil
	}
	for {
		rows, err := s.db.QueryContext(ctx, s.bind("SELECT viewer,hash FROM nn_history_body_blocks b WHERE NOT EXISTS (SELECT 1 FROM nn_history_body_refs r WHERE r.viewer=b.viewer AND r.hash=b.hash) LIMIT 256"))
		if err != nil {
			return err
		}
		type identity struct{ viewer, hash string }
		batch := []identity{}
		for rows.Next() {
			var row identity
			if err = rows.Scan(&row.viewer, &row.hash); err != nil {
				break
			}
			batch = append(batch, row)
		}
		if err == nil {
			err = rows.Err()
		}
		closeErr := rows.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if len(batch) == 0 {
			return nil
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for _, row := range batch {
			if _, err = tx.ExecContext(ctx, s.bind("DELETE FROM nn_history_body_blocks WHERE viewer=? AND hash=? AND NOT EXISTS (SELECT 1 FROM nn_history_body_refs r WHERE r.viewer=nn_history_body_blocks.viewer AND r.hash=nn_history_body_blocks.hash)"), row.viewer, row.hash); err != nil {
				break
			}
		}
		if err != nil {
			tx.Rollback()
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
}

func (s *sqlHistoryRepository) queryBodies(ctx context.Context, viewer string, q historyQuery) (historyQueryResult, error) {
	started := time.Now()
	result := historyQueryResult{Items: []map[string]any{}, Database: s.engine}
	search := q.Q
	q.Q = ""
	originalCursor := q.Before
	q.Before = ""
	cursorStamp, cursorID := "", ""
	if originalCursor != "" {
		var err error
		cursorStamp, cursorID, err = parseHistoryCursor(originalCursor)
		if err != nil {
			return result, err
		}
	}
	var lastStamp, lastID string
	for {
		where, args, err := s.where(ctx, viewer, q, true)
		if err != nil {
			return result, err
		}
		args = append(args, 100)
		rows, err := s.db.QueryContext(ctx, s.bind("SELECT payload,metadata,search_text,created,id FROM nn_history_records WHERE "+where+" ORDER BY created DESC,id DESC LIMIT ?"), args...)
		if err != nil {
			return result, err
		}
		type candidate struct{ payload, metadata, search, stamp, id string }
		batch := []candidate{}
		for rows.Next() {
			var row candidate
			if err = rows.Scan(&row.payload, &row.metadata, &row.search, &row.stamp, &row.id); err != nil {
				break
			}
			batch = append(batch, row)
		}
		if err == nil {
			err = rows.Err()
		}
		closeErr := rows.Close()
		if err != nil {
			return result, err
		}
		if closeErr != nil {
			return result, closeErr
		}
		for _, row := range batch {
			q.Before = historyCursor(row.stamp, row.id)
			value, err := s.unpack(ctx, viewer, row.payload)
			if err != nil {
				return result, err
			}
			var meta map[string]any
			if err = json.Unmarshal([]byte(row.metadata), &meta); err != nil {
				return result, err
			}
			mergeHistoryTrustedMetadata(value, meta)
			matches := historyBodySearch(value, search)
			if !s.legacy && row.search != "" {
				matches = strings.Contains(row.search, strings.ToLower(search))
			}
			if !matches {
				continue
			}
			result.Total++
			if originalCursor != "" && (row.stamp > cursorStamp || row.stamp == cursorStamp && row.id >= cursorID) {
				continue
			}
			if len(result.Items) >= q.Limit {
				if result.NextCursor == "" {
					result.NextCursor = historyCursor(lastStamp, lastID)
				}
				continue
			}
			if !q.IncludeContent {
				value = meta
			}
			value["source"] = "local"
			value["database"] = s.engine
			result.Items = append(result.Items, value)
			lastStamp, lastID = row.stamp, row.id
		}
		if len(batch) < 100 {
			break
		}
	}
	if err := refreshHistorySummaries(ctx, s, viewer, q, result.Items); err != nil {
		return result, err
	}
	result.ElapsedMs = time.Since(started).Milliseconds()
	return result, nil
}

func (s *sqlHistoryRepository) initializeBodies(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, s.categorySQL("SELECT body_version FROM nn_history_records LIMIT 0"))
	if err != nil {
		if _, err = s.db.ExecContext(ctx, s.categorySQL("ALTER TABLE nn_history_records ADD COLUMN body_version INTEGER NOT NULL DEFAULT 0")); err != nil {
			return err
		}
	} else {
		rows.Close()
	}
	exists := "IF NOT EXISTS "
	if s.engine == "mysql" {
		exists = ""
	}
	if _, err = s.db.ExecContext(ctx, s.categorySQL("CREATE INDEX "+exists+s.table+"_body_migration ON nn_history_records(body_version)")); err != nil && !historyDuplicateIndex(err) {
		return err
	}
	return nil
}

func (s *sqlHistoryRepository) migrateBodies(ctx context.Context) error {
	for {
		count, err := s.migrateBodiesBatch(ctx, 100)
		if err != nil {
			return err
		}
		if count == 0 {
			return s.collectBodies(ctx)
		}
	}
}

// The caller schedules batches; opening a large legacy database remains inexpensive.
func (s *sqlHistoryRepository) migrateBodiesBatch(ctx context.Context, limit int) (int, error) {
	if s.leaves != nil {
		total := 0
		for _, cat := range []string{"text", "image"} {
			n, e := s.leaves[cat].migrateBodiesBatch(ctx, limit)
			total += n
			if e != nil {
				return total, e
			}
		}
		return total, nil
	}
	if limit < 1 || limit > 100 {
		return 0, errors.New("invalid history body migration batch")
	}
	rows, err := s.db.QueryContext(ctx, s.bind("SELECT viewer,scope,id,payload,metadata,deleted FROM nn_history_records WHERE body_version=0 LIMIT ?"), limit)
	if err != nil {
		return 0, err
	}
	type pending struct {
		viewer, scope, id, payload, metadata string
		deleted                              bool
	}
	batch := []pending{}
	bytes := 0
	for rows.Next() {
		var row pending
		if err = rows.Scan(&row.viewer, &row.scope, &row.id, &row.payload, &row.metadata, &row.deleted); err != nil {
			break
		}
		batch = append(batch, row)
		bytes += len(row.payload) + len(row.metadata)
		if bytes >= 4*1024*1024 {
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	closeErr := rows.Close()
	if err != nil {
		return 0, err
	}
	if closeErr != nil {
		return 0, closeErr
	}
	if len(batch) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, row := range batch {
		payload := row.metadata
		if !row.deleted {
			value, err := historyBodyDecode(row.payload, func(hash string) (historyBodyBlock, error) {
				var b historyBodyBlock
				err := tx.QueryRowContext(ctx, s.bodies.bind("SELECT hash,raw_size,data FROM nn_history_body_blocks WHERE viewer=? AND hash=?"), row.viewer, hash).Scan(&b.Hash, &b.RawSize, &b.Data)
				return b, err
			})
			if err != nil {
				return 0, err
			}
			payload, err = s.bodies.PackTx(ctx, tx, "records", row.viewer, row.scope, row.id, value)
			if err != nil {
				return 0, err
			}
		}
		if _, err = tx.ExecContext(ctx, s.bind("UPDATE nn_history_records SET payload=?,search_text='',body_version=1 WHERE viewer=? AND scope=? AND id=? AND body_version=0"), payload, row.viewer, row.scope, row.id); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return len(batch), nil
}

func (s *sqlHistoryRepository) collectBodies(ctx context.Context) error {
	if s.leaves != nil {
		for _, leaf := range s.leaves {
			if e := leaf.collectBodies(ctx); e != nil {
				return e
			}
		}
		return nil
	}
	if s.bodies == nil {
		return nil
	}
	// Journal and pending references share the table and have independent lifetimes.
	_, err := s.db.ExecContext(ctx, s.bodies.bind(s.categorySQL("DELETE FROM nn_history_body_refs WHERE source='records' AND NOT EXISTS (SELECT 1 FROM nn_history_records r WHERE r.viewer=nn_history_body_refs.viewer AND r.scope=nn_history_body_refs.scope AND r.id=nn_history_body_refs.id AND r.deleted=0)")))
	if err != nil {
		return err
	}
	return s.bodies.Collect(ctx)
}
