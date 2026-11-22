package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type historySQLExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *sqlHistoryRepository) categorySQL(query string) string {

	if s.table == "" || s.table == "nn_history_records" {
		return query
	}
	for _, prefix := range []string{"nn_history_filter_", "nn_history_category_", "nn_history_page", "nn_history_caller", "nn_history_key"} {
		query = strings.ReplaceAll(query, prefix, s.table+"_"+strings.TrimPrefix(prefix, "nn_history_"))
	}
	return strings.ReplaceAll(query, "nn_history_records", s.table)
}
func openSQLHistory(ctx context.Context, c historyDatabaseConfig) (historyRepository, error) {
	r, e := openSQLHistoryTable(ctx, c, "nn_history_text_records")
	if e != nil {
		return nil, e
	}
	text := r.(*sqlHistoryRepository)
	r, e = openSQLHistoryTable(ctx, c, "nn_history_image_records")
	if e != nil {
		text.Close()
		return nil, e
	}
	image := r.(*sqlHistoryRepository)
	s := &sqlHistoryRepository{db: text.db, engine: text.engine, version: text.version, retentionDays: text.retentionDays, bodies: &sqlHistoryBodyStore{db: text.db, engine: text.engine, categories: map[string]*sqlHistoryBodyStore{"text": text.bodies, "image": image.bodies}}, leaves: map[string]*sqlHistoryRepository{"text": text, "image": image}}
	if e = s.initializeCategoryStorage(ctx); e != nil {
		s.Close()
		return nil, e
	}
	return s, nil
}
func (s *sqlHistoryRepository) locateCategory(ctx context.Context, viewer, scope, id string) (*sqlHistoryRepository, error) {
	var cat string
	e := s.db.QueryRowContext(ctx, s.bind("SELECT model_category FROM nn_history_record_locations WHERE viewer=? AND scope=? AND id=?"), viewer, scope, id).Scan(&cat)
	if e != nil {
		return nil, e
	}
	leaf := s.leaves[cat]
	if leaf == nil {
		return nil, errors.New("invalid history category location")
	}
	return leaf, nil
}
func (s *sqlHistoryRepository) claimCategory(ctx context.Context, tx *sql.Tx, viewer, scope, id, cat string) error {
	stmt := "INSERT INTO nn_history_record_locations(viewer,scope,id,model_category) VALUES(?,?,?,?) ON CONFLICT(viewer,scope,id) DO NOTHING"
	if s.engine == "mysql" {
		stmt = "INSERT IGNORE INTO nn_history_record_locations(viewer,scope,id,model_category) VALUES(?,?,?,?)"
	}
	if _, e := tx.ExecContext(ctx, s.bind(stmt), viewer, scope, id, cat); e != nil {
		return e
	}
	var stored string
	if e := tx.QueryRowContext(ctx, s.bind("SELECT model_category FROM nn_history_record_locations WHERE viewer=? AND scope=? AND id=?"), viewer, scope, id).Scan(&stored); e != nil {
		return e
	}
	if stored != cat {
		return errors.New("history category cannot change")
	}
	return nil
}
func (s *sqlHistoryRepository) saveCategory(ctx context.Context, viewer, scope string, value map[string]any) error {
	cat := historyModelCategory(value)
	if raw, ok := value["modelCategory"]; ok && raw != nil && historyText(raw) != "text" && historyText(raw) != "image" {
		return errors.New("invalid history category")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = s.claimCategory(ctx, tx, viewer, scope, historyText(value["id"]), cat); e != nil {
		return e
	}
	if e = s.leaves[cat].saveRecord(ctx, viewer, scope, value, tx); e != nil {
		return e
	}
	return tx.Commit()
}

const historyRecordColumns = "viewer,scope,id,created,storage_id,caller_id,key_name,payload,metadata,search_text,deleted,model_id,deployment_id,key_id,model_label,key_label,key_prefix,model_category,filter_version,body_version"

func historyCategoryBatchSQL(after bool) string {
	where := "NOT EXISTS (SELECT 1 FROM nn_history_record_locations l WHERE l.viewer=r.viewer AND l.scope=r.scope AND l.id=r.id)"
	if after {
		where = "(r.viewer,r.scope,r.id)>(?,?,?) AND " + where
	}
	return "SELECT " + historyRecordColumns + " FROM nn_history_records r WHERE " + where + " ORDER BY viewer,scope,id LIMIT 64"
}

// Opening is the read barrier: no repository escapes before every committed batch
// is verified and the source is replaced by a storage-free compatibility view.
func (s *sqlHistoryRepository) initializeCategoryStorage(ctx context.Context) error {
	key := "TEXT"
	if s.engine == "mysql" {
		key = "VARCHAR(200)"
	}
	for _, stmt := range []string{fmt.Sprintf("CREATE TABLE IF NOT EXISTS nn_history_record_locations(viewer %s NOT NULL,scope VARCHAR(10) NOT NULL,id %s NOT NULL,model_category VARCHAR(10) NOT NULL,PRIMARY KEY(viewer,scope,id))", key, key), "CREATE TABLE IF NOT EXISTS nn_history_category_migration(name VARCHAR(40) PRIMARY KEY,generation INTEGER NOT NULL)"} {
		if _, e := s.db.ExecContext(ctx, stmt); e != nil {
			return e
		}
	}
	if s.engine == "sqlite" {
		for _, cat := range []string{"text", "image"} {
			table := "nn_history_" + cat + "_records"
			for _, event := range []string{"INSERT", "UPDATE"} {
				query := "CREATE TRIGGER IF NOT EXISTS " + table + "_identity_" + strings.ToLower(event) + " BEFORE " + event + " ON " + table + " BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM nn_history_record_locations l WHERE l.viewer=NEW.viewer AND l.scope=NEW.scope AND l.id=NEW.id AND l.model_category='" + cat + "') THEN RAISE(ABORT,'history category identity missing') END;END"
				if _, e := s.db.ExecContext(ctx, query); e != nil {
					return e
				}
			}
		}
	}
	// A DDL interruption after rename retains the full recovery source. Restore its
	// name before resuming; no caller can read while this opening barrier is active.
	var recovery int
	if s.engine == "sqlite" {
		if e := s.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='nn_history_records_recovery'").Scan(&recovery); e != nil {
			return e
		}
	} else {
		if e := s.db.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_name='nn_history_records_recovery' AND table_type='BASE TABLE' AND table_schema="+map[string]string{"postgres": "current_schema()", "mysql": "DATABASE()"}[s.engine]).Scan(&recovery); e != nil {
			return e
		}
	}
	if recovery > 0 {
		var exists int
		query := "SELECT count(*) FROM sqlite_master WHERE name='nn_history_records'"
		if s.engine != "sqlite" {
			query = "SELECT count(*) FROM information_schema.tables WHERE table_name='nn_history_records' AND table_schema=" + map[string]string{"postgres": "current_schema()", "mysql": "DATABASE()"}[s.engine]
		}
		if e := s.db.QueryRowContext(ctx, query).Scan(&exists); e != nil {
			return e
		}
		if exists == 0 {
			if _, e := s.db.ExecContext(ctx, "ALTER TABLE nn_history_records_recovery RENAME TO nn_history_records"); e != nil {
				return e
			}
		}
	}
	// Metadata catalogs distinguish a legacy table from the compatibility view.
	var kind string
	var e error
	if s.engine == "sqlite" {
		e = s.db.QueryRowContext(ctx, "SELECT type FROM sqlite_master WHERE name='nn_history_records'").Scan(&kind)
	} else {
		e = s.db.QueryRowContext(ctx, "SELECT table_type FROM information_schema.tables WHERE table_name='nn_history_records' AND table_schema="+map[string]string{"postgres": "current_schema()", "mysql": "DATABASE()"}[s.engine]).Scan(&kind)
	}
	if e != nil && e != sql.ErrNoRows {
		return e
	}
	if kind == "table" || kind == "BASE TABLE" {
		// Complete old projections before copying, including pre-category versions.
		old := &sqlHistoryRepository{db: s.db, engine: s.engine}
		if e = old.initializeBodies(ctx); e != nil {
			return e
		}
		if e = old.initializeFilters(ctx); e != nil {
			return e
		}
		insertStatements := map[string]*sql.Stmt{}
		for _, cat := range []string{"text", "image"} {
			query := "INSERT INTO nn_history_" + cat + "_records(" + historyRecordColumns + ") VALUES(" + strings.TrimSuffix(strings.Repeat("?,", 20), ",") + ") ON CONFLICT(viewer,scope,id) DO NOTHING"
			if s.engine == "mysql" {
				query = strings.Replace(query, "INSERT INTO", "INSERT IGNORE INTO", 1)
				query = strings.Split(query, " ON CONFLICT")[0]
			}
			statement, err := s.db.PrepareContext(ctx, s.bind(query))
			if err != nil {
				return err
			}
			defer statement.Close()
			insertStatements[cat] = statement
		}
		var after []any
		for {
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			inserts := map[string]*sql.Stmt{}
			for cat, statement := range insertStatements {
				inserts[cat] = tx.StmtContext(ctx, statement)
			}
			// A location is the resumable checkpoint and global ID binding. Source stays
			// intact until all bodies/metadata have been byte-verified in target leaves.
			rows, err := tx.QueryContext(ctx, s.bind(historyCategoryBatchSQL(len(after) != 0)), after...)
			if err != nil {
				tx.Rollback()
				return err
			}
			batch := [][]any{}
			batchBytes := 0
			for rows.Next() {
				values := make([]any, 20)
				dst := make([]any, 20)
				for i := range values {
					dst[i] = &values[i]
				}
				if err = rows.Scan(dst...); err != nil {
					break
				}
				for i, v := range values {
					if b, ok := v.([]byte); ok {
						values[i] = string(b)
					}
				}
				batch = append(batch, values)
				batchBytes += len(fmt.Sprint(values[7])) + len(fmt.Sprint(values[8]))
				if batchBytes >= 8<<20 {
					break
				}
			}
			rowErr := rows.Err()
			rows.Close()
			if err != nil || rowErr != nil {
				tx.Rollback()
				if err != nil {
					return err
				}
				return rowErr
			}
			for _, v := range batch {
				var meta map[string]any
				if err = json.Unmarshal([]byte(fmt.Sprint(v[8])), &meta); err != nil {
					break
				}
				cat := historyModelCategory(meta)
				v[17] = cat
				if err = s.claimCategory(ctx, tx, fmt.Sprint(v[0]), fmt.Sprint(v[1]), fmt.Sprint(v[2]), cat); err != nil {
					break
				}
				if historyBodyIsPacked(fmt.Sprint(v[7])) {
					value, decodeErr := historyBodyDecode(fmt.Sprint(v[7]), func(hash string) (historyBodyBlock, error) {
						var block historyBodyBlock
						e := tx.QueryRowContext(ctx, s.bind("SELECT hash,raw_size,data FROM nn_history_body_blocks WHERE viewer=? AND hash=?"), v[0], hash).Scan(&block.Hash, &block.RawSize, &block.Data)
						return block, e
					})
					if decodeErr != nil {
						err = decodeErr
						break
					}
					v[7], err = s.leaves[cat].bodies.PackTx(ctx, tx, "records", fmt.Sprint(v[0]), fmt.Sprint(v[1]), fmt.Sprint(v[2]), value)
					if err != nil {
						break
					}
				}
				if _, err = inserts[cat].ExecContext(ctx, v...); err != nil {
					break
				}
				var body, metadata string
				err = tx.QueryRowContext(ctx, s.bind("SELECT payload,metadata FROM nn_history_"+cat+"_records WHERE viewer=? AND scope=? AND id=?"), v[0], v[1], v[2]).Scan(&body, &metadata)
				if err != nil {
					break
				}
				if body != fmt.Sprint(v[7]) || metadata != fmt.Sprint(v[8]) {
					err = errors.New("history migration body verification failed")
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
			if len(batch) == 0 {
				break
			}
			// Advance only after commit; reopening resumes from the durable locations.
			last := batch[len(batch)-1]
			after = []any{last[0], last[1], last[2]}
		}
		if e = s.verifyCategoryMigration(ctx); e != nil {
			return e
		}

		if s.engine != "mysql" {
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			statements := []string{"ALTER TABLE nn_history_records RENAME TO nn_history_records_recovery", "CREATE VIEW nn_history_records AS SELECT " + historyRecordColumns + " FROM nn_history_text_records UNION ALL SELECT " + historyRecordColumns + " FROM nn_history_image_records", "INSERT INTO nn_history_category_migration(name,generation) VALUES('records',1) ON CONFLICT(name) DO UPDATE SET generation=excluded.generation"}
			if s.engine != "sqlite" {
				statements = append(statements, "DROP TABLE nn_history_records_recovery")
			}
			for _, statement := range statements {
				if _, err = tx.ExecContext(ctx, statement); err != nil {
					return err
				}
			}
			if err = tx.Commit(); err != nil {
				return err
			}
			if err = s.createCategoryViewTriggers(ctx); err != nil {
				return err
			}
			return s.retireCategoryRecovery(ctx)
		}
		if _, e = s.db.ExecContext(ctx, "ALTER TABLE nn_history_records RENAME TO nn_history_records_recovery"); e != nil {
			return e
		}
	}
	if kind == "view" || kind == "VIEW" {
		if e = s.createCategoryViewTriggers(ctx); e != nil {
			return e
		}
		return s.retireCategoryRecovery(ctx)
	}
	if _, e = s.db.ExecContext(ctx, "CREATE VIEW nn_history_records AS SELECT "+historyRecordColumns+" FROM nn_history_text_records UNION ALL SELECT "+historyRecordColumns+" FROM nn_history_image_records"); e != nil {
		return e
	}
	stmt := "INSERT INTO nn_history_category_migration(name,generation) VALUES('records',1) ON CONFLICT(name) DO UPDATE SET generation=excluded.generation"
	if s.engine == "mysql" {
		stmt = "INSERT INTO nn_history_category_migration(name,generation) VALUES('records',1) ON DUPLICATE KEY UPDATE generation=VALUES(generation)"
	}
	_, e = s.db.ExecContext(ctx, stmt)
	if e == nil {
		e = s.createCategoryViewTriggers(ctx)
	}
	if e == nil {
		_, e = s.db.ExecContext(ctx, "DROP TABLE IF EXISTS nn_history_records_recovery")
	}
	return e
}

func (s *sqlHistoryRepository) retireCategoryRecovery(ctx context.Context) error {
	if s.engine == "sqlite" {
		var exists int
		if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='nn_history_records_recovery'").Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			// Securely erasing a multi-GB copy in DROP can repeatedly roll back at the initialization deadline.
			if err := deleteHistoryBatches(ctx, s.db, "DELETE FROM nn_history_records_recovery WHERE rowid IN (SELECT rowid FROM nn_history_records_recovery LIMIT 256)"); err != nil {
				return err
			}
		}
	}
	_, err := s.db.ExecContext(ctx, "DROP TABLE IF EXISTS nn_history_records_recovery")
	return err
}

func (s *sqlHistoryRepository) createCategoryViewTriggers(ctx context.Context) error {
	if s.engine != "sqlite" {
		return nil
	}
	columns := strings.Split(historyRecordColumns, ",")
	values := []string{}
	updates := []string{}
	for _, c := range columns {
		v := "NEW." + c
		if c == "model_category" {
			v = "COALESCE(NULLIF(NEW.model_category,''),CASE WHEN json_extract(NEW.metadata,'$.modelCategory')='image' OR (json_extract(NEW.metadata,'$.modelCategory') IS NULL AND json_extract(NEW.metadata,'$.modelType')='image') THEN 'image' ELSE 'text' END)"
		}
		if c == "filter_version" || c == "deleted" || c == "body_version" {
			v = "COALESCE(NEW." + c + ",0)"
		}
		if c == "model_id" || c == "deployment_id" || c == "key_id" || c == "model_label" || c == "key_label" || c == "key_prefix" {
			v = "COALESCE(NEW." + c + ",'')"
		}
		values = append(values, v)
		updates = append(updates, c+"=NEW."+c)
	}
	for _, op := range []string{"INSERT", "UPDATE", "DELETE"} {
		query := "CREATE TRIGGER IF NOT EXISTS nn_history_category_" + strings.ToLower(op) + " INSTEAD OF " + op + " ON nn_history_records BEGIN "
		if op == "UPDATE" {
			query += "SELECT CASE WHEN NEW.viewer<>OLD.viewer OR NEW.scope<>OLD.scope OR NEW.id<>OLD.id OR COALESCE(NEW.model_category,'text')<>COALESCE(OLD.model_category,'text') THEN RAISE(ABORT,'history identity/category cannot change') END;"
		}
		if op == "INSERT" {
			query += "INSERT OR IGNORE INTO nn_history_record_locations(viewer,scope,id,model_category) VALUES(NEW.viewer,NEW.scope,NEW.id," + values[17] + "); SELECT CASE WHEN (SELECT model_category FROM nn_history_record_locations WHERE viewer=NEW.viewer AND scope=NEW.scope AND id=NEW.id)<>" + values[17] + " THEN RAISE(ABORT,'history category cannot change') END;"
		}
		for _, cat := range []string{"text", "image"} {
			table := "nn_history_" + cat + "_records"
			switch op {
			case "INSERT":
				query += "INSERT OR IGNORE INTO " + table + "(" + historyRecordColumns + ") SELECT " + strings.Join(values, ",") + " WHERE " + values[17] + "='" + cat + "';"
			case "UPDATE":
				query += "UPDATE " + table + " SET " + strings.Join(updates, ",") + " WHERE viewer=OLD.viewer AND scope=OLD.scope AND id=OLD.id;"
			case "DELETE":
				query += "DELETE FROM " + table + " WHERE viewer=OLD.viewer AND scope=OLD.scope AND id=OLD.id;"
			}
		}
		query += " END"
		if _, e := s.db.ExecContext(ctx, query); e != nil {
			return e
		}
	}
	return nil
}

// Join only the small identity directory, then point-read each body by its PK.
// A UNION of payload columns forces SQLite to materialize and index gigabytes.
func historyCategoryVerificationSQL(category string) string {
	return "SELECT count(*) FROM nn_history_record_locations l JOIN nn_history_records r ON r.viewer=l.viewer AND r.scope=l.scope AND r.id=l.id LEFT JOIN nn_history_" + category + "_records t ON t.viewer=l.viewer AND t.scope=l.scope AND t.id=l.id WHERE l.model_category='" + category + "' AND (t.id IS NULL OR t.payload<>r.payload OR t.metadata<>r.metadata)"
}
func (s *sqlHistoryRepository) verifyCategoryMigration(ctx context.Context) error {
	var missing int
	if e := s.db.QueryRowContext(ctx, "SELECT count(*) FROM nn_history_records r WHERE NOT EXISTS(SELECT 1 FROM nn_history_record_locations l WHERE l.viewer=r.viewer AND l.scope=r.scope AND l.id=r.id AND l.model_category IN ('text','image'))").Scan(&missing); e != nil {
		return e
	}
	if missing != 0 {
		return errors.New("history migration identity verification failed")
	}
	for _, category := range []string{"text", "image"} {
		var mismatch int
		if e := s.db.QueryRowContext(ctx, historyCategoryVerificationSQL(category)).Scan(&mismatch); e != nil {
			return e
		}
		if mismatch != 0 {
			return errors.New("history migration final verification failed")
		}
	}
	return nil
}
