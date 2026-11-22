package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"modernc.org/sqlite"
)

// Match Go's Unicode lowering without storing another copy of request bodies.
var historySQLiteLowerError = sqlite.RegisterDeterministicScalarFunction("nn_history_lower", 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
	return strings.ToLower(historyText(args[0])), nil
})

type sqlHistoryRepository struct {
	db              *sql.DB
	engine, version string
	legacy          bool
	retentionDays   int
	table           string
	leaves          map[string]*sqlHistoryRepository
	bodies          *sqlHistoryBodyStore
}

func (s *sqlHistoryRepository) unpack(ctx context.Context, viewer, raw string) (map[string]any, error) {
	if s.bodies != nil {
		return s.bodies.Unpack(ctx, viewer, raw)
	}
	var value map[string]any
	e := json.Unmarshal([]byte(raw), &value)
	return value, e
}
func (s *sqlHistoryRepository) retentionCutoff(ctx context.Context) time.Time {
	return historyCutoff(historyRetentionTime(ctx), s.retentionDays)
}
func (s *sqlHistoryRepository) Version() string { return s.version }
func (s *sqlHistoryRepository) Close() error {
	if s.leaves != nil {
		for _, leaf := range s.leaves {
			if leaf.db != s.db {
				leaf.db.Close()
			}
		}
	}
	return s.db.Close()
}
func (s *sqlHistoryRepository) bind(query string) string {
	query = s.categorySQL(query)
	if s.engine != "postgres" {
		return query
	}
	var b strings.Builder
	n := 0
	for _, r := range query {
		if r == '?' {
			n++
			b.WriteString("$" + strconv.Itoa(n))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func openSQLHistoryTable(ctx context.Context, c historyDatabaseConfig, table string) (historyRepository, error) {
	driver, dsn := c.Engine, ""
	switch c.Engine {
	case "sqlite":
		if historySQLiteLowerError != nil {
			return nil, historySQLiteLowerError
		}
		if err := historyDirectory(c.SQLiteDirectory); err != nil {
			return nil, err
		}
		dsn = filepath.Join(c.SQLiteDirectory, "history.sqlite")
		if info, err := os.Lstat(dsn); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
			return nil, errors.New("unsafe history database file")
		} else if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	case "mysql":
		config := mysql.NewConfig()
		config.User = c.Username
		config.Passwd = c.Password
		config.Net = "tcp"
		config.Addr = historyEndpoint(c)
		config.DBName = c.Database
		config.ParseTime = true
		config.Timeout = 5 * time.Second
		config.ReadTimeout = 30 * time.Second
		config.WriteTimeout = 30 * time.Second
		config.Collation = "utf8mb4_bin"
		if c.TLS {
			config.TLSConfig = "true"
		}
		dsn = config.FormatDSN()
	case "postgres":
		driver = "pgx"
		u := url.URL{Scheme: "postgres", Host: historyEndpoint(c), Path: c.Database, User: url.UserPassword(c.Username, c.Password)}
		query := u.Query()
		query.Set("connect_timeout", "5")
		if c.TLS {
			query.Set("sslmode", "verify-full")
		} else {
			query.Set("sslmode", "disable")
		}
		u.RawQuery = query.Encode()
		dsn = u.String()
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, errors.New("history database cannot open")
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)
	if c.Engine == "sqlite" {
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(0)
		if _, err = db.ExecContext(ctx, "PRAGMA busy_timeout=5000; PRAGMA secure_delete=ON; PRAGMA auto_vacuum=INCREMENTAL;"); err != nil {
			db.Close()
			return nil, err
		}
	}
	s := &sqlHistoryRepository{db: db, engine: c.Engine, table: table, retentionDays: c.RetentionDays}
	fail := func(err error) (historyRepository, error) { db.Close(); return nil, err }
	bodyCategory := ""
	if table == "nn_history_text_records" {
		bodyCategory = "text"
	} else if table == "nn_history_image_records" {
		bodyCategory = "image"
	}
	s.bodies, err = newSQLHistoryBodyStore(ctx, db, c.Engine, bodyCategory)
	if err != nil {
		return fail(err)
	}
	if err = db.PingContext(ctx); err != nil {
		return fail(errors.New("history database connection failed"))
	}
	versionSQL := "SELECT version()"
	if c.Engine == "sqlite" {
		versionSQL = "SELECT sqlite_version()"
	}
	if err = db.QueryRowContext(ctx, versionSQL).Scan(&s.version); err != nil {
		return fail(errors.New("history database version unavailable"))
	}
	if c.Engine != "sqlite" {
		major, _ := strconv.Atoi(strings.Split(s.version, ".")[0])
		if c.Engine == "postgres" {
			parts := strings.Fields(s.version)
			if len(parts) > 1 {
				major, _ = strconv.Atoi(strings.Split(parts[1], ".")[0])
			}
		}
		if c.Engine == "mysql" && (major < 8 || strings.Contains(strings.ToLower(s.version), "mariadb")) || c.Engine == "postgres" && major < 16 {
			return fail(errors.New("unsupported history database version"))
		}
	}
	payloadType := "TEXT"
	keyType := "TEXT"
	if c.Engine == "mysql" {
		payloadType = "LONGTEXT"
		keyType = "VARCHAR(200)"
	}
	statements := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS nn_history_records (viewer %s NOT NULL,scope VARCHAR(10) NOT NULL,id %s NOT NULL,created VARCHAR(40) NOT NULL,storage_id %s NOT NULL,caller_id %s NOT NULL,key_name %s NOT NULL,payload %s NOT NULL,metadata %s NOT NULL,search_text %s NOT NULL,deleted INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(viewer,scope,id))`, keyType, keyType, keyType, keyType, keyType, payloadType, payloadType, payloadType),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS nn_history_candidates (viewer %s NOT NULL,id %s NOT NULL,email VARCHAR(254) NOT NULL,active INTEGER NOT NULL,PRIMARY KEY(viewer,id))`, keyType, keyType),
	}
	for _, stmt := range statements {
		if _, err = db.ExecContext(ctx, s.categorySQL(stmt)); err != nil {
			return fail(errors.New("history database initialization failed"))
		}
	}
	indexes := []string{"CREATE INDEX nn_history_page ON nn_history_records(viewer,scope,created,id)", "CREATE INDEX nn_history_caller ON nn_history_records(viewer,scope,caller_id,created)", "CREATE INDEX nn_history_key ON nn_history_records(viewer,scope,key_name,created)"}
	for _, stmt := range indexes {
		if c.Engine != "mysql" {
			stmt = strings.Replace(stmt, "CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1)
		}
		if _, err = db.ExecContext(ctx, s.categorySQL(stmt)); err != nil {
			var sqlErr *mysql.MySQLError
			if !errors.As(err, &sqlErr) || sqlErr.Number != 1061 {
				return fail(errors.New("history database index creation failed"))
			}
		}
	}
	if c.Engine == "sqlite" {
		if _, err = db.ExecContext(ctx, "PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL;"); err != nil {
			return fail(err)
		}
	}
	if c.Engine == "sqlite" {
		if err = os.Chmod(dsn, 0600); err != nil {
			return fail(err)
		}
	}
	if err = s.initializeBodies(ctx); err != nil {
		return fail(err)
	}
	if err = s.initializeFilters(ctx); err != nil {
		return fail(err)
	}
	return s, nil
}
func (s *sqlHistoryRepository) Save(ctx context.Context, viewer, scope string, value map[string]any) error {
	if s.leaves != nil {
		return s.saveCategory(ctx, viewer, scope, value)
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = s.saveRecord(ctx, viewer, scope, value, tx); e != nil {
		return e
	}
	return tx.Commit()
}
func (s *sqlHistoryRepository) saveRecord(ctx context.Context, viewer, scope string, value map[string]any, target historySQLExecutor) error {
	id := historyText(value["id"])
	stamp, err := time.Parse(time.RFC3339Nano, historyText(value["createdAt"]))
	if id == "" || err != nil || viewer == "" || (scope != "own" && scope != "others") {
		return errors.New("invalid history record")
	}
	if s.engine == "sqlite" && stamp.Before(s.retentionCutoff(ctx)) {
		return nil
	}
	recoverHistoryUsage(value)
	meta, err := json.Marshal(historyRecordMetadata(value))
	if err != nil {
		return err
	}
	storage := historyText(value["storageId"])
	if storage == "" {
		storage = id
	}
	stmt := `INSERT INTO nn_history_records(viewer,scope,id,created,storage_id,caller_id,key_name,payload,metadata,search_text,model_id,deployment_id,key_id,model_label,key_label,key_prefix,model_category,filter_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,2)`
	if s.engine == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE id=id"
	} else {
		stmt += " ON CONFLICT(viewer,scope,id) DO NOTHING"
	}
	args := []any{viewer, scope, id, stamp.UTC().Format("2006-01-02T15:04:05.000000000Z"), storage, historyText(value["callerAccountId"]), strings.ToLower(historyText(value["keyName"])), string(meta), string(meta), ""}
	args = append(args, historyFilterFields(value)...)
	inserted, err := target.ExecContext(ctx, s.bind(stmt), args...)
	if err != nil {
		return err
	}
	count, err := inserted.RowsAffected()
	if err != nil || count == 0 {
		return err
	}
	tx, ok := target.(*sql.Tx)
	if !ok {
		return errors.New("history body transaction required")
	}
	packed, err := s.bodies.PackTx(ctx, tx, "records", viewer, scope, id, value)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, s.bind("UPDATE nn_history_records SET payload=?,body_version=1 WHERE viewer=? AND scope=? AND id=?"), packed, viewer, scope, id)
	return err
}
func historyLike(s string) string {
	return "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(strings.ToLower(s)) + "%"
}
func (s *sqlHistoryRepository) where(ctx context.Context, viewer string, q historyQuery, cursor bool) (string, []any, error) {
	where := "viewer=? AND scope=? AND deleted=0"
	args := []any{viewer, q.Scope}
	if s.engine == "sqlite" && !s.legacy {
		where += " AND created>=?"
		args = append(args, s.retentionCutoff(ctx).Format("2006-01-02T15:04:05.000000000Z"))
	}
	searchColumn := "search_text"
	if s.engine == "sqlite" && !s.legacy && q.Q != "" {
		searchColumn = `(CASE WHEN search_text<>'' THEN search_text ELSE nn_history_lower(COALESCE(json_extract(payload,'$.request'),'')||char(10)||COALESCE(json_extract(payload,'$.response'),'')||char(10)||COALESCE(json_extract(payload,'$.model'),'')||char(10)||COALESCE(json_extract(payload,'$.modelName'),'')) END)`
	}
	for _, filter := range []struct{ column, value string }{{searchColumn, q.Q}, {"key_name", q.KeyName}} {
		if filter.value != "" {
			where += " AND " + filter.column + " LIKE ? ESCAPE '!'"
			args = append(args, historyLike(filter.value))
		}
	}
	for _, filter := range []struct{ column, value string }{{"caller_id", q.CallerID}, {"storage_id", q.StorageID}} {
		if filter.value != "" {
			where += " AND " + filter.column + "=?"
			args = append(args, filter.value)
		}
	}
	if q.ModelCategory != "" {
		where += " AND model_category=?"
		args = append(args, historyStorageLeaf(q.ModelCategory))
		where += s.speechFilter(q.ModelCategory)
	}
	if q.TestModelType != "" {
		agent, modelType := "json_extract(metadata,'$.agent')", "json_extract(metadata,'$.modelType')"
		switch s.engine {
		case "postgres":
			agent, modelType = "(metadata::jsonb ->> 'agent')", "(metadata::jsonb ->> 'modelType')"
		case "mysql":
			agent, modelType = "JSON_UNQUOTE(JSON_EXTRACT(metadata,'$.agent'))", "JSON_UNQUOTE(JSON_EXTRACT(metadata,'$.modelType'))"
		}
		where += " AND " + agent + "=? AND COALESCE(" + modelType + ",'') IN ('',?)"
		args = append(args, "NEWNOA Model Test", q.TestModelType)
		pairs := make([]string, 0, len(q.TestGrants))
		for _, grant := range q.TestGrants {
			pairs = append(pairs, "(key_id=? AND (deployment_id=? OR (deployment_id='' AND model_id=?)))")
			args = append(args, grant.KeyID, grant.Model, grant.Model)
		}
		where += " AND (" + strings.Join(pairs, " OR ") + ")"
	}
	if q.Model != "" {
		where += " AND (model_id=? OR deployment_id=?)"
		args = append(args, q.Model, q.Model)
	}
	if q.KeyID != "" {
		where += " AND key_id=?"
		args = append(args, q.KeyID)
	}
	for _, filter := range []struct{ op, value string }{{">=", q.From}, {"<=", q.To}} {
		if filter.value != "" {
			stamp, _ := time.Parse(time.RFC3339Nano, filter.value)
			where += " AND created" + filter.op + "?"
			args = append(args, stamp.UTC().Format("2006-01-02T15:04:05.000000000Z"))
		}
	}
	if cursor && q.Before != "" {
		stamp, id, err := parseHistoryCursor(q.Before)
		if err != nil {
			return "", nil, err
		}
		where += " AND (created<? OR (created=? AND id<?))"
		args = append(args, stamp, stamp, id)
	}
	return where, args, nil
}
func (s *sqlHistoryRepository) Query(ctx context.Context, viewer string, q historyQuery) (historyQueryResult, error) {
	if s.leaves != nil && q.ModelCategory != "" {
		if leaf := s.leaves[historyStorageLeaf(q.ModelCategory)]; leaf != nil {
			return leaf.Query(ctx, viewer, q)
		}
	}
	started := time.Now()
	result := historyQueryResult{Items: []map[string]any{}, Database: s.engine}
	if err := q.validate(); err != nil {
		return result, err
	}
	if s.bodies != nil && q.Q != "" {
		return s.queryBodies(ctx, viewer, q)
	}
	where, args, err := s.where(ctx, viewer, q, false)
	if err != nil {
		return result, err
	}
	if err = s.db.QueryRowContext(ctx, s.bind("SELECT count(*) FROM nn_history_records WHERE "+where), args...).Scan(&result.Total); err != nil {
		return result, err
	}
	where, args, err = s.where(ctx, viewer, q, true)
	if err != nil {
		return result, err
	}
	column := "metadata"
	if q.IncludeContent {
		column = "payload"
	}
	args = append(args, q.Limit+1)
	rows, err := s.db.QueryContext(ctx, s.bind("SELECT "+column+",created,id FROM nn_history_records WHERE "+where+" ORDER BY created DESC,id DESC LIMIT ?"), args...)
	if err != nil {
		return result, err
	}
	type candidate struct{ raw, stamp, id string }
	batch := []candidate{}
	for rows.Next() {
		var row candidate
		if err = rows.Scan(&row.raw, &row.stamp, &row.id); err != nil {
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
	var lastStamp, lastID string
	for _, row := range batch {
		if len(result.Items) == q.Limit {
			result.NextCursor = historyCursor(lastStamp, lastID)
			break
		}
		value, err := s.unpack(ctx, viewer, row.raw)
		if err != nil {
			return result, err
		}
		value["source"] = "local"
		value["database"] = s.engine
		result.Items = append(result.Items, value)
		lastStamp, lastID = row.stamp, row.id
	}
	if err = refreshHistorySummaries(ctx, s, viewer, q, result.Items); err != nil {
		return result, err
	}
	result.ElapsedMs = time.Since(started).Milliseconds()
	return result, nil
}
func (s *sqlHistoryRepository) Get(ctx context.Context, viewer, scope, id string) (map[string]any, error) {
	if s.leaves != nil {
		leaf, e := s.locateCategory(ctx, viewer, scope, id)
		if e != nil {
			return nil, e
		}
		return leaf.Get(ctx, viewer, scope, id)
	}
	var raw, metadata string
	where := "viewer=? AND scope=? AND id=? AND deleted=0"
	args := []any{viewer, scope, id}
	if s.engine == "sqlite" && !s.legacy {
		where += " AND created>=?"
		args = append(args, s.retentionCutoff(ctx).Format("2006-01-02T15:04:05.000000000Z"))
	}
	err := s.db.QueryRowContext(ctx, s.bind("SELECT payload,metadata FROM nn_history_records WHERE "+where), args...).Scan(&raw, &metadata)
	if err != nil {
		return nil, errors.New("history not found")
	}
	var value map[string]any
	value, err = s.unpack(ctx, viewer, raw)
	if err == nil {
		var meta map[string]any
		if err = json.Unmarshal([]byte(metadata), &meta); err == nil {
			mergeHistoryTrustedMetadata(value, meta)
		}
	}
	return value, err
}
func (s *sqlHistoryRepository) Delete(ctx context.Context, viewer, scope, id string) error {
	if s.leaves != nil {
		leaf, e := s.locateCategory(ctx, viewer, scope, id)
		if e == sql.ErrNoRows {
			return nil
		}
		if e != nil {
			return e
		}
		return leaf.Delete(ctx, viewer, scope, id)
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.ExecContext(ctx, s.bind("UPDATE nn_history_records SET deleted=1,payload=metadata,search_text='' WHERE viewer=? AND scope=? AND id=?"), viewer, scope, id); e != nil {
		return e
	}
	if s.bodies != nil {
		if e = s.bodies.DeleteRefsTx(ctx, tx, "records", viewer, scope, id); e != nil {
			return e
		}
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	return s.collectBodies(ctx)
}
func (s *sqlHistoryRepository) Expire(ctx context.Context, viewer string, days int) error {
	if s.leaves != nil {
		for _, leaf := range s.leaves {
			if e := leaf.Expire(ctx, viewer, days); e != nil {
				return e
			}
		}
		return nil
	}
	if s.engine == "sqlite" {
		return s.expireSQLite(ctx, viewer, historyCutoff(time.Now(), days))
	}
	if days == 0 {
		return nil
	}
	cut := time.Now().AddDate(0, 0, -days).UTC().Format("2006-01-02T15:04:05.000000000Z")
	_, err := s.db.ExecContext(ctx, s.bind("UPDATE nn_history_records SET deleted=1,payload=metadata,search_text='' WHERE viewer=? AND created<?"), viewer, cut)
	if err != nil {
		return err
	}
	return s.collectBodies(ctx)
}
func (s *sqlHistoryRepository) Candidate(ctx context.Context, viewer, id, email string, active bool) error {
	if len(email) > 254 || id == "" || viewer == "" {
		return errors.New("invalid history candidate")
	}
	stmt := "INSERT INTO nn_history_candidates(viewer,id,email,active) VALUES(?,?,?,?)"
	if s.engine == "mysql" {
		stmt += " ON DUPLICATE KEY UPDATE email=VALUES(email),active=VALUES(active)"
	} else {
		stmt += " ON CONFLICT(viewer,id) DO UPDATE SET email=excluded.email,active=excluded.active"
	}
	n := 0
	if active {
		n = 1
	}
	_, err := s.db.ExecContext(ctx, s.bind(stmt), viewer, id, strings.ToLower(email), n)
	return err
}
func (s *sqlHistoryRepository) Accounts(ctx context.Context, viewer, q string, limit int, before ...string) ([]map[string]any, error) {
	return s.accounts(ctx, viewer, q, limit, false, before...)
}
func (s *sqlHistoryRepository) CandidateStates(ctx context.Context, viewer, before string, limit int) ([]map[string]any, error) {
	return s.accounts(ctx, viewer, "", limit, true, before)
}
func (s *sqlHistoryRepository) accounts(ctx context.Context, viewer, q string, limit int, includeInactive bool, before ...string) ([]map[string]any, error) {
	if limit < 1 || limit > 100 || len(q) > 254 {
		return nil, errors.New("invalid account search")
	}
	where := "viewer=? AND email LIKE ? ESCAPE '!'"
	if !includeInactive {
		where += " AND active=1"
	}
	args := []any{viewer, historyLike(q)}
	if len(before) > 0 && before[0] != "" {
		email, id, err := parseHistoryCursor(before[0])
		if err != nil {
			return nil, err
		}
		where += " AND (email>? OR (email=? AND id>?))"
		args = append(args, email, email, id)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, s.bind("SELECT id,email,active FROM nn_history_candidates WHERE "+where+" ORDER BY email,id LIMIT ?"), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, email string
		var active bool
		if err = rows.Scan(&id, &email, &active); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": id, "email": email, "active": active})
	}
	return out, rows.Err()
}

func historyDuplicateIndex(err error) bool {
	var e *mysql.MySQLError
	return errors.As(err, &e) && e.Number == 1061
}

func (s *sqlHistoryRepository) speechFilter(category string) string {
	if category != "audio" && category != "text" {
		return ""
	}
	field := "json_extract(metadata,'$.modelType')"
	if s.engine == "postgres" {
		field = "metadata::jsonb ->> 'modelType'"
	} else if s.engine == "mysql" {
		field = "JSON_UNQUOTE(JSON_EXTRACT(metadata,'$.modelType'))"
	}
	if category == "audio" {
		return " AND (" + field + ")='asr'"
	}
	return " AND COALESCE(" + field + ",'')<>'asr'"
}
