package main

import (
	"context"
	"fmt"
)

var historyFilterColumns = []string{"model_id", "deployment_id", "key_id", "model_label", "key_label", "key_prefix", "model_category"}

func (s *sqlHistoryRepository) initializeFilters(ctx context.Context) error {
	if s.leaves != nil {
		for _, leaf := range s.leaves {
			if e := leaf.initializeFilters(ctx); e != nil {
				return e
			}
		}
		return nil
	}
	kind := "TEXT"
	if s.engine == "mysql" {
		kind = "VARCHAR(200)"
	}
	for _, column := range append(append([]string{}, historyFilterColumns...), "filter_version") {
		rows, err := s.db.QueryContext(ctx, s.categorySQL("SELECT "+column+" FROM nn_history_records LIMIT 0"))
		if err == nil {
			rows.Close()
			continue
		}
		columnKind := kind
		if column == "model_label" || column == "key_label" || column == "key_prefix" {
			columnKind = "TEXT"
		}
		definition := columnKind
		if s.engine == "mysql" {
			definition += " COLLATE utf8mb4_bin"
		}
		definition += " NOT NULL"
		if columnKind != "TEXT" || s.engine != "mysql" {
			definition += " DEFAULT ''"
		}
		if column == "filter_version" {
			definition = "INTEGER NOT NULL DEFAULT 0"
		}
		if _, err = s.db.ExecContext(ctx, s.categorySQL("ALTER TABLE nn_history_records ADD COLUMN "+column+" "+definition)); err != nil {
			return err
		}
	}
	exists := ""
	if s.engine != "mysql" {
		exists = "IF NOT EXISTS "
	}
	if _, err := s.db.ExecContext(ctx, s.categorySQL("CREATE INDEX "+exists+"nn_history_filter_backfill ON nn_history_records(filter_version)")); err != nil && !historyDuplicateIndex(err) {
		return err
	}
	for {
		rows, err := s.db.QueryContext(ctx, s.categorySQL("SELECT viewer,scope,id,metadata FROM nn_history_records WHERE filter_version<2 LIMIT 200"))
		if err != nil {
			return err
		}
		type pending struct {
			viewer, scope, id string
			fields            []any
		}
		batch := []pending{}
		for rows.Next() {
			var p pending
			var raw string
			if err = rows.Scan(&p.viewer, &p.scope, &p.id, &raw); err != nil {
				break
			}
			p.fields, err = decodeHistoryFilterFields(raw)
			if err != nil {
				break
			}
			batch = append(batch, p)
		}
		rowErr := rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if rowErr != nil {
			return rowErr
		}
		if len(batch) == 0 {
			break
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for _, p := range batch {
			args := append(p.fields, p.viewer, p.scope, p.id)
			_, err = tx.ExecContext(ctx, s.bind("UPDATE nn_history_records SET model_id=?,deployment_id=?,key_id=?,model_label=?,key_label=?,key_prefix=?,model_category=?,filter_version=2 WHERE viewer=? AND scope=? AND id=? AND filter_version<2"), args...)
			if err != nil {
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
	for _, column := range []string{"model_id", "deployment_id", "key_id", "model_category"} {
		name := "nn_history_filter_" + column
		exists := ""
		if s.engine != "mysql" {
			exists = "IF NOT EXISTS "
		}
		_, err := s.db.ExecContext(ctx, s.categorySQL(fmt.Sprintf("CREATE INDEX %s%s ON nn_history_records(viewer,scope,%s,deleted,created,id)", exists, name, column)))
		if err != nil && !historyDuplicateIndex(err) {
			return err
		}
	}
	if s.engine == "sqlite" {
		// Keep category option scans off the large immutable request/response payload pages.
		for _, index := range []struct{ name, columns string }{
			{"nn_history_category_keys", "viewer,scope,model_category,deleted,key_id,key_label,key_prefix"},
			{"nn_history_category_models", "viewer,scope,model_category,deleted,deployment_id,model_id,model_label"},
		} {
			if _, err := s.db.ExecContext(ctx, s.categorySQL("CREATE INDEX IF NOT EXISTS "+index.name+" ON nn_history_records("+index.columns+")")); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *sqlHistoryRepository) Filters(ctx context.Context, viewer string, q historyFilterQuery) (historyFilterResult, error) {
	if s.leaves != nil && q.ModelCategory != "" {
		if leaf := s.leaves[historyStorageLeaf(q.ModelCategory)]; leaf != nil {
			return leaf.Filters(ctx, viewer, q)
		}
	}
	if err := q.validate(); err != nil {
		return historyFilterResult{}, err
	}
	id, name, prefix := "key_id", "key_label", "key_prefix"
	if q.Kind == "models" {
		id = "CASE WHEN deployment_id<>'' THEN deployment_id ELSE model_id END"
		name = "model_label"
		prefix = "''"
	}
	where := "viewer=? AND scope=? AND deleted=0 AND " + id + "<>''"
	args := []any{viewer, q.Scope}
	if s.engine == "sqlite" && !s.legacy {
		where += " AND created>=?"
		args = append(args, s.retentionCutoff(ctx).Format("2006-01-02T15:04:05.000000000Z"))
	}
	if q.ModelCategory != "" {
		where += " AND model_category=?"
		args = append(args, historyStorageLeaf(q.ModelCategory))
		where += s.speechFilter(q.ModelCategory)
	}
	if q.KeyID != "" {
		where += " AND key_id=?"
		args = append(args, q.KeyID)
	}
	if q.Before != "" {
		_, last, _ := parseHistoryCursor(q.Before)
		where += " AND " + id + ">?"
		args = append(args, last)
	}
	if q.Q != "" {
		where += " AND (LOWER(" + id + ") LIKE ? ESCAPE '!' OR LOWER(" + name + ") LIKE ? ESCAPE '!' OR LOWER(" + prefix + ") LIKE ? ESCAPE '!')"
		args = append(args, historyLike(q.Q), historyLike(q.Q), historyLike(q.Q))
	}
	query := "SELECT " + id + " AS filter_id,MAX(" + name + ") AS filter_name,MAX(" + prefix + ") AS filter_prefix FROM nn_history_records WHERE " + where + " GROUP BY " + id
	query += " ORDER BY filter_id LIMIT ?"
	args = append(args, q.Limit+1)
	rows, err := s.db.QueryContext(ctx, s.bind(query), args...)
	if err != nil {
		return historyFilterResult{}, err
	}
	defer rows.Close()
	items := []historyFilterItem{}
	for rows.Next() {
		var item historyFilterItem
		if err = rows.Scan(&item.ID, &item.Name, &item.Prefix); err != nil {
			return historyFilterResult{}, err
		}
		items = append(items, item)
	}
	return finishHistoryFilters(items, q), rows.Err()
}
