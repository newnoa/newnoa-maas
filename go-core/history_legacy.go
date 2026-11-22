package main

import (
	"context"
	"errors"
	"time"
)

type legacyHistoryRepository struct {
	*sqlHistoryRepository
	journal *chatHistory
}

func openLegacyHistoryRepository(h *chatHistory) (historyRepository, error) {
	// Capture has always written UTC timestamps; normalize its optional fractional seconds for keyset queries.
	_, err := h.db.Exec(`DROP VIEW IF EXISTS nn_history_records; CREATE VIEW nn_history_records AS SELECT account AS viewer,'own' AS scope,id,
 CASE WHEN instr(created,'.')>0 THEN substr(created,1,19)||'.'||substr(substr(created,21)||'000000000',1,9)||'Z' ELSE substr(created,1,19)||'.000000000Z' END AS created,
 COALESCE(NULLIF(json_extract(payload,'$.storageId'),''),id) AS storage_id,COALESCE(json_extract(payload,'$.callerAccountId'),'') AS caller_id,lower(COALESCE(json_extract(payload,'$.keyName'),'')) AS key_name,payload,
 json_set(json_remove(payload,'$.request','$.response','$._newnoaBody'),'$.contentSummary',substr(COALESCE(json_extract(payload,'$.request'),''),1,120)) AS metadata,
 lower(COALESCE(json_extract(payload,'$.request'),'')||char(10)||COALESCE(json_extract(payload,'$.response'),'')||char(10)||model||char(10)||COALESCE(json_extract(payload,'$.modelName'),'')) AS search_text,
 COALESCE(json_extract(payload,'$.model'),'') AS model_id,COALESCE(json_extract(payload,'$.deploymentId'),'') AS deployment_id,COALESCE(json_extract(payload,'$.keyId'),'') AS key_id,COALESCE(NULLIF(json_extract(payload,'$.modelName'),''),json_extract(payload,'$.model'),json_extract(payload,'$.deploymentId'),'') AS model_label,COALESCE(NULLIF(json_extract(payload,'$.keyName'),''),json_extract(payload,'$.keyId'),'') AS key_label,COALESCE(json_extract(payload,'$.keyPrefix'),'') AS key_prefix,1 AS filter_version,CASE WHEN json_extract(payload,'$.modelCategory')='image' OR (json_extract(payload,'$.modelCategory') IS NULL AND json_extract(payload,'$.modelType')='image') THEN 'image' ELSE 'text' END AS model_category,
 CASE WHEN deleted_at IS NULL THEN 0 ELSE 1 END AS deleted FROM exchanges;`)
	if err != nil {
		return nil, err
	}
	for _, cat := range []string{"text", "image"} {
		if _, err = h.db.Exec("CREATE VIEW IF NOT EXISTS nn_history_" + cat + "_records AS SELECT * FROM nn_history_records WHERE model_category='" + cat + "'"); err != nil {
			return nil, err
		}
	}
	var version string
	if err = h.db.QueryRow("SELECT sqlite_version()").Scan(&version); err != nil {
		return nil, err
	}
	return &legacyHistoryRepository{sqlHistoryRepository: &sqlHistoryRepository{db: h.db.DB, engine: "sqlite", version: version, legacy: true, bodies: h.bodies}, journal: h}, nil
}
func (s *legacyHistoryRepository) Close() error { return nil }
func (s *legacyHistoryRepository) Save(ctx context.Context, viewer, scope string, value map[string]any) error {
	if scope != "own" {
		return errors.New("legacy history only supports own records during migration")
	}
	s.journal.bodyMu.Lock()
	defer s.journal.bodyMu.Unlock()
	raw, err := s.journal.encodeBody(ctx, "journal", viewer, scope, historyText(value["id"]), value)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "INSERT INTO exchanges(account,id,created,model,agent,payload,pending) VALUES(?,?,?,?,?,?,0)", viewer, historyText(value["id"]), historyText(value["createdAt"]), historyText(value["model"]), historyText(value["agent"]), string(raw))
	return err
}
func (s *legacyHistoryRepository) Enrich(ctx context.Context, viewer, scope, id string, metadata map[string]any) error {
	if scope != "own" {
		return nil
	}
	var raw string
	var deleted any
	if err := s.db.QueryRowContext(ctx, "SELECT payload,deleted_at FROM exchanges WHERE account=? AND id=?", viewer, id).Scan(&raw, &deleted); err != nil {
		return err
	}
	if deleted != nil {
		return nil
	}
	value, err := s.journal.decodeBody(ctx, viewer, raw)
	if err != nil {
		return err
	}
	mergeHistoryTrustedMetadata(value, metadata)
	s.journal.bodyMu.Lock()
	defer s.journal.bodyMu.Unlock()
	encoded, err := s.journal.encodeBody(ctx, "journal", viewer, scope, id, value)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "UPDATE exchanges SET payload=? WHERE account=? AND id=? AND deleted_at IS NULL", string(encoded), viewer, id)
	return err
}
func (s *legacyHistoryRepository) Delete(ctx context.Context, viewer, scope, id string) error {
	if scope != "own" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, "UPDATE exchanges SET deleted_at=COALESCE(deleted_at,CURRENT_TIMESTAMP),pending=0 WHERE account=? AND id=?", viewer, id)
	return err
}
func (s *legacyHistoryRepository) Expire(ctx context.Context, viewer string, days int) error {
	if days == 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, "UPDATE exchanges SET deleted_at=COALESCE(deleted_at,CURRENT_TIMESTAMP) WHERE account=? AND julianday(created)<julianday(?)", viewer, time.Now().AddDate(0, 0, -days).UTC().Format(time.RFC3339Nano))
	return err
}
func (s *legacyHistoryRepository) Candidate(context.Context, string, string, string, bool) error {
	return errors.New("history candidates wait for legacy migration")
}
func (s *legacyHistoryRepository) Accounts(context.Context, string, string, int, ...string) ([]map[string]any, error) {
	return []map[string]any{}, nil
}
func (s *legacyHistoryRepository) CandidateStates(context.Context, string, string, int) ([]map[string]any, error) {
	return []map[string]any{}, nil
}

func (s *legacyHistoryRepository) Query(ctx context.Context, viewer string, q historyQuery) (historyQueryResult, error) {
	if q.ModelCategory == "text" || q.ModelCategory == "image" || q.ModelCategory == "audio" {
		leaf := *s.sqlHistoryRepository
		leaf.table = "nn_history_" + historyStorageLeaf(q.ModelCategory) + "_records"
		return leaf.Query(ctx, viewer, q)
	}
	return s.sqlHistoryRepository.Query(ctx, viewer, q)
}
func (s *legacyHistoryRepository) Filters(ctx context.Context, viewer string, q historyFilterQuery) (historyFilterResult, error) {
	if q.ModelCategory == "text" || q.ModelCategory == "image" || q.ModelCategory == "audio" {
		leaf := *s.sqlHistoryRepository
		leaf.table = "nn_history_" + historyStorageLeaf(q.ModelCategory) + "_records"
		return leaf.Filters(ctx, viewer, q)
	}
	return s.sqlHistoryRepository.Filters(ctx, viewer, q)
}
