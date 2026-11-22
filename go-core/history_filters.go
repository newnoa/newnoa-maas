package main

import (
	"context"
	"encoding/json"
	"errors"
)

type historyFilterQuery struct {
	ModelCategory string `json:"modelCategory"`
	Scope         string `json:"scope"`
	Kind          string `json:"kind"`
	Q             string `json:"q"`
	KeyID         string `json:"keyId"`
	Before        string `json:"before"`
	Limit         int    `json:"limit"`
}
type historyFilterItem struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Prefix string `json:"prefix,omitempty"`
}
type historyFilterResult struct {
	Items      []historyFilterItem `json:"items"`
	NextCursor string              `json:"nextCursor,omitempty"`
}

func (q *historyFilterQuery) validate() error {
	common := historyQuery{ModelCategory: q.ModelCategory, Scope: q.Scope, Q: q.Q, KeyID: q.KeyID, Limit: q.Limit}
	if err := common.validate(); err != nil {
		return err
	}
	q.Scope, q.Limit = common.Scope, common.Limit
	if len(q.Before) > 2000 {
		return errors.New("invalid history filter cursor")
	}
	if q.Kind != "keys" && q.Kind != "models" {
		return errors.New("invalid history filter kind")
	}
	if q.Kind == "keys" && q.KeyID != "" {
		return errors.New("key qualifier requires model filters")
	}
	if q.Before != "" {
		kind, _, err := parseHistoryCursor(q.Before)
		if err != nil || kind != q.Kind+"/"+q.KeyID {
			return errors.New("invalid history filter cursor")
		}
	}
	return nil
}

// Projections contain only historical local metadata, including revoked/deleted entity snapshots.
func historyFilterFields(value map[string]any) []any {
	model := historyText(value["model"])
	deployment := historyText(value["deploymentId"])
	name := historyText(value["modelName"])
	if name == "" {
		name = model
	}
	if name == "" {
		name = deployment
	}
	key := historyText(value["keyId"])
	keyName := historyText(value["keyName"])
	if keyName == "" {
		keyName = key
	}
	return []any{model, deployment, key, name, keyName, historyText(value["keyPrefix"]), historyModelCategory(value)}
}
func decodeHistoryFilterFields(raw string) ([]any, error) {
	var value map[string]any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, err
	}
	return historyFilterFields(value), nil
}
func finishHistoryFilters(items []historyFilterItem, q historyFilterQuery) historyFilterResult {
	result := historyFilterResult{Items: items}
	if len(items) > q.Limit {
		result.Items = items[:q.Limit]
		result.NextCursor = historyCursor(q.Kind+"/"+q.KeyID, result.Items[q.Limit-1].ID)
	}
	return result
}
func (s *unavailableHistoryRepository) Filters(context.Context, string, historyFilterQuery) (historyFilterResult, error) {
	return historyFilterResult{}, s.err
}
