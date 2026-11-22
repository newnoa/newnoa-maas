package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Only API-derived identity and display snapshots can enrich an immutable local body.
func mergeHistoryTrustedMetadata(value, metadata map[string]any) {
	settled := value["pointsStatus"] == "known" && metadata["pointsStatus"] != "known" || value["billingPhase"] == "final" && metadata["billingPhase"] != "final"
	for _, key := range []string{"scope", "callerAccountId", "callerEmail", "publisherAccountId", "deploymentId", "keyId", "modelName", "keyName", "keyPrefix", "inputTokens", "outputTokens", "cacheWriteTokens", "cachedInputTokens", "pointsConsumed", "pointsStatus", "settlementStatus", "settlementReason", "modelCategory", "imageUsage", "imagePricing", "imageCosts", "imageMetadata", "usageSource", "billingPhase", "longContextPricing", "longContextApplied", "longContextThreshold", "pointsMultiplier", "effectivePointsMultiplier", "callerSource", "reasoningEffort", "clientIp", "firstTokenMs", "durationMs", "outputTps"} {
		if field, ok := metadata[key]; ok {
			if field == nil && (key == "usageSource" || key == "billingPhase") {
				continue
			}
			if field == nil && (key == "callerSource" || key == "reasoningEffort" || key == "clientIp" || key == "firstTokenMs" || key == "durationMs" || key == "outputTps") {
				continue
			}
			if settled && (key == "settlementStatus" || key == "settlementReason" || key == "imageUsage" || key == "imagePricing" || key == "imageCosts" || key == "pointsStatus" || key == "pointsConsumed" || key == "usageSource" || key == "billingPhase" || strings.HasSuffix(key, "Tokens") || strings.HasPrefix(key, "longContext") || strings.HasSuffix(key, "Multiplier")) {
				continue
			}
			if field == nil && (key == "inputTokens" || key == "outputTokens" || key == "cacheWriteTokens" || key == "cachedInputTokens") {
				continue
			}
			value[key] = field
		}
	}
}
func (s *sqlHistoryRepository) Enrich(ctx context.Context, viewer, scope, id string, metadata map[string]any) error {
	if s.leaves != nil {
		leaf, e := s.locateCategory(ctx, viewer, scope, id)
		if e != nil {
			return e
		}
		if cat := historyText(metadata["modelCategory"]); cat != "" && leaf.table != "nn_history_"+cat+"_records" {
			return errors.New("history category cannot change")
		}
		return leaf.Enrich(ctx, viewer, scope, id, metadata)
	}
	if s.engine == "sqlite" && expiredHistoryAtCutoff(metadata, s.retentionCutoff(ctx)) {
		return nil
	}
	var raw string
	var deleted bool
	if err := s.db.QueryRowContext(ctx, s.bind("SELECT metadata,deleted FROM nn_history_records WHERE viewer=? AND scope=? AND id=?"), viewer, scope, id).Scan(&raw, &deleted); err != nil {
		return err
	}
	if deleted {
		return nil
	}
	var value map[string]any
	if json.Unmarshal([]byte(raw), &value) != nil {
		return errors.New("invalid history metadata")
	}
	mergeHistoryTrustedMetadata(value, metadata)
	if s.table == "nn_history_image_records" {
		value["modelCategory"] = "image"
	} else if s.table == "nn_history_text_records" {
		value["modelCategory"] = "text"
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	args := []any{string(encoded), historyText(value["callerAccountId"]), strings.ToLower(historyText(value["keyName"]))}
	args = append(args, historyFilterFields(value)...)
	args = append(args, viewer, scope, id)
	_, err = s.db.ExecContext(ctx, s.bind("UPDATE nn_history_records SET metadata=?,caller_id=?,key_name=?,model_id=?,deployment_id=?,key_id=?,model_label=?,key_label=?,key_prefix=?,model_category=?,filter_version=2 WHERE viewer=? AND scope=? AND id=? AND deleted=0"), args...)
	return err
}
func (s *mongoHistoryRepository) Enrich(ctx context.Context, viewer, scope, id string, metadata map[string]any) error {
	if s.leaves != nil {
		leaf, e := s.locateCategory(ctx, viewer, scope, id)
		if e != nil {
			return e
		}
		if cat := historyText(metadata["modelCategory"]); cat != "" && leaf != s.leaves[cat] {
			return errors.New("history category cannot change")
		}
		return leaf.Enrich(ctx, viewer, scope, id, metadata)
	}
	var row struct {
		Metadata string
		Deleted  bool
	}
	identity := bson.M{"viewer": viewer, "scope": scope, "id": id}
	if err := s.records.FindOne(ctx, identity).Decode(&row); err != nil {
		return err
	}
	if row.Deleted {
		return nil
	}
	var value map[string]any
	if json.Unmarshal([]byte(row.Metadata), &value) != nil {
		return errors.New("invalid history metadata")
	}
	mergeHistoryTrustedMetadata(value, metadata)
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	update := mongoHistoryFilterFields(value)
	update["metadata"] = string(encoded)
	update["caller"] = historyText(value["callerAccountId"])
	update["key"] = strings.ToLower(historyText(value["keyName"]))
	identity["deleted"] = false
	_, err = s.records.UpdateOne(ctx, identity, bson.M{"$set": update})
	return err
}
func (s *unavailableHistoryRepository) Enrich(context.Context, string, string, string, map[string]any) error {
	return s.err
}
