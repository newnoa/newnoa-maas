package main

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const mongoHistoryFilterVersion = 3

func mongoHistoryFilterFields(value map[string]any) bson.M {
	fields := historyFilterFields(value)
	out := bson.M{"filter_version": mongoHistoryFilterVersion, "agent": historyText(value["agent"]), "model_type": historyText(value["modelType"])}
	for i, column := range historyFilterColumns {
		out[column] = fields[i]
	}
	return out
}
func (s *mongoHistoryRepository) initializeFilters(ctx context.Context) error {
	if s.leaves != nil {
		for _, leaf := range s.leaves {
			if e := leaf.initializeFilters(ctx); e != nil {
				return e
			}
		}
		return nil
	}
	if _, err := s.records.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "filter_version", Value: 1}}}); err != nil {
		return err
	}
	for {
		cursor, err := s.records.Find(ctx, bson.M{"filter_version": bson.M{"$ne": mongoHistoryFilterVersion}}, options.Find().SetLimit(200).SetProjection(bson.M{"metadata": 1}))
		if err != nil {
			return err
		}
		type record struct {
			ID       bson.ObjectID `bson:"_id"`
			Metadata string
		}
		batch := []record{}
		if err = cursor.All(ctx, &batch); err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		for _, r := range batch {
			var meta map[string]any
			if err = json.Unmarshal([]byte(r.Metadata), &meta); err != nil {
				return err
			}
			update := mongoHistoryFilterFields(meta)
			if _, err = s.records.UpdateOne(ctx, bson.M{"_id": r.ID, "filter_version": bson.M{"$ne": mongoHistoryFilterVersion}}, bson.M{"$set": update}); err != nil {
				return err
			}
		}
	}
	indexes := []mongo.IndexModel{}
	for _, column := range []string{"model_id", "deployment_id", "key_id"} {
		indexes = append(indexes, mongo.IndexModel{Keys: bson.D{{Key: "viewer", Value: 1}, {Key: "scope", Value: 1}, {Key: column, Value: 1}, {Key: "deleted", Value: 1}, {Key: "created", Value: -1}, {Key: "id", Value: -1}}})
	}
	_, err := s.records.Indexes().CreateMany(ctx, indexes)
	return err
}
func (s *mongoHistoryRepository) Filters(ctx context.Context, viewer string, q historyFilterQuery) (historyFilterResult, error) {
	if s.leaves != nil && q.ModelCategory != "" {
		if leaf := s.leaves[historyStorageLeaf(q.ModelCategory)]; leaf != nil {
			return leaf.Filters(ctx, viewer, q)
		}
	}
	if err := q.validate(); err != nil {
		return historyFilterResult{}, err
	}
	match := bson.M{"viewer": viewer, "scope": q.Scope, "deleted": false, "bodyPending": bson.M{"$ne": true}}
	if q.ModelCategory != "" {
		match["model_category"] = historyStorageLeaf(q.ModelCategory)
		if q.ModelCategory == "audio" {
			match["model_type"] = "asr"
		} else if q.ModelCategory == "text" {
			match["model_type"] = bson.M{"$ne": "asr"}
		}
	}
	if q.KeyID != "" {
		match["key_id"] = q.KeyID
	}
	var id any = "$key_id"
	name, prefix := "$key_label", "$key_prefix"
	if q.Kind == "models" {
		id = bson.M{"$cond": bson.A{bson.M{"$ne": bson.A{"$deployment_id", ""}}, "$deployment_id", "$model_id"}}
		name = "$model_label"
		prefix = ""
	}
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$match", Value: match}},
		bson.D{{Key: "$project", Value: bson.M{"_id": id, "name": name, "prefix": prefix}}},
	}
	filter := bson.M{"_id": bson.M{"$ne": ""}}
	if q.Before != "" {
		_, last, _ := parseHistoryCursor(q.Before)
		filter["_id"] = bson.M{"$gt": last}
	}
	if q.Q != "" {
		regex := bson.M{"$regex": regexp.QuoteMeta(strings.ToLower(q.Q)), "$options": "i"}
		filter["$or"] = bson.A{bson.M{"_id": regex}, bson.M{"name": regex}, bson.M{"prefix": regex}}
	}
	pipeline = append(pipeline, bson.D{{Key: "$match", Value: filter}}, bson.D{{Key: "$group", Value: bson.M{"_id": "$_id", "name": bson.M{"$max": "$name"}, "prefix": bson.M{"$max": "$prefix"}}}}, bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}}, bson.D{{Key: "$limit", Value: q.Limit + 1}})
	cursor, err := s.records.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true))
	if err != nil {
		return historyFilterResult{}, err
	}
	defer cursor.Close(ctx)
	items := []historyFilterItem{}
	for cursor.Next(ctx) {
		var row struct {
			ID           string `bson:"_id"`
			Name, Prefix string
		}
		if err = cursor.Decode(&row); err != nil {
			return historyFilterResult{}, err
		}
		items = append(items, historyFilterItem{ID: row.ID, Name: row.Name, Prefix: row.Prefix})
	}
	return finishHistoryFilters(items, q), cursor.Err()
}
