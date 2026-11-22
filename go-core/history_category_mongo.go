package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"time"
)

func openMongoHistory(ctx context.Context, c historyDatabaseConfig) (historyRepository, error) {
	r, e := openMongoHistoryLeaf(ctx, c, "text")
	if e != nil {
		return nil, e
	}
	text := r.(*mongoHistoryRepository)
	// Both leaves share one client and ownership; standalone Mongo has no cross-
	// collection transaction, so location is claimed first and Save is retryable.
	image := *text
	image.records = text.client.Database(c.Database).Collection("nn_history_image_records")
	image.chunks = text.client.Database(c.Database).Collection("nn_history_image_chunks")
	image.blocks = text.client.Database(c.Database).Collection("nn_history_image_body_blocks")
	r, e = openMongoHistoryLeaf(ctx, c, "image")
	if e != nil {
		text.Close()
		return nil, e
	}
	created := r.(*mongoHistoryRepository)
	if e = created.Close(); e != nil {
		text.Close()
		return nil, e
	}
	s := &mongoHistoryRepository{client: text.client, candidates: text.candidates, version: text.version, leaves: map[string]*mongoHistoryRepository{"text": text, "image": &image}, locations: text.client.Database(c.Database).Collection("nn_history_record_locations")}
	if _, e = s.locations.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "viewer", Value: 1}, {Key: "scope", Value: 1}, {Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)}); e != nil {
		s.Close()
		return nil, e
	}
	if e = s.migrateCategories(ctx, c.Database); e != nil {
		s.Close()
		return nil, e
	}
	s.records = s.client.Database(c.Database).Collection("nn_history_records")
	s.chunks = s.client.Database(c.Database).Collection("nn_history_chunks")
	return s, nil
}
func (s *mongoHistoryRepository) locateCategory(ctx context.Context, v, scope, id string) (*mongoHistoryRepository, error) {
	var row struct {
		Category string `bson:"model_category"`
	}
	e := s.locations.FindOne(ctx, bson.M{"viewer": v, "scope": scope, "id": id}).Decode(&row)
	if e != nil {
		return nil, e
	}
	leaf := s.leaves[row.Category]
	if leaf == nil {
		return nil, errors.New("invalid category location")
	}
	return leaf, nil
}
func (s *mongoHistoryRepository) saveCategory(ctx context.Context, v, scope string, value map[string]any) error {
	id := historyText(value["id"])
	if _, e := time.Parse(time.RFC3339Nano, historyText(value["createdAt"])); e != nil || v == "" || id == "" || (scope != "own" && scope != "others") {
		return errors.New("invalid history record")
	}
	cat := historyModelCategory(value)
	if raw, ok := value["modelCategory"]; ok && raw != nil && historyText(raw) != "text" && historyText(raw) != "image" {
		return errors.New("invalid history category")
	}
	key := bson.M{"viewer": v, "scope": scope, "id": historyText(value["id"])}
	_, e := s.locations.UpdateOne(ctx, key, bson.M{"$setOnInsert": bson.M{"model_category": cat}}, options.UpdateOne().SetUpsert(true))
	if e != nil {
		return e
	}
	leaf, e := s.locateCategory(ctx, v, scope, historyText(value["id"]))
	if e != nil {
		return e
	}
	if leaf != s.leaves[cat] {
		return errors.New("history category cannot change")
	}
	return leaf.Save(ctx, v, scope, value)
}
func (s *mongoHistoryRepository) migrateCategories(ctx context.Context, database string) error {
	db := s.client.Database(database)
	marker := db.Collection("nn_history_category_migration")
	if _, e := marker.UpdateOne(ctx, bson.M{"_id": "records"}, bson.M{"$set": bson.M{"state": "migrating"}}, options.UpdateOne().SetUpsert(true)); e != nil {
		return e
	}
	old := db.Collection("nn_history_records")
	infos, e := db.ListCollectionSpecifications(ctx, bson.M{"name": "nn_history_records"})
	if e != nil {
		return e
	}
	if len(infos) > 0 && infos[0].Type == "collection" {
		for {
			cursor, e := old.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "viewer", Value: 1}, {Key: "scope", Value: 1}, {Key: "id", Value: 1}}).SetLimit(32))
			if e != nil {
				return e
			}
			var docs []bson.M
			if e = cursor.All(ctx, &docs); e != nil {
				return e
			}
			if len(docs) == 0 {
				break
			}
			for _, doc := range docs {
				viewer, _ := doc["viewer"].(string)
				scope, _ := doc["scope"].(string)
				id, _ := doc["id"].(string)
				var meta map[string]any
				raw, _ := doc["metadata"].(string)
				if e = json.Unmarshal([]byte(raw), &meta); e != nil {
					return e
				}
				cat := historyModelCategory(meta)
				key := bson.M{"viewer": viewer, "scope": scope, "id": id}
				_, e = s.locations.UpdateOne(ctx, key, bson.M{"$setOnInsert": bson.M{"model_category": cat}}, options.UpdateOne().SetUpsert(true))
				if e != nil {
					return e
				}
				claimed, e := s.locateCategory(ctx, viewer, scope, id)
				if e != nil {
					return e
				}
				if claimed != s.leaves[cat] {
					return errors.New("history migration category conflict")
				}
				chunks, e := db.Collection("nn_history_chunks").Find(ctx, key)
				if e != nil {
					return e
				}
				for chunks.Next(ctx) {
					var chunk bson.M
					if e = chunks.Decode(&chunk); e != nil {
						chunks.Close(ctx)
						return e
					}
					chunkID := chunk["_id"]
					if _, e = s.leaves[cat].chunks.ReplaceOne(ctx, bson.M{"_id": chunkID}, chunk, options.Replace().SetUpsert(true)); e != nil {
						chunks.Close(ctx)
						return e
					}
					var copied bson.M
					if e = s.leaves[cat].chunks.FindOne(ctx, bson.M{"_id": chunkID}).Decode(&copied); e != nil {
						chunks.Close(ctx)
						return e
					}
					if copied["data"] != chunk["data"] || copied["digest"] != chunk["digest"] {
						chunks.Close(ctx)
						return errors.New("history chunk migration verification failed")
					}
				}
				e = chunks.Err()
				chunks.Close(ctx)
				if e != nil {
					return e
				}
				if historyBodyIsPacked(fmt.Sprint(doc["payload"])) {
					legacy := &mongoHistoryRepository{blocks: db.Collection("nn_history_body_blocks")}
					value, decodeErr := historyBodyDecode(fmt.Sprint(doc["payload"]), func(hash string) (historyBodyBlock, error) { return legacy.loadBlock(ctx, viewer, hash) })
					if decodeErr != nil {
						return decodeErr
					}
					header, blocks, encodeErr := historyBodyEncode(value)
					if encodeErr != nil {
						return encodeErr
					}
					if e = claimed.writeBodyBlocks(ctx, viewer, header, blocks); e != nil {
						return e
					}
				}
				// Rebuild stale/absent query projections from the same authoritative metadata
				// that selected the physical leaf; preserve the original bill/body snapshots.
				projection := mongoHistoryFilterFields(meta)
				projection["model_category"] = cat
				for field, value := range projection {
					doc[field] = value
				}
				if _, e = s.leaves[cat].records.ReplaceOne(ctx, key, doc, options.Replace().SetUpsert(true)); e != nil {
					return e
				}
				var copied bson.M
				if e = s.leaves[cat].records.FindOne(ctx, key).Decode(&copied); e != nil {
					return e
				}
				for field, value := range projection {
					if fmt.Sprint(copied[field]) != fmt.Sprint(value) {
						return errors.New("history filter projection migration verification failed")
					}
				}
				for _, field := range []string{"payload", "metadata", "bodyDigest"} {
					if copied[field] != doc[field] {
						return errors.New("history document migration verification failed")
					}
				}
				if _, e = s.leaves[cat].Get(ctx, viewer, scope, id); e != nil && doc["deleted"] != true {
					return e
				}
				// Remove old header only after leaf header/chunks verify. Restart retries the
				// same ID; opening is the read barrier, never a mixed-generation query.
				if _, e = old.DeleteOne(ctx, key); e != nil {
					return e
				}
				if _, e = db.Collection("nn_history_chunks").DeleteMany(ctx, key); e != nil {
					return e
				}
			}
		}
		if e = old.Drop(ctx); e != nil {
			return e
		}
		if e = db.Collection("nn_history_chunks").Drop(ctx); e != nil {
			return e
		}
	}
	// An interrupted header-drop/chunk-drop cutover may leave only old chunks.
	chunkInfo, e := db.ListCollectionSpecifications(ctx, bson.M{"name": "nn_history_chunks"})
	if e != nil {
		return e
	}
	if len(chunkInfo) > 0 && chunkInfo[0].Type == "collection" {
		if e = db.Collection("nn_history_chunks").Drop(ctx); e != nil {
			return e
		}
	}
	for _, kind := range []string{"records", "chunks"} {
		name := "nn_history_" + kind
		existing, e := db.ListCollectionSpecifications(ctx, bson.M{"name": name})
		if e != nil {
			return e
		}
		if len(existing) > 0 {
			continue
		}
		command := bson.D{{Key: "create", Value: name}, {Key: "viewOn", Value: "nn_history_text_" + kind}, {Key: "pipeline", Value: bson.A{bson.M{"$unionWith": "nn_history_image_" + kind}}}}
		if e = db.RunCommand(ctx, command).Err(); e != nil {
			return e
		}
	}
	_, e = marker.UpdateOne(ctx, bson.M{"_id": "records"}, bson.M{"$set": bson.M{"state": "ready", "generation": 1}}, options.UpdateOne().SetUpsert(true))
	return e
}
