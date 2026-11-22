package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoHistoryBodyDocument struct {
	Viewer      string   `bson:"viewer"`
	Scope       string   `bson:"scope"`
	ID          string   `bson:"id"`
	Payload     string   `bson:"payload"`
	Metadata    string   `bson:"metadata"`
	BodyDigest  string   `bson:"bodyDigest"`
	BodyFields  []string `bson:"bodyFields"`
	BodyToken   string   `bson:"bodyToken"`
	BodyHashes  []string `bson:"bodyHashes"`
	BodyPending bool     `bson:"bodyPending"`
	BodyCleanup bool     `bson:"bodyCleanup"`
	Deleted     bool     `bson:"deleted"`
}

func (s *mongoHistoryRepository) loadBody(ctx context.Context, row mongoHistoryBodyDocument) (map[string]any, error) {
	if s.leaves != nil {
		leaf, err := s.locateCategory(ctx, row.Viewer, row.Scope, row.ID)
		if err != nil {
			return nil, err
		}
		return leaf.loadBody(ctx, row)
	}
	if row.BodyPending {
		return nil, errors.New("history body publication incomplete")
	}
	var value map[string]any
	var err error
	if historyBodyIsPacked(row.Payload) {
		value, err = historyBodyDecode(row.Payload, func(hash string) (historyBodyBlock, error) { return s.loadBlock(ctx, row.Viewer, hash) })
	} else {
		value, err = s.loadLegacyBody(ctx, row)
	}
	if err != nil {
		return nil, err
	}
	var metadata map[string]any
	if json.Unmarshal([]byte(row.Metadata), &metadata) != nil {
		return nil, errors.New("invalid history metadata")
	}
	mergeHistoryTrustedMetadata(value, metadata)
	return value, nil
}

func (s *mongoHistoryRepository) loadBlock(ctx context.Context, viewer, hash string) (historyBodyBlock, error) {
	var row struct {
		Hash    string `bson:"hash"`
		RawSize int    `bson:"rawSize"`
		Data    []byte `bson:"data"`
	}
	err := s.blocks.FindOne(ctx, bson.M{"viewer": viewer, "hash": hash}).Decode(&row)
	return historyBodyBlock{Hash: row.Hash, RawSize: row.RawSize, Data: row.Data}, err
}

func mongoHistoryBodyHashes(blocks []historyBodyBlock) []string {
	hashes := make([]string, 0, len(blocks))
	for _, block := range blocks {
		hashes = append(hashes, block.Hash)
	}
	return hashes
}

func (s *mongoHistoryRepository) writeBodyBlocks(ctx context.Context, viewer, header string, blocks []historyBodyBlock) error {
	for _, block := range blocks {
		key := bson.M{"viewer": viewer, "hash": block.Hash}
		update := bson.M{"$setOnInsert": bson.M{"rawSize": block.RawSize, "data": block.Data}, "$inc": bson.M{"generation": 1}}
		if _, err := s.blocks.UpdateOne(ctx, key, update, options.UpdateOne().SetUpsert(true)); err != nil {
			if !mongo.IsDuplicateKeyError(err) {
				return err
			}
			if _, err = s.blocks.UpdateOne(ctx, key, update, options.UpdateOne().SetUpsert(true)); err != nil {
				return err
			}
		}
	}
	// Verify durable bytes before publishing or removing any legacy plaintext.
	_, err := historyBodyDecode(header, func(hash string) (historyBodyBlock, error) { return s.loadBlock(ctx, viewer, hash) })
	return err
}

func (s *mongoHistoryRepository) publishBody(ctx context.Context, row mongoHistoryBodyDocument, header string, blocks []historyBodyBlock) error {
	if err := s.writeBodyBlocks(ctx, row.Viewer, header, blocks); err != nil {
		return err
	}
	filter := bson.M{"viewer": row.Viewer, "scope": row.Scope, "id": row.ID, "bodyToken": row.BodyToken, "deleted": false, "payload": row.Payload}
	result, err := s.records.UpdateOne(ctx, filter, bson.M{"$set": bson.M{"payload": header, "bodyPending": false, "bodyCleanup": true}, "$unset": bson.M{"bodyDigest": "", "bodyFields": ""}})
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return errors.New("history changed during body publication")
	}
	return s.cleanupLegacyBody(ctx, row)
}

func (s *mongoHistoryRepository) cleanupLegacyBody(ctx context.Context, row mongoHistoryBodyDocument) error {
	identity := bson.M{"viewer": row.Viewer, "scope": row.Scope, "id": row.ID}
	if _, err := s.chunks.DeleteMany(ctx, identity); err != nil {
		return err
	}
	identity["bodyToken"] = row.BodyToken
	_, err := s.records.UpdateOne(ctx, identity, bson.M{"$unset": bson.M{"bodyCleanup": ""}})
	return err
}

// A durable reservation precedes block references, so standalone MongoDB needs no transaction.
func (s *mongoHistoryRepository) reserveBody(ctx context.Context, identity, document bson.M, header string, blocks []historyBodyBlock) (mongoHistoryBodyDocument, error) {
	document["bodyPending"] = true
	document["bodyToken"] = uuid.NewString()
	document["bodyHashes"] = mongoHistoryBodyHashes(blocks)
	_, err := s.records.UpdateOne(ctx, identity, bson.M{"$setOnInsert": document}, options.UpdateOne().SetUpsert(true))
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		return mongoHistoryBodyDocument{}, err
	}
	var row mongoHistoryBodyDocument
	if err = s.records.FindOne(ctx, identity).Decode(&row); err != nil {
		return row, err
	}
	if row.BodyPending && row.Payload != header {
		return row, errors.New("pending history body differs from retry")
	}
	return row, nil
}

// migrateBodies performs a bounded maintenance batch; failed rows retain their readable source.
func (s *mongoHistoryRepository) migrateBodies(ctx context.Context, limit int) (int, error) {
	if s.leaves != nil {
		total := 0
		for _, cat := range []string{"text", "image"} {
			n, e := s.leaves[cat].migrateBodies(ctx, limit)
			total += n
			if e != nil {
				return total, e
			}
		}
		return total, nil
	}
	if limit < 1 {
		return 0, nil
	}
	filter := bson.M{"deleted": false, "$or": bson.A{bson.M{"bodyToken": bson.M{"$exists": false}}, bson.M{"bodyPending": true}, bson.M{"bodyDigest": bson.M{"$exists": true}}, bson.M{"bodyCleanup": true}}}
	cursor, err := s.records.Find(ctx, filter, options.Find().SetLimit(int64(limit)))
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)
	migrated := 0
	for cursor.Next(ctx) {
		var row mongoHistoryBodyDocument
		if err = cursor.Decode(&row); err != nil {
			return migrated, err
		}
		if row.BodyCleanup && historyBodyIsPacked(row.Payload) {
			if err = s.cleanupLegacyBody(ctx, row); err != nil {
				return migrated, err
			}
			migrated++
			continue
		}
		if row.BodyPending {
			if _, err = historyBodyDecode(row.Payload, func(hash string) (historyBodyBlock, error) { return s.loadBlock(ctx, row.Viewer, hash) }); err != nil {
				return migrated, err
			}
			result, e := s.records.UpdateOne(ctx, bson.M{"viewer": row.Viewer, "scope": row.Scope, "id": row.ID, "bodyToken": row.BodyToken, "bodyPending": true, "deleted": false}, bson.M{"$set": bson.M{"bodyPending": false}})
			if e != nil {
				return migrated, e
			}
			migrated += int(result.ModifiedCount)
			continue
		}
		value, e := s.loadLegacyBody(ctx, row)
		if e != nil {
			return migrated, e
		}
		header, blocks, e := historyBodyEncode(value)
		if e != nil {
			return migrated, e
		}
		identity := bson.M{"viewer": row.Viewer, "scope": row.Scope, "id": row.ID, "payload": row.Payload, "deleted": false}
		if row.BodyToken == "" {
			token := uuid.NewString()
			identity["bodyToken"] = bson.M{"$exists": false}
			result, e := s.records.UpdateOne(ctx, identity, bson.M{"$set": bson.M{"bodyToken": token, "bodyHashes": mongoHistoryBodyHashes(blocks)}})
			if e != nil {
				return migrated, e
			}
			if result.MatchedCount == 0 {
				continue
			}
			row.BodyToken = token
		}
		if err = s.publishBody(ctx, row, header, blocks); err != nil {
			return migrated, err
		}
		migrated++
	}
	return migrated, cursor.Err()
}

// Record reservations precede block writes; generation checks protect concurrent upserts from collection.
func (s *mongoHistoryRepository) collectBodies(ctx context.Context, limit int) (int, error) {
	if s.leaves != nil {
		total := 0
		for _, cat := range []string{"text", "image"} {
			n, e := s.leaves[cat].collectBodies(ctx, limit)
			total += n
			if e != nil {
				return total, e
			}
		}
		return total, nil
	}
	if limit < 1 {
		return 0, nil
	}
	cursor, err := s.records.Find(ctx, bson.M{"deleted": true, "payload": bson.M{"$exists": true}}, options.Find().SetLimit(int64(limit)))
	if err != nil {
		return 0, err
	}
	for cursor.Next(ctx) {
		var row mongoHistoryBodyDocument
		if err = cursor.Decode(&row); err != nil {
			cursor.Close(ctx)
			return 0, err
		}

		if _, err = s.chunks.DeleteMany(ctx, bson.M{"viewer": row.Viewer, "scope": row.Scope, "id": row.ID}); err != nil {
			cursor.Close(ctx)
			return 0, err
		}
		if _, err = s.records.UpdateOne(ctx, bson.M{"viewer": row.Viewer, "scope": row.Scope, "id": row.ID, "deleted": true}, bson.M{"$unset": bson.M{"payload": "", "bodyToken": "", "bodyHashes": "", "bodyDigest": "", "bodyFields": "", "bodyPending": "", "bodyCleanup": ""}}); err != nil {
			cursor.Close(ctx)
			return 0, err
		}
	}
	err = cursor.Err()
	cursor.Close(ctx)
	if err != nil {
		return 0, err
	}
	// Scan bounded oldest-first batches, including orphan blocks from an interrupted publication.
	sweep, err := s.blocks.Find(ctx, bson.M{}, options.Find().SetLimit(int64(limit)).SetSort(bson.D{{Key: "gcChecked", Value: 1}, {Key: "_id", Value: 1}}).SetProjection(bson.M{"_id": 1, "viewer": 1, "hash": 1, "generation": 1}))
	if err != nil {
		return 0, err
	}
	defer sweep.Close(ctx)
	removed := 0
	for sweep.Next(ctx) {
		var row struct {
			ID         bson.ObjectID `bson:"_id"`
			Viewer     string        `bson:"viewer"`
			Hash       string        `bson:"hash"`
			Generation int64         `bson:"generation"`
		}
		if err = sweep.Decode(&row); err != nil {
			return removed, err
		}
		// A writer reserves this hash before touching the block. A later touch invalidates our delete.
		err = s.records.FindOne(ctx, bson.M{"viewer": row.Viewer, "bodyHashes": row.Hash, "deleted": false}, options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			return removed, err
		}
		if errors.Is(err, mongo.ErrNoDocuments) {
			result, e := s.blocks.DeleteOne(ctx, bson.M{"_id": row.ID, "generation": row.Generation})
			if e != nil {
				return removed, e
			}
			removed += int(result.DeletedCount)
		}
		if _, e := s.blocks.UpdateOne(ctx, bson.M{"_id": row.ID}, bson.M{"$set": bson.M{"gcChecked": time.Now().UTC()}}); e != nil {
			return removed, e
		}
	}
	return removed, sweep.Err()
}

func mongoHistoryBefore(created, id string, q historyQuery) bool {
	if q.Before == "" {
		return true
	}
	stamp, beforeID, _ := parseHistoryCursor(q.Before)
	return created < stamp || (created == stamp && id < beforeID)
}

// Search expands only metadata-filtered candidates in cursor batches and retains total before pagination.
func (s *mongoHistoryRepository) queryBodies(ctx context.Context, viewer string, q historyQuery) (historyQueryResult, error) {
	out := historyQueryResult{Items: []map[string]any{}, Database: "mongodb"}
	metadataQuery := q
	metadataQuery.Q = ""
	metadataQuery.Before = ""
	filter, err := mongoHistoryFilter(viewer, metadataQuery, false)
	if err != nil {
		return out, err
	}
	cursor, err := s.records.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "created", Value: -1}, {Key: "id", Value: -1}}).SetBatchSize(32))
	if err != nil {
		return out, err
	}
	defer cursor.Close(ctx)
	var lastStamp, lastID string
	for cursor.Next(ctx) {
		var doc struct {
			Body    mongoHistoryBodyDocument `bson:",inline"`
			Created string                   `bson:"created"`
		}
		if err = cursor.Decode(&doc); err != nil {
			return out, err
		}
		value, e := s.loadBody(ctx, doc.Body)
		if e != nil {
			return out, e
		}
		if !historyBodySearch(value, q.Q) {
			continue
		}
		out.Total++
		if !mongoHistoryBefore(doc.Created, doc.Body.ID, q) {
			continue
		}
		if len(out.Items) == q.Limit {
			if out.NextCursor == "" {
				out.NextCursor = historyCursor(lastStamp, lastID)
			}
			continue
		}
		if !q.IncludeContent {
			value = nil
			if err = json.Unmarshal([]byte(doc.Body.Metadata), &value); err != nil {
				return out, err
			}
		}
		value["source"] = "local"
		value["database"] = "mongodb"
		out.Items = append(out.Items, value)
		lastStamp, lastID = doc.Created, doc.Body.ID
	}
	return out, cursor.Err()
}
