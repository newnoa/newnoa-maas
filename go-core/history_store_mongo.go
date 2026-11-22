package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type mongoHistoryRepository struct {
	client              *mongo.Client
	records, candidates *mongo.Collection
	chunks              *mongo.Collection
	blocks              *mongo.Collection
	version             string
	leaves              map[string]*mongoHistoryRepository
	locations           *mongo.Collection
}

func (s *mongoHistoryRepository) Version() string { return s.version }
func (s *mongoHistoryRepository) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.client.Disconnect(ctx)
}
func openMongoHistoryLeaf(ctx context.Context, c historyDatabaseConfig, category string) (historyRepository, error) {
	opts := options.Client().ApplyURI("mongodb://" + historyEndpoint(c)).SetConnectTimeout(5 * time.Second).SetServerSelectionTimeout(5 * time.Second).SetMaxPoolSize(4)
	auth := c.AuthDatabase
	if auth == "" {
		auth = "admin"
	}
	opts.SetAuth(options.Credential{Username: c.Username, Password: c.Password, AuthSource: auth})
	if c.TLS {
		opts.ApplyURI("mongodb://" + historyEndpoint(c) + "/?tls=true")
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, errors.New("MongoDB connection failed")
	}
	s := &mongoHistoryRepository{client: client, records: client.Database(c.Database).Collection("nn_history_" + category + "_records"), candidates: client.Database(c.Database).Collection("nn_history_candidates"), chunks: client.Database(c.Database).Collection("nn_history_" + category + "_chunks"), blocks: client.Database(c.Database).Collection("nn_history_" + category + "_body_blocks")}
	fail := func(err error) (historyRepository, error) { s.Close(); return nil, err }
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return fail(errors.New("MongoDB connection failed"))
	}
	var build struct {
		Version string `bson:"version"`
	}
	if err = client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&build); err != nil {
		return fail(errors.New("MongoDB version unavailable"))
	}
	s.version = build.Version
	major, _ := strconv.Atoi(strings.Split(build.Version, ".")[0])
	if major < 8 {
		return fail(errors.New("MongoDB 8 or newer is required"))
	}
	_, err = s.records.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "viewer", Value: 1}, {Key: "scope", Value: 1}, {Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "viewer", Value: 1}, {Key: "scope", Value: 1}, {Key: "created", Value: -1}, {Key: "id", Value: -1}}},
		{Keys: bson.D{{Key: "viewer", Value: 1}, {Key: "scope", Value: 1}, {Key: "caller", Value: 1}, {Key: "created", Value: -1}}},
		{Keys: bson.D{{Key: "viewer", Value: 1}, {Key: "scope", Value: 1}, {Key: "key", Value: 1}, {Key: "created", Value: -1}}},
	})
	if err != nil {
		return fail(errors.New("MongoDB indexes cannot be created"))
	}
	_, err = s.candidates.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "viewer", Value: 1}, {Key: "id", Value: 1}}, Options: options.Index().SetUnique(true)})
	if err != nil {
		return fail(err)
	}
	_, err = s.chunks.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "viewer", Value: 1}, {Key: "scope", Value: 1}, {Key: "id", Value: 1}, {Key: "digest", Value: 1}, {Key: "field", Value: 1}, {Key: "sequence", Value: 1}}, Options: options.Index().SetUnique(true)})
	if err != nil {
		return fail(err)
	}
	_, err = s.blocks.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "viewer", Value: 1}, {Key: "hash", Value: 1}}, Options: options.Index().SetUnique(true)})
	if err != nil {
		return fail(err)
	}
	_, err = s.blocks.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "gcChecked", Value: 1}, {Key: "_id", Value: 1}}})
	if err != nil {
		return fail(err)
	}
	_, err = s.records.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "viewer", Value: 1}, {Key: "bodyHashes", Value: 1}, {Key: "deleted", Value: 1}}})
	if err != nil {
		return fail(err)
	}
	if err = s.initializeFilters(ctx); err != nil {
		return fail(err)
	}
	return s, nil
}
func (s *mongoHistoryRepository) Save(ctx context.Context, viewer, scope string, value map[string]any) error {
	if s.leaves != nil {
		return s.saveCategory(ctx, viewer, scope, value)
	}
	id := historyText(value["id"])
	stamp, err := time.Parse(time.RFC3339Nano, historyText(value["createdAt"]))
	if viewer == "" || id == "" || err != nil || (scope != "own" && scope != "others") {
		return errors.New("invalid history record")
	}
	identity := bson.M{"viewer": viewer, "scope": scope, "id": id}
	var existing struct {
		Pending bool `bson:"bodyPending"`
	}
	if err = s.records.FindOne(ctx, identity).Decode(&existing); err == nil && !existing.Pending {
		return nil
	} else if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return err
	}
	headerRaw, blocks, err := historyBodyEncode(value)
	if err != nil {
		return err
	}
	metadata, err := json.Marshal(historyRecordMetadata(value))
	if err != nil {
		return err
	}
	storage := historyText(value["storageId"])
	if storage == "" {
		storage = id
	}
	document := bson.M{"viewer": viewer, "scope": scope, "id": id, "created": stamp.UTC().Format("2006-01-02T15:04:05.000000000Z"), "storage": storage, "caller": historyText(value["callerAccountId"]), "key": strings.ToLower(historyText(value["keyName"])), "search": strings.ToLower(historyText(value["model"]) + "\n" + historyText(value["modelName"])), "payload": headerRaw, "metadata": string(metadata), "deleted": false}
	for key, value := range mongoHistoryFilterFields(value) {
		document[key] = value
	}
	row, err := s.reserveBody(ctx, identity, document, headerRaw, blocks)
	if err != nil {
		return err
	}
	if !row.BodyPending {
		return nil
	}
	return s.publishBody(ctx, row, headerRaw, blocks)
}
func mongoHistoryFilter(viewer string, q historyQuery, cursor bool) (bson.M, error) {
	f := bson.M{"viewer": viewer, "scope": q.Scope, "deleted": false, "bodyPending": bson.M{"$ne": true}}
	for _, pair := range []struct{ k, v string }{{"search", q.Q}, {"key", q.KeyName}} {
		if pair.v != "" {
			f[pair.k] = bson.M{"$regex": regexp.QuoteMeta(strings.ToLower(pair.v))}
		}
	}
	if q.ModelCategory != "" {
		f["model_category"] = historyStorageLeaf(q.ModelCategory)
		if q.ModelCategory == "audio" {
			f["model_type"] = "asr"
		} else if q.ModelCategory == "text" {
			f["model_type"] = bson.M{"$ne": "asr"}
		}
	}
	if q.Model != "" {
		f["$and"] = bson.A{bson.M{"$or": bson.A{bson.M{"model_id": q.Model}, bson.M{"deployment_id": q.Model}}}}
	}
	if q.TestModelType != "" {
		f["agent"] = "NEWNOA Model Test"
		f["model_type"] = bson.M{"$in": bson.A{"", q.TestModelType}}
		pairs := bson.A{}
		for _, grant := range q.TestGrants {
			pairs = append(pairs, bson.M{"key_id": grant.KeyID, "$or": bson.A{
				bson.M{"deployment_id": grant.Model},
				bson.M{"deployment_id": "", "model_id": grant.Model},
			}})
		}
		and, _ := f["$and"].(bson.A)
		f["$and"] = append(and, bson.M{"$or": pairs})
	}
	if q.KeyID != "" {
		f["key_id"] = q.KeyID
	}
	if q.CallerID != "" {
		f["caller"] = q.CallerID
	}
	if q.StorageID != "" {
		f["storage"] = q.StorageID
	}
	date := bson.M{}
	for _, pair := range []struct{ k, v string }{{"$gte", q.From}, {"$lte", q.To}} {
		if pair.v != "" {
			stamp, _ := time.Parse(time.RFC3339Nano, pair.v)
			date[pair.k] = stamp.UTC().Format("2006-01-02T15:04:05.000000000Z")
		}
	}
	if len(date) > 0 {
		f["created"] = date
	}
	if cursor && q.Before != "" {
		stamp, id, err := parseHistoryCursor(q.Before)
		if err != nil {
			return nil, err
		}
		f["$or"] = bson.A{bson.M{"created": bson.M{"$lt": stamp}}, bson.M{"created": stamp, "id": bson.M{"$lt": id}}}
	}
	return f, nil
}
func (s *mongoHistoryRepository) Query(ctx context.Context, viewer string, q historyQuery) (historyQueryResult, error) {
	if s.leaves != nil && q.ModelCategory != "" {
		if leaf := s.leaves[historyStorageLeaf(q.ModelCategory)]; leaf != nil {
			return leaf.Query(ctx, viewer, q)
		}
	}
	start := time.Now()
	out := historyQueryResult{Items: []map[string]any{}, Database: "mongodb"}
	if err := q.validate(); err != nil {
		return out, err
	}
	if q.Q != "" {
		result, err := s.queryBodies(ctx, viewer, q)
		if err == nil {
			err = refreshHistorySummaries(ctx, s, viewer, q, result.Items)
		}
		result.ElapsedMs = time.Since(start).Milliseconds()
		return result, err
	}
	filter, err := mongoHistoryFilter(viewer, q, false)
	pipeline := mongo.Pipeline{bson.D{{Key: "$match", Value: filter}}}
	if err != nil {
		return out, err
	}
	counter, err := s.records.Aggregate(ctx, append(pipeline, bson.D{{Key: "$count", Value: "total"}}))
	if err != nil {
		return out, err
	}
	if counter.Next(ctx) {
		var count struct{ Total int64 }
		if err = counter.Decode(&count); err != nil {
			counter.Close(ctx)
			return out, err
		}
		out.Total = count.Total
	}
	err = counter.Err()
	counter.Close(ctx)
	if err != nil {
		return out, err
	}
	filter, err = mongoHistoryFilter(viewer, q, true)
	pipeline = mongo.Pipeline{bson.D{{Key: "$match", Value: filter}}}
	if err != nil {
		return out, err
	}
	pipeline = append(pipeline, bson.D{{Key: "$sort", Value: bson.D{{Key: "created", Value: -1}, {Key: "id", Value: -1}}}}, bson.D{{Key: "$limit", Value: q.Limit + 1}}, bson.D{{Key: "$project", Value: bson.M{"metadata": 1, "created": 1, "id": 1}}})
	cursor, err := s.records.Aggregate(ctx, pipeline)
	if err != nil {
		return out, err
	}
	defer cursor.Close(ctx)
	var lastStamp, lastID string
	for cursor.Next(ctx) {
		if len(out.Items) == q.Limit {
			out.NextCursor = historyCursor(lastStamp, lastID)
			break
		}
		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			return out, err
		}
		var value map[string]any
		if q.IncludeContent {
			value, err = s.Get(ctx, viewer, q.Scope, historyText(doc["id"]))
		} else {
			err = json.Unmarshal([]byte(historyText(doc["metadata"])), &value)
		}
		if err != nil {
			return out, err
		}
		value["source"] = "local"
		value["database"] = "mongodb"
		out.Items = append(out.Items, value)
		lastStamp, lastID = historyText(doc["created"]), historyText(doc["id"])
	}
	if err = cursor.Err(); err != nil {
		return out, err
	}
	if err = refreshHistorySummaries(ctx, s, viewer, q, out.Items); err != nil {
		return out, err
	}
	out.ElapsedMs = time.Since(start).Milliseconds()
	return out, nil
}
func (s *mongoHistoryRepository) Get(ctx context.Context, viewer, scope, id string) (map[string]any, error) {
	if s.leaves != nil {
		leaf, e := s.locateCategory(ctx, viewer, scope, id)
		if e != nil {
			return nil, e
		}
		return leaf.Get(ctx, viewer, scope, id)
	}
	var row mongoHistoryBodyDocument
	if err := s.records.FindOne(ctx, bson.M{"viewer": viewer, "scope": scope, "id": id, "deleted": false}).Decode(&row); err != nil {
		return nil, errors.New("history not found")
	}
	return s.loadBody(ctx, row)
}

func (s *mongoHistoryRepository) loadLegacyBody(ctx context.Context, header mongoHistoryBodyDocument) (map[string]any, error) {
	viewer, scope, id := header.Viewer, header.Scope, header.ID
	var value map[string]any
	if err := json.Unmarshal([]byte(header.Payload), &value); err != nil {
		return nil, err
	}
	for _, field := range header.BodyFields {
		cursor, err := s.chunks.Find(ctx, bson.M{"viewer": viewer, "scope": scope, "id": id, "digest": header.BodyDigest, "field": field}, options.Find().SetSort(bson.D{{Key: "sequence", Value: 1}}))
		if err != nil {
			return nil, err
		}
		var content strings.Builder
		sequence := 0
		for cursor.Next(ctx) {
			var chunk struct {
				Data     string
				Sequence int
			}
			if err = cursor.Decode(&chunk); err != nil {
				cursor.Close(ctx)
				return nil, err
			}
			if chunk.Sequence != sequence || content.Len()+len(chunk.Data) > historyBodyMaxBytes {
				cursor.Close(ctx)
				return nil, errors.New("invalid history chunks")
			}
			content.WriteString(chunk.Data)
			sequence++
		}
		err = cursor.Err()
		cursor.Close(ctx)
		if err != nil {
			return nil, err
		}
		value[field] = content.String()
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	if header.BodyDigest != "" && hex.EncodeToString(digest[:]) != header.BodyDigest {
		return nil, errors.New("history chunk checksum mismatch")
	}
	return value, nil
}

func (s *mongoHistoryRepository) Delete(ctx context.Context, viewer, scope, id string) error {
	if s.leaves != nil {
		leaf, e := s.locateCategory(ctx, viewer, scope, id)
		if e == mongo.ErrNoDocuments {
			return nil
		}
		if e != nil {
			return e
		}
		return leaf.Delete(ctx, viewer, scope, id)
	}
	_, err := s.records.UpdateOne(ctx, bson.M{"viewer": viewer, "scope": scope, "id": id}, bson.M{"$set": bson.M{"deleted": true}})
	return err
}
func (s *mongoHistoryRepository) Expire(ctx context.Context, viewer string, days int) error {
	if s.leaves != nil {
		for _, leaf := range s.leaves {
			if e := leaf.Expire(ctx, viewer, days); e != nil {
				return e
			}
		}
		return nil
	}
	if days == 0 {
		return nil
	}
	cut := time.Now().AddDate(0, 0, -days).UTC().Format("2006-01-02T15:04:05.000000000Z")
	_, err := s.records.UpdateMany(ctx, bson.M{"viewer": viewer, "created": bson.M{"$lt": cut}}, bson.M{"$set": bson.M{"deleted": true}})
	return err
}
func (s *mongoHistoryRepository) Candidate(ctx context.Context, viewer, id, email string, active bool) error {
	if len(email) > 254 || viewer == "" || id == "" {
		return errors.New("invalid history candidate")
	}
	_, err := s.candidates.UpdateOne(ctx, bson.M{"viewer": viewer, "id": id}, bson.M{"$set": bson.M{"email": strings.ToLower(email), "active": active}}, options.UpdateOne().SetUpsert(true))
	return err
}
func (s *mongoHistoryRepository) Accounts(ctx context.Context, viewer, q string, limit int, before ...string) ([]map[string]any, error) {
	return s.accounts(ctx, viewer, q, limit, false, before...)
}
func (s *mongoHistoryRepository) CandidateStates(ctx context.Context, viewer, before string, limit int) ([]map[string]any, error) {
	return s.accounts(ctx, viewer, "", limit, true, before)
}
func (s *mongoHistoryRepository) accounts(ctx context.Context, viewer, q string, limit int, includeInactive bool, before ...string) ([]map[string]any, error) {
	if limit < 1 || limit > 100 || len(q) > 254 {
		return nil, errors.New("invalid history account query")
	}
	filter := bson.M{"viewer": viewer, "email": bson.M{"$regex": regexp.QuoteMeta(strings.ToLower(q))}}
	if !includeInactive {
		filter["active"] = true
	}
	if len(before) > 0 && before[0] != "" {
		email, id, err := parseHistoryCursor(before[0])
		if err != nil {
			return nil, err
		}
		filter["$or"] = bson.A{bson.M{"email": bson.M{"$gt": email}}, bson.M{"email": email, "id": bson.M{"$gt": id}}}
	}
	cursor, err := s.candidates.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "email", Value: 1}, {Key: "id", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	out := []map[string]any{}
	for cursor.Next(ctx) {
		var row struct {
			ID     string `bson:"id"`
			Email  string `bson:"email"`
			Active bool   `bson:"active"`
		}
		if err = cursor.Decode(&row); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": row.ID, "email": row.Email, "active": row.Active})
	}
	return out, cursor.Err()
}
