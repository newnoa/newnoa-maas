package main

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func mongoRecoveryFixture(t *testing.T) (context.Context, historyDatabaseConfig, *mongoHistoryRepository) {
	t.Helper()
	port, _ := strconv.Atoi(os.Getenv("CORE_CATEGORY_MONGODB_PORT"))
	if port == 0 {
		t.Skip("requires authorized dedicated Mongo sandbox")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	c := defaultHistoryDatabase(t.TempDir())
	c.Engine = "mongodb"
	c.Host = os.Getenv("CORE_CATEGORY_MONGODB_HOST")
	c.Port = port
	c.Username = os.Getenv("CORE_CATEGORY_MONGODB_USER")
	c.Password = os.Getenv("CORE_CATEGORY_MONGODB_PASSWORD")
	c.AuthDatabase = "admin"
	c.Database = "test_core_live_recovery_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	r, err := openMongoHistoryLeaf(ctx, c, "text")
	if err != nil {
		t.Fatal(err)
	}
	s := r.(*mongoHistoryRepository)
	t.Cleanup(func() {
		if err := s.client.Database(c.Database).Drop(context.Background()); err != nil {
			t.Error("own database cleanup", err)
		}
		s.Close()
	})
	return ctx, c, s
}
func mongoRecoveryValidator(t *testing.T, ctx context.Context, s *mongoHistoryRepository, collection string, validator bson.M) {
	t.Helper()
	if err := s.client.Database(s.records.Database().Name()).RunCommand(ctx, bson.D{{Key: "collMod", Value: collection}, {Key: "validator", Value: validator}}).Err(); err != nil {
		t.Fatal(err)
	}
}
func mongoRecoveryInsert(t *testing.T, ctx context.Context, collection *mongo.Collection, value any) {
	t.Helper()
	if _, err := collection.InsertOne(ctx, value); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryMongoRecoveryLive(t *testing.T) {
	t.Run("interruptedPublicationAndReservationRemainRetryable", func(t *testing.T) {
		ctx, _, s := mongoRecoveryFixture(t)
		value := recoveryBody(t, "one", "text")
		mongoRecoveryValidator(t, ctx, s, "nn_history_text_records", bson.M{"bodyPending": true})
		if err := s.Save(ctx, "viewer", "own", value); err == nil {
			t.Fatal("unacknowledged publication accepted")
		}
		var pending mongoHistoryBodyDocument
		if err := s.records.FindOne(ctx, bson.M{"id": "one"}).Decode(&pending); err != nil || !pending.BodyPending {
			t.Fatal("durable retry reservation missing", err)
		}
		if _, err := s.loadBody(ctx, pending); err == nil {
			t.Fatal("pending publication became visible")
		}
		changed := recoveryBody(t, "one", "text")
		changed["request"] = "conflicting retry"
		if err := s.Save(ctx, "viewer", "own", changed); err == nil {
			t.Fatal("pending body identity changed")
		}
		mongoRecoveryValidator(t, ctx, s, "nn_history_text_records", bson.M{})
		n, err := s.migrateBodies(ctx, 10)
		if err != nil || n != 1 {
			t.Fatal("checkpoint reconciliation failed", n, err)
		}
		got, err := s.Get(ctx, "viewer", "own", "one")
		if err != nil || got["request"] != value["request"] {
			t.Fatal("reconciled body changed", err)
		}
		// A stale writer cannot overwrite a record whose reservation has already changed.
		pending.BodyToken = "stale-token"
		header, blocks, err := historyBodyEncode(value)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.publishBody(ctx, pending, header, blocks); err == nil {
			t.Fatal("stale writer published over new reservation")
		}
		got, err = s.Get(ctx, "viewer", "own", "one")
		if err != nil || got["response"] != value["response"] {
			t.Fatal("stale publisher damaged record", err)
		}
	})
	t.Run("legacyChunkAndPackedCategoryMigrationVerifyBeforeRetirement", func(t *testing.T) {
		ctx, c, s := mongoRecoveryFixture(t)
		db := s.client.Database(c.Database)
		old := db.Collection("nn_history_records")
		chunks := db.Collection("nn_history_chunks")
		legacyBlocks := db.Collection("nn_history_body_blocks")
		for _, kind := range []string{"chunks", "packed"} {
			value := recoveryBody(t, kind, "image")
			value["request"] = "[TEST] exact legacy " + kind
			meta, _ := json.Marshal(historyRecordMetadata(value))
			raw, _ := json.Marshal(value)
			document := bson.M{"viewer": "viewer", "scope": "own", "id": kind, "created": value["createdAt"], "payload": string(raw), "metadata": string(meta), "deleted": false}
			if kind == "chunks" {
				digest := historyBodyHash(raw)
				delete(value, "request")
				delete(value, "response")
				header, _ := json.Marshal(value)
				document["payload"] = string(header)
				document["bodyDigest"] = digest
				document["bodyFields"] = bson.A{"request", "response"}
				for field, data := range map[string]string{"request": "[TEST] exact legacy chunks", "response": "data: exact\r\n\r\n"} {
					mongoRecoveryInsert(t, ctx, chunks, bson.M{"viewer": "viewer", "scope": "own", "id": kind, "field": field, "digest": digest, "sequence": 0, "data": data})
				}
			} else {
				header, blocks, err := historyBodyEncode(value)
				if err != nil {
					t.Fatal(err)
				}
				document["payload"] = header
				document["bodyToken"] = "published-legacy-token"
				document["bodyHashes"] = mongoHistoryBodyHashes(blocks)
				for _, b := range blocks {
					mongoRecoveryInsert(t, ctx, legacyBlocks, bson.M{"viewer": "viewer", "hash": b.Hash, "rawSize": b.RawSize, "data": b.Data, "generation": int64(1)})
				}
			}
			mongoRecoveryInsert(t, ctx, old, document)
		}
		r, err := openMongoHistory(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		composite := r.(*mongoHistoryRepository)
		for _, id := range []string{"chunks", "packed"} {
			got, err := composite.Get(ctx, "viewer", "own", id)
			if err != nil || got["request"] != "[TEST] exact legacy "+id || got["pointsConsumed"] != "0.000000000" {
				t.Fatal("migration changed legacy bytes or known zero", id, err)
			}
		}
		infos, err := db.ListCollectionSpecifications(ctx, bson.M{"name": "nn_history_records"})
		if err != nil || len(infos) != 1 || infos[0].Type != "view" {
			t.Fatal("unverified old collection retired", err)
		}
		if n, err := composite.migrateBodies(ctx, 10); err != nil || n < 1 {
			t.Fatal("composite body migration", n, err)
		}
		if _, err := composite.collectBodies(ctx, 10); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("legacyCategoryMigrationFailsClosedAndResumesAfterRepair", func(t *testing.T) {
		for _, fault := range []string{"metadata", "category-conflict", "missing-block", "target-validator"} {
			t.Run(fault, func(t *testing.T) {
				ctx, c, s := mongoRecoveryFixture(t)
				db := s.client.Database(c.Database)
				old := db.Collection("nn_history_records")
				value := recoveryBody(t, "one", "image")
				header, blocks, err := historyBodyEncode(value)
				if err != nil {
					t.Fatal(err)
				}
				meta, _ := json.Marshal(historyRecordMetadata(value))
				document := bson.M{"viewer": "viewer", "scope": "own", "id": "one", "created": value["createdAt"], "payload": header, "metadata": string(meta), "deleted": false}
				legacy := db.Collection("nn_history_body_blocks")
				for _, b := range blocks {
					mongoRecoveryInsert(t, ctx, legacy, bson.M{"viewer": "viewer", "hash": b.Hash, "rawSize": b.RawSize, "data": b.Data, "generation": int64(1)})
				}
				if fault == "metadata" {
					document["metadata"] = "{bad"
				}
				mongoRecoveryInsert(t, ctx, old, document)
				if fault == "category-conflict" {
					mongoRecoveryInsert(t, ctx, db.Collection("nn_history_record_locations"), bson.M{"viewer": "viewer", "scope": "own", "id": "one", "model_category": "text"})
				}
				if fault == "missing-block" {
					if _, err = legacy.DeleteOne(ctx, bson.M{"hash": blocks[0].Hash}); err != nil {
						t.Fatal(err)
					}
				}
				if fault == "target-validator" {
					leaf, err := openMongoHistoryLeaf(ctx, c, "image")
					if err != nil {
						t.Fatal(err)
					}
					leaf.Close()
					mongoRecoveryValidator(t, ctx, s, "nn_history_image_records", bson.M{"id": bson.M{"$ne": "one"}})
				}
				if r, err := openMongoHistory(ctx, c); err == nil {
					r.Close()
					t.Fatal("unverifiable legacy migration opened", fault)
				}
				if n, err := old.CountDocuments(ctx, bson.M{"id": "one"}); err != nil || n != 1 {
					t.Fatal("failed migration removed recovery source", n, err)
				}
				switch fault {
				case "metadata":
					_, err = old.UpdateOne(ctx, bson.M{"id": "one"}, bson.M{"$set": bson.M{"metadata": string(meta)}})
				case "category-conflict":
					_, err = db.Collection("nn_history_record_locations").DeleteMany(ctx, bson.M{})
				case "missing-block":
					b := blocks[0]
					_, err = legacy.InsertOne(ctx, bson.M{"viewer": "viewer", "hash": b.Hash, "rawSize": b.RawSize, "data": b.Data, "generation": int64(1)})
				case "target-validator":
					mongoRecoveryValidator(t, ctx, s, "nn_history_image_records", bson.M{})
				}
				if err != nil {
					t.Fatal(err)
				}
				r, err := openMongoHistory(ctx, c)
				if err != nil {
					t.Fatal("migration cannot retry after durable repair", fault, err)
				}
				defer r.Close()
				got, err := r.Get(ctx, "viewer", "own", "one")
				if err != nil || got["request"] != value["request"] {
					t.Fatal("retry lost original", err)
				}
			})
		}
	})
	t.Run("malformedBodyAndMetadataRefuseSearchUntilRepaired", func(t *testing.T) {
		ctx, _, s := mongoRecoveryFixture(t)
		value := recoveryBody(t, "one", "text")
		if err := s.Save(ctx, "viewer", "own", value); err != nil {
			t.Fatal(err)
		}
		var saved mongoHistoryBodyDocument
		if err := s.records.FindOne(ctx, bson.M{"id": "one"}).Decode(&saved); err != nil {
			t.Fatal(err)
		}
		if _, err := s.records.UpdateOne(ctx, bson.M{"id": "one"}, bson.M{"$set": bson.M{"metadata": "{bad"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Query(ctx, "viewer", historyQuery{Scope: "own", Q: "immutable", Limit: 10}); err == nil {
			t.Fatal("search returned corrupt metadata")
		}
		if _, err := s.records.UpdateOne(ctx, bson.M{"id": "one"}, bson.M{"$set": bson.M{"metadata": saved.Metadata}}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.records.UpdateOne(ctx, bson.M{"id": "one"}, bson.M{"$set": bson.M{"payload": "{bad"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, "viewer", "own", "one"); err == nil {
			t.Fatal("malformed legacy payload accepted")
		}
		if _, err := s.records.UpdateOne(ctx, bson.M{"id": "one"}, bson.M{"$set": bson.M{"payload": saved.Payload}}); err != nil {
			t.Fatal(err)
		}
		got, err := s.Query(ctx, "viewer", historyQuery{Scope: "own", Q: "immutable", Limit: 10})
		if err != nil || got.Total != 1 {
			t.Fatal("repair failed", err)
		}
		if _, err = s.records.UpdateOne(ctx, bson.M{"id": "one"}, bson.M{"$set": bson.M{"bodyHashes": 17}}); err != nil {
			t.Fatal(err)
		}
		if _, err = s.Query(ctx, "viewer", historyQuery{Scope: "own", Q: "immutable", Limit: 10}); err == nil {
			t.Fatal("malformed reservation accepted")
		}
	})
	t.Run("maintenancePropagatesLegacyFailureAndRetriesAfterRepair", func(t *testing.T) {
		ctx, c, s := mongoRecoveryFixture(t)
		h, err := openChatHistoryAt(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer h.db.Close()
		value := recoveryBody(t, "legacy", "text")
		raw, _ := json.Marshal(value)
		meta, _ := json.Marshal(historyRecordMetadata(value))
		mongoRecoveryInsert(t, ctx, s.records, bson.M{"viewer": "viewer", "scope": "own", "id": "legacy", "created": value["createdAt"], "payload": "{broken", "metadata": string(meta), "deleted": false})
		child := &historyManager{historyRuntime: &historyRuntime{}, store: s, config: c, journal: h}
		parent := &historyManager{coordinator: true, children: map[string]*historyManager{"viewer": child}}
		if err = parent.compressHistoryBodies(ctx); err == nil {
			t.Fatal("maintenance ignored unverified legacy body")
		}
		if _, err = s.records.UpdateOne(ctx, bson.M{"id": "legacy"}, bson.M{"$set": bson.M{"payload": string(raw)}}); err != nil {
			t.Fatal(err)
		}
		if err = parent.compressHistoryBodies(ctx); err != nil {
			t.Fatal("maintenance cannot retry repaired legacy body", err)
		}
		got, err := s.Get(ctx, "viewer", "own", "legacy")
		if err != nil || got["request"] != value["request"] {
			t.Fatal("maintenance changed body", err)
		}
		mongoRecoveryInsert(t, ctx, s.blocks, bson.M{"viewer": "viewer", "hash": "bad-gc", "generation": "invalid", "rawSize": 1, "data": []byte{0}})
		if err = child.compressHistoryBodies(ctx); err == nil {
			t.Fatal("maintenance silently skipped failed collector")
		}
		if _, err = s.blocks.DeleteOne(ctx, bson.M{"hash": "bad-gc"}); err != nil {
			t.Fatal(err)
		}
		if err = child.compressHistoryBodies(ctx); err != nil {
			t.Fatal("collector retry failed", err)
		}
	})
	t.Run("orphanCollectionDoesNotSkipBadBsonOrLosePinnedBodies", func(t *testing.T) {
		ctx, _, s := mongoRecoveryFixture(t)
		value := recoveryBody(t, "one", "text")
		if err := s.Save(ctx, "viewer", "own", value); err != nil {
			t.Fatal(err)
		}
		mongoRecoveryInsert(t, ctx, s.blocks, bson.M{"viewer": "viewer", "hash": "bad", "generation": "bad integer", "rawSize": 1, "data": []byte{0}})
		if _, err := s.collectBodies(ctx, 100); err == nil {
			t.Fatal("corrupt GC identity silently skipped")
		}
		if _, err := s.Get(ctx, "viewer", "own", "one"); err != nil {
			t.Fatal("GC error damaged pinned body", err)
		}
		if _, err := s.blocks.DeleteOne(ctx, bson.M{"hash": "bad"}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.collectBodies(ctx, 100); err != nil {
			t.Fatal("collector cannot recover", err)
		}
	})
}
