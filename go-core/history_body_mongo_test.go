package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoHistoryPendingRecordsAreHidden(t *testing.T) {
	f, err := mongoHistoryFilter("v", historyQuery{Scope: "own"}, false)
	if err != nil || f["bodyPending"] == nil {
		t.Fatalf("pending records must be hidden: %v %v", f, err)
	}
}

func TestMongoHistorySearchCandidateDecode(t *testing.T) {
	raw, err := bson.Marshal(bson.M{"viewer": "viewer", "scope": "own", "id": "id", "created": "stamp", "payload": "{}", "metadata": "{}"})
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Body    mongoHistoryBodyDocument `bson:",inline"`
		Created string                   `bson:"created"`
	}
	if err = bson.Unmarshal(raw, &doc); err != nil || doc.Body.Viewer != "viewer" || doc.Body.ID != "id" || doc.Created != "stamp" {
		t.Fatalf("candidate identity missing: %+v %v", doc, err)
	}
	q := historyQuery{Before: historyCursor("2026-11-17T00:00:00.000000000Z", "b")}
	if mongoHistoryBefore("2026-11-17T00:00:00.000000000Z", "b", q) || !mongoHistoryBefore("2026-11-17T00:00:00.000000000Z", "a", q) {
		t.Fatal("cursor ordering changed")
	}
}

func TestMongoHistoryCompressedBodiesLive(t *testing.T) {
	password := os.Getenv("HISTORY_DATABASE_PASSWORD")
	if password == "" {
		t.Skip("requires authorized dedicated history sandbox")
	}
	for _, port := range []int{27018, 27019} {
		t.Run(fmt.Sprint(port), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			c := historySandboxDatabase(t, defaultHistoryDatabase(t.TempDir()))
			c.Engine = "mongodb"
			c.Host = "127.0.0.1"
			c.Port = port
			c.Username = "newnoa"
			c.Password = password
			repository, err := openMongoHistoryLeaf(ctx, c, "text")
			if err != nil {
				t.Fatal(err)
			}
			s := repository.(*mongoHistoryRepository)
			defer s.Close()
			viewer := "[TEST]-compressed-" + uuid.NewString()
			t.Log("fixture", viewer)
			defer func() {
				for _, collection := range []string{"nn_history_text_records", "nn_history_candidates", "nn_history_text_chunks", "nn_history_text_body_blocks"} {
					if _, e := s.client.Database(c.Database).Collection(collection).DeleteMany(context.Background(), bson.M{"viewer": bson.M{"$in": bson.A{viewer, viewer + "-other"}}}); e != nil {
						t.Error("cleanup", e)
					}
				}
			}()
			body := strings.Repeat("中文 [A-Z] %_\n", 1600000) + "尾部NEEDLE跨块"
			record := func(id string) map[string]any {
				return map[string]any{"id": id, "createdAt": "2026-11-17T00:00:00Z", "request": body, "response": "完整响应\x00SSE\n", "model": "model", "keyId": "k"}
			}
			if err = s.Save(ctx, viewer, "own", record("a")); err != nil {
				t.Fatal(err)
			}
			storedBytes := int64(0)
			stored, err := s.blocks.Find(ctx, bson.M{"viewer": viewer})
			if err != nil {
				t.Fatal(err)
			}
			for stored.Next(ctx) {
				var block struct{ Data []byte }
				if err = stored.Decode(&block); err != nil {
					stored.Close(ctx)
					t.Fatal(err)
				}
				storedBytes += int64(len(block.Data))
			}
			err = stored.Err()
			stored.Close(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if storedBytes >= int64(len(body))/10 {
				t.Fatal("body compression ineffective", storedBytes, len(body))
			}
			count, err := s.blocks.CountDocuments(ctx, bson.M{"viewer": viewer})
			if err != nil || count == 0 {
				t.Fatal("missing compressed blocks", count, err)
			}
			if err = s.Save(ctx, viewer, "own", record("b")); err != nil {
				t.Fatal(err)
			}
			same, _ := s.blocks.CountDocuments(ctx, bson.M{"viewer": viewer})
			if same != count {
				t.Fatal("duplicate body blocks", count, same)
			}
			if err = s.Save(ctx, viewer, "others", record("scope-copy")); err != nil {
				t.Fatal(err)
			}
			same, _ = s.blocks.CountDocuments(ctx, bson.M{"viewer": viewer})
			if same != count {
				t.Fatal("same account scope copy duplicated body", same, count)
			}
			if err = s.Save(ctx, viewer+"-other", "own", record("private-copy")); err != nil {
				t.Fatal(err)
			}
			privateCount, _ := s.blocks.CountDocuments(ctx, bson.M{"viewer": viewer + "-other"})
			if privateCount != count {
				t.Fatal("account blocks not isolated", privateCount, count)
			}
			got, err := s.Get(ctx, viewer, "own", "b")
			if err != nil || got["request"] != body || got["response"] != "完整响应\x00SSE\n" {
				t.Fatal("lossless large restore", err)
			}
			var doc bson.M
			if err = s.records.FindOne(ctx, bson.M{"viewer": viewer, "id": "b"}).Decode(&doc); err != nil {
				t.Fatal(err)
			}
			if !historyBodyIsPacked(historyText(doc["payload"])) || strings.Contains(historyText(doc["payload"]), "尾部NEEDLE") {
				t.Fatal("plaintext record retained")
			}
			oldCount, _ := s.chunks.CountDocuments(ctx, bson.M{"viewer": viewer})
			if oldCount != 0 {
				t.Fatal("plaintext search chunks retained", oldCount)
			}
			q := historyQuery{Scope: "own", Q: "尾部needle跨块", Limit: 1}
			page, err := s.Query(ctx, viewer, q)
			if err != nil || page.Total != 2 || len(page.Items) != 1 || page.NextCursor == "" {
				t.Fatal("search paging", page, err)
			}
			if _, exists := page.Items[0]["request"]; exists {
				t.Fatal("metadata search leaked request body")
			}
			if _, exists := page.Items[0]["response"]; exists {
				t.Fatal("metadata search leaked response body")
			}
			q.Before = page.NextCursor
			page, err = s.Query(ctx, viewer, q)
			if err != nil || page.Total != 2 || len(page.Items) != 1 || page.NextCursor != "" {
				t.Fatal("search cursor", page, err)
			}
			if _, err = s.Get(ctx, viewer+"-other", "own", "b"); err == nil {
				t.Fatal("cross account read")
			}
			if err = s.Delete(ctx, viewer, "own", "a"); err != nil {
				t.Fatal(err)
			}
			if _, err = s.collectBodies(ctx, 1000); err != nil {
				t.Fatal(err)
			}
			got, err = s.Get(ctx, viewer, "own", "b")
			if err != nil || got["request"] != body {
				t.Fatal("shared block removed", err)
			}
			// Pre-chunk inline documents must not retain plaintext after tombstone collection.
			inline := map[string]any{"id": "inline", "createdAt": "2026-11-17T00:00:00Z", "request": "inline legacy"}
			inlineRaw, _ := json.Marshal(inline)
			_, err = s.records.InsertOne(ctx, bson.M{"viewer": viewer, "scope": "own", "id": "inline", "deleted": false, "payload": string(inlineRaw), "metadata": "{}"})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Delete(ctx, viewer, "own", "inline"); err != nil {
				t.Fatal(err)
			}
			if _, err = s.collectBodies(ctx, 1000); err != nil {
				t.Fatal(err)
			}
			var tombstone bson.M
			if err = s.records.FindOne(ctx, bson.M{"viewer": viewer, "id": "inline"}).Decode(&tombstone); err != nil {
				t.Fatal(err)
			}
			if _, exists := tombstone["payload"]; exists {
				t.Fatal("deleted inline legacy plaintext retained")
			}
			// Legacy chunks must stay readable until a verified migration publishes the manifest.
			legacy := record("legacy")
			legacy["request"] = "旧明文跨块 NEEDLE"
			raw, _ := json.Marshal(legacy)
			sum := sha256.Sum256(raw)
			digest := hex.EncodeToString(sum[:])
			header := record("legacy")
			delete(header, "request")
			delete(header, "response")
			headerRaw, _ := json.Marshal(header)
			metadata, _ := json.Marshal(historyRecordMetadata(legacy))
			_, err = s.records.InsertOne(ctx, bson.M{"viewer": viewer, "scope": "own", "id": "legacy", "created": "2026-11-17T00:00:00.000000000Z", "deleted": false, "payload": string(headerRaw), "metadata": string(metadata), "bodyDigest": digest, "bodyFields": bson.A{"request", "response"}})
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"request", "response"} {
				_, err = s.chunks.InsertOne(ctx, bson.M{"viewer": viewer, "scope": "own", "id": "legacy", "digest": digest, "field": field, "sequence": 0, "data": legacy[field], "search": strings.ToLower(historyText(legacy[field]))})
				if err != nil {
					t.Fatal(err)
				}
			}
			got, err = s.Get(ctx, viewer, "own", "legacy")
			if err != nil || got["request"] != legacy["request"] {
				t.Fatal("legacy read", err)
			}
			_, legacyBlocks, e := historyBodyEncode(legacy)
			if e != nil {
				t.Fatal(e)
			}
			bad := legacyBlocks[0]
			_, err = s.blocks.UpdateOne(ctx, bson.M{"viewer": viewer, "hash": bad.Hash}, bson.M{"$setOnInsert": bson.M{"rawSize": bad.RawSize, "data": []byte("corrupt"), "generation": int64(1)}}, options.UpdateOne().SetUpsert(true))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.migrateBodies(ctx, 1000); err == nil {
				t.Fatal("corrupt migration accepted")
			}
			got, err = s.Get(ctx, viewer, "own", "legacy")
			if err != nil || got["request"] != legacy["request"] {
				t.Fatal("failed migration damaged original", err)
			}
			_, err = s.blocks.UpdateOne(ctx, bson.M{"viewer": viewer, "hash": bad.Hash}, bson.M{"$set": bson.M{"data": bad.Data}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.migrateBodies(ctx, 1000); err != nil {
				t.Fatal(err)
			}
			if _, err = s.migrateBodies(ctx, 1000); err != nil {
				t.Fatal("idempotent migration", err)
			}
			got, err = s.Get(ctx, viewer, "own", "legacy")
			if err != nil || got["request"] != legacy["request"] {
				t.Fatal("legacy migrated", err)
			}
			oldCount, _ = s.chunks.CountDocuments(ctx, bson.M{"viewer": viewer})
			if oldCount != 0 {
				t.Fatal("legacy plaintext retained", oldCount)
			}
			// Reserve a pending reference before GC; racing writers and collector must keep every published body.
			var wg sync.WaitGroup
			failures := make(chan error, 16)
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					v := record(fmt.Sprintf("race-%d", i))
					v["request"] = "共享并发块"
					if e := s.Save(ctx, viewer, "own", v); e != nil {
						failures <- e
					}
					if _, e := s.collectBodies(ctx, 1000); e != nil {
						failures <- e
					}
				}(i)
			}
			wg.Wait()
			close(failures)
			for e := range failures {
				t.Error(e)
			}
			for i := 0; i < 8; i++ {
				if _, err = s.Get(ctx, viewer, "own", fmt.Sprintf("race-%d", i)); err != nil {
					t.Fatal("concurrent collection", err)
				}
			}
			// An incomplete publication remains hidden and retryable, including across repository reopen.
			pending := record("pending")
			pending["request"] = "pending body"
			packed, blocks, e := historyBodyEncode(pending)
			if e != nil {
				t.Fatal(e)
			}
			token := uuid.NewString()
			hashes := []string{}
			for _, block := range blocks {
				hashes = append(hashes, block.Hash)
			}
			_, err = s.records.InsertOne(ctx, bson.M{"viewer": viewer, "scope": "own", "id": "pending", "created": "2026-11-17T00:00:00.000000000Z", "deleted": false, "payload": packed, "metadata": "{}", "bodyPending": true, "bodyToken": token, "bodyHashes": hashes})
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := openMongoHistoryLeaf(ctx, c, "text")
			if err != nil {
				t.Fatal(err)
			}
			if err = reopened.Save(ctx, viewer, "own", pending); err != nil {
				reopened.Close()
				t.Fatal("resume pending after reopen", err)
			}
			if err = reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err = s.Get(ctx, viewer, "own", "pending"); err != nil {
				t.Fatal("resumed publication", err)
			}
			orphan := historyBodyHash([]byte(uuid.NewString()))
			if _, err = s.blocks.InsertOne(ctx, bson.M{"viewer": viewer, "hash": orphan, "rawSize": 1, "data": []byte("orphan"), "generation": int64(1)}); err != nil {
				t.Fatal(err)
			}
			if _, err = s.collectBodies(ctx, 1000); err != nil {
				t.Fatal(err)
			}
			orphans, _ := s.blocks.CountDocuments(ctx, bson.M{"viewer": viewer, "hash": orphan})
			if orphans != 0 {
				t.Fatal("orphan block retained", orphans)
			}

			var selected mongoHistoryBodyDocument
			if err = s.records.FindOne(ctx, bson.M{"viewer": viewer, "scope": "own", "id": "b"}).Decode(&selected); err != nil || len(selected.BodyHashes) == 0 {
				t.Fatal("missing record block", err)
			}
			hash := selected.BodyHashes[0]

			_, err = s.blocks.UpdateOne(ctx, bson.M{"viewer": viewer, "hash": hash}, bson.M{"$set": bson.M{"data": []byte("corrupt")}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Get(ctx, viewer, "own", "b"); err == nil {
				t.Fatal("corrupt block accepted")
			}
			t.Logf("MongoDB %s restored %d bytes, dedup blocks=%d, compressed binary bytes=%d; Unicode search/paging, legacy migration, isolation and concurrent GC", s.Version(), len(body), count, storedBytes)
		})
	}
}
