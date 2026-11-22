package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/v2/bson"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Dedicated fixtures only: unique task databases are created and dropped; no
// existing deployment database or shared history tables are touched.
func TestHistoryCategoryDedicatedEngines(t *testing.T) {
	for _, engine := range []string{"postgres", "mysql", "mongodb"} {
		t.Run(engine, func(t *testing.T) {
			prefix := "CORE_CATEGORY_" + strings.ToUpper(engine)
			port, _ := strconv.Atoi(os.Getenv(prefix + "_PORT"))
			if port == 0 {
				t.Skip("dedicated fixture unavailable")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			c := defaultHistoryDatabase(t.TempDir())
			c.Engine = engine
			c.Host = os.Getenv(prefix + "_HOST")
			if c.Host == "" {
				c.Host = "127.0.0.1"
			}
			c.Port = port
			c.Username = os.Getenv(prefix + "_USER")
			c.Password = os.Getenv(prefix + "_PASSWORD")
			c.AuthDatabase = "admin"
			c.Database = fmt.Sprintf("test_core_category_%d", time.Now().UnixNano())
			if engine != "mongodb" {
				admin := c
				admin.Database = "postgres"
				if engine == "mysql" {
					admin.Database = "mysql"
				}
				var driver, dsn string
				if engine == "postgres" {
					driver = "pgx"
					u := url.URL{Scheme: "postgres", Host: net.JoinHostPort(admin.Host, strconv.Itoa(admin.Port)), Path: "/postgres", User: url.UserPassword(admin.Username, admin.Password)}
					params := u.Query()
					params.Set("sslmode", "disable")
					u.RawQuery = params.Encode()
					dsn = u.String()
				} else {
					driver = "mysql"
					cfg := mysql.NewConfig()
					cfg.User = admin.Username
					cfg.Passwd = admin.Password
					cfg.Net = "tcp"
					cfg.Addr = net.JoinHostPort(admin.Host, strconv.Itoa(admin.Port))
					cfg.DBName = "mysql"
					dsn = cfg.FormatDSN()
				}
				db, e := sql.Open(driver, dsn)
				if e != nil {
					t.Fatal(e)
				}
				a := &sqlHistoryRepository{db: db, engine: engine}
				if _, e = a.db.ExecContext(ctx, "CREATE DATABASE "+c.Database); e != nil {
					a.Close()
					t.Fatal(e)
				}
				defer func() {
					if _, e := a.db.ExecContext(context.Background(), "DROP DATABASE "+c.Database); e != nil {
						t.Error(e)
					}
					a.Close()
				}()
			}
			var r historyRepository
			var e error
			if engine == "mongodb" {
				r, e = openMongoHistoryLeaf(ctx, c, "text")
				if e == nil {
					m := r.(*mongoHistoryRepository)
					m.records = m.client.Database(c.Database).Collection("nn_history_records")
					m.chunks = m.client.Database(c.Database).Collection("nn_history_chunks")
					m.blocks = m.client.Database(c.Database).Collection("nn_history_body_blocks")
				}
			} else {
				r, e = openSQLHistoryTable(ctx, c, "nn_history_records")
			}
			if e != nil {
				t.Fatal(e)
			}

			for i := 0; i < 8; i++ {
				cat := "text"
				if i%2 == 0 {
					cat = "image"
				}
				v := map[string]any{"id": fmt.Sprintf("[TEST] %02d", i), "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": cat, "model": "[TEST] model", "request": "[TEST] 中文 immutable", "response": strings.Repeat("[TEST] response", 30000), "pointsConsumed": nil, "imageUsage": map[string]any{"textInputTokens": 0, "imageInputTokens": nil}}
				if e = r.Save(ctx, "viewer", "own", v); e != nil {
					t.Fatal(e)
				}
			}
			if engine == "mongodb" {
				m := r.(*mongoHistoryRepository)
				// Both legacy shapes must be rebuilt on migration, including rows copied
				// before an interruption and rows retried after restart.
				for _, id := range []string{"[TEST] 00", "[TEST] 04"} {
					if _, e = m.records.UpdateOne(ctx, bson.M{"id": id}, bson.M{"$unset": bson.M{"model_category": "", "filter_version": "", "model_id": "", "model_label": ""}}); e != nil {
						t.Fatal(e)
					}
				}
				for _, id := range []string{"[TEST] 02", "[TEST] 06"} {
					if _, e = m.records.UpdateOne(ctx, bson.M{"id": id}, bson.M{"$set": bson.M{"model_category": "text", "filter_version": 2, "model_id": "stale", "model_label": "stale"}}); e != nil {
						t.Fatal(e)
					}
				}
			}
			r.Close()
			if engine == "mongodb" {
				fixture, e := openMongoHistoryLeaf(ctx, c, "image")
				if e != nil {
					t.Fatal(e)
				}
				m := fixture.(*mongoHistoryRepository)
				command := bson.D{{Key: "collMod", Value: "nn_history_image_records"}, {Key: "validator", Value: bson.M{"$expr": bson.M{"$ne": bson.A{"$id", "[TEST] 04"}}}}}
				if e = m.client.Database(c.Database).RunCommand(ctx, command).Err(); e != nil {
					t.Fatal(e)
				}
				m.Close()
				if failed, e := openHistoryRepository(ctx, c); e == nil {
					failed.Close()
					t.Fatal("Mongo migration interruption was not injected")
				}
				fixture, e = openMongoHistoryLeaf(ctx, c, "image")
				if e != nil {
					t.Fatal(e)
				}
				m = fixture.(*mongoHistoryRepository)
				remaining, e := m.client.Database(c.Database).Collection("nn_history_records").CountDocuments(ctx, bson.M{})
				if e != nil || remaining != 4 {
					t.Fatalf("Mongo resumable checkpoint remaining=%d %v", remaining, e)
				}
				if e = m.client.Database(c.Database).RunCommand(ctx, bson.D{{Key: "collMod", Value: "nn_history_image_records"}, {Key: "validator", Value: bson.M{}}}).Err(); e != nil {
					t.Fatal(e)
				}
				m.Close()
			}
			r, e = openHistoryRepository(ctx, c)
			if e != nil {
				t.Fatal(e)
			}
			defer r.Close()
			if m, ok := r.(*mongoHistoryRepository); ok {
				defer m.client.Database(c.Database).Drop(context.Background())
			}
			if engine == "mongodb" {
				m := r.(*mongoHistoryRepository)
				cursor, e := m.leaves["image"].records.Find(ctx, bson.M{})
				if e != nil {
					t.Fatal(e)
				}
				for cursor.Next(ctx) {
					var doc bson.M
					if e = cursor.Decode(&doc); e != nil {
						t.Fatal(e)
					}
					if doc["model_category"] != "image" || doc["model_id"] != "[TEST] model" || doc["model_label"] != "[TEST] model" {
						t.Fatalf("stale migrated projection: %v", doc["id"])
					}
					version, ok := doc["filter_version"].(int32)
					if !ok || version != 2 {
						t.Fatalf("projection version: %v", doc["filter_version"])
					}
				}
				if e = cursor.Err(); e != nil {
					t.Fatal(e)
				}
				cursor.Close(ctx)
				models, e := r.Filters(ctx, "viewer", historyFilterQuery{ModelCategory: "image", Kind: "models"})
				if e != nil || len(models.Items) != 1 || models.Items[0].ID != "[TEST] model" {
					t.Fatalf("migrated model options %+v %v", models, e)
				}
			}
			conflict := map[string]any{"id": "[TEST] 00", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "modelCategory": "text"}
			if e = r.Save(ctx, "viewer", "own", conflict); e == nil {
				t.Fatal("cross category identity accepted")
			}
			for _, kind := range []string{"models", "keys"} {
				if _, e = r.Filters(ctx, "viewer", historyFilterQuery{ModelCategory: "image", Kind: kind}); e != nil {
					t.Fatal(e)
				}
			}
			for _, cat := range []string{"text", "image", ""} {
				q := historyQuery{ModelCategory: cat, Limit: 2}
				p, e := r.Query(ctx, "viewer", q)
				want := int64(4)
				if cat == "" {
					want = 8
				}
				if e != nil || p.Total != want || len(p.Items) != 2 {
					t.Fatalf("page%s %+v %v", cat, p, e)
				}
				q.Before = p.NextCursor
				p2, e := r.Query(ctx, "viewer", q)
				if e != nil || len(p2.Items) != 2 || p2.Items[0]["id"] == p.Items[0]["id"] {
					t.Fatalf("next %+v %v", p2, e)
				}
			}
			if e = r.Enrich(ctx, "viewer", "own", "[TEST] 00", map[string]any{"pointsConsumed": "0.000000000", "pointsStatus": "known", "imageCosts": map[string]any{"input": "0"}}); e != nil {
				t.Fatal(e)
			}
			value, e := r.Get(ctx, "viewer", "own", "[TEST] 00")
			if e != nil || value["pointsConsumed"] != "0.000000000" {
				t.Fatalf("snapshot%v %v", value, e)
			}
			for _, format := range []string{"json", "csv"} {
				out, e := writeHistoryExport(ctx, r, "viewer", historyQuery{ModelCategory: "image"}, format, t.TempDir(), nil, nil)
				if e != nil || out.Count != 4 {
					t.Fatalf("export %+v %v", out, e)
				}
			}
			if e = r.Delete(ctx, "viewer", "own", "[TEST] 00"); e != nil {
				t.Fatal(e)
			}
			if e = r.Save(ctx, "viewer", "own", value); e != nil {
				t.Fatal(e)
			}
			if _, e = r.Get(ctx, "viewer", "own", "[TEST] 00"); e == nil {
				t.Fatal("deleted ID revived")
			}
			if s, ok := r.(*sqlHistoryRepository); ok {
				var n int
				if e = s.db.QueryRowContext(ctx, "SELECT count(*) FROM nn_history_image_records").Scan(&n); e != nil || n != 4 {
					t.Fatal(n, e)
				}
			} else {
				m := r.(*mongoHistoryRepository)
				n, e := m.leaves["image"].records.CountDocuments(ctx, bson.M{})
				if e != nil || n != 4 {
					t.Fatal(n, e)
				}
			}
		})
	}
}
