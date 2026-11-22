package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestHydratedPendingHistoryUploadsRespectAPICaptureFields(t *testing.T) {
	// Mirror ChatHistory.FIELDS at the API trust boundary, independently of Core's projection helper.
	accepted := map[string]bool{}
	for _, field := range strings.Fields("id createdAt finishedAt durationMs model agent keyId protocol request response requestTruncated responseTruncated httpStatus interrupted contentType transportPath requestId conversationId storageId storageMode modelType attachments inputTokens outputTokens cacheWriteTokens cachedInputTokens pointsConsumed pointsStatus") {
		accepted[field] = true
	}
	account := "00000000-0000-0000-0000-000000000001"
	request := "00000000-0000-0000-0000-000000000123"
	for _, mode := range []string{"archive", "legacy"} {
		t.Run(mode, func(t *testing.T) {
			var fail atomic.Bool
			fail.Store(true)
			var uploads atomic.Int64
			var invalid atomic.Value
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v1/me":
					json.NewEncoder(w).Encode(map[string]any{"id": account})
				case "/api/v1/me/history/settings":
					json.NewEncoder(w).Encode(map[string]any{"canSyncOwn": true, "cloudEnabled": true})
				case "/api/v1/client/requests/" + request + "/consumption":
					json.NewEncoder(w).Encode(map[string]any{"requestId": request, "pointsStatus": "known", "pointsConsumed": "0.000000", "modelName": "[TEST] trusted model", "keyName": "[TEST] trusted key", "keyPrefix": "nn_test", "callerAccountId": account, "callerEmail": "[TEST]@example.test", "publisherAccountId": account, "deploymentId": "[TEST] deployment", "longContextApplied": true, "effectivePointsMultiplier": "1.500000"})
				case "/api/v1/me/history":
					uploads.Add(1)
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					capture := body
					if archive, ok := body["archive"].(map[string]any); ok {
						compressed, err := base64.StdEncoding.DecodeString(historyText(archive["bodyBase64"]))
						if err != nil {
							t.Error(err)
							w.WriteHeader(400)
							return
						}
						decoder, err := zstd.NewReader(nil)
						if err != nil {
							t.Error(err)
							w.WriteHeader(400)
							return
						}
						raw, err := decoder.DecodeAll(compressed, nil)
						decoder.Close()
						if err != nil {
							t.Error(err)
							w.WriteHeader(400)
							return
						}
						if historyHash(raw) != archive["sha256"] || len(raw) != int(archive["rawBytes"].(float64)) {
							t.Error("archive checksum/length changed")
							w.WriteHeader(400)
							return
						}
						capture = nil
						if json.Unmarshal(raw, &capture) != nil {
							t.Error("invalid archive JSON")
							w.WriteHeader(400)
							return
						}
						metadata := body["record"].(map[string]any)
						for field := range metadata {
							if !accepted[field] {
								invalid.Store("record." + field)
								w.WriteHeader(400)
								return
							}
						}
					}
					for field := range capture {
						if !accepted[field] {
							invalid.Store(field)
							w.WriteHeader(400)
							return
						}
					}
					if capture["request"] != "[TEST] original request 中文" || capture["response"] != "[TEST] original response 中文" || capture["protocol"] != "/v1/chat/completions" {
						t.Error("capture body/protocol altered", capture)
						w.WriteHeader(400)
						return
					}
					if fail.Load() {
						w.WriteHeader(503)
						return
					}
					json.NewEncoder(w).Encode(map[string]any{"saved": true})
				default:
					t.Error("unexpected boundary fixture route", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), api.URL)
			configureTestHistorySQLite(t, c.home, account)
			c.control = api.URL
			h, err := c.historyStore()
			if err != nil {
				t.Fatal(err)
			}
			m, err := h.manager.forAccount(account)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { c.shutdown(); m.store.Close(); h.db.Close() }()
			now := time.Now().UTC().Format(time.RFC3339Nano)
			record := map[string]any{"id": request, "requestId": request, "createdAt": now, "finishedAt": now, "durationMs": 1, "model": "[TEST] model", "agent": "[TEST] agent", "keyId": request, "protocol": "/v1/chat/completions", "request": "[TEST] original request 中文", "response": "[TEST] original response 中文", "requestTruncated": false, "responseTruncated": false, "httpStatus": 200, "interrupted": false, "storageMode": "cloud", "pointsStatus": "pending"}
			if err = h.save(account, record, true); err != nil {
				t.Fatal(err)
			}
			if err = h.hydrateConsumption(context.Background(), account, request, "Bearer original-key"); err != nil {
				t.Fatal(err)
			}
			upload := func() error {
				if mode == "archive" {
					return c.uploadHistoryPending(context.Background(), m, account, "original-session")
				}
				_, err := c.legacyHistoryCommand("history.sync", json.RawMessage(`{"session":"original-session"}`))
				return err
			}
			if err = upload(); err == nil {
				t.Fatal("failed upload acknowledged")
			}
			if bad := invalid.Load(); bad != nil {
				t.Fatalf("hydrated local-only field escaped API upload boundary: %v", bad)
			}
			var pending int
			if err = h.db.QueryRow("SELECT pending FROM exchanges WHERE account=? AND id=?", account, request).Scan(&pending); err != nil || pending != 1 {
				t.Fatal("failed upload lost pending capture", pending, err)
			}
			fail.Store(false)
			if err = upload(); err != nil {
				t.Fatal(err)
			}
			if err = h.db.QueryRow("SELECT pending FROM exchanges WHERE account=? AND id=?", account, request).Scan(&pending); err != nil || pending != 0 {
				t.Fatal("successful upload not acknowledged", pending, err)
			}
			var raw string
			if err = h.db.QueryRow("SELECT payload FROM exchanges WHERE account=? AND id=?", account, request).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			local, decodeErr := h.decodeBody(context.Background(), account, raw)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if local["consumptionMetadataHydrated"] != true || local["modelName"] != "[TEST] trusted model" || local["keyName"] != "[TEST] trusted key" || local["callerAccountId"] != account || local["response"] != record["response"] {
				t.Fatal("upload stripped local display snapshot", local)
			}
			if uploads.Load() != 2 {
				t.Fatal(fmt.Sprintf("unexpected upload attempts: %d", uploads.Load()))
			}
		})
	}
}
