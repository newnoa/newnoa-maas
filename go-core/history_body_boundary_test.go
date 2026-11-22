package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func boundaryHistoryArchive(t *testing.T, raw []byte, codec string) historyArchive {
	t.Helper()
	var compressed []byte
	if codec == "gzip-v1" {
		var buffer bytes.Buffer
		writer := gzip.NewWriter(&buffer)
		if _, err := writer.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		compressed = buffer.Bytes()
	} else {
		writer, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(historyBodyChunkMax))
		if err != nil {
			t.Fatal(err)
		}
		compressed = writer.EncodeAll(raw, nil)
		writer.Close()
	}
	return historyArchive{ID: historyHash(raw), Codec: codec, RawBytes: len(raw), CompressedBytes: len(compressed), SHA256: historyHash(raw), BodyBase64: base64.StdEncoding.EncodeToString(compressed)}
}

func TestHistoryBodyBoundaryArchiveRejectsUnverifiedBytes(t *testing.T) {
	raw := []byte(strings.Repeat("[TEST] 正文\n", 64))
	for _, codec := range []string{"gzip-v1", "zstd-v1"} {
		t.Run(codec, func(t *testing.T) {
			valid := boundaryHistoryArchive(t, raw, codec)
			decoded, err := decodeHistoryArchive(valid, len(raw))
			if err != nil || !bytes.Equal(decoded, raw) {
				t.Fatalf("valid archive did not round trip: %v", err)
			}
			for _, tc := range []struct {
				name   string
				mutate func(*historyArchive)
				limit  int
			}{
				{"negative-length", func(a *historyArchive) { a.RawBytes = -1 }, len(raw)},
				{"declared-expansion", func(a *historyArchive) { a.RawBytes++ }, len(raw)},
				{"forged-compressed-length", func(a *historyArchive) { a.CompressedBytes++ }, len(raw)},
				{"invalid-base64", func(a *historyArchive) { a.BodyBase64 = "%%%" }, len(raw)},
				{"unsupported-format", func(a *historyArchive) { a.Codec = "plain-json" }, len(raw)},
				{"corrupt-stream", func(a *historyArchive) {
					a.BodyBase64 = base64.StdEncoding.EncodeToString([]byte("broken"))
					a.CompressedBytes = 6
				}, len(raw)},
				{"truncated-stream", func(a *historyArchive) {
					compressed, err := base64.StdEncoding.DecodeString(a.BodyBase64)
					if err != nil {
						t.Fatal(err)
					}
					compressed = compressed[:len(compressed)-2]
					a.CompressedBytes = len(compressed)
					a.BodyBase64 = base64.StdEncoding.EncodeToString(compressed)
				}, len(raw)},
				{"forged-checksum", func(a *historyArchive) { a.SHA256 = strings.Repeat("0", 64) }, len(raw)},
				{"forged-raw-length", func(a *historyArchive) { a.RawBytes-- }, len(raw)},
				{"hidden-expansion", func(a *historyArchive) { a.RawBytes = 32 }, 32},
			} {
				t.Run(tc.name, func(t *testing.T) {
					item := valid
					tc.mutate(&item)
					if got, err := decodeHistoryArchive(item, tc.limit); err == nil || got != nil {
						t.Fatalf("unverified archive exposed bytes: len=%d err=%v", len(got), err)
					}
				})
			}
		})
	}
}

func TestHistoryBodyBoundaryLocalManifestAndBlockIdentity(t *testing.T) {
	body := "[TEST] durable local body"
	header, blocks, err := historyBodyEncode(map[string]any{"id": "local-record", "request": body})
	if err != nil || len(blocks) != 1 {
		t.Fatalf("encode fixture: blocks=%d err=%v", len(blocks), err)
	}
	var original struct {
		Manifest historyBodyManifest `json:"_newnoaBody"`
	}
	if err := json.Unmarshal([]byte(header), &original); err != nil {
		t.Fatal(err)
	}
	missing := errors.New("[TEST] missing body block")
	for _, tc := range []struct {
		name        string
		mutate      func(map[string]any, *historyBodyManifest, *historyBodyBlock)
		unavailable bool
		loadErr     error
	}{
		{name: "unsupported-version", mutate: func(_ map[string]any, m *historyBodyManifest, _ *historyBodyBlock) { m.Version = 2 }},
		{name: "missing-fields", mutate: func(_ map[string]any, m *historyBodyManifest, _ *historyBodyBlock) { m.Fields = nil }},
		{name: "unexpected-field", mutate: func(_ map[string]any, m *historyBodyManifest, _ *historyBodyBlock) {
			m.Fields["credential"] = m.Fields["request"]
			delete(m.Fields, "request")
		}},
		{name: "ambiguous-body", mutate: func(record map[string]any, _ *historyBodyManifest, _ *historyBodyBlock) {
			record["request"] = "[TEST] competing plaintext"
		}},
		{name: "negative-body-size", mutate: func(_ map[string]any, m *historyBodyManifest, _ *historyBodyBlock) {
			f := m.Fields["request"]
			f.Size = -1
			m.Fields["request"] = f
		}},
		{name: "invalid-part-hash", mutate: func(_ map[string]any, m *historyBodyManifest, _ *historyBodyBlock) {
			f := m.Fields["request"]
			f.Parts[0].Hash = strings.Repeat("A", 64)
			m.Fields["request"] = f
		}},
		{name: "missing-part", mutate: func(_ map[string]any, m *historyBodyManifest, _ *historyBodyBlock) {
			f := m.Fields["request"]
			f.Parts = nil
			m.Fields["request"] = f
		}},
		{name: "nil-loader", unavailable: true},
		{name: "loader-failure", loadErr: missing},
		{name: "wrong-block-identity", mutate: func(_ map[string]any, _ *historyBodyManifest, b *historyBodyBlock) { b.Hash = strings.Repeat("0", 64) }},
		{name: "wrong-block-size", mutate: func(_ map[string]any, _ *historyBodyManifest, b *historyBodyBlock) { b.RawSize++ }},
		{name: "oversized-compressed-block", mutate: func(_ map[string]any, _ *historyBodyManifest, b *historyBodyBlock) {
			b.Data = make([]byte, historyBodyChunkMax+1025)
		}},
		{name: "corrupt-compression", mutate: func(_ map[string]any, _ *historyBodyManifest, b *historyBodyBlock) { b.Data = []byte("broken zstd") }},
		{name: "substituted-body", mutate: func(_ map[string]any, _ *historyBodyManifest, b *historyBodyBlock) {
			archive := boundaryHistoryArchive(t, bytes.Repeat([]byte("x"), len(body)), "zstd-v1")
			b.Data, _ = base64.StdEncoding.DecodeString(archive.BodyBase64)
		}},
		{name: "decompression-over-part-limit", mutate: func(_ map[string]any, _ *historyBodyManifest, b *historyBodyBlock) {
			archive := boundaryHistoryArchive(t, bytes.Repeat([]byte("x"), len(body)+1), "zstd-v1")
			b.Data, _ = base64.StdEncoding.DecodeString(archive.BodyBase64)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var copyManifest historyBodyManifest
			encoded, err := json.Marshal(original.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &copyManifest); err != nil {
				t.Fatal(err)
			}
			record := map[string]any{"id": "local-record"}
			block := blocks[0]
			if tc.mutate != nil {
				tc.mutate(record, &copyManifest, &block)
			}
			record[historyBodyMarker] = copyManifest
			raw, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			loader := func(string) (historyBodyBlock, error) { return block, tc.loadErr }
			if tc.unavailable {
				loader = nil
			}
			if got, err := historyBodyDecode(string(raw), loader); err == nil || got != nil {
				t.Fatalf("invalid body returned partial record: %v %v", got, err)
			} else if tc.loadErr != nil && !errors.Is(err, missing) {
				t.Fatalf("loader failure lost cause: %v", err)
			}
			// Rejection must not mutate the previously valid manifest or block.
			got, err := historyBodyDecode(header, func(string) (historyBodyBlock, error) { return blocks[0], nil })
			if err != nil || got["request"] != body {
				t.Fatalf("valid stored body no longer readable: %v %v", got, err)
			}
		})
	}
}

func TestHistoryBodyBoundaryCloudFailurePreservesRecordAndCursor(t *testing.T) {
	const account = "00000000-0000-0000-0000-000000000001"
	const snapshot = "00000000-0000-0000-0000-000000000099"
	for _, failure := range []string{"codec-confusion", "malformed-manifest", "field-limit", "wrong-block-identity", "invalid-utf8"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			created := time.Now().UTC().Format(time.RFC3339Nano)
			var repaired atomic.Bool
			validBody := []byte("[TEST] cloud body")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body := validBody
				if failure == "invalid-utf8" && !repaired.Load() {
					body = []byte{0xff, 0xfe}
				}
				block := boundaryHistoryArchive(t, body, "zstd-v1")
				switch r.URL.Path {
				case "/api/v1/me/history/settings":
					json.NewEncoder(w).Encode(map[string]any{"canSyncOthers": true})
				case "/api/v1/me/history/sync/start":
					json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "snapshot": snapshot, "after": "0", "upper": "2", "totalRecords": 1})
				case "/api/v1/me/history/sync/batch":
					size := len(body)
					if failure == "field-limit" && !repaired.Load() {
						size = (32 << 20) + 1
					}
					manifest := map[string]any{"schemaVersion": 1, "record": map[string]any{"id": "r", "createdAt": created, "model": "[TEST] model"}, "fields": map[string]any{
						"request":  map[string]any{"rawBytes": size, "sha256": historyHash(body), "blocks": []string{block.ID}},
						"response": map[string]any{"rawBytes": 0, "sha256": historyHash(nil), "blocks": []string{}},
					}}
					raw, err := json.Marshal(manifest)
					if err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					if failure == "malformed-manifest" && !repaired.Load() {
						raw = []byte(`{"schemaVersion":`)
					}
					item := boundaryHistoryArchive(t, raw, "zstd-json-manifest-v1")
					if failure == "codec-confusion" && !repaired.Load() {
						item.Codec = "zstd-v1"
					}
					item.ID = "r"
					json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "nextCursor": "1", "done": true, "items": []any{struct {
						historyArchive
						Kind     string         `json:"kind"`
						Sequence string         `json:"sequence"`
						Metadata map[string]any `json:"metadata"`
					}{item, "record", "1", map[string]any{"scope": "others", "publisherAccountId": account}}}})
				case "/api/v1/me/history/sync/blocks":
					if failure == "wrong-block-identity" && !repaired.Load() {
						block.ID = strings.Repeat("0", 64)
					}
					json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "blocks": []historyArchive{block}})
				default:
					t.Errorf("unexpected cloud path %s", r.URL.Path)
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			c := newCore(t.TempDir(), server.URL)
			configureTestHistorySQLite(t, c.home, account)
			c.control = server.URL
			h, err := c.historyStore()
			if err != nil {
				t.Fatal(err)
			}
			defer h.db.Close()
			m, err := h.manager.forAccount(account)
			if err != nil {
				t.Fatal(err)
			}
			defer m.store.Close()
			defer c.shutdown()
			if err := m.commit(ctx, account, "others", "record", "r", map[string]any{"id": "r", "createdAt": created, "model": "[TEST] model", "request": "[TEST] stored body", "response": "[TEST] stored reply"}); err != nil {
				t.Fatal(err)
			}
			before, err := m.store.Get(ctx, account, "others", "r")
			if err != nil {
				t.Fatal(err)
			}
			if err := c.syncHistory(ctx, m, account, "[TEST] session", "others", "job"); err == nil {
				t.Fatal("untrusted cloud event accepted")
			}
			after, err := m.store.Get(ctx, account, "others", "r")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected cloud event changed stored record: before=%v after=%v err=%v", before, after, err)
			}
			var cursors int
			if err := h.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM history_sync_cursors WHERE viewer=? AND scope='others'", account).Scan(&cursors); err != nil || cursors != 0 {
				t.Fatalf("rejected event advanced cursor: count=%d err=%v", cursors, err)
			}
			repaired.Store(true)
			if err := c.syncHistory(ctx, m, account, "[TEST] session", "others", "job"); err != nil {
				t.Fatalf("repaired snapshot could not resume: %v", err)
			}
			var cursor string
			if err := h.db.QueryRowContext(ctx, "SELECT cursor FROM history_sync_cursors WHERE viewer=? AND scope='others'", account).Scan(&cursor); err != nil || cursor != "2" {
				t.Fatalf("repaired snapshot failed to commit cursor: %q %v", cursor, err)
			}
		})
	}
}

func TestHistoryBodyBoundaryMetadataCannotEraseDurableEvidence(t *testing.T) {
	for _, terminal := range []map[string]any{{"pointsStatus": "known"}, {"billingPhase": "final"}} {
		value := map[string]any{"id": "immutable-id", "request": "[TEST] request", "response": "[TEST] response", "inputTokens": 12, "outputTokens": 4, "pointsConsumed": "0.125", "usageSource": "upstream", "firstTokenMs": 8, "callerSource": "agent", "keyName": "old key"}
		for key, field := range terminal {
			value[key] = field
		}
		before := make(map[string]any, len(value))
		for key, field := range value {
			before[key] = field
		}
		mergeHistoryTrustedMetadata(value, map[string]any{"id": "injected-id", "request": "replace request", "response": "replace response", "pointsStatus": "pending", "billingPhase": nil, "usageSource": nil, "pointsConsumed": "999", "inputTokens": 0, "outputTokens": nil, "firstTokenMs": nil, "callerSource": nil, "keyName": "[TEST] refreshed key"})
		before["keyName"] = "[TEST] refreshed key"
		if !reflect.DeepEqual(value, before) {
			t.Fatalf("metadata erased immutable or terminal evidence: want=%v got=%v", before, value)
		}
	}
	provisional := map[string]any{"inputTokens": 7, "cachedInputTokens": 2, "usageSource": "estimated", "billingPhase": "provisional"}
	mergeHistoryTrustedMetadata(provisional, map[string]any{"inputTokens": nil, "cachedInputTokens": nil, "usageSource": nil, "billingPhase": nil})
	if !reflect.DeepEqual(provisional, map[string]any{"inputTokens": 7, "cachedInputTokens": 2, "usageSource": "estimated", "billingPhase": "provisional"}) {
		t.Fatalf("partial metadata erased provisional evidence: %v", provisional)
	}
}
