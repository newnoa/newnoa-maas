package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

type historyArchive struct {
	ID              string `json:"id"`
	Codec           string `json:"codec"`
	RawBytes        int    `json:"rawBytes"`
	CompressedBytes int    `json:"compressedBytes"`
	SHA256          string `json:"sha256"`
	BodyBase64      string `json:"bodyBase64"`
}
type historyManifest struct {
	SchemaVersion int            `json:"schemaVersion"`
	Record        map[string]any `json:"record"`
	Fields        map[string]struct {
		RawBytes int      `json:"rawBytes"`
		SHA256   string   `json:"sha256"`
		Blocks   []string `json:"blocks"`
	} `json:"fields"`
}

func historyHash(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func decodeHistoryArchive(item historyArchive, limit int) ([]byte, error) {
	if item.RawBytes < 0 || item.RawBytes > limit || item.CompressedBytes < 0 || item.CompressedBytes > limit+65536 || len(item.BodyBase64) > base64.StdEncoding.EncodedLen(limit+65536) {
		return nil, errors.New("history archive exceeds bounds")
	}
	compressed, err := base64.StdEncoding.DecodeString(item.BodyBase64)
	if err != nil || len(compressed) != item.CompressedBytes {
		return nil, errors.New("invalid history compressed length")
	}
	var reader io.ReadCloser
	switch item.Codec {
	case "zstd-v1", "zstd-json-manifest-v1":
		decoder, err := zstd.NewReader(bytes.NewReader(compressed), zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(8<<20), zstd.WithDecoderMaxWindow(8<<20))
		if err != nil {
			return nil, errors.New("invalid history zstd")
		}
		defer decoder.Close()
		raw, err := io.ReadAll(io.LimitReader(decoder, int64(limit)+1))
		if err != nil || len(raw) != item.RawBytes || len(raw) > limit || historyHash(raw) != item.SHA256 {
			return nil, errors.New("history archive checksum or length mismatch")
		}
		return raw, nil
	case "gzip-v1":
		reader, err = gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			return nil, errors.New("invalid history gzip")
		}
		defer reader.Close()
	default:
		return nil, errors.New("unsupported history archive codec")
	}
	raw, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
	if err != nil || len(raw) != item.RawBytes || len(raw) > limit || historyHash(raw) != item.SHA256 {
		return nil, errors.New("history archive checksum or length mismatch")
	}
	return raw, nil
}
func historySyncRemote(ctx context.Context, control, path, session string, body, out any) error {
	if _, err := controlBase(control); err != nil {
		return err
	}
	method := "GET"
	var raw []byte
	var err error
	if body != nil {
		method = "POST"
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	requestCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, method, strings.TrimRight(control, "/")+"/api/v1"+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+session)
	request.Header.Set("Content-Type", "application/json")
	client := connectorControlClient()
	client.Timeout = 60 * time.Second
	response, err := client.Do(request)
	if err != nil {
		return errors.New("history synchronization unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return &historyCloudHTTPError{status: response.StatusCode}
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
	if err != nil || len(data) > 8<<20 {
		return errors.New("history synchronization response exceeds bounds")
	}
	if out != nil && json.Unmarshal(data, out) != nil {
		return errors.New("invalid history synchronization response")
	}
	return nil
}
func (m *historyManager) cursorEngine() string {
	raw, _ := json.Marshal(m.config)
	return historyHash(raw)
}

// Local export and retention settings do not change the database's cloud synchronization position.
func (m *historyManager) preserveHistoryCursors(ctx context.Context, previous, next historyDatabaseConfig) error {
	a, _ := json.Marshal(previous)
	b, _ := json.Marshal(next)
	_, err := m.journal.db.ExecContext(ctx, `INSERT INTO history_sync_cursors(viewer,scope,engine,cursor)
		SELECT viewer,scope,?,cursor FROM history_sync_cursors WHERE engine=? AND (?='' OR viewer=?)
		ON CONFLICT(viewer,scope,engine) DO UPDATE SET cursor=excluded.cursor
		WHERE CAST(excluded.cursor AS INTEGER)>CAST(history_sync_cursors.cursor AS INTEGER)`, historyHash(b), historyHash(a), m.owner, m.owner)
	return err
}
func (c *core) syncHistory(ctx context.Context, m *historyManager, account, session, scope, job string) (syncErr error) {
	if m.recordingDisabled() {
		return errors.New("history recording is disabled")
	}
	defer func() {
		var remoteErr *historyCloudHTTPError
		if errors.As(syncErr, &remoteErr) && remoteErr.status == 401 {
			c.revokeHistorySession(session)
		}
	}()
	var settings struct {
		CanSyncOwn    bool `json:"canSyncOwn"`
		CanSyncOthers bool `json:"canSyncOthers"`
		CloudEnabled  bool `json:"cloudEnabled"`
	}
	if err := historySyncRemote(ctx, c.control, "/me/history/settings", session, nil, &settings); err != nil {
		return err
	}
	if scope == "own" && (!settings.CanSyncOwn || !settings.CloudEnabled) || scope == "others" && !settings.CanSyncOthers {
		return errors.New("history cloud synchronization is not permitted")
	}
	if scope == "own" {
		if err := c.uploadHistoryPending(ctx, m, account, session); err != nil {
			return err
		}
	}
	m.mu.Lock()
	engine := m.cursorEngine()
	m.mu.Unlock()
	cursor := "0"
	err := m.journal.db.QueryRowContext(ctx, "SELECT cursor FROM history_sync_cursors WHERE viewer=? AND scope=? AND engine=?", account, scope, engine).Scan(&cursor)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return err
	}
	var snapshot struct {
		SchemaVersion   int    `json:"schemaVersion"`
		Snapshot        string `json:"snapshot"`
		After           string `json:"after"`
		Upper           string `json:"upper"`
		TotalRecords    int64  `json:"totalRecords"`
		TotalCandidates int64  `json:"totalCandidates"`
	}
	if err = historySyncRemote(ctx, c.control, "/me/history/sync/start", session, map[string]any{"scope": scope, "cursor": cursor}, &snapshot); err != nil {
		return err
	}
	after, err := strconv.ParseInt(snapshot.After, 10, 64)
	if err != nil || after < 0 {
		return errors.New("invalid history starting cursor")
	}
	upper, err := strconv.ParseInt(snapshot.Upper, 10, 64)
	if err != nil || upper < after || snapshot.SchemaVersion != 1 || !nodePattern.MatchString(snapshot.Snapshot) {
		return errors.New("invalid history snapshot")
	}
	cache := filepath.Join(m.home, ".newnoa", "history-blocks", account)
	if err = historyDirectory(cache); err != nil {
		return err
	}
	done := int64(0)
	for {
		if err = ctx.Err(); err != nil {
			return err
		}
		var batch struct {
			SchemaVersion int `json:"schemaVersion"`
			Items         []struct {
				historyArchive
				Sequence string         `json:"sequence"`
				Kind     string         `json:"kind"`
				Metadata map[string]any `json:"metadata"`
			} `json:"items"`
			NextCursor string `json:"nextCursor"`
			Done       bool   `json:"done"`
		}
		path := "/me/history/sync/batch?snapshot=" + url.QueryEscape(snapshot.Snapshot) + "&after=" + strconv.FormatInt(after, 10)
		if err = historySyncRemote(ctx, c.control, path, session, nil, &batch); err != nil {
			return err
		}
		next, err := strconv.ParseInt(batch.NextCursor, 10, 64)
		if err != nil || next < after || next > upper || batch.SchemaVersion != 1 || len(batch.Items) > 32 || (!batch.Done && next == after) {
			return errors.New("invalid history batch cursor")
		}
		previous := after
		for _, item := range batch.Items {
			sequence, e := strconv.ParseInt(item.Sequence, 10, 64)
			if e != nil || sequence <= previous || sequence > next {
				return errors.New("invalid history event sequence")
			}
			previous = sequence
			if item.Kind == "candidate" {
				if scope != "others" {
					return errors.New("candidate in own scope")
				}
				if err = m.commit(ctx, account, scope, "candidate", item.ID, item.Metadata); err != nil {
					return err
				}
				done++
				continue
			}
			if historyText(item.Metadata["scope"]) != scope {
				return errors.New("history event scope mismatch")
			}
			if item.Kind == "delete" {
				if err = m.commit(ctx, account, scope, "delete", item.ID, map[string]any{}); err != nil {
					return err
				}
				done++
				continue
			}
			if scope == "own" && historyText(item.Metadata["callerAccountId"]) != account || scope == "others" && historyText(item.Metadata["publisherAccountId"]) != account {
				return errors.New("history event account mismatch")
			}
			if item.Kind != "record" || item.Codec != "zstd-json-manifest-v1" {
				return errors.New("invalid history record event")
			}
			raw, e := decodeHistoryArchive(item.historyArchive, 1<<20)
			if e != nil {
				return e
			}
			var manifest historyManifest
			if json.Unmarshal(raw, &manifest) != nil || manifest.SchemaVersion != 1 || manifest.Record == nil || historyText(manifest.Record["id"]) != item.ID || len(manifest.Fields) != 2 {
				return errors.New("invalid history manifest")
			}
			m.mu.Lock()
			expired := m.config.expiresRecord(manifest.Record, time.Now())
			m.mu.Unlock()
			if expired {
				done++
				continue
			}
			refs := 0
			for _, field := range []string{"request", "response"} {
				part, exists := manifest.Fields[field]
				limit := 8 << 20
				if field == "request" {
					limit = 32 << 20
				}
				if !exists || part.RawBytes < 0 || part.RawBytes > limit {
					return errors.New("invalid history field size")
				}
				refs += len(part.Blocks)
				if refs > 4096 {
					return errors.New("too many history blocks")
				}
				var content bytes.Buffer
				for _, id := range part.Blocks {
					if len(id) != 64 || strings.Trim(id, "0123456789abcdef") != "" {
						return errors.New("invalid history block id")
					}
					name := filepath.Join(cache, id)
					block, e := os.ReadFile(name)
					if e != nil || len(block) > 256<<10 || historyHash(block) != id {
						var blocks struct {
							SchemaVersion int              `json:"schemaVersion"`
							Blocks        []historyArchive `json:"blocks"`
						}
						if e = historySyncRemote(ctx, c.control, "/me/history/sync/blocks", session, map[string]any{"snapshot": snapshot.Snapshot, "ids": []string{id}}, &blocks); e != nil {
							return e
						}
						if blocks.SchemaVersion != 1 || len(blocks.Blocks) != 1 || blocks.Blocks[0].ID != id || blocks.Blocks[0].SHA256 != id {
							return errors.New("invalid history block response")
						}
						block, e = decodeHistoryArchive(blocks.Blocks[0], 256<<10)
						if e != nil {
							return e
						}
						if e = atomicFile(name, block); e != nil {
							return e
						}
					}
					if content.Len()+len(block) > part.RawBytes {
						return errors.New("history field exceeds manifest")
					}
					content.Write(block)
				}
				if content.Len() != part.RawBytes || historyHash(content.Bytes()) != part.SHA256 || !utf8.Valid(content.Bytes()) {
					return errors.New("history field checksum mismatch")
				}
				manifest.Record[field] = content.String()
			}
			for key, value := range item.Metadata {
				manifest.Record[key] = value
			}
			if err = m.commit(ctx, account, scope, "record", item.ID, manifest.Record); err != nil {
				return err
			}
			done++
		}
		if batch.Done {
			next = upper
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if _, err = m.journal.db.ExecContext(ctx, "INSERT INTO history_sync_cursors(viewer,scope,engine,cursor) VALUES(?,?,?,?) ON CONFLICT(viewer,scope,engine) DO UPDATE SET cursor=excluded.cursor", account, scope, engine, strconv.FormatInt(next, 10)); err != nil {
			return err
		}
		after = next
		total := snapshot.TotalRecords + snapshot.TotalCandidates
		if total > 0 {
			m.progress(job, int(min(done, total)*99/total))
		}
		if batch.Done {
			return nil
		}
	}
}
func (c *core) uploadHistoryPending(ctx context.Context, m *historyManager, account, session string) error {
	c.flushHosted(ctx, m.journal)
	for {
		var raw, id string
		err := m.journal.db.QueryRowContext(ctx, "SELECT id,payload FROM exchanges WHERE account=? AND pending=1 AND deleted_at IS NULL ORDER BY created,id LIMIT 1", account).Scan(&id, &raw)
		if err != nil {
			if err.Error() == "sql: no rows in result set" {
				return nil
			}
			return err
		}
		value, err := m.journal.decodeBody(ctx, account, raw)
		if err != nil {
			return err
		}
		m.mu.Lock()
		expired := m.config.expiresRecord(value, time.Now())
		m.mu.Unlock()
		if expired {
			if _, err = m.journal.db.ExecContext(ctx, "UPDATE exchanges SET pending=0 WHERE account=? AND id=?", account, id); err != nil {
				return err
			}
			continue
		}
		if value["transportPath"] == "core-hosted-direct" && m.journal.hostedPending(account, id) {
			return errors.New("history awaits usage settlement")
		}
		value = historyCapturePayload(value)
		capture, err := json.Marshal(value)
		if err != nil {
			return err
		}
		raw = string(capture)
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(8<<20))
		if err != nil {
			return err
		}
		compressed := encoder.EncodeAll([]byte(raw), nil)
		encoder.Close()
		metadata := historyCapturePayload(historyRecordMetadata(value))
		var receipt struct {
			Deleted bool `json:"deleted"`
			Saved   bool `json:"saved"`
			Paused  bool `json:"paused"`
		}
		body := map[string]any{"record": metadata, "archive": map[string]any{"codec": "zstd-json-v1", "rawBytes": len(raw), "sha256": historyHash([]byte(raw)), "bodyBase64": base64.StdEncoding.EncodeToString(compressed)}}
		if err = historySyncRemote(ctx, c.control, "/me/history", session, body, &receipt); err != nil {
			return err
		}
		if !receipt.Deleted && (!receipt.Saved || receipt.Paused) {
			return errors.New("history upload paused without persistence")
		}
		if _, err = m.journal.db.ExecContext(ctx, "UPDATE exchanges SET pending=0 WHERE account=? AND id=?", account, id); err != nil {
			return err
		}
	}
}

func (c *core) autoHistorySync(ctx context.Context, h *chatHistory) {
	if h.manager == nil {
		return
	}
	m := h.manager
	if !m.coordinator {
		m.reconnect(ctx)
	}
	m.mu.Lock()
	busy := m.busy != ""
	m.mu.Unlock()
	if busy {
		return
	}
	c.proxySessionMu.RLock()
	session := c.proxySession
	c.proxySessionMu.RUnlock()
	if session == "" {
		return
	}
	hash := sha256.Sum256([]byte(session))
	c.historyIdentityMu.Lock()
	account := ""
	if c.historyIdentityHash == hash {
		account = c.historyIdentityAccount
	}
	c.historyIdentityMu.Unlock()
	if account == "" {
		return
	}
	m, err := h.manager.forAccount(account)
	if err != nil {
		return
	}
	if m.recordingDisabled() {
		return
	}
	m.reconnect(ctx)
	m.flushPending(ctx)
	var settings struct {
		CloudEnabled bool `json:"cloudEnabled"`
		CanSyncOwn   bool `json:"canSyncOwn"`
	}
	if historySyncRemote(ctx, c.control, "/me/history/settings", session, nil, &settings) != nil || !settings.CloudEnabled || !settings.CanSyncOwn {
		return
	}
	jobCtx, id, err := m.start("sync", account, "own")
	if err != nil {
		return
	}
	m.mu.Lock()
	m.syncJobs[account+"/own"] = id
	m.mu.Unlock()
	stop := context.AfterFunc(ctx, func() {
		m.mu.Lock()
		if job := m.jobs[id]; job != nil {
			job.cancel()
		}
		m.mu.Unlock()
	})
	defer stop()
	err = c.syncHistory(jobCtx, m, account, session, "own", id)
	m.finish(id, err, historyExportResult{})
}
