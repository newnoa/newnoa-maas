package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	peernet "github.com/newnoa/newnoa-maas/go-server/network"
	"io"
	"log"
	_ "modernc.org/sqlite"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// History is product data, separate from telemetry; never store authentication headers.
const historyBodyLimit = 8 * 1024 * 1024
const historyChunkSize = 64 * 1024
const historySessionExpr = "COALESCE(NULLIF(CASE WHEN json_valid(payload) THEN json_extract(payload,'$.storageId') END,''),id)"

type chatHistory struct {
	db              *historyJournalDatabase
	bodies          *sqlHistoryBodyStore
	bodyMu          sync.Mutex
	control         string
	manager         *historyManager
	consumptionWork chan historyConsumptionWork
}

func (h *chatHistory) expire(account string, days int) error {
	if days != 7 && days != 30 && days != 90 && days != 365 {
		return errors.New("invalid history retention")
	}
	if err := h.expireKnown(account); err != nil {
		return err
	}
	if _, err := h.db.Exec("INSERT INTO history_settings(account,retention_days) VALUES(?,?) ON CONFLICT(account) DO UPDATE SET retention_days=excluded.retention_days", account, days); err != nil {
		return err
	}
	return h.expireKnown(account)
}
func (h *chatHistory) expireKnown(account string) error {
	if h.manager != nil {
		return nil
	}
	_, err := h.db.Exec("UPDATE exchanges SET deleted_at=CURRENT_TIMESTAMP,pending=0 WHERE account=? AND deleted_at IS NULL AND julianday(created) < julianday('now') - COALESCE((SELECT retention_days FROM history_settings WHERE account=?),365)", account, account)
	return err
}

func openChatHistory(home string) (*chatHistory, error) {
	return openChatHistoryAt(filepath.Join(home, ".newnoa"))
}
func openChatHistoryAt(directory string) (*chatHistory, error) {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(directory, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(directory, "chat-history.sqlite")
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return nil, errors.New("unsafe history file")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA busy_timeout=5000; PRAGMA secure_delete=ON; PRAGMA auto_vacuum=INCREMENTAL; PRAGMA journal_mode=WAL;
 CREATE TABLE IF NOT EXISTS exchanges(account TEXT NOT NULL,id TEXT NOT NULL,created TEXT NOT NULL,model TEXT NOT NULL,agent TEXT NOT NULL,payload TEXT NOT NULL,pending INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(account,id));
 CREATE TABLE IF NOT EXISTS hosted_usage_outbox(request_id TEXT PRIMARY KEY,account TEXT NOT NULL,model_id TEXT NOT NULL,revision INTEGER NOT NULL,key_id TEXT NOT NULL,route TEXT NOT NULL DEFAULT 'core-hosted-direct',report_token TEXT NOT NULL,payload TEXT NOT NULL,created_at INTEGER NOT NULL DEFAULT(unixepoch()),attempts INTEGER NOT NULL DEFAULT 0,next_attempt INTEGER NOT NULL DEFAULT 0,last_error TEXT);
 CREATE TABLE IF NOT EXISTS private_usage_outbox(request_id TEXT NOT NULL,node_id TEXT NOT NULL,payload TEXT NOT NULL,created_at INTEGER NOT NULL DEFAULT(unixepoch()),attempts INTEGER NOT NULL DEFAULT 0,next_attempt INTEGER NOT NULL DEFAULT 0,last_error TEXT,PRIMARY KEY(request_id,node_id));
 CREATE TABLE IF NOT EXISTS history_settings(account TEXT PRIMARY KEY,retention_days INTEGER NOT NULL CHECK(retention_days IN (7,30,90,365)));
`)
	if err == nil {
		_, err = db.Exec(historyConsumptionSchema)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	var deletedColumn int
	if err = db.QueryRow("SELECT count(*) FROM pragma_table_info('exchanges') WHERE name='deleted_at'").Scan(&deletedColumn); err != nil {
		db.Close()
		return nil, err
	}
	var exchangeKind string
	if err = db.QueryRow("SELECT type FROM sqlite_master WHERE name='exchanges'").Scan(&exchangeKind); err != nil {
		db.Close()
		return nil, err
	}
	if deletedColumn == 0 && exchangeKind == "table" {
		if _, err = db.Exec("ALTER TABLE exchanges ADD COLUMN deleted_at TEXT"); err != nil {
			db.Close()
			return nil, err
		}
	}
	if err = splitHistoryJournal(db); err != nil {
		db.Close()
		return nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		db.Close()
		return nil, err
	}
	bodyDB, bodies, err := openJournalBodies(directory)
	if err != nil {
		db.Close()
		return nil, err
	}
	h := &chatHistory{db: &historyJournalDatabase{DB: db, bodyDB: bodyDB}, bodies: bodies}
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS proxy_compression_daily(account TEXT NOT NULL,day TEXT NOT NULL,before_tokens INTEGER NOT NULL,after_tokens INTEGER NOT NULL,requests INTEGER NOT NULL,PRIMARY KEY(account,day))`); err != nil {
		h.db.Close()
		return nil, err
	}
	if err = h.migrateLegacyJournalBodyCategories(); err != nil {
		h.db.Close()
		return nil, err
	}
	return h, nil
}
func (h *chatHistory) save(account string, payload map[string]any, pending bool) error {
	var manager *historyManager
	if h.manager != nil {
		disabled, err := h.manager.accountRecordingDisabled(account)
		if err != nil {
			return err
		}
		if disabled {
			return nil
		}
		m := h.manager
		if m.coordinator {
			m.childrenMu.RLock()
			child := m.children[account]
			if child == nil {
				// Keep cold writes ahead of publication, so disabling cannot overtake them.
				parent := m
				defer func() { parent.childrenMu.RUnlock(); parent.initializeAsync(account) }()
			} else {
				m.childrenMu.RUnlock()
			}
			m = child
		}
		if m != nil {
			manager = m
			m.mu.Lock()
			defer m.mu.Unlock()
			expired := m.config.Engine == "off" || m.config.expiresRecord(payload, time.Now())
			if expired {
				return nil
			}
		}
	}
	h.bodyMu.Lock()
	defer h.bodyMu.Unlock()
	recoverHistoryUsage(payload)
	raw, err := h.encodeBody(context.Background(), "journal", account, "own", historyText(payload["id"]), payload)
	if err != nil {
		return err
	}
	tx, err := h.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec("INSERT INTO exchanges(account,id,created,model,agent,payload,pending) VALUES(?,?,?,?,?,?,?)", account, payload["id"], payload["createdAt"], payload["model"], payload["agent"], string(raw), pending)
	if err != nil {
		return err
	}
	if err = queueConsumption(context.Background(), tx, account, payload); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if manager != nil {
		h.bodyMu.Unlock()
		err := manager.commitLocked(context.Background(), account, "own", "record", historyText(payload["id"]), payload)
		h.bodyMu.Lock()
		return err
	}
	if h.manager != nil {
		encoded, e := h.encodeBody(context.Background(), "pending", account, "own", historyText(payload["id"]), payload)
		if e != nil {
			return e
		}
		_, err = queueEncodedHistoryEvent(context.Background(), h.db.DB, account, "own", "record", historyText(payload["id"]), payload, encoded)
		return err
	}
	return h.expireKnown(account)
}
func (h *chatHistory) list(account, before string, limit int) ([]map[string]any, error) {
	return h.listSession(account, "", before, limit)
}
func (h *chatHistory) listSession(account, storage, before string, limit int) ([]map[string]any, error) {
	if err := h.expireKnown(account); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, errors.New("history page limit must be 1-100")
	}
	where := "account=? AND deleted_at IS NULL"
	args := []any{account}
	if storage != "" {
		where += " AND " + historySessionExpr + "=?"
		args = append(args, storage)
	}
	if before != "" {
		var created string
		cursorArgs := append(append([]any{}, args...), before)
		if err := h.db.QueryRow("SELECT created FROM exchanges WHERE "+where+" AND id=?", cursorArgs...).Scan(&created); err != nil {
			return nil, errors.New("history cursor not found")
		}
		where += " AND (created,id)<(?,?)"
		args = append(args, created, before)
	}
	args = append(args, limit)
	rows, err := h.db.Query("SELECT id,created,model,agent,pending,"+historySessionExpr+",CASE WHEN json_valid(payload) THEN json_extract(payload,'$.conversationId') END,CASE WHEN json_valid(payload) THEN json_extract(payload,'$.modelType') END,CASE WHEN json_valid(payload) THEN json_extract(payload,'$.protocol') END,CASE WHEN json_valid(payload) THEN json_extract(payload,'$.httpStatus') END,CASE WHEN json_valid(payload) THEN json_array_length(payload,'$.attachments') END,CASE WHEN json_valid(payload) THEN json_extract(payload,'$.keyId') END,CASE WHEN json_valid(payload) THEN json_object('callerSource',json_extract(payload,'$.callerSource'),'reasoningEffort',json_extract(payload,'$.reasoningEffort'),'clientIp',json_extract(payload,'$.clientIp'),'firstTokenMs',json_extract(payload,'$.firstTokenMs'),'durationMs',json_extract(payload,'$.durationMs'),'outputTps',json_extract(payload,'$.outputTps')) ELSE '{}' END FROM exchanges WHERE "+where+" ORDER BY created DESC,id DESC LIMIT ?", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []map[string]any{}
	for rows.Next() {
		var id, created, model, agent, storageID string
		var conversation, modelType, protocol, keyID sql.NullString
		var callMetadata string
		var httpStatus, attachmentCount sql.NullInt64
		var pending bool
		if err := rows.Scan(&id, &created, &model, &agent, &pending, &storageID, &conversation, &modelType, &protocol, &httpStatus, &attachmentCount, &keyID, &callMetadata); err != nil {
			return nil, err
		}
		value := map[string]any{"id": id, "createdAt": created, "model": model, "agent": agent, "pending": pending, "storageId": storageID}
		var metadata map[string]any
		if json.Unmarshal([]byte(callMetadata), &metadata) == nil {
			for key, field := range metadata {
				if field != nil {
					value[key] = field
				}
			}
		}
		if value["callerSource"] == nil {
			if source := peernet.KnownCallerSource(agent); source != "" {
				value["callerSource"] = source
			}
		}
		if keyID.Valid && keyID.String != "" {
			value["keyId"] = keyID.String
		}
		if protocol.Valid {
			value["protocol"] = protocol.String
		}
		if httpStatus.Valid {
			value["httpStatus"] = httpStatus.Int64
		}
		if attachmentCount.Valid {
			value["attachmentCount"] = attachmentCount.Int64
		}
		if modelType.Valid && historyModelType(modelType.String) != "" {
			value["modelType"] = modelType.String
		}
		if conversation.Valid && conversation.String != "" {
			value["conversationId"] = conversation.String
		}
		result = append(result, value)
	}
	return result, rows.Err()
}
func (h *chatHistory) get(account, id string) (map[string]any, error) {
	if err := h.expireKnown(account); err != nil {
		return nil, err
	}
	var raw string
	if err := h.db.QueryRow("SELECT payload FROM exchanges WHERE account=? AND id=? AND deleted_at IS NULL", account, id).Scan(&raw); err != nil {
		return nil, errors.New("history not found")
	}
	value, err := h.decodeBody(context.Background(), account, raw)
	if err == nil && value["transportPath"] == "core-hosted-direct" {
		value["usagePending"] = h.hostedPending(account, id)
	}
	return value, err
}

type historyWriter struct {
	http.ResponseWriter
	timing                                  peernet.CallTiming
	body                                    bytes.Buffer
	status                                  int
	truncated, writeFailed, noAuthorization bool
}

func (w *historyWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func historyRejectBeforeAuthorization(w http.ResponseWriter, message string, status int) {
	if writer, ok := w.(*historyWriter); ok {
		writer.noAuthorization = true
	}
	http.Error(w, message, status)
}
func (w *historyWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *historyWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	w.timing.Observe(data, strings.Contains(w.Header().Get("Content-Type"), "text/event-stream"), time.Now())
	left := historyBodyLimit - w.body.Len()
	if len(data) > left {
		w.truncated = true
	}
	if left > len(data) {
		left = len(data)
	}
	w.body.Write(data[:left])
	n, err := w.ResponseWriter.Write(data)
	if err != nil {
		w.writeFailed = true
	}
	return n, err
}
func (h *chatHistory) capture(next http.Handler, account, keyID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || !historyInferencePath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		raw, err := readInferenceBody(r)
		if err != nil {
			http.Error(w, "request too large", 413)
			return
		}
		request, captured, attachments, captureErr := historyPayload(raw, r.Header.Get("Content-Type"))
		if captureErr != nil {
			next.ServeHTTP(w, r)
			return
		}
		conversationID := r.Header.Get("X-Newnoa-Conversation-Id")
		if len(r.Header.Values("X-Newnoa-Conversation-Id")) > 1 {
			http.Error(w, "multiple conversation IDs are not allowed", 400)
			return
		}
		if metadata, ok := request["metadata"].(map[string]any); ok {
			if rawID, exists := metadata["conversation_id"]; exists {
				id, valid := rawID.(string)
				if !valid {
					http.Error(w, "conversation ID must be a string", 400)
					return
				}
				if id != "" {
					if conversationID != "" && conversationID != id {
						http.Error(w, "conflicting conversation IDs", 400)
						return
					}
					conversationID = id
				}
			}
		}
		if len(conversationID) > 200 {
			http.Error(w, "conversation ID exceeds 200 bytes", 400)
			return
		}
		key := conversationID
		if key == "" {
			key = uuid.NewString()
		}
		digest := sha256.Sum256([]byte(key))
		var policy struct {
			ID            string `json:"id"`
			Mode          string `json:"mode"`
			RetentionDays int    `json:"retentionDays"`
		}
		err = historyRemote(r.Context(), h.control, "/client/history/policy", r.Header.Get("Authorization"), map[string]any{"conversationKey": hex.EncodeToString(digest[:])}, &policy)
		mode := policy.Mode
		if err == nil && (mode != "off" && mode != "local" && mode != "cloud" || !nodePattern.MatchString(policy.ID) || (policy.RetentionDays != 0 && policy.RetentionDays != 7 && policy.RetentionDays != 30 && policy.RetentionDays != 90 && policy.RetentionDays != 365)) {
			err = errors.New("invalid history policy")
		}
		if err == nil && policy.RetentionDays != 0 && h.manager == nil {
			err = h.expire(account, policy.RetentionDays)
		}
		if err != nil && h.manager != nil {
			policy.ID = uuid.NewString()
			mode = "local"
			err = nil
		}
		if err != nil {
			w.Header().Set("X-Newnoa-History", "unavailable")
			next.ServeHTTP(w, r)
			return
		}
		if h.manager != nil {
			if !h.manager.enabled(account) {
				next.ServeHTTP(w, r)
				return
			}
			disabled, managerErr := h.manager.accountRecordingDisabled(account)
			if managerErr != nil {
				log.Printf("history_config_unavailable account=%s", account)
				w.Header().Set("X-Newnoa-History", "unavailable")
				next.ServeHTTP(w, r)
				return
			}
			if disabled {
				next.ServeHTTP(w, r)
				return
			}
			h.manager.initializeAsync(account)
			if mode == "off" {
				mode = "local"
			}
		}
		if mode == "off" {
			next.ServeHTTP(w, r)
			return
		}
		// Share the frozen conversation policy with Go Server cloud capture.
		r.Header.Set("X-Newnoa-Conversation-Id", key)
		if mode == "cloud" {
			r.Header.Set("X-Newnoa-History-Capture", "core")
		}
		model, _ := request["model"].(string)
		agent := r.Header.Get("X-Newnoa-Agent")
		if agent == "" {
			agent = r.UserAgent()
		}
		if len(model) > 200 {
			model = strings.ToValidUTF8(model[:200], "?")
		}
		if len(agent) > 200 {
			agent = strings.ToValidUTF8(agent[:200], "?")
		}
		started := time.Now()
		writer := &historyWriter{ResponseWriter: w, timing: peernet.CallTiming{Started: started}}
		completed := false
		defer func() {
			// A proxy can panic with ErrAbortHandler after a broken stream; retain partial output.
			if writer.status == 0 {
				writer.status = http.StatusOK
			}
			raw = captured
			requestTruncated := len(raw) > modelTestRequestLimit
			if requestTruncated {
				raw = raw[:modelTestRequestLimit]
			}
			value := map[string]any{"id": uuid.NewString(), "storageId": policy.ID, "storageMode": mode, "createdAt": started.UTC().Format("2006-01-02T15:04:05.000000000Z"), "finishedAt": time.Now().UTC().Format("2006-01-02T15:04:05.000000000Z"), "durationMs": time.Since(started).Milliseconds(), "model": model, "agent": agent, "keyId": keyID, "protocol": r.URL.Path, "request": strings.ReplaceAll(strings.ToValidUTF8(string(raw), "?"), "\x00", "?"), "response": strings.ReplaceAll(strings.ToValidUTF8(writer.body.String(), "?"), "\x00", "?"), "requestTruncated": requestTruncated, "responseTruncated": writer.truncated, "httpStatus": writer.status, "interrupted": !completed || writer.writeFailed || r.Context().Err() != nil, "contentType": writer.Header().Get("Content-Type"), "transportPath": writer.Header().Get("X-Newnoa-Path"), "requestId": writer.Header().Get("X-Newnoa-Request")}
			for key, field := range peernet.CallMetadata(r, request) {
				value[key] = field
			}
			capture := newUsageCapture(strings.Contains(historyText(value["contentType"]), "text/event-stream"))
			capture.protocol = r.URL.Path
			capture.feed(writer.body.Bytes())
			var output *int64
			if !writer.truncated {
				output = capture.counts().Output
			}
			writer.timing.Finish(value, output, time.Now())
			if modelType := historyModelType(writer.Header().Get("X-Newnoa-Model-Type")); modelType != "" {
				value["modelType"] = modelType
			}
			if category := writer.Header().Get("X-Newnoa-Model-Category"); category == "text" || category == "image" {
				value["modelCategory"] = category
			}
			if len(attachments) > 0 {
				value["attachments"] = attachments
			}
			if conversationID != "" {
				value["conversationId"] = conversationID
			}
			if requestID := writer.Header().Get("X-Newnoa-Request"); nodePattern.MatchString(requestID) {
				value["id"] = requestID
			} else if value["requestId"] == "" {
				delete(value, "requestId")
			}
			cloudCapture := mode == "cloud" && nodePattern.MatchString(historyText(value["requestId"]))
			if writer.noAuthorization {
				historyKnownNoAuthorization(value)
			}
			if err = h.save(account, value, cloudCapture); err != nil {
				fmt.Fprintln(os.Stderr, "Chat history save failed")
				return
			}
			if consumptionBearer(r.Header.Get("Authorization")) && historyConsumptionPending(value) {
				select {
				case h.consumptionWork <- historyConsumptionWork{account: account, id: historyText(value["id"]), authorization: r.Header.Get("Authorization")}:
				default:
				}
			}
			if cloudCapture {
				if writer.Header().Get("X-Newnoa-Path") == "core-hosted-direct" && h.hostedPending(account, value["id"].(string)) {
					return
				}
				if _, err = h.get(account, value["id"].(string)); err != nil {
					return
				}
				syncHistory := func(ctx context.Context) {
					var receipt struct {
						Deleted bool `json:"deleted"`
						Saved   bool `json:"saved"`
						Paused  bool `json:"paused"`
					}
					if syncErr := historyRemote(ctx, h.control, "/client/history", r.Header.Get("Authorization"), historyCapturePayload(value), &receipt); syncErr == nil && (receipt.Deleted || receipt.Saved && !receipt.Paused) {
						h.db.Exec("UPDATE exchanges SET pending=0,deleted_at=CASE WHEN ? THEN COALESCE(deleted_at,CURRENT_TIMESTAMP) ELSE deleted_at END WHERE account=? AND id=?", receipt.Deleted, account, value["id"])
					}
				}
				if writer.Header().Get("X-Newnoa-Path") == "easytier-direct" {
					go func() {
						ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
						defer cancel()
						syncHistory(ctx)
					}()
				} else {
					syncHistory(r.Context())
				}
			}
		}()
		next.ServeHTTP(writer, r)
		completed = true
	})
}

func historyRemote(ctx context.Context, control, path, authorization string, body any, out any) error {
	if _, err := controlBase(control); err != nil {
		return err
	}
	return historyRemoteAt(ctx, control, path, authorization, body, out)
}

func (c *core) controlRemote(ctx context.Context, path, authorization string, body, out any) error {
	if !c.publicStateless {
		return historyRemote(ctx, c.control, path, authorization, body, out)
	}
	if err := validatePublicControlOrigin(c.control); err != nil {
		return err
	}
	return historyRemoteAt(ctx, c.control, path, authorization, body, out)
}

func historyRemoteAt(ctx context.Context, control, path, authorization string, body, out any) error {
	method := "GET"
	var raw []byte
	if body != nil {
		method = "POST"
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(control, "/")+"/api/v1"+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", authorization)
	req.Header.Set("Content-Type", "application/json")
	client := connectorControlClient()
	client.Timeout = 3 * time.Second
	res, err := client.Do(req)
	if err != nil {
		return errors.New("history cloud unavailable")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return &historyCloudHTTPError{status: res.StatusCode}
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(res.Body, maxFrame)).Decode(out)
	}
	return nil
}
func (c *core) historyStore() (*chatHistory, error) {
	c.historyMu.Lock()
	defer c.historyMu.Unlock()
	if c.history == nil {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := c.safeConfigFile(filepath.Join(c.home, ".newnoa", "chat-history.sqlite"+suffix)); err != nil {
				return nil, err
			}
		}
		h, err := c.openManagedHistoryJournal()
		if err != nil {
			return nil, err
		}
		h.control = c.control
		manager, err := newHistoryCoordinator(c.home, h)
		if err != nil {
			h.db.Close()
			return nil, err
		}
		manager.notify = c.historyEvents.publish
		h.manager = manager
		c.history = h
		c.startHostedSync(h)
	}
	return c.history, nil
}
func (c *core) legacyHistoryCommand(method string, raw json.RawMessage) (any, error) {
	var input struct {
		Session       string `json:"session"`
		StorageID     string `json:"storageId"`
		Mode          string `json:"mode"`
		Enabled       *bool  `json:"enabled"`
		RetentionDays *int   `json:"retentionDays"`
		ID            string `json:"id"`
		Before        string `json:"before"`
		Field         string `json:"field"`
		Offset        int    `json:"offset"`
	}
	if json.Unmarshal(raw, &input) != nil {
		return nil, errors.New("invalid history command")
	}
	var account struct {
		ID string `json:"id"`
	}
	if err := historyRemote(context.Background(), c.control, "/me", "Bearer "+input.Session, nil, &account); err != nil {
		return nil, err
	}
	if !nodePattern.MatchString(account.ID) {
		return nil, errors.New("invalid history account")
	}
	h, err := c.historyStore()
	if err != nil {
		return nil, err
	}
	var currentSettings map[string]any
	if err := historyRemote(context.Background(), c.control, "/me/history/settings", "Bearer "+input.Session, nil, &currentSettings); err == nil {
		if rawDays, ok := currentSettings["retentionDays"].(float64); ok {
			if rawDays != float64(int(rawDays)) {
				return nil, errors.New("invalid history retention")
			}
			if err = h.expire(account.ID, int(rawDays)); err != nil {
				return nil, err
			}
		}
	}
	switch method {
	case "history.settings":
		if currentSettings != nil {
			return currentSettings, nil
		}
		return nil, errors.New("history settings unavailable")
	case "history.configure":
		var settings map[string]any
		mode := input.Mode
		enabled := input.Enabled != nil && *input.Enabled
		if input.Enabled == nil && mode != "off" {
			enabled = true
		}
		if mode == "off" {
			mode, enabled = "local", false
		}
		if mode != "local" && mode != "cloud" {
			return nil, errors.New("invalid history settings")
		}
		body := map[string]any{"enabled": enabled, "mode": mode}
		if input.RetentionDays != nil {
			body["retentionDays"] = *input.RetentionDays
		}
		err = historyRemote(context.Background(), c.control, "/me/history/settings", "Bearer "+input.Session, body, &settings)
		if err == nil {
			if days, ok := settings["retentionDays"].(float64); ok {
				err = h.expire(account.ID, int(days))
			}
		}
		return settings, err
	case "history.list":
		return h.list(account.ID, input.Before, 50)
	case "history.conversation":
		if input.StorageID == "" || len(input.StorageID) > 200 || !utf8.ValidString(input.StorageID) {
			return nil, errors.New("invalid history conversation")
		}
		rows, err := h.listSession(account.ID, input.StorageID, input.Before, 51)
		if err != nil {
			return nil, err
		}
		var next any
		if len(rows) > 50 {
			rows = rows[:50]
			next = rows[len(rows)-1]["id"]
		}
		return map[string]any{"items": rows, "nextCursor": next}, nil
	case "history.get":
		value, err := h.get(account.ID, input.ID)
		if err != nil {
			return nil, err
		}
		delete(value, "request")
		delete(value, "response")
		return value, nil
	case "history.content":
		if input.Field != "request" && input.Field != "response" {
			return nil, errors.New("invalid history field")
		}
		value, err := h.get(account.ID, input.ID)
		if err != nil {
			return nil, err
		}
		content, _ := value[input.Field].(string)
		if input.Offset < 0 || input.Offset > len(content) || (input.Offset < len(content) && !utf8.RuneStart(content[input.Offset])) {
			return nil, errors.New("invalid history offset")
		}
		end := min(input.Offset+historyChunkSize, len(content))
		for end < len(content) && !utf8.RuneStart(content[end]) {
			end--
		}
		return map[string]any{"text": content[input.Offset:end], "nextOffset": end, "done": end == len(content)}, nil
	case "history.delete":
		_, err = h.db.Exec("UPDATE exchanges SET deleted_at=COALESCE(deleted_at,CURRENT_TIMESTAMP),pending=0 WHERE account=? AND id=?", account.ID, input.ID)
		return map[string]any{"deleted": err == nil}, err
	case "history.sync":
		c.flushHosted(context.Background(), h)
		rows, err := h.db.Query("SELECT payload FROM exchanges WHERE account=? AND pending=1 AND deleted_at IS NULL ORDER BY created,id LIMIT 10", account.ID)
		if err != nil {
			return nil, err
		}
		var payloads []map[string]any
		for rows.Next() {
			var data string
			if err = rows.Scan(&data); err != nil {
				rows.Close()
				return nil, err
			}
			value, decodeErr := h.decodeBody(context.Background(), account.ID, data)
			if decodeErr != nil {
				rows.Close()
				return nil, decodeErr
			}
			payloads = append(payloads, value)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		count := 0
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, value := range payloads {
			if value["transportPath"] == "core-hosted-direct" && h.hostedPending(account.ID, value["id"].(string)) {
				continue
			}
			if _, err = h.get(account.ID, value["id"].(string)); err != nil {
				continue
			}
			var receipt struct {
				Deleted bool `json:"deleted"`
			}
			if err = historyRemote(ctx, c.control, "/me/history", "Bearer "+input.Session, historyCapturePayload(value), &receipt); err != nil {
				return map[string]any{"synced": count}, err
			}
			if _, err = h.db.Exec("UPDATE exchanges SET pending=0,deleted_at=CASE WHEN ? THEN COALESCE(deleted_at,CURRENT_TIMESTAMP) ELSE deleted_at END WHERE account=? AND id=?", receipt.Deleted, account.ID, value["id"]); err != nil {
				return nil, err
			}
			count++
		}
		var remaining int
		if err = h.db.QueryRow("SELECT count(*) FROM exchanges WHERE account=? AND pending=1 AND deleted_at IS NULL", account.ID).Scan(&remaining); err != nil {
			return nil, err
		}
		return map[string]any{"synced": count, "remaining": remaining}, nil
	}
	return nil, errors.New("history method unavailable")
}
func (c *core) recordInference(next http.Handler) http.Handler {
	return c.updateGuard(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Resolve each caller account, then use the server policy pinned at conversation start.
		if r.Method != "POST" || !historyInferencePath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		h, err := c.historyStore()
		if err != nil {
			w.Header().Set("X-Newnoa-History", "unavailable")
			next.ServeHTTP(w, r)
			return
		}
		var identity struct {
			AccountID string `json:"accountId"`
			KeyID     string `json:"keyId"`
		}
		if err := historyRemote(r.Context(), c.control, "/client/history/identity", r.Header.Get("Authorization"), nil, &identity); err != nil || !nodePattern.MatchString(identity.AccountID) {
			next.ServeHTTP(w, r)
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), compressionAccountContext{}, identity.AccountID))
		h.capture(next, identity.AccountID, identity.KeyID).ServeHTTP(w, r)
	}))
}
