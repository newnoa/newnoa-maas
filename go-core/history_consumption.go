package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

const historyConsumptionSchema = `CREATE TABLE IF NOT EXISTS history_consumption_pending(account TEXT NOT NULL,id TEXT NOT NULL,request_id TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_attempt INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(account,id)); CREATE INDEX IF NOT EXISTS history_consumption_retry ON history_consumption_pending(account,next_attempt,id);`

const historyConsumptionBackfillBatch = 32

// Commit progress with each bounded JSON scan; new captures enqueue independently.
func (h *chatHistory) backfillConsumption(ctx context.Context) (bool, error) {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var after int64
	err = tx.QueryRowContext(ctx, "SELECT value FROM history_meta WHERE key='consumption-backfill-v1'").Scan(&after)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	if after < 0 {
		return true, nil
	}
	var upper sql.NullInt64
	if err = tx.QueryRowContext(ctx, "SELECT max(rowid) FROM (SELECT rowid FROM exchanges WHERE rowid>? ORDER BY rowid LIMIT ?)", after, historyConsumptionBackfillBatch).Scan(&upper); err != nil {
		return false, err
	}
	if upper.Valid {
		_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO history_consumption_pending(account,id,request_id) SELECT account,id,json_extract(payload,'$.requestId') FROM exchanges WHERE rowid>? AND rowid<=? AND deleted_at IS NULL AND json_valid(payload) AND length(json_extract(payload,'$.requestId'))=36 AND (COALESCE(json_extract(payload,'$.pointsStatus'),'pending')='pending' OR json_extract(payload,'$.billingPhase') IN ('pending','provisional') OR (COALESCE(json_extract(payload,'$.consumptionMetadataHydrated'),0)=0 AND (COALESCE(json_extract(payload,'$.modelName'),'')='' OR COALESCE(json_extract(payload,'$.keyName'),'')='')))`, after, upper.Int64)
		if err != nil {
			return false, err
		}
		after = upper.Int64
	} else {
		after = -1
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO history_meta(key,value) VALUES('consumption-backfill-v1',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", after); err != nil {
		return false, err
	}
	return !upper.Valid, tx.Commit()
}

type historyConsumptionWork struct{ account, id, authorization string }

func historyConsumptionPending(value map[string]any) bool {
	return value["pointsStatus"] == "pending" || value["billingPhase"] == "pending" || value["billingPhase"] == "provisional"
}

func queueConsumption(ctx context.Context, tx *sql.Tx, account string, value map[string]any) error {
	request := historyText(value["requestId"])
	hydrated, _ := value["consumptionMetadataHydrated"].(bool)
	missingNames := !hydrated && (historyText(value["modelName"]) == "" || historyText(value["keyName"]) == "")
	if !nodePattern.MatchString(request) || !historyConsumptionPending(value) && !missingNames {
		return nil
	}
	_, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO history_consumption_pending(account,id,request_id) VALUES(?,?,?)", account, historyText(value["id"]), request)
	return err
}
func (h *chatHistory) hydrateConsumption(ctx context.Context, account, id, authorization string) error {
	if authorization == "" || h.manager == nil {
		return errors.New("consumption authorization unavailable")
	}
	var request, raw string
	err := h.db.QueryRowContext(ctx, "SELECT p.request_id,e.payload FROM history_consumption_pending p JOIN exchanges e ON e.account=p.account AND e.id=p.id WHERE p.account=? AND p.id=? AND e.deleted_at IS NULL", account, id).Scan(&request, &raw)
	if err == sql.ErrNoRows {
		_, err = h.db.ExecContext(ctx, "DELETE FROM history_consumption_pending WHERE account=? AND id=?", account, id)
		return err
	}
	if err != nil {
		return err
	}
	defer h.db.ExecContext(ctx, "UPDATE history_consumption_pending SET attempts=min(attempts+1,20),next_attempt=unixepoch()+min(900,5*(1<<min(attempts,8))) WHERE account=? AND id=?", account, id)
	if !nodePattern.MatchString(request) {
		return errors.New("invalid consumption request")
	}
	var snapshot map[string]any
	if err = historyRemote(ctx, h.control, "/client/requests/"+request+"/consumption", authorization, nil, &snapshot); err != nil {
		return err
	}
	status := historyText(snapshot["pointsStatus"])
	if historyText(snapshot["requestId"]) != request || status != "known" && status != "pending" && status != "unavailable" || status == "known" && snapshot["pointsConsumed"] == nil {
		return errors.New("invalid consumption snapshot")
	}
	value, err := h.decodeBody(ctx, account, raw)
	if err != nil {
		return err
	}
	mergeHistoryTrustedMetadata(value, snapshot)
	m, err := h.manager.forAccount(account)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.Engine == "off" {
		return errHistoryDisabled
	}
	if err = m.commitLocked(ctx, account, "own", "record", id, value); err != nil {
		return err
	}
	// A valid unavailable snapshot is a completed naming lookup, including deleted model/key evidence.
	value["consumptionMetadataHydrated"] = true
	h.bodyMu.Lock()
	defer h.bodyMu.Unlock()
	encoded, err := h.encodeBody(ctx, "journal", account, "own", id, value)
	if err != nil {
		return err
	}
	if _, err = h.db.ExecContext(ctx, "UPDATE exchanges SET payload=? WHERE account=? AND id=? AND deleted_at IS NULL", string(encoded), account, id); err != nil {
		return err
	}
	if !historyConsumptionPending(value) {
		_, err = h.db.ExecContext(ctx, "DELETE FROM history_consumption_pending WHERE account=? AND id=?", account, id)
	}
	return err
}
func (c *core) flushConsumption(ctx context.Context, h *chatHistory) {
	c.proxySessionMu.RLock()
	session := c.proxySession
	c.proxySessionMu.RUnlock()
	if session == "" {
		return
	}
	account, err := c.historyIdentity(session)
	if err != nil {
		return
	}
	h.flushConsumptionAccount(ctx, account, "Bearer "+session)
}
func (h *chatHistory) flushConsumptionAccount(ctx context.Context, account, authorization string) {
	rows, err := h.db.QueryContext(ctx, "SELECT id FROM history_consumption_pending WHERE account=? AND next_attempt<=unixepoch() ORDER BY next_attempt,id LIMIT 8", account)
	if err != nil {
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) != nil {
			break
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		h.hydrateConsumption(ctx, account, id, authorization)
	}
}
func (c *core) runConsumption(ctx context.Context, h *chatHistory) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	// Caller credentials remain in bounded process memory only; restart uses the current verified session.
	type credential struct {
		work    historyConsumptionWork
		expires time.Time
	}
	credentials := map[string]credential{}
	c.flushConsumption(ctx, h)
	for {
		select {
		case <-ctx.Done():
			return
		case work := <-h.consumptionWork:
			key := work.account + "/" + work.id
			if len(credentials) < 32 || credentials[key].work.authorization != "" {
				credentials[key] = credential{work, time.Now().Add(5 * time.Minute)}
			}
			if work.id == "" {
				h.flushConsumptionAccount(ctx, work.account, work.authorization)
			} else {
				h.hydrateConsumption(ctx, work.account, work.id, work.authorization)
			}
		case now := <-ticker.C:
			c.flushConsumption(ctx, h)
			for key, auth := range credentials {
				if now.After(auth.expires) {
					delete(credentials, key)
					continue
				}
				if auth.work.id == "" {
					h.flushConsumptionAccount(ctx, auth.work.account, auth.work.authorization)
					continue
				}
				var due int
				if h.db.QueryRowContext(ctx, "SELECT count(*) FROM history_consumption_pending WHERE account=? AND id=? AND next_attempt<=unixepoch()", auth.work.account, auth.work.id).Scan(&due) == nil && due > 0 {
					h.hydrateConsumption(ctx, auth.work.account, auth.work.id, auth.work.authorization)
				}
			}
		}
	}
}

func historyKnownNoAuthorization(value map[string]any) {
	status, _ := value["httpStatus"].(int)
	if status >= 400 && status <= 599 {
		value["pointsConsumed"] = "0.000000"
		value["pointsStatus"] = "known"
	}
}

func consumptionBearer(authorization string) bool {
	return strings.HasPrefix(authorization, "Bearer ") && len(authorization) > 7
}
