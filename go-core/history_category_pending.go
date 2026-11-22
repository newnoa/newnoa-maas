package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// Queue rows keep references only; categorized event entities retain exact event
// bytes until the durable sequence is acknowledged. Existing IDs stay unchanged.
func initializeCategoryPending(db *sql.DB) error {
	for _, cat := range []string{"text", "image"} {
		if _, e := db.Exec("CREATE TABLE IF NOT EXISTS history_" + cat + "_pending_entities(seq INTEGER PRIMARY KEY,payload TEXT NOT NULL)"); e != nil {
			return e
		}
	}
	if _, e := db.Exec("CREATE TRIGGER IF NOT EXISTS history_pending_entities_delete AFTER DELETE ON history_pending BEGIN DELETE FROM history_text_pending_entities WHERE seq=OLD.seq; DELETE FROM history_image_pending_entities WHERE seq=OLD.seq; END"); e != nil {
		return e
	}
	for {
		rows, e := db.Query("SELECT seq,payload FROM history_pending WHERE kind='record' AND json_extract(payload,'$.categoryRef') IS NULL ORDER BY seq LIMIT 64")
		if e != nil {
			return e
		}
		type item struct {
			seq int64
			raw string
		}
		var batch []item
		batchBytes := 0
		for rows.Next() {
			var v item
			if e = rows.Scan(&v.seq, &v.raw); e != nil {
				break
			}
			batch = append(batch, v)
			batchBytes += len(v.raw)
			if batchBytes >= 8<<20 {
				break
			}
		}
		rowErr := rows.Err()
		rows.Close()
		if rowErr != nil {
			return rowErr
		}
		if e != nil {
			return e
		}
		for _, v := range batch {
			var value map[string]any
			if e = json.Unmarshal([]byte(v.raw), &value); e != nil {
				return e
			}
			tx, e := db.Begin()
			if e != nil {
				return e
			}
			cat := historyModelCategory(value)
			if _, e = tx.Exec("INSERT OR IGNORE INTO history_"+cat+"_pending_entities(seq,payload) VALUES(?,?)", v.seq, v.raw); e == nil {
				_, e = tx.Exec("UPDATE history_pending SET payload=? WHERE seq=?", fmt.Sprintf(`{"categoryRef":%q,"createdAt":%q}`, cat, historyText(value["createdAt"])), v.seq)
			}
			if e != nil {
				tx.Rollback()
				return e
			}
			if e = tx.Commit(); e != nil {
				return e
			}
		}
		if len(batch) == 0 {
			return nil
		}
	}
}
func queueHistoryEvent(ctx context.Context, db *sql.DB, viewer, scope, kind, id string, value map[string]any) (int64, error) {
	return queueEncodedHistoryEvent(ctx, db, viewer, scope, kind, id, value, "")
}
func queueEncodedHistoryEvent(ctx context.Context, db *sql.DB, viewer, scope, kind, id string, value map[string]any, encoded string) (int64, error) {
	raw, e := json.Marshal(value)
	if e != nil {
		return 0, e
	}
	if encoded != "" {
		raw = []byte(encoded)
	}
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback()
	result, e := tx.ExecContext(ctx, "INSERT INTO history_pending(viewer,scope,kind,id,payload) VALUES(?,?,?,?,?)", viewer, scope, kind, id, string(raw))
	if e != nil {
		return 0, e
	}
	seq, e := result.LastInsertId()
	if e != nil {
		return 0, e
	}
	if kind == "record" {
		cat := historyModelCategory(value)
		if _, e = tx.ExecContext(ctx, "INSERT INTO history_"+cat+"_pending_entities(seq,payload) VALUES(?,?)", seq, string(raw)); e != nil {
			return 0, e
		}
		reference, _ := json.Marshal(map[string]any{"categoryRef": cat, "createdAt": value["createdAt"]})
		if _, e = tx.ExecContext(ctx, "UPDATE history_pending SET payload=? WHERE seq=?", string(reference), seq); e != nil {
			return 0, e
		}
	}
	return seq, tx.Commit()
}
func resolveHistoryEvent(ctx context.Context, db *sql.DB, seq int64, raw string) (string, error) {
	var ref struct {
		Category string `json:"categoryRef"`
	}
	if json.Unmarshal([]byte(raw), &ref) != nil {
		return "", errors.New("invalid history event")
	}
	if ref.Category == "" {
		return raw, nil
	}
	if ref.Category != "text" && ref.Category != "image" {
		return "", errors.New("invalid history event category")
	}
	var result string
	e := db.QueryRowContext(ctx, "SELECT payload FROM history_"+ref.Category+"_pending_entities WHERE seq=?", seq).Scan(&result)
	return result, e
}
