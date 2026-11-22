package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

const exchangeCategoryExpr = "CASE WHEN json_extract(payload,'$.modelCategory')='image' OR (json_extract(payload,'$.modelCategory') IS NULL AND json_extract(payload,'$.modelType')='image') THEN 'image' ELSE 'text' END"

// The compatibility view owns no bodies. Its triggers preserve legacy readers,
// while global identity/row sequence remain in a small location directory.
func splitHistoryJournal(db *sql.DB) error {
	for _, cat := range []string{"text", "image"} {
		if _, e := db.Exec("CREATE TABLE IF NOT EXISTS " + cat + "_exchanges(account TEXT NOT NULL,id TEXT NOT NULL,created TEXT NOT NULL,model TEXT NOT NULL,agent TEXT NOT NULL,payload TEXT NOT NULL,pending INTEGER NOT NULL DEFAULT 0,deleted_at TEXT,PRIMARY KEY(account,id))"); e != nil {
			return e
		}
		if _, e := db.Exec("CREATE INDEX IF NOT EXISTS " + cat + "_exchange_page ON " + cat + "_exchanges(account,created,id);CREATE INDEX IF NOT EXISTS " + cat + "_exchange_session ON " + cat + "_exchanges(account," + historySessionExpr + ",created,id)"); e != nil {
			return e
		}
	}
	if _, e := db.Exec("CREATE TABLE IF NOT EXISTS history_exchange_locations(seq INTEGER PRIMARY KEY AUTOINCREMENT,account TEXT NOT NULL,id TEXT NOT NULL,model_category TEXT NOT NULL CHECK(model_category IN ('text','image')),UNIQUE(account,id))"); e != nil {
		return e
	}
	var kind string
	if e := db.QueryRow("SELECT type FROM sqlite_master WHERE name='exchanges'").Scan(&kind); e != nil {
		return e
	}
	if kind == "table" {
		for {
			tx, e := db.Begin()
			if e != nil {
				return e
			}
			rows, e := tx.Query("SELECT rowid,account,id,created,model,agent,payload,pending,deleted_at FROM exchanges e WHERE NOT EXISTS(SELECT 1 FROM history_exchange_locations l WHERE l.account=e.account AND l.id=e.id) ORDER BY rowid LIMIT 64")
			if e != nil {
				tx.Rollback()
				return e
			}
			type row struct {
				seq                                     int64
				account, id, created, model, agent, raw string
				pending                                 int
				deleted                                 sql.NullString
			}
			var batch []row
			batchBytes := 0
			for rows.Next() {
				var v row
				if e = rows.Scan(&v.seq, &v.account, &v.id, &v.created, &v.model, &v.agent, &v.raw, &v.pending, &v.deleted); e != nil {
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
			if e != nil || rowErr != nil {
				tx.Rollback()
				if e != nil {
					return e
				}
				return rowErr
			}
			for _, v := range batch {
				var value map[string]any
				if e = json.Unmarshal([]byte(v.raw), &value); e != nil {
					break
				}
				cat := historyModelCategory(value)
				if _, e = tx.Exec("INSERT OR IGNORE INTO "+cat+"_exchanges(account,id,created,model,agent,payload,pending,deleted_at) VALUES(?,?,?,?,?,?,?,?)", v.account, v.id, v.created, v.model, v.agent, v.raw, v.pending, v.deleted); e != nil {
					break
				}
				var raw string
				if e = tx.QueryRow("SELECT payload FROM "+cat+"_exchanges WHERE account=? AND id=?", v.account, v.id).Scan(&raw); e != nil {
					break
				}
				if raw != v.raw {
					e = fmt.Errorf("journal migration body mismatch")
					break
				}
				if _, e = tx.Exec("INSERT INTO history_exchange_locations(seq,account,id,model_category) VALUES(?,?,?,?)", v.seq, v.account, v.id, cat); e != nil {
					break
				}
			}
			if e != nil {
				tx.Rollback()
				return e
			}
			if e = tx.Commit(); e != nil {
				return e
			}
			if len(batch) == 0 {
				break
			}
		}
		tx, e := db.Begin()
		if e != nil {
			return e
		}
		defer tx.Rollback()
		var source, target int
		if e = tx.QueryRow("SELECT count(*) FROM exchanges").Scan(&source); e != nil {
			return e
		}
		if e = tx.QueryRow("SELECT (SELECT count(*) FROM text_exchanges)+(SELECT count(*) FROM image_exchanges)").Scan(&target); e != nil {
			return e
		}
		if source != target {
			return fmt.Errorf("journal migration identity mismatch")
		}
		if _, e = tx.Exec("DROP TABLE exchanges"); e != nil {
			return e
		}
		if e = createExchangeView(tx); e != nil {
			return e
		}
		return tx.Commit()
	}
	return nil
}
func createExchangeView(tx *sql.Tx) error {
	if _, e := tx.Exec("CREATE VIEW exchanges AS SELECT l.seq AS rowid,e.* FROM text_exchanges e JOIN history_exchange_locations l ON l.account=e.account AND l.id=e.id AND l.model_category='text' UNION ALL SELECT l.seq AS rowid,e.* FROM image_exchanges e JOIN history_exchange_locations l ON l.account=e.account AND l.id=e.id AND l.model_category='image'"); e != nil {
		return e
	}
	expr := strings.ReplaceAll(exchangeCategoryExpr, "payload", "NEW.payload")
	query := "CREATE TRIGGER exchanges_insert INSTEAD OF INSERT ON exchanges BEGIN INSERT OR IGNORE INTO history_exchange_locations(account,id,model_category) VALUES(NEW.account,NEW.id," + expr + "); SELECT CASE WHEN (SELECT model_category FROM history_exchange_locations WHERE account=NEW.account AND id=NEW.id)<>" + expr + " THEN RAISE(ABORT,'history category cannot change') END;"
	for _, cat := range []string{"text", "image"} {
		query += "INSERT OR IGNORE INTO " + cat + "_exchanges(account,id,created,model,agent,payload,pending,deleted_at) SELECT NEW.account,NEW.id,NEW.created,NEW.model,NEW.agent,NEW.payload,COALESCE(NEW.pending,0),NEW.deleted_at WHERE " + expr + "='" + cat + "';"
	}
	query += " END"
	if _, e := tx.Exec(query); e != nil {
		return e
	}
	for _, op := range []string{"UPDATE", "DELETE"} {
		query = "CREATE TRIGGER exchanges_" + strings.ToLower(op) + " INSTEAD OF " + op + " ON exchanges BEGIN "
		if op == "UPDATE" {
			query += "SELECT CASE WHEN NEW.account<>OLD.account OR NEW.id<>OLD.id THEN RAISE(ABORT,'history identity cannot change') END;"
			query += "SELECT CASE WHEN " + expr + "<>(SELECT model_category FROM history_exchange_locations WHERE account=OLD.account AND id=OLD.id) THEN RAISE(ABORT,'history category cannot change') END;"
		}
		for _, cat := range []string{"text", "image"} {
			if op == "UPDATE" {
				query += fmt.Sprintf("UPDATE %s_exchanges SET created=NEW.created,model=NEW.model,agent=NEW.agent,payload=NEW.payload,pending=NEW.pending,deleted_at=NEW.deleted_at WHERE account=OLD.account AND id=OLD.id;", cat)
			} else {
				query += "DELETE FROM " + cat + "_exchanges WHERE account=OLD.account AND id=OLD.id;"
			}
		}
		query += " END"
		if _, e := tx.Exec(query); e != nil {
			return e
		}
	}
	return nil
}
