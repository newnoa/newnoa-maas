package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type compressionAccountContext struct{}

type compressionDailyStats struct {
	Day          string `json:"day"`
	BeforeTokens int64  `json:"beforeTokens,string"`
	AfterTokens  int64  `json:"afterTokens,string"`
	Requests     int64  `json:"requests,string"`
	Encoding     string `json:"encoding"`
}

var compressionZone = time.FixedZone("UTC+8", 8*60*60)

func (c *core) recordCompressionStats(ctx context.Context, account string, before, after int, at time.Time) error {
	if account == "" || before <= 0 || after < 0 || after > before {
		return errors.New("invalid compression counters")
	}
	h, err := c.historyStore()
	if err != nil {
		return err
	}
	day := at.In(compressionZone).Format("2006-01-02")
	ctx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO proxy_compression_daily(account,day,before_tokens,after_tokens,requests) VALUES(?,?,?,?,1) ON CONFLICT(account,day) DO UPDATE SET before_tokens=before_tokens+excluded.before_tokens,after_tokens=after_tokens+excluded.after_tokens,requests=requests+1`, account, day, before, after); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM proxy_compression_daily WHERE day < ?`, at.In(compressionZone).AddDate(0, 0, -6).Format("2006-01-02")); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	c.historyEvents.publish(account, historyCompressionChanged)
	return nil
}

func (c *core) readCompressionStats(account string, at time.Time) (compressionDailyStats, error) {
	result := compressionDailyStats{Day: at.In(compressionZone).Format("2006-01-02"), Encoding: "o200k_base"}
	h, err := c.historyStore()
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err = h.db.ExecContext(ctx, `DELETE FROM proxy_compression_daily WHERE day < ?`, at.In(compressionZone).AddDate(0, 0, -6).Format("2006-01-02")); err != nil {
		return result, err
	}
	err = h.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(before_tokens),0),COALESCE(SUM(after_tokens),0),COALESCE(SUM(requests),0) FROM proxy_compression_daily WHERE account=? AND day=?`, account, result.Day).Scan(&result.BeforeTokens, &result.AfterTokens, &result.Requests)
	return result, err
}

func (c *core) compressionStatsCommand(raw json.RawMessage) (compressionDailyStats, error) {
	var input struct {
		Session string `json:"session"`
	}
	var fields map[string]json.RawMessage
	if len(raw) > 8192 || !strictCompressionJSON(raw, 4) || json.Unmarshal(raw, &fields) != nil || len(fields) != 1 || fields["session"] == nil || json.Unmarshal(raw, &input) != nil {
		return compressionDailyStats{}, errors.New("invalid compression statistics command")
	}
	account, err := c.historyIdentity(input.Session)
	if err != nil {
		return compressionDailyStats{}, err
	}
	return c.readCompressionStats(account, time.Now())
}
