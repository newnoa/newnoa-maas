package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var errHostedCheckpointUnavailable = errors.New("hosted checkpoint unavailable")
var errHostedCheckpointRejected = errors.New("hosted checkpoint rejected")

// checkpointHosted retries delivery, never inference. Sequence and serialized
// observation remain identical after an ambiguous response or lost ack.
func (c *core) checkpointHosted(ctx context.Context, d hostedDispatch, sequence int64, snapshot json.RawMessage) error {
	if sequence < 1 || sequence > 9007199254740991 || len(snapshot) > 4096 || !json.Valid(snapshot) {
		return errors.New("invalid hosted checkpoint")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	payload := struct {
		Sequence int64           `json:"sequence"`
		Snapshot json.RawMessage `json:"snapshot"`
	}{sequence, snapshot}
	for attempt := 0; attempt < 2; attempt++ {
		var ack struct {
			Accepted  bool   `json:"accepted"`
			RequestID string `json:"requestId"`
			Sequence  int64  `json:"sequence"`
		}
		err := c.hostedRemote(ctx, "/dispatches/"+d.RequestID+"/meter", "Bearer "+d.ReportToken, "", payload, &ack)
		if err == nil {
			if ack.Accepted && ack.RequestID == d.RequestID && ack.Sequence == sequence {
				return nil
			}
			// A committed checkpoint can have a corrupt/mismatched ack. Keep its
			// exact sequence/payload retryable, never treat it as a rejection.
		} else {
			var rejected *hostedControlHTTPError
			if errors.As(err, &rejected) && rejected.status >= 400 && rejected.status < 500 && rejected.status != 408 && rejected.status != 429 {
				return errHostedCheckpointRejected
			}
		}
		if ctx.Err() != nil {
			return errHostedCheckpointUnavailable
		}
	}
	return errHostedCheckpointUnavailable
}

// Keep the existing native retry row bound to its admitted identity while the
// request is active. PostgreSQL checkpoints, not this local delivery aid, own billing.
func (c *core) persistHostedProgress(ctx context.Context, h *chatHistory, account string, d hostedDispatch, report usageReport) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	c.hostedMu.Lock()
	defer c.hostedMu.Unlock()
	result, err := h.db.ExecContext(ctx, `UPDATE hosted_usage_outbox SET payload=? WHERE request_id=? AND account=? AND model_id=? AND revision=? AND key_id=? AND report_token=?`, string(raw), d.RequestID, account, d.DeploymentID, d.Revision, d.KeyID, d.ReportToken)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return errors.New("hosted checkpoint progress was not persisted")
	}
	return nil
}
