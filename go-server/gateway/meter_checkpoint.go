package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type meterCheckpoint struct {
	gateway         *gateway
	request, permit string
	sequence        int64
	lastSnapshot    []byte
	failureStage    string
}

func (m *meterCheckpoint) persist(ctx context.Context, snapshot any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(snapshot)
	if err != nil || len(raw) > 4096 {
		return errors.New("invalid meter snapshot")
	}
	next := m.sequence + 1
	if next > 9007199254740991 {
		return errors.New("meter sequence exhausted")
	}
	body := map[string]any{"gatewayId": m.gateway.id, "permit": m.permit, "sequence": next, "snapshot": json.RawMessage(raw)}
	checkpoint, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	for attempt := 0; attempt < 2; attempt++ {
		var ack struct {
			Accepted  bool   `json:"accepted"`
			RequestID string `json:"requestId"`
			Sequence  int64  `json:"sequence"`
		}
		err = m.gateway.control(checkpoint, "/requests/"+m.request+"/meter", body, &ack)
		if err == nil {
			if !ack.Accepted || ack.RequestID != m.request || ack.Sequence != next {
				return errors.New("invalid meter acknowledgement")
			}
			m.sequence = next
			m.lastSnapshot = append(m.lastSnapshot[:0], raw...)
			return nil
		}
		var rejected *controlError
		if errors.As(err, &rejected) || checkpoint.Err() != nil {
			return err
		}
	}
	return err
}
