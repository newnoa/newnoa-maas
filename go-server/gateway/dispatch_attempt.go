package main

import (
	"context"
	"errors"
	"log"
	"time"
)

// Delivery compensation never retries admission or inference; API owns atomic abort/tombstone semantics.
func (g *gateway) abortDispatchAttempt(ctx context.Context, params map[string]any) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer cancel()
	var ack struct {
		Accepted bool `json:"accepted"`
	}
	if err := g.control(cleanup, "/dispatch-attempts/abort", params, &ack); err != nil {
		return err
	}
	if !ack.Accepted {
		return errors.New("dispatch abort not acknowledged")
	}
	return nil
}

func (g *gateway) compensateDispatchDelivery(ctx context.Context, params map[string]any, cause error) {
	// Explicit API rejection is not an ambiguous delivery and may precede attempt persistence.
	var rejected *controlError
	if errors.As(cause, &rejected) {
		return
	}
	if err := g.abortDispatchAttempt(ctx, params); err != nil {
		// Do not log params, caller credentials, tokens or raw control errors.
		log.Printf("dispatch_abort_pending attempt=%s", params["attemptId"])
	}
}
