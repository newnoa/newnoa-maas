package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Only locally validated caller errors reach this path; upstream errors never do.
func (g *gateway) callerViolation(w http.ResponseWriter, r *http.Request, secret, code, message string, status int) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 2*time.Second)
	defer cancel()
	var outcome struct {
		Blocked bool `json:"blocked"`
	}
	err := g.control(ctx, "/inference-protection/events", map[string]any{"secret": secret, "code": code, "eventId": uuid.NewString(), "gatewayId": g.id}, &outcome)
	if err != nil {
		// Validation still rejects the request if attribution cannot be persisted.
		log.Printf("inference_violation_unrecorded code=%s", code)
	}
	if outcome.Blocked {
		fail(w, http.StatusForbidden, "API key isolated by security protection")
		return
	}
	fail(w, status, message)
}
