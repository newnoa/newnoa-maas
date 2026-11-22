package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// Readiness is deliberately separate from the dependency-free /health probe.
// It authorizes no inference and never retries or forwards internal credentials.
func (o *owner) ready(r *http.Request) bool {
	if !o.build.matches() {
		return false
	}
	ctx, cancel := context.WithTimeout(r.Context(), 1500*time.Millisecond)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, o.api+"/ready", nil)
	if err != nil {
		return false
	}
	response, err := o.control.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(body) > 4096 {
		return false
	}
	var result struct {
		Ready bool `json:"ready"`
	}
	return json.Unmarshal(body, &result) == nil && result.Ready
}
