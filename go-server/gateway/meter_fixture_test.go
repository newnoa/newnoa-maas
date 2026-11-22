package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
)

type meterFixtureEvidenceKey struct{}

// Existing tests assert selected upstream evidence, now owned by the API's
// checkpoint rather than redundant finalizer counters. Keep the wire body intact.
func meterFixtureSelectedEvidence(r *http.Request, body map[string]any) map[string]any {
	if reported, ok := r.Context().Value(meterFixtureEvidenceKey{}).(map[string]any); ok {
		for key, value := range reported {
			body[key] = value
		}
	}
	return body
}

// Existing feature fixtures use the new API's successful checkpoint contract.
// Meter failure/barrier tests deliberately use raw httptest servers instead.
func meterFixtureServer(next http.Handler) *httptest.Server {
	var mu sync.Mutex
	snapshots := map[string]map[string]any{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/internal/requests/") && strings.HasSuffix(r.URL.Path, "/meter") {
			var body struct {
				Sequence int64          `json:"sequence"`
				Snapshot map[string]any `json:"snapshot"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.Sequence < 1 {
				http.Error(w, "invalid fixture checkpoint", 400)
				return
			}
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/internal/requests/"), "/meter")
			mu.Lock()
			snapshots[id] = body.Snapshot
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": id, "sequence": body.Sequence})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/finalize") {
			raw, err := io.ReadAll(r.Body)
			var body map[string]any
			if err == nil {
				err = json.Unmarshal(raw, &body)
			}
			if err == nil && body["meterSequence"] != nil {
				for _, key := range []string{"inputTokens", "outputTokens", "cachedInputTokens", "cacheWriteTokens"} {
					if _, present := body[key]; present {
						err = errors.New("metered fixture closure contains raw counter key")
					}
				}
				id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/internal/requests/"), "/finalize")
				mu.Lock()
				reported, _ := snapshots[id]["reported"].(map[string]any)
				mu.Unlock()
				r = r.WithContext(context.WithValue(r.Context(), meterFixtureEvidenceKey{}, reported))
			}
			if err != nil {
				http.Error(w, "invalid fixture closure", 409)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
		}
		next.ServeHTTP(w, r)
	}))
}
