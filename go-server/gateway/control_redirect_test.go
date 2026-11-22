package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestControlRedirectCannotForwardInternalRequest(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		var calls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.Write([]byte(`{"model":"fixture"}`)) }))
		control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", target.URL)
			w.WriteHeader(status)
		}))
		g := newGateway(control.URL, "private-internal-token", "fixture", nil)
		var result map[string]any
		err := g.control(context.Background(), "/authorize", map[string]any{"secret": "consumer-secret"}, &result)
		control.Close()
		target.Close()
		if err == nil || calls.Load() != 0 {
			t.Fatalf("redirect %d: error=%v target calls=%d", status, err, calls.Load())
		}
	}
}
