package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestDeviceControlRequestsNeverFollowRedirects(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		var calls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.Write([]byte(`{"epoch":1}`)) }))
		control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer node_fixture" {
				t.Error("device credential missing")
			}
			w.Header().Set("Location", target.URL)
			w.WriteHeader(status)
		}))
		n := &connector{control: control.URL, credential: "node_fixture", client: connectorControlClient()}
		for _, path := range []string{"/connector/fixture/leases", "/connector/fixture/heartbeat", "/connector/dispatches/fixture/consume", "/connector/evaluations/fixture/consume"} {
			var result map[string]any
			if err := n.call(context.Background(), path, map[string]any{"permit": "fixture"}, &result); err == nil {
				t.Errorf("redirect %d accepted for %s", status, path)
			}
		}
		control.Close()
		target.Close()
		if calls.Load() != 0 {
			t.Fatalf("redirect %d forwarded %d device requests", status, calls.Load())
		}
	}
}
