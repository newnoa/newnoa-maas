package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestControlRedirectCannotForwardOwnerRequest(t *testing.T) {
	for _, status := range []int{301, 302, 303, 307, 308} {
		var calls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.Write([]byte(`{"port":20000}`)) }))
		control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", target.URL)
			w.WriteHeader(status)
		}))
		o := newOwner("fixture", control.URL, "private-internal-token")
		var result map[string]any
		err := o.call(context.Background(), "/route", map[string]any{"nodeId": "fixture"}, &result)
		control.Close()
		target.Close()
		if err == nil || calls.Load() != 0 {
			t.Fatalf("redirect %d: error=%v target calls=%d", status, err, calls.Load())
		}
	}
}
