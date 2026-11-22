package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrivateDirectControlRoutesAreGone(t *testing.T) {
	calls := 0
	api := meterFixtureServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", nil)
	for _, path := range []string{"/private/target", "/private/dispatches"} {
		for _, paired := range []bool{false, true} {
			r := httptest.NewRequest("POST", path, strings.NewReader(`{"model":"fixture"}`))
			if paired {
				r.Header.Set("Authorization", "Bearer ses_fixture")
				r.Header.Set("X-Api-Key", "nn_fixture")
			}
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			if w.Code != http.StatusGone || calls != 0 {
				t.Fatalf("path=%s status=%d API calls=%d", path, w.Code, calls)
			}
		}
	}
}
