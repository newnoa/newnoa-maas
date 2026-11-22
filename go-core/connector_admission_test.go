package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConnectorAdmissionRejectionDoesNotExecute(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
	}))
	defer api.Close()
	n := &connector{control: api.URL, purpose: "serving", credential: "node_test", client: api.Client()}
	r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"fixture"}`))
	r.Header.Set("X-Newnoa-Request", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	r.Header.Set("X-Newnoa-Permit", "permit_test")
	r.Header.Set("X-Newnoa-Gateway", "test")
	r.Header.Set("X-Newnoa-Epoch", "1")
	w := httptest.NewRecorder()
	n.ServeHTTP(w, r)
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" || w.Header().Get("X-Newnoa-Outcome") != "not-started" {
		t.Fatalf("lost admission rejection: %d %v", w.Code, w.Header())
	}
}
