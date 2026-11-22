package main

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBusyGatewayRejectsUploadBeforeRead(t *testing.T) {
	g := newGateway("http://127.0.0.1:1", "internal", "fixture", nil)
	r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader("body"))
	r.Header.Set("Authorization", "Bearer ses_fixture")
	r.Header.Set("X-Api-Key", "nn_fixture")
	r.ContentLength = -1
	release := g.uploads.Reserve(r.RemoteAddr, 32<<20)
	if release == nil {
		t.Fatal("fixture reservation failed")
	}
	defer release()
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	body, _ := io.ReadAll(r.Body)
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" || string(body) != "body" {
		t.Fatalf("result %d, body %q", w.Code, body)
	}
}
