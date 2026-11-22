package main

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOwnerChecksBeforeBodyRead(t *testing.T) {
	o := newOwner("fixture", "http://127.0.0.1:1", "internal")
	o.uploads = make(chan struct{}, 1)
	o.uploads <- struct{}{}
	for _, tc := range []struct {
		path, auth string
		code       int
	}{
		{"/plugin", "", 403},
		{"/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/messages", "", 401},
		{"/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/messages", "Bearer internal", 429},
	} {
		r := httptest.NewRequest("POST", tc.path, strings.NewReader("body"))
		r.Header.Set("Authorization", tc.auth)
		w := httptest.NewRecorder()
		o.ServeHTTP(w, r)
		body, _ := io.ReadAll(r.Body)
		if w.Code != tc.code || string(body) != "body" {
			t.Fatalf("result %d, body %q", w.Code, body)
		}
	}
}
