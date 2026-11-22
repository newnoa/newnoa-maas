package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDirectBusinessLoginCanClaimAndReleaseOnlyItsLocalRuntime(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer valid-session" {
			w.WriteHeader(401)
			return
		}
		w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	account := &browserAccount{core: c}
	handler := account.controlHandler(api.URL)
	call := func(token, action string) int {
		r := httptest.NewRequest("POST", "http://127.0.0.1/api/local/account", strings.NewReader(action))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	if got := call("bad", `{"action":"claim"}`); got != 401 {
		t.Fatalf("unauthenticated claim %d", got)
	}
	if got := call("valid-session", `{"action":"claim"}`); got != 200 {
		t.Fatalf("claim %d", got)
	}
	if got := call("bad", `{"action":"release"}`); got == 200 {
		t.Fatal("foreign session released runtime")
	}
	if got := call("valid-session", `{"action":"claim","account":"forged"}`); got != 400 {
		t.Fatalf("unknown field %d", got)
	}
	if got := call("valid-session", `{"action":"release"}`); got != 200 {
		t.Fatalf("release %d", got)
	}
	if account.active {
		t.Fatal("runtime session still active")
	}
}
