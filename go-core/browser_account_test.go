package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBrowserAccountPreservesCompleteRemoteImageEnvelope(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	body := `{"method":"remote.command","params":{"content":"` + strings.Repeat("a", 16384) + `"}}`
	handler := (&browserAccount{core: c}).handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, err := io.ReadAll(r.Body)
		if err != nil || string(got) != body {
			t.Error("account wrapper truncated authenticated image command")
		}
		w.WriteHeader(http.StatusNoContent)
	}), api.URL)
	r := httptest.NewRequest(http.MethodPost, "/api/local/core", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer fixture")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("request rejected: %d", w.Code)
	}
}

func TestBrowserDisconnectCancelsAccountOwnedRequest(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
	}))
	defer control.Close()
	c := newCore(t.TempDir(), control.URL)
	defer c.shutdown()
	a := &browserAccount{core: c}
	started, stopped := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(a.handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(stopped) }), control.URL))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/local/core", nil)
	request.Header.Set("Authorization", "Bearer first")
	go func() {
		response, _ := http.DefaultClient.Do(request)
		if response != nil {
			response.Body.Close()
		}
	}()
	<-started
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		a.mu.Lock()
		for _, r := range a.requests {
			r.cancel()
		}
		a.mu.Unlock()
		<-stopped
		server.Close()
		t.Fatal("account wrapper detached incoming cancellation")
	}
	server.Close()
}

func TestExpirySerializesWithNewAccountClaim(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		if r.Header.Get("Authorization") == "Bearer second" {
			id = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
		}
		json.NewEncoder(w).Encode(map[string]string{"id": id})
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	a := &browserAccount{core: c}
	r := httptest.NewRequest("GET", "http://127.0.0.1/", nil)
	if err := a.authenticate(r, "first", api.URL); err != nil {
		t.Fatal(err)
	}
	wait, cancelled := make(chan struct{}), make(chan struct{})
	a.requests = map[uint64]browserAccountRequest{1: {cancel: func() { delete(a.requests, 1); close(cancelled) }, done: wait}}
	expired := make(chan error, 1)
	go func() { expired <- a.expire("first") }()
	<-cancelled
	claimed := make(chan error, 1)
	go func() { claimed <- a.authenticate(r, "second", api.URL) }()
	select {
	case err := <-claimed:
		close(wait)
		<-expired
		t.Fatalf("new account claimed before old cleanup finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(wait)
	if err := <-expired; err != nil {
		t.Fatal(err)
	}
	if err := <-claimed; err != nil {
		t.Fatal(err)
	}
	if !a.active || a.ownerID != "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee" {
		t.Fatal("new account did not retain runtime ownership")
	}
}

func TestBrowserAccountLoginSwitchAndLogoutOwnTheWholeCore(t *testing.T) {
	var validMu sync.Mutex
	valid := map[string]bool{"first": true}
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validMu.Lock()
		ok := valid[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		validMu.Unlock()
		if !ok {
			w.WriteHeader(401)
			return
		}
		id := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		if r.Header.Get("Authorization") == "Bearer second" {
			id = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
		}
		json.NewEncoder(w).Encode(map[string]string{"id": id})
	}))
	defer control.Close()
	c := newCore(t.TempDir(), control.URL)
	defer c.shutdown()
	downstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/login":
			if r.URL.RawQuery == "failed" {
				w.WriteHeader(401)
				return
			}
			validMu.Lock()
			valid["second"] = true
			validMu.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"token": "second", "account": map[string]string{"id": "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"}})
		case "/api/v1/auth/logout":
			validMu.Lock()
			delete(valid, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
			validMu.Unlock()
			w.Write([]byte(`{"ok":true}`))
		default:
			w.Write([]byte(`{"ok":true}`))
		}
	})
	handler := (&browserAccount{core: c}).handler(downstream, control.URL)
	call := func(path, token string) int {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	if code := call("/api/v1/me", "first"); code != 200 {
		t.Fatal(code)
	}
	if code := call("/api/local/model-test", "second"); code != 401 {
		t.Fatalf("second account entered model test: %d", code)
	}
	if code := call("/api/v1/me", "second"); code != 401 {
		t.Fatalf("idle core accepted second session: %d", code)
	}
	c.server = &http.Server{}
	if code := call("/api/v1/auth/login?failed", ""); code != 401 || c.server == nil {
		t.Fatal("failed login disrupted owner")
	}
	if code := call("/api/v1/auth/login", ""); code != 200 || c.server != nil {
		t.Fatal("successful login did not stop old proxy")
	}
	if code := call("/api/local/core", "first"); code != 401 {
		t.Fatalf("old tab still has local access: %d", code)
	}
	if code := call("/api/v1/auth/logout", "first"); code != 401 {
		t.Fatal("old tab may log out new owner")
	}
	if code := call("/api/local/core", "second"); code != 200 {
		t.Fatal(code)
	}
	if code := call("/api/v1/auth/logout", "second"); code != 200 {
		t.Fatal(code)
	}
	if code := call("/api/v1/me", "first"); code != 401 {
		t.Fatal("retired session reclaimed core")
	}
	if code := call("/api/v1/me", "third"); code != 401 {
		t.Fatal("logout permitted silent account takeover")
	}
	if code := call("/api/v1/auth/login", ""); code != 200 {
		t.Fatal("explicit login could not claim released core")
	}
}

func TestBrowserAccountSwitchCancelsOldRequestsBeforeCleanup(t *testing.T) {
	var validMu sync.Mutex
	valid := map[string]bool{"first": true}
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		validMu.Lock()
		ok := valid[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		validMu.Unlock()
		if !ok {
			w.WriteHeader(401)
			return
		}
		id := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		if r.Header.Get("Authorization") == "Bearer second" {
			id = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
		}
		w.Write([]byte(`{"id":"` + id + `"}`))
	}))
	defer control.Close()
	c := newCore(t.TempDir(), control.URL)
	defer c.shutdown()
	started, finished := make(chan struct{}), make(chan struct{})
	handler := (&browserAccount{core: c}).handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "login") {
			validMu.Lock()
			valid["second"] = true
			validMu.Unlock()
			w.Write([]byte(`{"token":"second","account":{"id":"bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"}}`))
			return
		}
		close(started)
		<-r.Context().Done()
		close(finished)
	}), control.URL)
	go func() {
		r := httptest.NewRequest("GET", "/api/local/model-test", nil)
		r.Header.Set("Authorization", "Bearer first")
		handler.ServeHTTP(httptest.NewRecorder(), r)
	}()
	<-started
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/api/v1/auth/login", nil))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("account switch did not cancel old request")
	}
	select {
	case <-finished:
	default:
		t.Fatal("old request survived account switch")
	}
}

func TestBrowserAccountTransientIdentityFailureDoesNotExpireOwner(t *testing.T) {
	fail := false
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			http.Error(w, "offline", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
	}))
	defer control.Close()
	c := newCore(t.TempDir(), control.URL)
	defer c.shutdown()
	account := &browserAccount{core: c}
	if err := account.authenticate(httptest.NewRequest("POST", "/login", nil), "first", control.URL); err != nil {
		t.Fatal(err)
	}
	c.server = &http.Server{}
	fail = true
	_, _, err := account.begin("first", control.URL)
	if err == nil {
		t.Fatal("expected transient identity failure")
	}
	account.mu.Lock()
	active := account.active
	account.mu.Unlock()
	if !active || c.server == nil {
		t.Fatal("transient identity failure expired the account or stopped proxy")
	}
}

func TestBrowserAccountOwnershipSurvivesCoreRestartAndLogoutRequiresNewLogin(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		if r.Header.Get("Authorization") == "Bearer second" {
			id = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
		}
		json.NewEncoder(w).Encode(map[string]string{"id": id})
	}))
	defer control.Close()
	home := t.TempDir()
	first := newCore(home, control.URL)
	a := &browserAccount{core: first}
	if err := a.load(); err != nil {
		t.Fatal(err)
	}
	if _, release, err := a.begin("first", control.URL); err != nil {
		t.Fatal(err)
	} else {
		release()
	}
	first.shutdown()
	second := newCore(home, control.URL)
	b := &browserAccount{core: second}
	if err := b.load(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.begin("second", control.URL); !errors.Is(err, errBrowserAccountConflict) {
		t.Fatalf("restart accepted another account: %v", err)
	}
	if err := b.authenticate(httptest.NewRequest("POST", "/login", nil), "second", control.URL); err != nil {
		t.Fatal(err)
	}
	if err := b.logout("second", control.URL); err != nil {
		t.Fatal(err)
	}
	second.shutdown()
	third := newCore(home, control.URL)
	defer third.shutdown()
	c := &browserAccount{core: third}
	if err := c.load(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.begin("first", control.URL); !errors.Is(err, errBrowserAccountConflict) {
		t.Fatalf("restart silently reclaimed a logged-out core: %v", err)
	}
	if err := c.authenticate(httptest.NewRequest("POST", "/login", nil), "first", control.URL); err != nil {
		t.Fatal(err)
	}
}

func TestBrowserAccountConnectorTimeoutKeepsAccountLocked(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1")
	account := &browserAccount{core: c}
	started, done := make(chan struct{}), make(chan struct{})
	c.connector = &connector{done: done, cancel: func() { close(started) }}
	if err := account.stopRuntime(); err == nil {
		t.Fatal("expected connector shutdown timeout")
	}
	<-started
	if c.connector == nil {
		t.Fatal("timed out connector was forgotten")
	}
}
