package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDockerServiceSessionRotatesSingleFlightAndSurvivesRestart(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	var rotations atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/me" {
			w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","role":"user"}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/refresh") {
			rotations.Add(1)
		}
		json.NewEncoder(w).Encode(map[string]any{"sessionId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "token": "ses_" + strings.Repeat("a", 43), "refreshToken": "ref_" + strings.Repeat("b", 43), "expiresAt": time.Now().Add(15 * time.Minute), "account": map[string]any{"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "role": "user"}})
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	a := &browserAccount{core: c}
	s := newDockerServiceSession(a, api.URL)
	req := httptest.NewRequest("POST", "http://localhost/api/local/session", nil)
	req.Header.Set("X-Newnoa-Session", "1")
	req.Header.Set("Authorization", "Bearer login")
	out := httptest.NewRecorder()
	s.handler().ServeHTTP(out, req)
	if out.Code != 200 {
		t.Fatalf("create %d: %s", out.Code, out.Body)
	}
	a.mu.Lock()
	managed := a.serviceManaged
	a.mu.Unlock()
	if !managed {
		t.Fatal("initial service login was not bound to service maintenance")
	}
	cookies := out.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatal("missing restricted cookie")
	}
	if strings.Contains(out.Body.String(), "refreshToken") {
		t.Fatal("refresh leaked to browser")
	}
	info, err := os.Stat(s.path())
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("unsafe service state", err)
	}
	s.mu.Lock()
	s.state.ExpiresAt = time.Now().Add(-time.Minute)
	s.mu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.current(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if rotations.Load() != 1 {
		t.Fatalf("parallel rotations: %d", rotations.Load())
	}
	restored := newDockerServiceSession(&browserAccount{core: c}, api.URL)
	if err := restored.load(); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		cookie bool
		site   string
		want   int
	}{{false, "", 401}, {true, "cross-site", 403}, {true, "same-origin", 200}} {
		r := httptest.NewRequest("GET", "http://localhost/api/local/session", nil)
		r.Header.Set("X-Newnoa-Session", "1")
		r.Header.Set("Sec-Fetch-Site", row.site)
		if row.cookie {
			r.AddCookie(cookies[0])
		}
		w := httptest.NewRecorder()
		restored.handler().ServeHTTP(w, r)
		if w.Code != row.want {
			t.Fatalf("cookie %v site %s: %d", row.cookie, row.site, w.Code)
		}
	}
}
func TestDockerRejectsAgentMutationsAndDevRetainsStatus(t *testing.T) {
	c := newCore(t.TempDir(), "http://localhost")
	defer c.shutdown()
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	for _, method := range []string{"agent.detect", "agent.preview", "agent.apply", "agent.restore"} {
		if _, err := c.handle(method, []byte(`{}`)); err == nil {
			t.Fatalf("docker allowed %s", method)
		}
	}
	value, err := c.handle("proxy.status", []byte(`{}`))
	if err != nil || value.(map[string]any)["runtimeMode"] != "docker" {
		t.Fatal(value, err)
	}
	t.Setenv("NEWNOA_RUNTIME_MODE", "dev")
	value, _ = c.handle("proxy.status", []byte(`{}`))
	if value.(map[string]any)["runtimeMode"] != "dev" {
		t.Fatal(value)
	}
}

func TestServiceLostRefreshResponseRestartsWithSameOperationAndOld401CannotClearNewToken(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	var operation atomic.Value
	var requests atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/refresh") {
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			if requests.Add(1) == 1 {
				operation.Store(body["operationId"])
				connection, _, _ := w.(http.Hijacker).Hijack()
				connection.Close()
				return
			}
			if body["operationId"] != operation.Load().(string) {
				t.Error("lost response changed operation")
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"sessionId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "token": "ses_" + strings.Repeat("n", 43), "refreshToken": "ref_" + strings.Repeat("r", 43), "expiresAt": time.Now().Add(15 * time.Minute), "account": map[string]any{"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "role": "user"}})
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	a := &browserAccount{core: c}
	s := newDockerServiceSession(a, api.URL)
	old := "ses_" + strings.Repeat("o", 43)
	s.state = dockerSessionState{SessionID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Token: old, RefreshToken: "ref_" + strings.Repeat("a", 43), ExpiresAt: time.Now().Add(-time.Minute), CookieHash: strings.Repeat("a", 64), Account: map[string]any{"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "role": "user"}}
	s.bind()
	if err := s.save(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.current(context.Background()); err == nil {
		t.Fatal("lost response accepted")
	}
	restored := newDockerServiceSession(a, api.URL)
	if err := restored.load(); err != nil {
		t.Fatal(err)
	}
	if restored.state.PendingOperation != operation.Load().(string) {
		t.Fatal("operation not persisted before send")
	}
	if _, err := restored.current(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := a.expire(old); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	active := a.active
	a.mu.Unlock()
	if !active {
		t.Fatal("old 401 cleared renewed runtime")
	}
}
func TestServiceMaintainsWithoutBrowserAndRevocationStopsIt(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	var rotated atomic.Int32
	var revoked atomic.Bool
	var stopped atomic.Bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/me" {
			if revoked.Load() {
				w.WriteHeader(401)
			}
			return
		}
		if strings.HasSuffix(r.URL.Path, "/refresh") {
			rotated.Add(1)
		}
		json.NewEncoder(w).Encode(map[string]any{"sessionId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "token": "ses_" + strings.Repeat("n", 43), "refreshToken": "ref_" + strings.Repeat("r", 43), "expiresAt": time.Now().Add(15 * time.Minute), "account": map[string]any{"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "role": "user"}})
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	done := make(chan struct{})
	close(done)
	c.connector = &connector{done: done, cancel: func() { stopped.Store(true) }}
	a := &browserAccount{core: c}
	s := newDockerServiceSession(a, api.URL)
	s.state = dockerSessionState{SessionID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Token: "ses_" + strings.Repeat("o", 43), RefreshToken: "ref_" + strings.Repeat("a", 43), ExpiresAt: time.Now().Add(-time.Minute), CookieHash: strings.Repeat("a", 64), Account: map[string]any{"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "role": "user"}}
	s.bind()
	s.save()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.maintainEvery(ctx, time.Millisecond)
	deadline := time.Now().Add(time.Second)
	for rotated.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if rotated.Load() != 1 {
		t.Fatal("headless renewal missing")
	}
	revoked.Store(true)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		empty := s.state.SessionID == ""
		s.mu.Unlock()
		if empty {
			if !stopped.Load() {
				t.Fatal("true service revocation did not stop the connector")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("revoked service remained available")
}

func TestBrowserDevelopmentSupportsAgentCommandsButDockerRejectsThem(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	for _, mode := range []string{"docker", "dev"} {
		t.Setenv("NEWNOA_RUNTIME_MODE", mode)
		r := httptest.NewRequest("POST", "/api/local/core", strings.NewReader(`{"method":"agent.detect","params":{"agent":"codex"}}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		browserLocal(c, api.URL).ServeHTTP(w, r)
		if mode == "docker" && w.Code != 403 {
			t.Fatal("docker agent allowed", w.Code)
		}
		if mode == "dev" && w.Code == 403 {
			t.Fatal("dev agent blocked")
		}
	}
}

func TestOfflineLogoutPersistsRevocationIntentAcrossRestart(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	var online atomic.Bool
	var revoked atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/revoke") {
			if !online.Load() {
				w.WriteHeader(503)
				return
			}
			revoked.Add(1)
			w.WriteHeader(204)
			return
		}
		w.WriteHeader(500)
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	a := &browserAccount{core: c}
	s := newDockerServiceSession(a, api.URL)
	s.state = dockerSessionState{SessionID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Token: "ses_" + strings.Repeat("o", 43), RefreshToken: "ref_" + strings.Repeat("a", 43), ExpiresAt: time.Now().Add(-time.Hour), CookieHash: hash([]byte("browser-cookie")), Account: map[string]any{"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "role": "user"}}
	s.bind()
	s.save()
	request := httptest.NewRequest("DELETE", "http://localhost/api/local/session", nil)
	request.Header.Set("X-Newnoa-Session", "1")
	request.AddCookie(&http.Cookie{Name: serviceCookie, Value: "browser-cookie"})
	response := httptest.NewRecorder()
	s.handler().ServeHTTP(response, request)
	if response.Code != 503 {
		t.Fatal("offline logout error hidden", response.Code)
	}
	if _, err := s.current(context.Background()); err == nil {
		t.Fatal("closing session still accessible")
	}
	restored := newDockerServiceSession(a, api.URL)
	if err := restored.load(); err != nil {
		t.Fatal(err)
	}
	if !restored.state.Closing {
		t.Fatal("revocation intent lost")
	}
	online.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); restored.maintainEvery(ctx, time.Millisecond) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		restored.mu.Lock()
		empty := restored.state.SessionID == ""
		restored.mu.Unlock()
		if empty {
			if revoked.Load() != 1 {
				t.Fatal("server revoke missing")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("logout intent not resumed")
}

func TestFailedRuntimeCleanupCannotReportSuccessUntilConnectorStops(t *testing.T) {
	c := newCore(t.TempDir(), "http://localhost")
	defer c.shutdown()
	a := &browserAccount{core: c, active: true, claimed: true, tokenHash: sha256.Sum256([]byte("current"))}
	done := make(chan struct{})
	c.connector = &connector{done: done, cancel: func() {}}
	if err := a.expire("current"); err == nil {
		close(done)
		t.Fatal("stuck connector cleanup accepted")
	}
	retried := make(chan error, 1)
	go func() { retried <- a.expire("current") }()
	select {
	case err := <-retried:
		close(done)
		t.Fatalf("failed cleanup was silently forgotten: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(done)
	if err := <-retried; err != nil {
		t.Fatal(err)
	}
}

func TestVerificationStartedBeforeRefreshCannotRestoreOldRuntimeToken(t *testing.T) {
	verified, release := make(chan struct{}), make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(verified)
		<-release
		w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	a := &browserAccount{core: c}
	s := newDockerServiceSession(a, api.URL)
	s.state = dockerSessionState{Token: "old", Account: map[string]any{"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}}
	s.bind()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, done, _ := a.begin("old", api.URL)
		if done != nil {
			done()
		}
	}()
	<-verified
	s.state.Token = "new"
	s.bind()
	close(release)
	<-finished
	a.mu.Lock()
	current := a.tokenHash
	a.mu.Unlock()
	if current != sha256.Sum256([]byte("new")) {
		t.Fatal("late verification rolled current bearer backwards")
	}
}

func TestRefreshCommitWindowOldBearerFailureDoesNotStopLiveRuntime(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	committed, release := make(chan struct{}), make(chan struct{})
	var oldInvalid, stopped atomic.Bool
	old, next := "ses_"+strings.Repeat("o", 43), "ses_"+strings.Repeat("n", 43)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/refresh") {
			oldInvalid.Store(true)
			close(committed)
			<-release
			json.NewEncoder(w).Encode(map[string]any{"sessionId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "token": next, "refreshToken": "ref_" + strings.Repeat("n", 43), "expiresAt": time.Now().Add(15 * time.Minute), "account": map[string]any{"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "role": "user"}})
			return
		}
		if r.URL.Path == "/api/v1/me" {
			if oldInvalid.Load() && r.Header.Get("Authorization") == "Bearer "+old {
				w.WriteHeader(401)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "role": "user"})
			return
		}
		w.WriteHeader(200)
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	defer c.shutdown()
	done := make(chan struct{})
	close(done)
	c.connector = &connector{done: done, cancel: func() { stopped.Store(true) }}
	a := &browserAccount{core: c}
	s := newDockerServiceSession(a, api.URL)
	s.state = dockerSessionState{SessionID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Token: old, RefreshToken: "ref_" + strings.Repeat("o", 43), ExpiresAt: time.Now().Add(-time.Minute), CookieHash: strings.Repeat("a", 64), Account: map[string]any{"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "role": "user"}}
	s.bind()
	result := make(chan error, 1)
	go func() { _, err := s.current(context.Background()); result <- err }()
	<-committed
	_, finish, err := a.begin(old, api.URL)
	if finish != nil {
		finish()
	}
	if err == nil {
		t.Error("invalidated old bearer accepted")
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if stopped.Load() {
		t.Fatal("normal refresh stopped live connector before Core received next bearer")
	}
}

func TestServiceSameOriginBehindPublicHTTPSIngress(t *testing.T) {
	for _, host := range []string{"dev-h5.example.invalid", "sit-h5.example.invalid", "dev-h5-cn.example.invalid"} {
		r := httptest.NewRequest("POST", "http://"+host+"/api/local/session", nil)
		r.Header.Set("Origin", "https://"+host)
		r.Header.Set("Sec-Fetch-Site", "same-origin")
		if !serviceSameOrigin(r) {
			t.Errorf("TLS terminates at ingress: %s rejected", host)
		}
		r.Header.Set("Origin", "https://other.example.invalid")
		if serviceSameOrigin(r) {
			t.Error("cross origin accepted")
		}
		r.Header.Set("Origin", "https://"+host)
		r.Header.Set("Sec-Fetch-Site", "cross-site")
		if serviceSameOrigin(r) {
			t.Error("cross site accepted")
		}
	}
	r := httptest.NewRequest("POST", "http://untrusted.example/api/local/session", nil)
	r.Header.Set("Origin", "https://untrusted.example")
	r.Header.Set("X-Forwarded-Proto", "https")
	if serviceSameOrigin(r) {
		t.Error("arbitrary forwarded scheme accepted")
	}
}
