package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

// Use independent readers so duplicate local frames and HTTP publishes stay observable.
func TestRemoteRevisionBurst(t *testing.T) {
	m, _, home := remoteFixture(t)
	session := "host-fixture"
	sockets := make(chan *websocket.Conn, 1)
	published := make(chan uint64, 64)
	polled := make(chan struct{}, 1)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/live-ticket"):
			serveRelayFixtureWS(w, r, m.hostID, false)
		case strings.HasSuffix(r.URL.Path, "/live"):
			fixtureSocket(func(ws *websocket.Conn) {
				sockets <- ws
				for {
					var beat string
					if websocket.Message.Receive(ws, &beat) != nil {
						return
					}
					if websocket.Message.Send(ws, beat) != nil {
						return
					}
				}
			}).ServeHTTP(w, r)
		case strings.HasSuffix(r.URL.Path, "/publish"):
			var body struct{ Snapshot struct{ Revision uint64 } }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			published <- body.Snapshot.Revision
		case strings.HasSuffix(r.URL.Path, "/poll"):
			polled <- struct{}{}
			w.Write([]byte(`{"commands":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	c := newCore(home, api.URL)
	c.control = api.URL
	c.remote = m
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); _ = c.remoteRelayConnection(ctx, m, session, func() {}) }()
	defer func() { cancel(); <-done }()
	next := func(ch <-chan uint64) uint64 {
		t.Helper()
		select {
		case rev := <-ch:
			return rev
		case <-time.After(2 * time.Second):
			t.Fatal("missing revision")
			return 0
		}
	}
	baseline := next(published)
	hostSocket := <-sockets
	local := httptest.NewServer(http.HandlerFunc(c.remoteEventsHTTP))
	defer local.Close()
	ticket, err := c.issueRemoteEventsTicket(m, "observer", local.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := websocket.NewConfig("ws"+strings.TrimPrefix(local.URL, "http")+remoteEventsPath, local.URL)
	cfg.Protocol = []string{"newnoa.remote", "ticket." + ticket["ticket"]}
	ws, err := cfg.DialContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	frames := make(chan uint64, 64)
	disconnected := make(chan struct{})
	go func() {
		defer close(disconnected)
		for {
			var frame struct{ Revision uint64 }
			if websocket.JSON.Receive(ws, &frame) != nil {
				return
			}
			frames <- frame.Revision
		}
	}()
	if got := next(frames); got != baseline {
		t.Fatalf("initial %d, want %d", got, baseline)
	}
	revoke := func(token string) {
		c.historyIdentityMu.Lock()
		c.historyIdentityHash = sha256.Sum256([]byte(token))
		c.historyIdentityAccount = m.owner
		c.historyIdentityMu.Unlock()
		c.revokeHistorySession(token)
	}
	revoke("other-session")
	const count = 12
	start := time.Now()
	for i := 0; i < count; i++ {
		m.mu.Lock()
		m.changedLocked(nil, "", "run.state", nil)
		m.mu.Unlock()
		if i == 0 {
			if err := websocket.JSON.Send(hostSocket, map[string]string{"type": "remote.changed", "kind": "commands", "hostId": m.hostID}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-polled:
			case <-time.After(120 * time.Millisecond):
				t.Fatal("command waited for publish window")
			}
		}
		time.Sleep(4 * time.Millisecond)
	}
	for _, ch := range []chan uint64{frames, published} {
		if got := next(ch); got != baseline+count {
			t.Fatalf("burst emitted %d before latest %d", got, baseline+count)
		}
	}
	if elapsed := time.Since(start); elapsed < 130*time.Millisecond {
		t.Fatalf("window ended early: %s", elapsed)
	}
	revoke("other-session")
	select {
	case rev := <-frames:
		t.Fatalf("duplicate local frame %d", rev)
	case rev := <-published:
		t.Fatalf("idle publish %d", rev)
	case <-time.After(350 * time.Millisecond):
	}
	m.mu.Lock()
	m.changedLocked(nil, "", "run.state", nil)
	m.mu.Unlock()
	time.Sleep(10 * time.Millisecond)
	revoke("observer")
	select {
	case <-disconnected:
	case <-time.After(110 * time.Millisecond):
		t.Fatal("revocation waited for window")
	}
	select {
	case rev := <-frames:
		t.Fatalf("sent revoked revision %d", rev)
	default:
	}
}
