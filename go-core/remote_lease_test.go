package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

func TestRemoteLiveOnlyHostAcknowledgementRenewsLease(t *testing.T) {
	m, _, _ := remoteFixture(t)
	m.session = "fixture"
	var beats, polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/live"):
			fixtureSocket(func(ws *websocket.Conn) {
				for {
					var msg string
					if websocket.Message.Receive(ws, &msg) != nil {
						return
					}
					beats.Add(1)
					if websocket.Message.Send(ws, `{"type":"remote.heartbeat","other":true}`) != nil {
						return
					}
					if websocket.Message.Send(ws, `{"type":"points.ping"}`) != nil {
						return
					}
				}
			}).ServeHTTP(w, r)
		case strings.HasSuffix(r.URL.Path, "/live-ticket"):
			serveRelayFixtureWS(w, r, m.hostID, false)
		default:
			polls.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	c.control = server.URL
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); c.remoteRelay(ctx, m) }()
	select {
	case <-m.ctx.Done():
	case <-time.After(remoteLease + 4*time.Second):
		t.Fatal("unacknowledged authority survived deadline")
	}
	<-done
	if beats.Load() < 2 || polls.Load() != 0 {
		t.Fatalf("heartbeats %d unexpected HTTP %d", beats.Load(), polls.Load())
	}
}
