package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

const relayFixtureTicket = "ws_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func fixtureSocket(handler websocket.Handler) websocket.Server {
	return websocket.Server{Handshake: func(c *websocket.Config, r *http.Request) error {
		c.Protocol = []string{"newnoa.points"}
		return nil
	}, Handler: handler}
}

func serveRelayFixtureWS(w http.ResponseWriter, r *http.Request, host string, commands bool) bool {
	switch r.URL.Path {
	case "/api/v1/me/points/live-ticket":
		json.NewEncoder(w).Encode(map[string]string{"ticket": relayFixtureTicket})
	case "/api/v1/me/points/live":
		fixtureSocket(func(ws *websocket.Conn) {
			if commands {
				_ = websocket.JSON.Send(ws, map[string]string{"type": "remote.changed", "kind": "commands", "hostId": host})
			}
			for {
				var msg string
				if websocket.Message.Receive(ws, &msg) != nil {
					return
				}
				if websocket.Message.Send(ws, msg) != nil {
					return
				}
			}
		}).ServeHTTP(w, r)
	default:
		return false
	}
	return true
}

func TestRemoteLiveIdleAndReconnect(t *testing.T) {
	m, _, _ := remoteFixture(t)
	m.session = "fixture"
	var polls, pubs, tickets, requests, beats atomic.Int32
	sockets := make(chan *websocket.Conn, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, "/live-ticket"):
			if r.Header.Get("X-NewNoa-Host-Id") != m.hostID || r.Header.Get("X-NewNoa-Host-Secret") != m.secret || r.Header.Get("X-NewNoa-Host-Epoch") != m.epoch || r.Header.Get("Authorization") != "Bearer fixture" {
				t.Error("host ticket binding")
			}
			tickets.Add(1)
			serveRelayFixtureWS(w, r, m.hostID, false)
		case strings.HasSuffix(r.URL.Path, "/live"):
			if r.URL.RawQuery != "" || r.Header.Get("Authorization") != "" || r.Header.Get("Sec-WebSocket-Protocol") != "newnoa.points, ticket."+relayFixtureTicket {
				t.Error("invalid upgrade")
			}
			fixtureSocket(func(ws *websocket.Conn) {
				sockets <- ws
				for {
					var beat string
					if websocket.Message.Receive(ws, &beat) != nil {
						return
					}
					if beat != `{"type":"remote.heartbeat"}` {
						t.Error("invalid heartbeat")
					}
					beats.Add(1)
					if websocket.Message.Send(ws, beat) != nil {
						return
					}
				}
			}).ServeHTTP(w, r)
		case strings.HasSuffix(r.URL.Path, "/poll"):
			polls.Add(1)
			w.Write([]byte(`{"commands":[]}`))
		case strings.HasSuffix(r.URL.Path, "/publish"):
			pubs.Add(1)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	c.control = server.URL
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.remoteRelay(ctx, m) }()
	defer func() { cancel(); <-done }()
	wait := func(check func() bool) {
		t.Helper()
		until := time.Now().Add(4 * time.Second)
		for !check() {
			if time.Now().After(until) {
				t.Fatal("timed out waiting for live relay")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	wait(func() bool { return pubs.Load() > 0 })
	ws := <-sockets
	send := func(kind, host string) {
		t.Helper()
		if err := websocket.JSON.Send(ws, map[string]string{"type": "remote.changed", "kind": kind, "hostId": host}); err != nil {
			t.Fatal(err)
		}
	}
	initial := requests.Load()
	send("commands", "other")
	send("state", m.hostID)
	send("receipts", m.hostID)
	time.Sleep(31 * time.Second)
	if got := beats.Load(); got != 2 {
		t.Fatalf("want initial heartbeat and one renewal after 31s, got %d", got)
	}
	if requests.Load() != initial || polls.Load() != 0 {
		t.Fatalf("idle HTTP requests %d -> %d, polls %d", initial, requests.Load(), polls.Load())
	}
	m.mu.Lock()
	m.changedLocked(nil, "", "executor.authorization", nil)
	m.mu.Unlock()
	wait(func() bool { return pubs.Load() == 2 })
	send("commands", m.hostID)
	wait(func() bool { return polls.Load() == 1 })
	if pubs.Load() != 2 {
		t.Fatal("empty queue wake published unchanged state")
	}
	before := pubs.Load()
	ws.Close()
	wait(func() bool { return tickets.Load() == 2 && pubs.Load() > before })
	if polls.Load() != 1 {
		t.Fatal("idle reconnect polled")
	}
}

func TestRemoteWakeDrainsFullBatchAndVerifiesEveryGrant(t *testing.T) {
	m, _, _ := remoteFixture(t)
	var polls, verified int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/poll"):
			polls++
			count := 16
			if polls > 1 {
				count = 1
			}
			commands := make([]map[string]any, 0, count)
			for i := 0; i < count; i++ {
				command := remoteEnvelope(randomToken(), "session.create", "", map[string]any{"agent": "codex", "cwd": m.home})
				commands = append(commands, map[string]any{"command": command, "grant": "invalid", "commandDigest": remoteDigest(command)})
			}
			json.NewEncoder(w).Encode(map[string]any{"commands": commands})
		case strings.HasSuffix(r.URL.Path, "/verify-grant"):
			verified++
			w.Write([]byte(`{"valid":false}`))
		default:
			t.Error("unexpected host call")
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	c.control = server.URL
	if err := c.fetchRemoteCommands(t.Context(), m, "fixture"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if polls != 2 || verified != 17 || len(m.operations) != 0 {
		t.Fatalf("polls %d, verified %d, operations %d", polls, verified, len(m.operations))
	}
}
