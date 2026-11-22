package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

func TestRemoteEventsBrowserThroughProductionWrappers(t *testing.T) {
	c, m, _, _ := remoteRPCFixture(t)
	mux := http.NewServeMux()
	mux.Handle("/api/local/core", browserLocal(c, c.control))
	mux.HandleFunc(remoteEventsPath, c.remoteEventsHTTP)
	server := httptest.NewServer(instrumentCoreHTTP(browserBoundary((&browserAccount{core: c}).handler(mux, c.control))))
	defer server.Close()
	origin := server.URL
	req, _ := http.NewRequest("POST", origin+"/api/local/core", strings.NewReader(`{"method":"remote.events.ticket","params":{}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer rpc-session")
	req.Header.Set("Origin", origin)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var issued map[string]string
	defer res.Body.Close()
	if json.NewDecoder(res.Body).Decode(&issued) != nil || len(issued) != 1 || len(issued["ticket"]) != 64 {
		t.Fatalf("ticket response status %d", res.StatusCode)
	}
	connect := func(origin, ticket string) (*websocket.Conn, error) {
		cfg, err := websocket.NewConfig("ws"+strings.TrimPrefix(server.URL, "http")+remoteEventsPath, origin)
		if err != nil {
			return nil, err
		}
		cfg.Protocol = []string{"newnoa.remote", "ticket." + ticket}
		return cfg.DialContext(t.Context())
	}
	key := issued["ticket"]
	if ws, err := connect("http://localhost:11111", key); err == nil {
		ws.Close()
		t.Fatal("cross-origin upgrade accepted")
	}
	ws, err := connect(origin, key)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if len(ws.Config().Protocol) != 1 || ws.Config().Protocol[0] != "newnoa.remote" {
		t.Fatal("ticket echoed")
	}
	if again, err := connect(origin, key); err == nil {
		again.Close()
		t.Fatal("ticket replay accepted")
	}
	read := func() uint64 {
		t.Helper()
		ws.SetReadDeadline(time.Now().Add(time.Second))
		var msg map[string]any
		if err := websocket.JSON.Receive(ws, &msg); err != nil {
			t.Fatal(err)
		}
		if len(msg) != 5 || msg["type"] != "remote.changed" || msg["kind"] != "state" || msg["hostId"] != m.hostID || msg["hostEpoch"] != m.epoch {
			t.Fatal("wrong invalidation schema")
		}
		return uint64(msg["revision"].(float64))
	}
	rev := read()
	m.mu.Lock()
	m.changedLocked(nil, "", "executor.authorization", nil)
	m.mu.Unlock()
	if read() != rev+1 {
		t.Fatal("missing local event")
	}
	c.historyIdentityMu.Lock()
	c.historyIdentityHash = sha256.Sum256([]byte("rpc-session"))
	c.historyIdentityAccount = m.owner
	c.historyIdentityMu.Unlock()
	c.revokeHistorySession("rpc-session")
	var extra string
	if websocket.Message.Receive(ws, &extra) == nil {
		t.Fatal("subscription survived session revocation")
	}
}

func TestRemoteDesktopUpgradeWithoutOrigin(t *testing.T) {
	c, m, _, _ := remoteRPCFixture(t)
	raw, err := callRemoteRPC(t, c, "remote.events.ticket", nil)
	if err != nil {
		t.Fatal(err)
	}
	issued := raw.(map[string]string)
	u, err := url.Parse(issued["url"])
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != remoteEventsPath || u.RawQuery != "" {
		t.Fatal("not a loopback endpoint")
	}
	// Electron's main-process WebSocket omits Origin.
	dial := func(origin string) int {
		t.Helper()
		conn, e := net.DialTimeout("tcp", u.Host, time.Second)
		if e != nil {
			t.Fatal(e)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(time.Second))
		fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: newnoa.remote, ticket.%s\r\n%s\r\n", u.Path, u.Host, issued["ticket"], origin)
		res, e := http.ReadResponse(bufio.NewReader(conn), nil)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		return res.StatusCode
	}
	if dial("Origin: http://localhost\r\n") == 101 || dial("") != 101 || dial("") == 101 {
		t.Fatal("desktop ticket origin or reuse")
	}
	expired, _ := c.issueRemoteEventsTicket(m, "rpc-session", "")
	issued = expired
	m.mu.Lock()
	ticket := m.live.tickets[issued["ticket"]]
	ticket.until = time.Now().Add(-time.Second)
	m.live.tickets[issued["ticket"]] = ticket
	m.mu.Unlock()
	if dial("") == 101 {
		t.Fatal("expired ticket accepted")
	}
}

func TestRemoteLocalFileWake(t *testing.T) {
	c, m, _, _ := remoteRPCFixture(t)
	initial := c.remoteLocalFiles()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go c.watchRemoteLocal(ctx, m, initial)
	m.mu.Lock()
	changed := m.changesLocked()
	m.mu.Unlock()
	if err := os.MkdirAll(c.home+"/.codex", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.home+"/.codex/auth.json", []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	case <-time.After(4 * time.Second):
		t.Fatal("external auth change missed")
	}
}
