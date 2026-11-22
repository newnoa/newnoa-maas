package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
)

func TestHistoryEventsBrowserScopedReplayAndReconnect(t *testing.T) {
	for _, mode := range []string{"dev", "docker"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("NEWNOA_RUNTIME_MODE", mode)
			c := &core{historyIdentityHash: sha256.Sum256([]byte("session")), historyIdentityAccount: "member"}
			mux := http.NewServeMux()
			mux.Handle("/api/local/core", browserLocal(c, ""))
			mux.HandleFunc(historyEventsPath, c.historyEventsHTTP)
			server := httptest.NewServer(instrumentCoreHTTP(browserBoundary((&browserAccount{core: c, active: true, claimed: true, ownerID: "member", tokenHash: c.historyIdentityHash}).handler(mux, ""))))
			defer server.Close()
			defer c.historyEvents.close()
			issue := func() string {
				req, _ := http.NewRequest("POST", server.URL+"/api/local/core", strings.NewReader(`{"method":"history.events.ticket","params":{"session":"forged"}}`))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Origin", server.URL)
				req.Header.Set("Authorization", "Bearer session")
				res, err := server.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer res.Body.Close()
				var values map[string]string
				if json.NewDecoder(res.Body).Decode(&values) != nil || len(values) != 1 || len(values["ticket"]) != 64 {
					t.Fatalf("ticket failed %d", res.StatusCode)
				}
				return values["ticket"]
			}
			connect := func(origin, ticket string) (*websocket.Conn, error) {
				cfg, _ := websocket.NewConfig("ws"+strings.TrimPrefix(server.URL, "http")+historyEventsPath, origin)
				cfg.Protocol = []string{"newnoa.history", "ticket." + ticket}
				return cfg.DialContext(t.Context())
			}
			key := issue()
			if ws, err := connect("http://localhost:11234", key); err == nil {
				ws.Close()
				t.Fatal("origin not bound")
			}
			ws, err := connect(server.URL, key)
			if err != nil {
				t.Fatal(err)
			}
			defer ws.Close()
			if replay, err := connect(server.URL, key); err == nil {
				replay.Close()
				t.Fatal("ticket replayed")
			}
			read := func(expected int) {
				_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
				var data map[string]any
				if err := websocket.JSON.Receive(ws, &data); err != nil || len(data) != 2 || data["type"] != "history.changed" || data["mask"] != float64(expected) {
					t.Fatalf("mask %d got %v, %v", expected, data, err)
				}
			}
			read(historyAllChanged)
			c.historyEvents.publish("another", historySettingsChanged)
			c.historyEvents.publish("member", historyExportChanged)
			c.historyEvents.publish("member", historyRecordsChanged)
			read(historyExportChanged | historyRecordsChanged)
			c.historyEvents.publishService()
			c.historyEvents.publish("member", historyCompressionChanged|historySVGChanged)
			read(historyServiceChanged | historyCompressionChanged | historySVGChanged)
			pending := issue()
			c.revokeHistorySession("session")
			var extra string
			if err = websocket.Message.Receive(ws, &extra); err == nil {
				t.Fatal("subscription survived logout")
			}
			if replay, err := connect(server.URL, pending); err == nil {
				replay.Close()
				t.Fatal("pending ticket survived logout")
			}
			if c.remote != nil || c.history != nil {
				t.Fatal("watching changes opened agent or history runtime")
			}
		})
	}
}

func TestHistoryEventsDesktopEndpointAndSessionSwitch(t *testing.T) {
	c := &core{historyIdentityHash: sha256.Sum256([]byte("first")), historyIdentityAccount: "member", proxySession: "first"}
	defer c.historyEvents.close()
	ticket, err := c.historyEventTicket(json.RawMessage(`{"session":"first"}`), "")
	if err != nil {
		t.Fatal(err)
	}
	endpointURL, err := url.Parse(ticket["url"])
	if err != nil {
		t.Fatal(err)
	}
	// The native WebSocket constructor omits Origin.
	endpoint, err := net.DialTimeout("tcp", endpointURL.Host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	fmt.Fprintf(endpoint, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: newnoa.history, ticket.%s\r\n\r\n", historyEventsPath, endpointURL.Host, ticket["ticket"])
	response, err := http.ReadResponse(bufio.NewReader(endpoint), nil)
	if err != nil || response.StatusCode != 101 {
		t.Fatalf("native handshake: %v %v", response, err)
	}
	c.setProxySession("second")
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.historyEvents.mu.Lock()
		n := len(c.historyEvents.observers)
		c.historyEvents.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("previous account retained socket")
		}
		time.Sleep(time.Millisecond)
	}
}
