package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFRPWebsocketUpgradeOnlyUsesFixedInternalTarget(t *testing.T) {
	backendCalls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendCalls++
		if r.RequestURI != "/~!frp" || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("X-Api-Key") != "" || r.Header.Get("Proxy-Authorization") != "" {
			t.Error("unsafe FRP forwarding")
		}
		conn, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		buffer.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		buffer.Flush()
		io.Copy(conn, conn)
	}))
	defer backend.Close()
	proxy, err := newFRPWebsocketProxy(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	gateway := newGateway("http://127.0.0.1:1", "internal", "fixture", nil)
	gateway.frp = proxy
	server := httptest.NewServer(instrumentHTTP(gateway, "newnoa-gateway", gatewayRoute))
	defer server.Close()
	conn, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprint(conn, "GET /~!frp HTTP/1.1\r\nHost: attacker.example\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nAuthorization: Bearer ses_do_not_forward\r\nCookie: session=do_not_forward\r\nX-Api-Key: nn_do_not_forward\r\nProxy-Authorization: Basic do_not_forward\r\n\r\n")
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: "GET"})
	if err != nil || response.StatusCode != 101 {
		t.Fatalf("upgrade failed %v %v", response, err)
	}
	fmt.Fprint(conn, "frp-inner-tls")
	raw := make([]byte, len("frp-inner-tls"))
	if _, err = io.ReadFull(reader, raw); err != nil || string(raw) != "frp-inner-tls" {
		t.Fatal("upgraded byte stream altered")
	}
	for _, route := range []string{"/~!frp", "/~!frp?target=http://attacker", "/~!frp/other"} {
		response, err := http.Get(server.URL + route)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode == 101 || response.StatusCode == 200 {
			t.Fatal("non-upgrade request reached FRPS")
		}
	}
	if backendCalls != 1 {
		t.Fatalf("unexpected internal calls %d", backendCalls)
	}
	for _, target := range []string{"http://public.example:17000", "http://[", "http://127.0.0.1", "http://127.0.0.1:0", "http://127.0.0.1:65536", "http://user@127.0.0.1:17000", "http://127.0.0.1:17000/path", "https://127.0.0.1:17000", "http://127.0.0.1:17000?target=x"} {
		if _, err := newFRPWebsocketProxy(target); err == nil {
			t.Fatalf("unsafe target accepted %s", target)
		}
	}
}

func TestFRPWebsocketRequiresConfiguredReachableInternalService(t *testing.T) {
	g := newGateway("http://127.0.0.1:1", "internal", "fixture", nil)
	defer g.peers.Reset()
	request := httptest.NewRequest("GET", "/~!frp", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	response := httptest.NewRecorder()
	g.ServeHTTP(response, request)
	if response.Code != 503 {
		t.Fatal("disabled FRPS bridge accepted request", response.Code)
	}
	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	address := closed.URL
	closed.Close()
	proxy, err := newFRPWebsocketProxy(address)
	if err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != 502 {
		t.Fatal("closed FRPS did not fail safely", response.Code)
	}
}

func TestFRPAdmissionIsPerGatewayAndReturnsConnectionCapacity(t *testing.T) {
	now := time.Unix(1000000, 0)
	var first, second frpAdmission
	peer := netip.MustParseAddr("192.0.2.1")
	for i := 0; i < frpStartsPerSecond; i++ {
		if !first.accept(peer, now) {
			t.Fatal("unexpected rate denial", i)
		}
		first.release(peer)
	}
	if first.accept(peer, now) {
		t.Fatal("closed connections bypassed start limit")
	}
	if !second.accept(peer, now) {
		t.Fatal("independent gateway shared state")
	}
	second.release(peer)
	now = now.Add(time.Second)
	for i := 0; i < frpConnectionsPerPeer; i++ {
		if !first.accept(peer, now) {
			t.Fatal("unexpected concurrency denial", i)
		}
		now = now.Add(time.Second)
	}
	if first.accept(peer, now) {
		t.Fatal("peer concurrency limit was exceeded")
	}
	for range frpConnectionsPerPeer {
		first.release(peer)
	}
	if len(first.peers) != 0 || first.active != 0 {
		t.Fatal("peer retained after disconnect")
	}
}

func TestFRPAdmissionGlobalCapacityReturnsAfterDisconnect(t *testing.T) {
	now := time.Unix(1000000, 0)
	var gate frpAdmission
	addr := netip.MustParseAddr("192.0.2.1")
	for i := 0; i < frpConnectionsPerGateway; i++ {
		if !gate.accept(addr, now) {
			t.Fatal("unexpected admission failure", i)
		}
		addr = addr.Next()
		now = now.Add(time.Second)
	}
	if gate.accept(addr, now) {
		t.Fatal("global connection bound exceeded")
	}
	gate.release(netip.MustParseAddr("192.0.2.1"))
	if !gate.accept(addr, now) {
		t.Fatal("capacity not reclaimed")
	}
}

func TestFRPBusyHandshakeFailsBeforeBackendAndRecovers(t *testing.T) {
	entered := make(chan struct{}, frpPendingHandshakes)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { entered <- struct{}{}; <-release; w.WriteHeader(403) }))
	defer upstream.Close()
	handler, err := newFRPWebsocketProxy(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	request := func() int {
		r := httptest.NewRequest("GET", "/~!frp", nil)
		r.Header.Set("Connection", "Upgrade")
		r.Header.Set("Upgrade", "websocket")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	var wg sync.WaitGroup
	unblock := sync.OnceFunc(func() { close(release) })
	defer func() { unblock(); wg.Wait() }()
	for i := 0; i < frpPendingHandshakes; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); request() }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("backend handshake not reached", i)
		}
	}
	if got := request(); got != 429 {
		t.Fatal("busy handshake reached backend", got)
	}
	unblock()
	wg.Wait()
	if got := request(); got != http.StatusForbidden {
		t.Fatalf("new handshake failed after prior requests closed: %d", got)
	}

}
