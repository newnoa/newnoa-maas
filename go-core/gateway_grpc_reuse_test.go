package main

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	rpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

func TestGatewayGRPCReusesTLSConnectionAndKeepsCallerHeadersPerStream(t *testing.T) {
	var mu sync.Mutex
	peers := map[string]bool{}
	rpcServer := rpc.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		peers[r.RemoteAddr] = true
		mu.Unlock()
		io.WriteString(w, r.Header.Get("Authorization"))
	}))
	defer rpcServer.Stop()
	gateway, ca := gatewayTLSFixture(t, rpcServer, false)
	endpoint := gatewayTrustFixture(t, "reuse-node", gateway.URL, ca)
	endpoint.Transport = "grpc"
	for _, token := range []string{"Bearer first", "Bearer second", "Bearer third"} {
		request, _ := http.NewRequest("GET", gateway.URL+"/v1/models", nil)
		request.Header.Set("Authorization", token)
		response, err := grpcGatewayRoundTrip(request, endpoint)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || string(body) != token {
			t.Fatalf("caller headers leaked: %q %v", body, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(peers) != 1 {
		t.Fatalf("TLS connections=%d want=1", len(peers))
	}
}

func TestGatewayGRPCPoolIdentityIncludesTrustTransportAndDialTarget(t *testing.T) {
	server, ca := gatewayTLSFixture(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), false)
	_, otherCA := gatewayTLSFixture(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), false)
	pool := &grpcGatewayPool{idleTimeout: time.Hour}
	defer pool.closeIdle()
	base := gatewayEndpoint{ID: "node", Address: server.URL, Transport: "grpc", TrustedCA: ca}
	first, release, err := pool.acquire(base)
	if err != nil {
		t.Fatal(err)
	}
	release()
	same := base
	same.ConnectionCount, same.LatencyTolerancePercent = 99, 20
	next, release, err := pool.acquire(same)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if next != first {
		t.Fatal("load metrics fragmented identical transport")
	}
	for _, dimension := range []string{"id", "address", "transport", "ca", "dial"} {
		endpoint := base
		switch dimension {
		case "id":
			endpoint.ID = "other"
		case "address":
			endpoint.Address += "/"
		case "transport":
			endpoint.Transport = ""
		case "ca":
			endpoint.TrustedCA = otherCA
		case "dial":
			endpoint.dialAddress = strings.TrimPrefix(server.URL, "https://")
		}
		connection, release, err := pool.acquire(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		release()
		if connection == first {
			t.Fatalf("shared %s identity", dimension)
		}
	}
}

func TestGatewayGRPCPoolIsBoundedClosesIdleAndProtectsActiveStreams(t *testing.T) {
	pool := &grpcGatewayPool{idleTimeout: time.Hour}
	defer pool.closeIdle()
	var releases []func()
	var cached []*grpc.ClientConn
	for i := 0; i < gatewayMaxNodes; i++ {
		connection, release, err := pool.acquire(gatewayEndpoint{ID: strconv.Itoa(i), Address: "https://127.0.0.1:1", Transport: "grpc"})
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
		cached = append(cached, connection)
	}
	connection, release, err := pool.acquire(gatewayEndpoint{ID: "overflow", Address: "https://127.0.0.1:1", Transport: "grpc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pool.entries) != gatewayMaxNodes {
		t.Fatal("cache grew beyond node bound")
	}
	release()
	release()
	if connection.GetState() != connectivity.Shutdown {
		t.Fatal("uncached connection leaked")
	}
	pool.closeIdle()
	for _, connection := range cached {
		if connection.GetState() == connectivity.Shutdown {
			t.Fatal("active connection evicted")
		}
	}
	for _, release := range releases {
		release()
		release()
	}
	connection, release, err = pool.acquire(gatewayEndpoint{ID: "replacement", Address: "https://127.0.0.1:1", Transport: "grpc"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pool.entries) != gatewayMaxNodes {
		t.Fatal("idle eviction changed cache bound")
	}
	release()
	pool.closeIdle()
	if len(pool.entries) != 0 || connection.GetState() != connectivity.Shutdown {
		t.Fatal("idle connections retained")
	}
}

func TestGatewayGRPCPoolExpiresIdleEntries(t *testing.T) {
	pool := &grpcGatewayPool{idleTimeout: 10 * time.Millisecond}
	connection, release, err := pool.acquire(gatewayEndpoint{Address: "https://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	release()
	deadline := time.After(time.Second)
	for {
		pool.mu.Lock()
		empty := len(pool.entries) == 0
		pool.mu.Unlock()
		if empty && connection.GetState() == connectivity.Shutdown {
			return
		}
		select {
		case <-deadline:
			pool.closeIdle()
			t.Fatal("idle connection did not expire")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestGatewayGRPCOneCancelledStreamDoesNotCloseSharedConnection(t *testing.T) {
	rpcServer := rpc.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ready")
		w.(http.Flusher).Flush()
		if r.URL.Path == "/v1/chat/completions" {
			<-r.Context().Done()
		}
	}))
	defer rpcServer.Stop()
	server, ca := gatewayTLSFixture(t, rpcServer, false)
	endpoint := gatewayEndpoint{ID: "concurrent", Address: server.URL, Transport: "grpc", TrustedCA: ca}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/chat/completions", strings.NewReader(`{}`))
	first, err := grpcGatewayRoundTrip(request, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Body.Close()
	key := grpcGatewayIdentity{endpoint.ID, endpoint.Address, endpoint.Transport, endpoint.TrustedCA, endpoint.dialAddress}
	request, _ = http.NewRequest("GET", server.URL+"/v1/models", nil)
	second, err := grpcGatewayRoundTrip(request, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Body.Close()
	cancel()
	deadline := time.After(time.Second)
	for {
		gatewayGRPCConnections.mu.Lock()
		users := gatewayGRPCConnections.entries[key].users
		gatewayGRPCConnections.mu.Unlock()
		if users == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("cancelled stream retained connection lease without body close")
		case <-time.After(time.Millisecond):
		}
	}
	body, err := io.ReadAll(second.Body)
	if err != nil || string(body) != "ready" {
		t.Fatalf("one stream cancelled another: %q %v", body, err)
	}
}

func TestGatewayGRPCWarmConnectionDoesNotBypassChangedDialTargetOrPin(t *testing.T) {
	firstRPC := rpc.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "first") }))
	defer firstRPC.Stop()
	secondRPC := rpc.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "second") }))
	defer secondRPC.Stop()
	first, firstCA := gatewayTLSFixture(t, firstRPC, false)
	second, secondCA := gatewayTLSFixture(t, secondRPC, false)
	endpoint := gatewayEndpoint{ID: "dial-pin", Address: "https://127.0.0.1:1", Transport: "grpc", TrustedCA: firstCA + secondCA, dialAddress: strings.TrimPrefix(first.URL, "https://")}
	for _, want := range []string{"first", "second", "pin-rejected"} {
		if want == "second" {
			endpoint.dialAddress = strings.TrimPrefix(second.URL, "https://")
		}
		if want == "pin-rejected" {
			endpoint.TrustedCA = firstCA
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		request, _ := http.NewRequestWithContext(ctx, "GET", endpoint.Address+"/v1/models", nil)
		response, err := grpcGatewayRoundTrip(request, endpoint)
		if want == "pin-rejected" {
			cancel()
			if err == nil {
				response.Body.Close()
				t.Fatal("changed pin reused previously trusted connection")
			}
			continue
		}
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		cancel()
		if err != nil || string(body) != want {
			t.Fatalf("wrong dial identity: %q want=%s err=%v", body, want, err)
		}
	}
}
