package main

import (
	"context"
	"encoding/json"
	rpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGatewayGRPCConcurrentNodesKeepTheirOwnPrivateCA(t *testing.T) {
	testGatewayConcurrentTrust(t, true)
}

func TestGatewayGRPCStreamsBodyStatusAndTrailersWithoutPrivateBypass(t *testing.T) {
	const model = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	var privateCalls, httpCalls atomic.Int32
	rpcServer := rpc.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			io.WriteString(w, "ready")
			return
		}
		if r.Header.Get("Authorization") != "Bearer ses_login" || r.Header.Get("X-Api-Key") != "nn_fixture_key_123" {
			t.Error("credentials changed")
		}
		if r.URL.Path == "/v1/models" {
			io.WriteString(w, `{"data":[{"id":"fixture"}]}`)
			return
		}
		if r.URL.Path == "/private/target" {
			privateCalls.Add(1)
			io.WriteString(w, `{"peer":{"available":false}}`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"model":"`+model+`"`) {
			t.Error("request body lost")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Trailer", "X-Final")
		w.WriteHeader(201)
		io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		io.WriteString(w, "data: last\n\n")
		w.Header().Set("X-Final", "finished")
	}))
	defer rpcServer.Stop()
	gateway, ca := gatewayTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			httpCalls.Add(1)
			w.WriteHeader(500)
			return
		}
		rpcServer.ServeHTTP(w, r)
	}), false)
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]any{{"id": "node", "address": gateway.URL, "trustedCa": ca, "transport": "grpc"}})
	}))
	defer control.Close()
	handler, err := privateInferenceWithSelector(control.URL, control.URL, newGatewaySelector(control.URL))
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"`+model+`","stream":true}`))
	request.Header.Set("Authorization", "Bearer ses_login")
	request.Header.Set("X-Api-Key", "nn_fixture_key_123")
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, request)
	if out.Code != 201 || out.Body.String() != "data: first\n\ndata: last\n\n" || out.Result().Trailer.Get("X-Final") != "finished" {
		t.Fatalf("status=%d body=%s trailer=%v", out.Code, out.Body.String(), out.Result().Trailer)
	}
	if privateCalls.Load() != 0 || httpCalls.Load() != 0 {
		t.Fatalf("private=%d plaintext/http fallback=%d", privateCalls.Load(), httpCalls.Load())
	}
	listing := httptest.NewRequest("GET", "/v1/models", nil)
	listing.Header.Set("Authorization", "Bearer ses_login")
	listing.Header.Set("X-Api-Key", "nn_fixture_key_123")
	listed := httptest.NewRecorder()
	handler.ServeHTTP(listed, listing)
	if listed.Code != 200 || listed.Body.String() != `{"data":[{"id":"fixture"}]}` {
		t.Fatalf("model listing status=%d body=%s", listed.Code, listed.Body.String())
	}
}

func TestGatewayGRPCResponseCloseCancelsServerAndNeverDowngrades(t *testing.T) {
	stopped := make(chan struct{})
	rpcServer := rpc.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(stopped)
	}))
	defer rpcServer.Stop()
	var httpCalls atomic.Int32
	gateway, ca := gatewayTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			rpcServer.ServeHTTP(w, r)
		} else {
			httpCalls.Add(1)
			w.WriteHeader(500)
		}
	}), false)
	endpoint := gatewayTrustFixture(t, "node", gateway.URL, ca)
	endpoint.Transport = "grpc"
	request, _ := http.NewRequestWithContext(context.Background(), "POST", gateway.URL+"/v1/chat/completions", strings.NewReader(`{}`))
	request = request.WithContext(context.WithValue(request.Context(), selectedGatewayContext{}, endpoint))
	response, err := (gatewayTransport{}).RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 5)
	if _, err = io.ReadFull(response.Body, first); err != nil || string(first) != "first" {
		t.Fatalf("read=%s %v", first, err)
	}
	response.Body.Close()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("closing response did not cancel RPC handler")
	}
	if httpCalls.Load() != 0 {
		t.Fatal("gRPC request downgraded to HTTP")
	}
}

type blockedGatewayUpload struct {
	closed chan struct{}
	once   sync.Once
}

func (body *blockedGatewayUpload) Read([]byte) (int, error) { <-body.closed; return 0, io.EOF }
func (body *blockedGatewayUpload) Close() error {
	body.once.Do(func() { close(body.closed) })
	return nil
}

func TestGatewayGRPCEarlyResponseClosesBlockedUpload(t *testing.T) {
	server := rpc.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403); io.WriteString(w, "rejected") }))
	defer server.Stop()
	gateway, ca := gatewayTLSFixture(t, server, false)
	endpoint := gatewayTrustFixture(t, "node", gateway.URL, ca)
	endpoint.Transport = "grpc"
	body := &blockedGatewayUpload{closed: make(chan struct{})}
	request, _ := http.NewRequestWithContext(context.Background(), "POST", gateway.URL+"/v1/chat/completions", body)
	request.ContentLength = -1
	request = request.WithContext(context.WithValue(request.Context(), selectedGatewayContext{}, endpoint))
	response, err := (gatewayTransport{}).RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 403 || string(content) != "rejected" {
		t.Fatalf("status=%d content=%s error=%v", response.StatusCode, content, err)
	}
	select {
	case <-body.closed:
	case <-time.After(time.Second):
		t.Fatal("early RPC response left upload reader blocked")
	}
}
