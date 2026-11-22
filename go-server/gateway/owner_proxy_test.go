package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const ownerProxyNode = "7d14eac2-e8ce-4719-b2fc-0d573e788024"

func TestOwnerTunnelProxyRejectsUnauthenticatedRequests(t *testing.T) {
	g := newGateway("", "internal-platform", "gateway-b", nil)
	request := httptest.NewRequest(http.MethodPost, "/_newnoa/owner/tunnels/"+ownerProxyNode+"/3/v1/chat/completions", strings.NewReader(`{}`))
	request.Header.Set("X-Newnoa-Permit", "client-forged")
	response := httptest.NewRecorder()
	g.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("Unauthenticated owner forwarding returned %d, want401", response.Code)
	}
}

func TestOwnerTunnelProxySameServerTLSProbe(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-local")
	var calls atomic.Int32
	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/tunnels/"+ownerProxyNode+"/3/probe/v1/models" || r.Method != "GET" || r.Header.Get("X-Newnoa-Gateway") != "gateway-a" || r.Header.Get("X-Newnoa-Permit") != "probe_fixture" || r.Header.Get("Authorization") != "Bearer internal-platform" {
			t.Error("same-server probe lost its API-authorized scope")
		}
		io.WriteString(w, `{"data":[{"id":"fixture"}]}`)
	}))
	defer bridge.Close()
	target, _ := url.Parse(bridge.URL)
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/internal/model-checks/"+ownerProxyNode+"/prepare" {
			t.Error("probe skipped API permit preparation")
		}
		io.WriteString(w, `{"permit":"probe_fixture"}`)
	}))
	defer control.Close()
	g := newGateway(control.URL, "internal-platform", "gateway-a", nil)
	g.ownerBridge = newOwnerTunnelProxy(g.token, target)
	public := httptest.NewTLSServer(g)
	defer public.Close()
	g.ownerOrigin = public.URL
	// Match the production CA bundle: verified private roots, never InsecureSkipVerify.
	bundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: public.Certificate().Raw})
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(bundle) {
		t.Fatal("invalid private CA bundle")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}
	defer transport.CloseIdleConnections()
	g.owner.Transport = transport
	job := modelCheck{ID: ownerProxyNode, NodeID: ownerProxyNode, OwnerID: "owner-local", Epoch: 3, Model: "fixture", Kind: "health", Path: "/v1/chat/completions", ProbeRoute: public.URL + ownerTunnelPrefix + "/tunnels/" + ownerProxyNode + "/3/probe"}
	result := g.checkModel(context.Background(), job)
	if result.Status != "succeeded" || calls.Load() != 1 {
		t.Fatalf("same-server probe failed: %#v calls=%d", result, calls.Load())
	}
	g.owner.Transport = &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: x509.NewCertPool()}}
	defer g.owner.CloseIdleConnections()
	if result = g.checkModel(context.Background(), job); result.Status == "succeeded" || calls.Load() != 1 {
		t.Fatal("untrusted owner certificate reached the bridge")
	}
}

func TestOwnerTunnelProxyRejectsCrossServerProbeBeforePermitPreparation(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-local")
	var calls atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Error("cross-server probe reached permit preparation or another Server")
	}))
	defer remote.Close()
	g := newGateway(remote.URL, "internal-platform", "gateway-a", nil)
	for _, owner := range []string{"owner-other", ""} {
		job := modelCheck{ID: ownerProxyNode, NodeID: ownerProxyNode, OwnerID: owner, Epoch: 3, Model: "fixture", Kind: "health", Path: "/v1/chat/completions", ProbeRoute: remote.URL + ownerTunnelPrefix + "/tunnels/" + ownerProxyNode + "/3/probe"}
		result := g.checkModel(context.Background(), job)
		if result.Status != "failed" || !strings.Contains(result.Error, "bound to another Go Server") || calls.Load() != 0 {
			t.Fatalf("owner=%q result=%#v calls=%d", owner, result, calls.Load())
		}
	}
}

func TestOwnerTunnelProxyRoutesAndHeaders(t *testing.T) {
	var calls atomic.Int32
	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		for _, name := range []string{"Cookie", "X-Api-Key", "Proxy-Authorization", "X-Forwarded-For", "X-Newnoa-Account", "Idempotency-Key"} {
			if r.Header.Get(name) != "" {
				t.Errorf("unapproved header %s crossed the owner boundary", name)
			}
		}
		if r.Header.Get("X-Newnoa-Permit") != "permit_fixture" || r.Header.Get("X-Newnoa-Request") != ownerProxyNode || r.Header.Get("X-Newnoa-Gateway") != "gateway-a" || r.Header.Get("X-Newnoa-Epoch") != "3" {
			t.Error("owner permit headers changed")
		}
		w.Header().Set("X-Newnoa-Outcome", "complete")
		io.WriteString(w, "forwarded")
	}))
	defer bridge.Close()
	target, _ := url.Parse(bridge.URL)
	proxy := newOwnerTunnelProxy("internal-platform", target)
	base := ownerTunnelPrefix + "/tunnels/" + ownerProxyNode + "/3/"
	for _, suffix := range []string{"v1/chat/completions", "evaluation/v1/embeddings", "probe/v1/models"} {
		method := http.MethodPost
		if strings.HasSuffix(suffix, "/models") {
			method = http.MethodGet
		}
		r := httptest.NewRequest(method, base+suffix, nil)
		r.Header.Set("Authorization", "Bearer internal-platform")
		for name, value := range map[string]string{"X-Newnoa-Permit": "permit_fixture", "X-Newnoa-Request": ownerProxyNode, "X-Newnoa-Gateway": "gateway-a", "X-Newnoa-Epoch": "3", "Cookie": "session=private", "X-Api-Key": "private", "X-Forwarded-For": "forged", "X-Newnoa-Account": "forged", "Idempotency-Key": "must-not-replay"} {
			r.Header.Set(name, value)
		}
		w := httptest.NewRecorder()
		proxy.ServeHTTP(w, r)
		if w.Code != 200 || w.Body.String() != "forwarded" || w.Header().Get("X-Newnoa-Outcome") != "complete" {
			t.Fatalf("allowed route %s failed: %d", suffix, w.Code)
		}
	}
	forwarded := calls.Load()
	for _, path := range []string{ownerTunnelPrefix + "/plugin", ownerTunnelPrefix + "/health", ownerTunnelPrefix + "/ready", ownerTunnelPrefix + "/internal/runtime", base + "v1/chat/completions?query=1", base + "v1/chat/completions?", base + "../v1/chat/completions", base + "%76%31/chat/completions", strings.Replace(base, "/3/", "/9223372036854775808/", 1) + "v1/chat/completions", base + "evaluation/v1/messages", base + "v1/models"} {
		r := httptest.NewRequest(http.MethodPost, path, nil)
		r.Header.Set("Authorization", "Bearer internal-platform")
		w := httptest.NewRecorder()
		proxy.ServeHTTP(w, r)
		if w.Code < 400 {
			t.Errorf("forbidden route accepted: %s", path)
		}
	}
	for _, token := range []string{"", "Bearer public-api-key", "Bearer internal-platform, Bearer wrong"} {
		r := httptest.NewRequest(http.MethodPost, base+"v1/chat/completions", nil)
		r.Header.Set("Authorization", token)
		w := httptest.NewRecorder()
		proxy.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Fatal("untrusted owner call was not rejected")
		}
	}
	if calls.Load() != forwarded {
		t.Fatal("rejected request reached the local owner")
	}
}

func TestOwnerTunnelProxyDoesNotReplayProbe(t *testing.T) {
	var calls atomic.Int32
	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 2 {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			connection.Close()
			return
		}
		io.WriteString(w, "OK")
	}))
	defer bridge.Close()
	target, _ := url.Parse(bridge.URL)
	proxy := newOwnerTunnelProxy("internal-platform", target)
	for attempt, status := range []int{http.StatusOK, http.StatusBadGateway} {
		request := httptest.NewRequest(http.MethodGet, ownerTunnelPrefix+"/tunnels/"+ownerProxyNode+"/3/probe/v1/models", nil)
		request.Header.Set("Authorization", "Bearer internal-platform")
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != status {
			t.Fatalf("probe%d returned%d, want%d", attempt, response.Code, status)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("ambiguous one-use probe was replayed")
	}
}

func TestOwnerTunnelProxyFlushesAndCancels(t *testing.T) {
	cancelled := make(chan struct{})
	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "12")
		io.WriteString(w, "first\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(cancelled)
	}))
	defer bridge.Close()
	target, _ := url.Parse(bridge.URL)
	proxy := httptest.NewServer(newOwnerTunnelProxy("internal-platform", target))
	defer proxy.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, proxy.URL+ownerTunnelPrefix+"/tunnels/"+ownerProxyNode+"/3/v1/chat/completions", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer internal-platform")
	response, err := proxy.Client().Do(request)
	if err != nil {
		t.Fatal("stream headers were buffered", err)
	}
	defer response.Body.Close()
	first := make([]byte, 6)
	if _, err = io.ReadFull(response.Body, first); err != nil || string(first) != "first\n" {
		t.Fatal("owner response was not flushed", err)
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("caller cancellation did not reach the owner")
	}
}
