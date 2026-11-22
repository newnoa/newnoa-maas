package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	rpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func gatewayTLSFixture(t *testing.T, handler http.Handler, wrongName bool) (*httptest.Server, string) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "gateway"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	if wrongName {
		leaf.IPAddresses = []net.IP{net.ParseIP("192.0.2.14")}
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, cert, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{leafDER}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func gatewayTrustFixture(t *testing.T, id, address, ca string) gatewayEndpoint {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"id": id, "address": address, "trustedCa": ca})
	if err != nil {
		t.Fatal(err)
	}
	var endpoint gatewayEndpoint
	if err = json.Unmarshal(raw, &endpoint); err != nil {
		t.Fatal(err)
	}
	return endpoint
}

func TestGatewayPrivateCAProbeRequiresExactCertificateAndIP(t *testing.T) {
	server, ca := gatewayTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ready") }), false)
	other, otherCA := gatewayTLSFixture(t, http.NotFoundHandler(), false)
	_ = other
	wrongName, wrongNameCA := gatewayTLSFixture(t, http.NotFoundHandler(), true)
	for _, tc := range []struct {
		name, address, ca string
		success           bool
	}{
		{"configured", server.URL, ca, true}, {"unconfigured", server.URL, "", false}, {"other-ca", server.URL, otherCA, false}, {"wrong-ip", wrongName.URL, wrongNameCA, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := probeGatewayReady(context.Background(), gatewayTrustFixture(t, "node", tc.address, tc.ca))
			if (err == nil) != tc.success {
				t.Fatalf("probe success=%t, error=%v", tc.success, err)
			}
		})
	}
}

func TestGatewayPrivateCAMetadataRejectsMalformedOrPlaintextTrust(t *testing.T) {
	server, ca := gatewayTLSFixture(t, http.NotFoundHandler(), false)
	for _, tc := range []struct{ name, address, ca string }{
		{"plain-private", "127.0.0.1:6666", ca}, {"plain-http", "http://127.0.0.1:6666", ca}, {"garbage", server.URL, "not a CA"}, {"trailing-data", server.URL, ca + "garbage"}, {"private-key", server.URL, "-----BEGIN PRIVATE KEY-----\nZm9v\n-----END PRIVATE KEY-----"}, {"oversized", server.URL, strings.Repeat(ca, 20)},
		{"non-certificate", server.URL, "-----BEGIN CERTIFICATE-----\nZHVtbXk=\n-----END CERTIFICATE-----"},
		{"certificate-headers", server.URL, strings.Replace(ca, "-----BEGIN CERTIFICATE-----", "-----BEGIN CERTIFICATE-----\nProc-Type: 4,ENCRYPTED", 1)},
		{"leaf-is-not-ca", server.URL, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if validGatewayEndpoint(gatewayTrustFixture(t, "node", tc.address, tc.ca)) {
				t.Fatal("invalid private CA metadata accepted")
			}
		})
	}
	if !validGatewayEndpoint(gatewayTrustFixture(t, "node", "https://example.test:6666", "")) {
		t.Fatal("public CA endpoint compatibility lost")
	}
	for _, endpoint := range []gatewayEndpoint{{ID: "node", Address: "https://127.0.0.1:6666", Transport: "unknown"}, {ID: "node", Address: "127.0.0.1:6666", Transport: "grpc"}} {
		if validGatewayEndpoint(endpoint) {
			t.Fatal("unsupported or plaintext transport accepted")
		}
	}
}

func TestGatewayTransportRejectsUnusableTrustBeforeSendingCredentials(t *testing.T) {
	for _, endpoint := range []gatewayEndpoint{
		{ID: "node", Address: "127.0.0.1:6666", TrustedCA: "not-pem"},
		{ID: "node", Address: "https://127.0.0.1:6666", TrustedCA: "not-pem"},
		{ID: "node", Address: "https://127.0.0.1:6666", TrustedCA: strings.Repeat("x", 16385)},
	} {
		if _, err := probeGatewayReady(context.Background(), endpoint); err == nil {
			t.Fatal("unusable trust accepted")
		}
	}
	if _, err := gatewayCertPool(""); err == nil {
		t.Fatal("missing CA accepted")
	}
	for _, endpoint := range []gatewayEndpoint{{Address: "http://127.0.0.1:6666"}, {Address: "https://127.0.0.1:6666", TrustedCA: "invalid"}} {
		request, _ := http.NewRequestWithContext(context.Background(), "GET", gatewayBase(endpoint)+"/ready", nil)
		if _, err := grpcGatewayRoundTrip(request, endpoint); err == nil {
			t.Fatal("invalid gRPC trust configuration accepted")
		}
	}
	server, _ := gatewayTLSFixture(t, http.NotFoundHandler(), false)
	request, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/ready", nil)
	if response, err := grpcGatewayRoundTrip(request, gatewayEndpoint{Address: server.URL, Transport: "grpc"}); err == nil {
		response.Body.Close()
		t.Fatal("untrusted gRPC server accepted by default trust")
	}
}

func TestGatewayPrivateCARoutesStreamingWithoutCrossNodeTrust(t *testing.T) {
	testGatewayConcurrentTrust(t, false)
}

func testGatewayConcurrentTrust(t *testing.T, useGRPC bool) {
	const modelA = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const modelB = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
	var callsA, callsB, privateCalls atomic.Int32
	makeHandler := func(label string, counter *atomic.Int32) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/ready" {
				io.WriteString(w, "ready")
				return
			}
			if r.Header.Get("Authorization") != "Bearer ses_login" || r.Header.Get("X-Api-Key") != "nn_fixture_key_123" {
				t.Error("paired credentials changed")
			}
			if r.URL.Path == "/private/target" {
				privateCalls.Add(1)
				io.WriteString(w, `{"peer":{"available":false}}`)
				return
			}
			counter.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: "+label+"\n\n")
			w.(http.Flusher).Flush()
			io.WriteString(w, "data: [DONE]\n\n")
		})
	}
	handlerA, handlerB := makeHandler("A", &callsA), makeHandler("B", &callsB)
	if useGRPC {
		serverA, serverB := rpc.NewServer(handlerA), rpc.NewServer(handlerB)
		t.Cleanup(serverA.Stop)
		t.Cleanup(serverB.Stop)
		handlerA, handlerB = serverA, serverB
	}
	serverA, caA := gatewayTLSFixture(t, handlerA, false)
	serverB, caB := gatewayTLSFixture(t, handlerB, false)
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/gateways" {
			t.Error("unexpected control path", r.URL.Path)
		}
		endpoint, ca := serverA.URL, caA
		if r.URL.Query().Get("model") == modelB {
			endpoint, ca = serverB.URL, caB
		}
		row := map[string]any{"id": r.URL.Query().Get("model"), "address": endpoint, "trustedCa": ca}
		if useGRPC {
			row["transport"] = "grpc"
		}
		json.NewEncoder(w).Encode([]map[string]any{row})
	}))
	defer control.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected fallback"); w.WriteHeader(503) }))
	defer fallback.Close()
	handler, err := privateInferenceWithSelector(control.URL, fallback.URL, newGatewaySelector(control.URL))
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			model, want := modelA, "A"
			if i%2 == 1 {
				model, want = modelB, "B"
			}
			req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"`+model+`","stream":true}`))
			req.Header.Set("Authorization", "Bearer ses_login")
			req.Header.Set("X-Api-Key", "nn_fixture_key_123")
			req.Header.Set("Content-Type", "application/json")
			out := httptest.NewRecorder()
			handler.ServeHTTP(out, req)
			if out.Code != 200 || out.Body.String() != "data: "+want+"\n\ndata: [DONE]\n\n" {
				t.Errorf("model=%s status=%d body=%s", model, out.Code, out.Body.String())
			}
		}(i)
	}
	group.Wait()
	if callsA.Load() != 4 || callsB.Load() != 4 || privateCalls.Load() != 0 {
		t.Fatalf("calls A=%d B=%d private=%d", callsA.Load(), callsB.Load(), privateCalls.Load())
	}
}

func TestGatewayTrustNeverFollowsRedirectOrAppliesToAnotherOrigin(t *testing.T) {
	var leaked atomic.Int32
	victim, ca := gatewayTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1); io.WriteString(w, `{}`) }), false)
	redirect, _ := gatewayTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, victim.URL+"/secret", http.StatusTemporaryRedirect)
	}), false)
	// A CA trusted for victim must not be applied even when the destination shares that CA.
	endpoint := gatewayTrustFixture(t, "node", redirect.URL, ca)
	request := httptest.NewRequest("POST", victim.URL+"/secret", nil)
	request = request.WithContext(context.WithValue(request.Context(), selectedGatewayContext{}, endpoint))
	if _, err := (gatewayTransport{}).RoundTrip(request); err == nil {
		t.Fatal("trust applied to another origin")
	}
	if leaked.Load() != 0 {
		t.Fatal("credential destination received mismatched-origin request")
	}
	redirect, redirectCA := gatewayTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, victim.URL+"/secret", http.StatusTemporaryRedirect)
	}), false)
	endpoint = gatewayTrustFixture(t, "node", redirect.URL, redirectCA)
	request = httptest.NewRequest("POST", redirect.URL+"/redirect", nil)
	request = request.WithContext(context.WithValue(request.Context(), selectedGatewayContext{}, endpoint))
	request.RequestURI = ""
	client := &http.Client{Transport: gatewayTransport{}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("unexpected redirect status %d", response.StatusCode)
	}
	if leaked.Load() != 0 {
		t.Fatal("credentials leaked through redirected gateway request")
	}
}

func TestGatewayPrivateCAHTTPCloseCancelsUpstream(t *testing.T) {
	stopped := make(chan struct{})
	server, ca := gatewayTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(stopped)
	}), false)
	endpoint := gatewayTrustFixture(t, "node", server.URL, ca)
	request, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/v1/models", nil)
	request = request.WithContext(context.WithValue(request.Context(), selectedGatewayContext{}, endpoint))
	response, err := (gatewayTransport{}).RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, 5)
	if _, err = io.ReadFull(response.Body, first); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("HTTP private-CA body close did not cancel upstream")
	}
}
