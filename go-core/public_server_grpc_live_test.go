package main

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	peernet "github.com/newnoa/newnoa-maas/go-server/network"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// This boundary test runs the real sibling Go Server, not a handler that mirrors its authentication.
func TestPublicKeyThroughRealServerTLSGRPC(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		preferred bool
	}{
		{"api-generated", "sk-" + strings.Repeat("A", 43), false},
		{"api-generated-private-preferred", "sk-" + strings.Repeat("B", 43), true},
		{"legacy", "nn_public_fixture_key_1234567890", false},
		{"custom", "custom_key_1234567890", false},
	} {
		t.Run(tc.name, func(t *testing.T) { testPublicKeyThroughRealServerTLSGRPC(t, tc.key, tc.preferred) })
	}
}

func testPublicKeyThroughRealServerTLSGRPC(t *testing.T, key string, preferred bool) {
	if _, err := os.Stat(filepath.Join("..", "newnoa-maas-server", "gateway", "go.mod")); os.IsNotExist(err) {
		t.Skip("real Go Server boundary requires sibling internal Server checkout; standalone Core unit/build tests remain independent")
	}
	const internal = "fixture-shared-internal-token-with-32-chars"
	const id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	var upstreamCalls, admissions, finishes atomic.Int32
	var meterSequence atomic.Int64
	var deny atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer upstream-only" || r.Header.Get("X-Newnoa-Public-Caller-Proof") != "" || r.Header.Get("X-Newnoa-Public-Caller-IP") != "" {
			t.Error("public proof or wrong credential reached upstream")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`)
	}))
	defer upstream.Close()
	certServer, ca := gatewayTLSFixture(t, http.NotFoundHandler(), false)
	pair := certServer.TLS.Certificates[0]
	certServer.Close()
	dir := t.TempDir()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pair.Certificate[0]})
	priv, err := x509.MarshalPKCS8PrivateKey(pair.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "cert.pem"), cert, 0600)
	os.WriteFile(filepath.Join(dir, "key.pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: priv}), 0600)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bind := listener.Addr().String()
	listener.Close()
	origin := "https://" + bind
	if preferred {
		config, _ := json.Marshal(map[string]string{"id": "public-proof-node", "address": origin, "internalAddress": bind, "trustedCa": ca, "transport": "grpc"})
		t.Setenv("NEWNOA_PUBLIC_GO_SERVER", string(config))
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var b map[string]any
		if r.Method == "POST" {
			json.NewDecoder(r.Body).Decode(&b)
		}
		switch r.URL.Path {
		case "/ready":
			io.WriteString(w, `{"ready":true}`)
		case "/api/v1/gateways":
			json.NewEncoder(w).Encode([]map[string]any{{"id": "public-proof-node", "address": origin, "trustedCa": ca, "transport": "grpc"}})
		case "/api/v1/internal/public/authorize":
			if b["secret"] != key {
				t.Error("Core public API caller changed")
			}
			io.WriteString(w, `{"hosted":false,"deploymentId":"`+id+`"}`)
		case "/api/v1/internal/authorize":
			if deny.Load() {
				w.WriteHeader(403)
				io.WriteString(w, `{"error":{"message":"API key revoked"}}`)
				return
			}
			if r.Header.Get("Authorization") != "Bearer "+internal || b["secret"] != key || b["session"] != nil {
				t.Error("Server did not authorize original key independently")
			}
			io.WriteString(w, `{"model":"fixture","deploymentId":"`+id+`"}`)
		case "/api/v1/internal/dispatches":
			admissions.Add(1)
			if b["secret"] != key || b["session"] != nil || b["clientIp"] != "203.0.113.9" || b["idempotencyKey"] == nil {
				t.Errorf("public request binding lost: secretOK=%t sessionAbsent=%t caller=%v idemPresent=%t", b["secret"] == key, b["session"] == nil, b["clientIp"], b["idempotencyKey"] != nil)
			}
			json.NewEncoder(w).Encode(map[string]any{"requestId": id, "permit": "fixture-permit", "endpoint": upstream.URL, "apiKey": "upstream-only", "model": "fixture"})
		case "/api/v1/internal/requests/" + id + "/meter":
			sequence, ok := b["sequence"].(float64)
			snapshot, hasSnapshot := b["snapshot"].(map[string]any)
			if r.Header.Get("Authorization") != "Bearer "+internal || b["gatewayId"] != "public-proof-node" || b["permit"] != "fixture-permit" || !ok || sequence != float64(meterSequence.Load()+1) || !hasSnapshot {
				t.Error("real Server checkpoint lost authenticated request identity/sequence")
				w.WriteHeader(403)
				return
			}
			if sequence == 1 && (upstreamCalls.Load() != 0 || snapshot["responseStarted"] != false) {
				t.Error("provider invoked before durable start checkpoint")
			}
			if sequence > 1 {
				reported, ok := snapshot["reported"].(map[string]any)
				if !ok || reported["inputTokens"] != float64(2) || reported["outputTokens"] != float64(1) || reported["cachedInputTokens"] != nil || reported["cacheWriteTokens"] != nil {
					t.Error("checkpoint lost actual usage or fabricated unknown cache")
				}
			}
			meterSequence.Store(int64(sequence))
			json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": id, "sequence": int64(sequence)})
		case "/api/v1/internal/requests/" + id + "/finalize":
			finishes.Add(1)
			if b["status"] != "succeeded" || meterSequence.Load() < 2 || b["meterSequence"] != float64(meterSequence.Load()) || b["executionClosed"] != true {
				t.Error("durable usage lifecycle closure lost")
			}
			// Actual 2/1 usage is verified on the persisted checkpoint above;
			// closure references API evidence rather than resending raw counters.
			for _, field := range []string{"inputTokens", "outputTokens", "cachedInputTokens", "cacheWriteTokens", "usageSource"} {
				if _, present := b[field]; present {
					t.Errorf("lifecycle-only closure sent %s", field)
				}
			}
			io.WriteString(w, `{}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	defer api.Close()
	binary := filepath.Join(dir, "gateway")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = filepath.Join("..", "newnoa-maas-server", "gateway")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build real test Go Server: %v %s", err, out)
	}
	log, err := os.Create(filepath.Join(dir, "gateway.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	child := exec.Command(binary)
	child.Env = append(os.Environ(), "NEWNOA_INTERNAL_TOKEN="+internal, "NEWNOA_CONTROL_URL="+api.URL, "NEWNOA_GATEWAY_ID=public-proof-node", "NEWNOA_GATEWAY_BIND="+bind, "NEWNOA_GATEWAY_ADVERTISE_ADDR="+origin, "NEWNOA_GATEWAY_TLS_CERT_FILE="+filepath.Join(dir, "cert.pem"), "NEWNOA_GATEWAY_TLS_KEY_FILE="+filepath.Join(dir, "key.pem"), "NEWNOA_EGRESS_ALLOW_HOSTS=127.0.0.1", "NEWNOA_FRP_OWNER_CONFIG=[]", "NEWNOA_OTEL_ENABLED=false")
	child.Stdout, child.Stderr = log, log
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		child.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- child.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			child.Process.Kill()
			<-done
		}
	}()
	endpoint := gatewayEndpoint{ID: "public-proof-node", Address: origin, Transport: "grpc", TrustedCA: ca}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := probeGatewayReady(t.Context(), endpoint); err == nil {
			break
		}
		if time.Now().After(deadline) {
			raw, _ := os.ReadFile(filepath.Join(dir, "gateway.log"))
			t.Fatalf("Go Server not ready: %s", raw)
		}
		time.Sleep(50 * time.Millisecond)
	}
	originalBody := `{"model":"fixture","messages":[{"role":"user","content":"fixture"}],"max_tokens":8}`
	for _, kind := range []string{"unsigned", "forged", "expired", "other-node", "body-changed", "duplicated-proof", "duplicated-caller"} {
		raw := originalBody
		direct := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(raw))
		direct.Header.Set("Authorization", "Bearer "+key)
		direct.Header.Set("Content-Type", "application/json")
		direct.Header.Set("Idempotency-Key", "negative-"+kind)
		if kind != "unsigned" {
			target := endpoint.ID
			at := time.Now()
			if kind == "other-node" {
				target = "another-node"
			}
			if kind == "expired" {
				at = at.Add(-time.Minute)
			}
			proof, err := peernet.SignPublicCaller(internal, target, direct, hash([]byte(raw)), "203.0.113.9", at)
			if err != nil {
				t.Fatal(err)
			}
			direct.Header.Set(peernet.PublicCallerProofHeader, proof)
			direct.Header.Set(peernet.PublicCallerIPHeader, "203.0.113.9")
			switch kind {
			case "forged":
				direct.Header.Set(peernet.PublicCallerProofHeader, proof+"x")
			case "body-changed":
				raw += " "
				direct.Body = io.NopCloser(strings.NewReader(raw))
				direct.ContentLength = int64(len(raw))
			case "duplicated-proof":
				direct.Header.Add(peernet.PublicCallerProofHeader, proof)
			case "duplicated-caller":
				direct.Header.Add(peernet.PublicCallerIPHeader, "203.0.113.8")
			}
		}
		response, err := grpcGatewayRoundTrip(direct, endpoint)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != 401 {
			t.Fatalf("untrusted real RPC case %s got%d", kind, response.StatusCode)
		}
	}
	if admissions.Load() != 0 || upstreamCalls.Load() != 0 {
		t.Fatal("untrusted RPC reached execution")
	}
	c := &core{control: api.URL, gateway: origin, gatewaySelector: newGatewaySelector(api.URL)}
	handler, err := c.publicProxyHandler(publicTestSecret, internal)
	if err != nil {
		t.Fatal(err)
	}
	defer c.publicSVGCancel()
	request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"fixture"}],"max_tokens":8}`))
	request.Header.Set("Authorization", "Bearer "+key)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
	request.Header.Set("X-Newnoa-Caller-IP", "203.0.113.9")
	deny.Store(true)
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, request.Clone(request.Context()))
	if denied.Code != 403 || upstreamCalls.Load() != 0 || admissions.Load() != 0 {
		t.Fatalf("signed request bypassed API revocation status=%d", denied.Code)
	}
	deny.Store(false)
	request.Body = io.NopCloser(strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"fixture"}],"max_tokens":8}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 200 || upstreamCalls.Load() != 1 || admissions.Load() != 1 || finishes.Load() != 1 || meterSequence.Load() < 2 {
		t.Fatalf("real Core→TLSgRPC→Server status=%d upstream=%d admissions=%d finalized=%d body=%s", response.Code, upstreamCalls.Load(), admissions.Load(), finishes.Load(), response.Body.String())
	}
}
