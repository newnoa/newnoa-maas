package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
)

type membershipWireResponse struct {
	RequestID        string  `json:"requestId"`
	Path             string  `json:"path"`
	Bytes            int     `json:"bytes"`
	Seconds          float64 `json:"elapsedSeconds"`
	Status           int     `json:"status"`
	FinishedAtMillis int64   `json:"finishedAtMillis"`
	Error            string  `json:"error,omitempty"`
}

func TestMembershipWireGatewayOwnerEnvironmentIsIsolated(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", "https://stale.example")
	t.Setenv("NEWNOA_FRP_OWNERS_JSON", `[{"id":"stale"}]`)
	t.Setenv("NEWNOA_FRP_OWNER_CONFIG", `[{"id":"other"}]`)
	env := membershipWireOwnerEnvironment("127.0.0.1:19000")
	values := map[string]string{}
	for _, assignment := range env {
		key, value, _ := strings.Cut(assignment, "=")
		values[key] = value
	}
	if values["NEWNOA_FRP_OWNER_ORIGIN"] != "http://127.0.0.1:19000" || values["NEWNOA_FRP_OWNERS_JSON"] != "" || values["NEWNOA_FRP_OWNER_CONFIG"] != "" {
		t.Fatal("live gateway inherited another owner's trust")
	}
}

func membershipWireOwnerEnvironment(address string) []string {
	var env []string
	for _, assignment := range os.Environ() {
		key, _, _ := strings.Cut(assignment, "=")
		if key != "NEWNOA_FRP_OWNER_ORIGIN" && key != "NEWNOA_FRP_OWNERS_JSON" && key != "NEWNOA_FRP_OWNER_CONFIG" {
			env = append(env, assignment)
		}
	}
	return append(env, "NEWNOA_FRP_OWNER_ORIGIN=http://"+address, "NEWNOA_FRP_OWNERS_JSON=", "NEWNOA_FRP_OWNER_CONFIG=")
}

// Opt-in; the root wrapper owns disposable Java/databases, FRPS and HTTP model fixtures.
func TestMembershipFRPJavaLive(t *testing.T) {
	api := os.Getenv("NEWNOA_WIRE_API")
	if api == "" {
		t.Skip("run workspace tests/membership-frp-wire-live.mjs")
	}
	directory := t.TempDir()
	var lease nodeLease
	if err := json.Unmarshal([]byte(os.Getenv("NEWNOA_WIRE_LEASE")), &lease); err != nil {
		t.Fatal(err)
	}
	models, keys := strings.Split(os.Getenv("NEWNOA_WIRE_MODELS"), ","), strings.Split(os.Getenv("NEWNOA_WIRE_KEYS"), ",")
	sessions := strings.Split(os.Getenv("NEWNOA_WIRE_SESSIONS"), ",")
	if len(models) != 2 || len(keys) != 2 || len(sessions) != 2 {
		t.Fatal("two fixture models and caller keys are required")
	}
	const gatewayID, ownerID = "wire-bound", "peer-fixture"
	var consumes, bandwidthCalls, bandwidthBytes, firstBandwidthAt atomic.Int64
	var bindingsMu sync.Mutex
	var bindings []map[string]any
	target, _ := url.Parse(api)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(response *http.Response) error {
		path := response.Request.URL.Path
		if response.StatusCode != 200 || !(strings.HasSuffix(path, "/internal/authorize") || strings.HasSuffix(path, "/internal/dispatches")) {
			return nil
		}
		raw, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			return err
		}
		response.Body = io.NopCloser(bytes.NewReader(raw))
		decoded := raw
		if response.Header.Get("Content-Encoding") == "gzip" {
			reader, err := gzip.NewReader(bytes.NewReader(raw))
			if err != nil {
				return err
			}
			decoded, err = io.ReadAll(reader)
			reader.Close()
			if err != nil {
				return err
			}
		}
		var binding map[string]any
		if err = json.Unmarshal(decoded, &binding); err != nil {
			return err
		}
		if binding["nodeId"] != os.Getenv("NEWNOA_WIRE_NODE") || binding["epoch"] != float64(lease.Epoch) || binding["ownerId"] != ownerID {
			t.Errorf("API changed publisher binding: %v", binding)
		}
		bindingsMu.Lock()
		bindings = append(bindings, map[string]any{"stage": path, "nodeId": binding["nodeId"], "epoch": binding["epoch"], "ownerId": binding["ownerId"]})
		bindingsMu.Unlock()
		return nil
	}
	spy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if bytes.Contains(raw, []byte("private-wire-prompt")) || bytes.Contains(raw, []byte(`"messages"`)) {
			t.Error("model payload traversed Java control plane")
		}
		if strings.HasSuffix(r.URL.Path, "/internal/authorize") || strings.HasSuffix(r.URL.Path, "/internal/dispatches") {
			var identity map[string]any
			if json.Unmarshal(raw, &identity) != nil || identity["gatewayId"] != gatewayID {
				t.Error("authorization/reservation used another Go Server")
			}
		}
		if strings.HasSuffix(r.URL.Path, "/consume") {
			consumes.Add(1)
		}
		if strings.HasSuffix(r.URL.Path, "/bandwidth") {
			firstBandwidthAt.CompareAndSwap(0, time.Now().UnixNano())
			var grant struct{ Bytes int64 }
			if json.Unmarshal(raw, &grant) != nil || grant.Bytes < 1 || grant.Bytes > 32768 {
				t.Error("invalid bandwidth chunk")
			}
			bandwidthCalls.Add(1)
			bandwidthBytes.Add(grant.Bytes)
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(spy.Close)
	n := &connector{ctx: context.Background(), control: spy.URL, nodeID: os.Getenv("NEWNOA_WIRE_NODE"),
		credential: os.Getenv("NEWNOA_WIRE_CREDENTIAL"), epoch: lease.Epoch,
		state: "online", purpose: "serving", client: spy.Client(), upstream: http.DefaultClient}
	stop, pin, err := n.startPeer("127.0.0.1:0", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	_, peerPort, _ := net.SplitHostPort(n.peerAddress)
	port, _ := strconv.Atoi(peerPort)
	address := os.Getenv("NEWNOA_WIRE_ENTRY_ADDR")
	if address == "" {
		t.Fatal("bound Go Server entry is required")
	}
	gateway := exec.Command(os.Getenv("NEWNOA_WIRE_GATEWAY"))
	gateway.Env = append(membershipWireOwnerEnvironment(address), "NEWNOA_CONTROL_URL="+spy.URL, "NEWNOA_GATEWAY_ID="+gatewayID,
		"NEWNOA_GATEWAY_BIND="+address, "NEWNOA_GATEWAY_ADVERTISE_ADDR="+address, "NEWNOA_FRP_OWNER_ID="+ownerID,
		"NEWNOA_EGRESS_ALLOW_HOSTS=127.0.0.1", "NEWNOA_EXPECTED_GATEWAY_SHA256=", "NEWNOA_STUN_SERVER=127.0.0.1:1", "TMPDIR="+directory)
	gateway.Stdout, gateway.Stderr = io.Discard, io.Discard
	membershipWireProcess(t, gateway)
	base := "http://" + address
	membershipWireWait(t, "actual gateway readiness", func() bool {
		response, err := http.Get(base + "/ready")
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == http.StatusOK
	})
	membershipWireWait(t, "bound gateway registration", func() bool {
		r, _ := http.NewRequest("GET", api+"/api/v1/gateways?model="+models[0], nil)
		r.Header.Set("Authorization", "Bearer "+sessions[0])
		r.Header.Set("X-Api-Key", keys[0])
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var endpoints []gatewayEndpoint
		return response.StatusCode == 200 && json.NewDecoder(response.Body).Decode(&endpoints) == nil && len(endpoints) == 1 && endpoints[0].ID == gatewayID && endpoints[0].LatencyTolerancePercent == 10
	})
	for _, path := range []string{"/private/target", "/private/dispatches"} {
		r, _ := http.NewRequest("POST", base+path, strings.NewReader(`{"model":"`+models[0]+`"}`))
		r.Header.Set("Authorization", "Bearer "+sessions[0])
		r.Header.Set("X-Api-Key", keys[0])
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusGone {
			t.Fatalf("retired direct endpoint %s status=%d", path, response.StatusCode)
		}
	}
	var callers []*httptest.Server
	for range 2 {
		handler, err := privateInferenceWithSelector(spy.URL, base, newGatewaySelector(spy.URL))
		if err != nil {
			t.Fatal(err)
		}
		caller := httptest.NewServer(handler)
		t.Cleanup(caller.Close)
		callers = append(callers, caller)
	}
	config := map[string]any{
		"serverAddr": lease.FRP.ServerAddr, "serverPort": lease.FRP.ServerPort, "loginFailExit": true,
		"transport": map[string]any{"protocol": lease.FRP.TransportProtocol, "tls": map[string]any{"enable": true, "trustedCaFile": os.Getenv("NEWNOA_WIRE_CA"), "serverName": lease.FRP.ServerName}},
		"log":       map[string]any{"level": "info"},
		"proxies": []any{map[string]any{"name": lease.FRP.ProxyName + "_relay", "type": "stcp", "secretKey": n.tunnelSecret,
			"localIP": "127.0.0.1", "localPort": port, "transport": map[string]any{"useEncryption": true}}},
	}
	configPath := filepath.Join(directory, "provider.toml")
	raw, err := toml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(directory, "publisher.log")
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	t.Cleanup(func() {
		if t.Failed() {
			raw, _ := os.ReadFile(logPath)
			t.Log("provider FRPC log:", string(raw))
		}
	})
	publisher := exec.Command(os.Getenv("NEWNOA_WIRE_FRPC"), "-c", configPath)
	publisher.Stdout, publisher.Stderr = log, log
	membershipWireProcess(t, publisher)
	membershipWireWait(t, "encrypted STCP publisher registration", func() bool {
		raw, _ := os.ReadFile(logPath)
		return strings.Contains(string(raw), "start proxy success")
	})
	publish := func(address string) {
		t.Helper()
		if err := n.call(context.Background(), "/connector/"+n.nodeID+"/peer", map[string]any{
			"epoch": lease.Epoch, "url": address, "certificateSha256": pin, "tunnelSecret": n.tunnelSecret, "overlay": false}, nil); err != nil {
			t.Fatal(err)
		}
	}
	// A fresh unused port prevents a direct route while retaining the real pinned TLS provider behind STCP.
	unreachable := "https://" + membershipWireAddress(t)
	publish(unreachable)
	infer := func(index int, identity string, caller ...int) membershipWireResponse {
		identityIndex := index
		if len(caller) > 0 {
			identityIndex = caller[0]
		}
		started := time.Now()
		body, _ := json.Marshal(map[string]any{"model": models[index], "messages": []any{map[string]string{"role": "user", "content": "private-wire-prompt"}}, "max_tokens": 16, "stream": false})
		request, _ := http.NewRequest("POST", callers[index].URL+"/v1/chat/completions", bytes.NewReader(body))
		request.Header.Set("Authorization", "Bearer "+sessions[identityIndex])
		request.Header.Set("X-Api-Key", keys[identityIndex])
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", identity)
		request.Header.Set("X-Newnoa-Path", "direct") // A caller cannot choose the admission transport.
		response, err := (&http.Client{Timeout: 45 * time.Second}).Do(request)
		if err != nil {
			return membershipWireResponse{Error: err.Error()}
		}
		defer response.Body.Close()
		content, err := io.ReadAll(response.Body)
		result := membershipWireResponse{RequestID: response.Header.Get("X-Newnoa-Request"), Path: response.Header.Get("X-Newnoa-Path"), Bytes: len(content), Seconds: time.Since(started).Seconds(), Status: response.StatusCode}
		result.FinishedAtMillis = time.Now().UnixMilli()
		if err != nil {
			result.Error = err.Error()
		}
		if !bytes.Contains(content, []byte(`"finish_reason":"stop"`)) || !bytes.Contains(content, []byte(strings.Repeat("x", 500000))) {
			result.Error = fmt.Sprintf("incomplete fixture response (%d bytes)", len(content))
			if response.StatusCode != 200 && len(content) < 1024 {
				result.Error += ": " + string(content)
			}
		}
		return result
	}
	started := time.Now()
	results := make(chan membershipWireResponse, 2)
	for i := range 2 {
		go func() { results <- infer(i, "wire-frp-"+strconv.Itoa(i)) }()
	}
	transfers := []membershipWireResponse{<-results, <-results}
	elapsed := time.Since(started).Seconds()
	pacedElapsed := time.Since(time.Unix(0, firstBandwidthAt.Load())).Seconds()
	bytesDelivered := 0
	for _, result := range transfers {
		if result.Error != "" || result.Status != 200 || result.Path != "peer-tunnel" || !nodePattern.MatchString(result.RequestID) {
			t.Fatalf("actual FRP transfer failed: %+v", result)
		}
		bytesDelivered += result.Bytes
	}
	if firstBandwidthAt.Load() == 0 || pacedElapsed < float64(bytesDelivered)/125000*0.95 || bandwidthCalls.Load() < 2 || bandwidthBytes.Load() < int64(bytesDelivered) {
		t.Fatalf("shared application payload rate exceeded or bandwidth admission missing: bytes=%d pacedSeconds=%.3f calls=%d grantBytes=%d", bytesDelivered, pacedElapsed, bandwidthCalls.Load(), bandwidthBytes.Load())
	}
	frpCalls, frpBytes := bandwidthCalls.Load(), bandwidthBytes.Load()
	publish("https://" + n.peerAddress)
	reachable := infer(0, "wire-lan-cannot-bypass-server")
	if reachable.Error != "" || reachable.Status != 200 || reachable.Path != "peer-tunnel" || reachable.Bytes != transfers[0].Bytes || bandwidthCalls.Load() <= frpCalls {
		t.Fatalf("reachable LAN bypassed Server FRP admission: %+v, extra bandwidth calls=%d", reachable, bandwidthCalls.Load()-frpCalls)
	}
	if reachable.Seconds < float64(reachable.Bytes)/125000*0.95 {
		t.Fatalf("reachable LAN bypassed Server FRP pacing: %.3fs", reachable.Seconds)
	}
	self := infer(1, "wire-publisher-self", 1)
	if self.Error != "" || self.Status != 200 || self.Path != "peer-tunnel" {
		t.Fatalf("publisher self request bypassed FRP or failed: %+v", self)
	}
	if consumes.Load() != 4 {
		t.Fatalf("provider consume calls=%d, want 4", consumes.Load())
	}

	if err := n.call(context.Background(), "/connector/"+n.nodeID+"/drain", map[string]any{"epoch": lease.Epoch}, nil); err != nil {
		t.Fatal(err)
	}
	offline := infer(0, "wire-offline-no-fallback")
	if offline.Status < 400 || consumes.Load() != 4 {
		t.Fatal("offline node was executed or replayed", offline)
	}
	bindingsMu.Lock()
	if len(bindings) != 8 {
		t.Errorf("authorization/reservation snapshots=%d; want 8", len(bindings))
	}
	bindingsMu.Unlock()
	evidence, _ := json.Marshal(map[string]any{"transfers": transfers, "reachable": reachable, "publisherSelf": self, "offlineStatus": offline.Status, "gatewayId": gatewayID, "ownerId": ownerID, "nodeId": n.nodeID, "epoch": lease.Epoch, "bindings": bindings, "actualEasyTierUdp": false, "responseBytes": bytesDelivered,
		"elapsedSeconds": elapsed, "pacedElapsedSeconds": pacedElapsed, "observedAggregateBytesPerSecond": float64(bytesDelivered) / elapsed,
		"pacedAggregateBytesPerSecond": float64(bytesDelivered) / pacedElapsed,
		"bandwidthAdmissionCalls":      frpCalls, "bandwidthAdmissionBytes": frpBytes, "consumeCalls": consumes.Load(),
		"actualGateways": 1, "actualCallers": 2, "retiredDirectEndpointsRejected": true, "actualEncryptedSTCP": true, "pinnedTLSProvider": true, "callerCoreHTTP": true,
		"reachableLANStillUsesFRP": true, "sharedInferenceAndFRPEntry": os.Getenv("NEWNOA_WIRE_ENTRY_ADDR"), "frpTransportProtocol": lease.FRP.TransportProtocol, "measurementPoint": "client-delivered model response payload after actual FRP traversal; not raw TCP/TLS wire bytes"})
	if err := os.WriteFile(os.Getenv("NEWNOA_WIRE_RESULT"), evidence, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("real FRPC/FRPS STCP: %d response bytes / %.3fs = %.0f B/s aggregate, reachable LAN still FRP %.3fs; rate measures client-delivered payload", bytesDelivered, elapsed, float64(bytesDelivered)/elapsed, reachable.Seconds)
}

func membershipWireAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	return address
}

func membershipWireWait(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out: " + description)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func membershipWireProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { cmd.Wait(); close(exited) }()
	t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		select {
		case <-exited:
		case <-time.After(3 * time.Second):
			cmd.Process.Kill()
			<-exited
		}
	})
}
