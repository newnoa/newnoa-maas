package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Opt-in: real TLS FRPC/FRPS, isolated control simulator; no model calls.
func TestConnectorRealFRPDiagnostics(t *testing.T) { testConnectorRealFRP(t, false) }
func TestConnectorRealFRPDrain(t *testing.T)       { testConnectorRealFRP(t, true) }
func testConnectorRealFRP(t *testing.T, drain bool) {
	frpc, frps := os.Getenv("NEWNOA_TEST_FRPC"), os.Getenv("NEWNOA_TEST_FRPS")
	if frpc == "" || frps == "" {
		t.Skip("set pinned FRPC and FRPS paths")
	}
	dir := t.TempDir()
	cert := filepath.Join(dir, "cert.pem")
	key := filepath.Join(dir, "key.pem")
	if out, err := exec.Command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", cert, "-days", "1", "-subj", "/CN=diagnostics.local", "-addext", "subjectAltName=DNS:diagnostics.local").CombinedOutput(); err != nil {
		t.Fatalf("certificate: %v %s", err, out)
	}
	ca, err := os.ReadFile(cert)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bind := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	remote := 0
	for p := 24000; p < 25000; p++ {
		l, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
		if e == nil {
			remote = p
			l.Close()
			break
		}
	}
	if remote == 0 {
		t.Fatal("no isolated remote port")
	}
	config := filepath.Join(dir, "frps.toml")
	raw := fmt.Sprintf("bindAddr = \"127.0.0.1\"\nbindPort = %d\nproxyBindAddr = \"127.0.0.1\"\ntransport.tls.force = true\ntransport.tls.certFile = %q\ntransport.tls.keyFile = %q\nlog.level = \"error\"\n", bind, cert, key)
	if err := os.WriteFile(config, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	process := exec.Command(frps, "-c", config)
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { process.Process.Kill(); process.Wait() }()
	modelStarted, modelRelease, drainSeen := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var stoppingTest atomic.Bool
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(modelStarted)
		<-modelRelease
		io.WriteString(w, `{"ok":true}`)
	}))
	defer model.Close()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(modelRelease) })
	var drainOnce sync.Once
	var accept atomic.Bool
	var epoch atomic.Int64
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/dispatches/"):
			json.NewEncoder(w).Encode(map[string]any{"endpoint": model.URL, "model": "fixture"})
		case strings.HasSuffix(r.URL.Path, "/drain"):
			if stoppingTest.Load() {
				drainOnce.Do(func() { close(drainSeen) })
			}
			io.WriteString(w, `{}`)

		case strings.HasSuffix(r.URL.Path, "/leases"):
			lease := nodeLease{Purpose: "serving", Epoch: epoch.Add(1), Credential: "frp_fixture"}
			lease.FRP.ServerAddr = "127.0.0.1"
			lease.FRP.ServerPort = bind
			lease.FRP.ServerName = "diagnostics.local"
			lease.FRP.TrustedCA = string(ca)
			lease.FRP.ProxyName = fmt.Sprintf("newnoa_aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee_%d", lease.Epoch)
			lease.FRP.RemotePort = remote
			json.NewEncoder(w).Encode(lease)
		case strings.HasSuffix(r.URL.Path, "/ready") && !accept.Load():
			w.WriteHeader(503)
		default:
			json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
	defer api.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &connector{ctx: ctx, cancel: cancel, done: make(chan struct{}), nodeID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", credential: "node_fixture", purpose: "serving", control: api.URL, binary: frpc, address: "127.0.0.1:1", client: api.Client(), upstream: api.Client(), server: &http.Server{}}
	local, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n.address = local.Addr().String()
	n.server = &http.Server{Handler: n}
	go n.server.Serve(local)
	go n.run()
	defer func() { cancel(); <-n.done }()
	wait := func(label string, predicate func(map[string]any) bool) {
		t.Helper()
		deadline := time.Now().Add(40 * time.Second)
		for time.Now().Before(deadline) {
			if predicate(n.status()) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s: %v", label, n.status())
	}
	wait("real tunnel ready rejected", func(s map[string]any) bool { return s["failureCode"] == "control_unavailable" && s["stage"] == "ready" })
	accept.Store(true)
	wait("recovered", func(s map[string]any) bool {
		return s["state"] == "online" && s["failureCode"] == "" && s["failureAt"] == ""
	})
	if drain {
		request, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d/v1/chat/completions", remote), strings.NewReader(`{"model":"fixture","messages":[]}`))
		request.Header.Set("X-Newnoa-Request", n.nodeID)
		request.Header.Set("X-Newnoa-Permit", "permit_fixture")
		request.Header.Set("X-Newnoa-Gateway", "gateway-a")
		request.Header.Set("X-Newnoa-Epoch", fmt.Sprint(n.status()["epoch"]))
		result := make(chan error, 1)
		go func() {
			response, err := (&http.Client{Timeout: 8 * time.Second}).Do(request)
			if err == nil {
				defer response.Body.Close()
				raw, readErr := io.ReadAll(response.Body)
				err = readErr
				if response.StatusCode != 200 || string(raw) != `{"ok":true}` {
					err = fmt.Errorf("in-flight response lost: %d %s", response.StatusCode, raw)
				}
			}
			result <- err
		}()
		select {
		case <-modelStarted:
		case <-time.After(3 * time.Second):
			t.Fatal("FRP request did not reach fixture model")
		}
		stoppingTest.Store(true)
		n.stop()
		select {
		case <-drainSeen:
		case <-time.After(2 * time.Second):
			t.Fatal("platform drain missing")
		}
		select {
		case <-n.done:
			t.Fatal("tunnel stopped before response finished")
		default:
		}
		releaseOnce.Do(func() { close(modelRelease) })
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		select {
		case <-n.done:
		case <-time.After(3 * time.Second):
			t.Fatal("connector failed to stop")
		}
		return
	}
	process.Process.Kill()
	wait("FRPS stopped", func(s map[string]any) bool {
		return s["failureCode"] == "tunnel_unavailable" || s["failureCode"] == "process_failed"
	})
}
