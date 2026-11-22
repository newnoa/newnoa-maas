package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"testing"
	"time"
)

func TestEncryptedSTCPConnectsPinnedProviderAndCleansItsVisitor(t *testing.T) {
	frpc, frps := os.Getenv("NEWNOA_TEST_FRPC"), os.Getenv("NEWNOA_TEST_FRPS")
	if frpc == "" || frps == "" {
		t.Skip("set pinned NEWNOA_TEST_FRPC and NEWNOA_TEST_FRPS")
	}
	directory := t.TempDir()
	t.Setenv("TMPDIR", directory)
	t.Setenv("NEWNOA_STUN_SERVER", "127.0.0.1:1")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(frpc)
	if err != nil {
		t.Fatal(err)
	}
	installed := filepath.Join(filepath.Dir(executable), "frpc")
	manifest := filepath.Join(filepath.Dir(executable), "frpc-manifest.json")
	if _, err := os.Stat(installed); !os.IsNotExist(err) {
		t.Fatal("test executable directory already owns frpc")
	}
	if err := os.WriteFile(installed, binary, 0700); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(installed)
	metadata, _ := json.Marshal(map[string]string{"sha256": hash(binary)})
	if err := os.WriteFile(manifest, metadata, 0600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(manifest)
	cert, key := filepath.Join(directory, "ca.pem"), filepath.Join(directory, "key.pem")
	if output, err := exec.Command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", cert, "-days", "1", "-subj", "/CN=peer-test.local", "-addext", "subjectAltName=DNS:peer-test.local").CombinedOutput(); err != nil {
		t.Fatalf("fixture certificate: %v %s", err, output)
	}
	ca, err := os.ReadFile(cert)
	if err != nil {
		t.Fatal(err)
	}
	port, err := loopbackPort()
	if err != nil {
		t.Fatal(err)
	}
	serverConfig := filepath.Join(directory, "frps.toml")
	raw := fmt.Sprintf("bindAddr=\"127.0.0.1\"\nbindPort=%d\ntransport.tls.force=true\ntransport.tls.certFile=%q\ntransport.tls.keyFile=%q\nlog.level=\"error\"\n", port, cert, key)
	if err := os.WriteFile(serverConfig, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	server := exec.Command(frps, "-c", serverConfig)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { server.Process.Kill(); server.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		connection, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err == nil {
			connection.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("FRPS did not listen")
		}
		time.Sleep(20 * time.Millisecond)
	}
	const node = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const deployment = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/peer/ready" {
			io.WriteString(w, `{"nodeId":"`+node+`","epoch":2}`)
			return
		}
		io.WriteString(w, "encrypted-fixture")
	}))
	defer provider.Close()
	_, providerPort, _ := net.SplitHostPort(provider.Listener.Addr().String())
	secret := strings.Repeat("a", 64)
	providerConfig := filepath.Join(directory, "provider.toml")
	raw = fmt.Sprintf("serverAddr=\"127.0.0.1\"\nserverPort=%d\ntransport.tls.enable=true\ntransport.tls.trustedCaFile=%q\ntransport.tls.serverName=\"peer-test.local\"\nlog.level=\"info\"\n[[proxies]]\nname=\"node_fixture_relay\"\ntype=\"stcp\"\nsecretKey=%q\nlocalIP=\"127.0.0.1\"\nlocalPort=%s\ntransport.useEncryption=true\n", port, cert, secret, providerPort)
	if err := os.WriteFile(providerConfig, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	logs := &peerLogTail{secrets: []string{secret}}
	publisher := exec.Command(frpc, "-c", providerConfig)
	publisher.Stdout, publisher.Stderr = logs, logs
	if err := publisher.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { publisher.Process.Kill(); publisher.Wait() }()
	deadline = time.Now().Add(8 * time.Second)
	for !strings.Contains(logs.snapshot(), "start proxy success") {
		if time.Now().After(deadline) {
			t.Fatalf("STCP publisher did not register: %s", logs.snapshot())
		}
		time.Sleep(20 * time.Millisecond)
	}
	digest := sha256.Sum256(provider.Certificate().Raw)
	route := Route{Available: true, Private: true, NodeID: node, Epoch: 2, URL: "https://127.0.0.1:1", Pin: hex.EncodeToString(digest[:]), Tunnel: &Tunnel{ServerAddr: "127.0.0.1", ServerPort: port, ServerName: "peer-test.local", TrustedCA: string(ca), ProxyName: "node_fixture_peer", RelayName: "node_fixture_relay", Secret: secret}}
	states := []string{}
	connection, err := Connect(context.Background(), route, deployment, "custom_consumer_key_123", func(event *Fallback) error { states = append(states, event.Status); return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if connection.Path() != "peer-tunnel" {
		t.Fatalf("wrong actual transport %s", connection.Path())
	}
	request, _ := http.NewRequest("POST", connection.URL()+"/v1/chat/completions", strings.NewReader("fixture"))
	response, err := connection.Client().Do(request)
	if err != nil {
		connection.Close()
		t.Fatal(err)
	}
	content, err := io.ReadAll(response.Body)
	response.Body.Close()
	connection.Close()
	if err != nil || string(content) != "encrypted-fixture" {
		t.Fatalf("bad encrypted response %s %v", content, err)
	}
	if strings.Join(states, ",") != "attempting,connected" {
		t.Fatalf("fallback evidence transition %v", states)
	}
	if len(peerTunnelSlots) != 0 {
		t.Fatal("visitor capacity leaked after close")
	}
}
