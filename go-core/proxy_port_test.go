package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func availableProxyPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func TestCorePortReservationRejectsInvalidAndConflictingPorts(t *testing.T) {
	if _, err := reserveProxyListener(1023, true); err == nil {
		t.Fatal("accepted a privileged port")
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	port := occupied.Addr().(*net.TCPAddr).Port
	if _, err = reserveProxyListener(port, true); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("port %d is unavailable", port)) {
		t.Fatalf("expected clear port conflict, got %v", err)
	}
}
func startProxyRPC(t *testing.T, c *core, port int) error {
	t.Helper()
	_, err := c.handle("proxy.start", json.RawMessage(fmt.Sprintf(`{"secret":%q,"port":%d}`, "nn_"+strings.Repeat("x", 40), port)))
	return err
}
func TestProxySelectedPortAndFailedStartPreservePreview(t *testing.T) {
	var calls int
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("X-Api-Key") != "nn_"+strings.Repeat("x", 40) {
			t.Error("platform credential not substituted")
		}
		io.WriteString(w, `{"data":[]}`)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	defer c.close()
	pr, err := c.preview("codex", "deployment-id")
	if err != nil {
		t.Fatal(err)
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	if err = startProxyRPC(t, c, occupied.Addr().(*net.TCPAddr).Port); err == nil {
		t.Fatal("occupied port accepted")
	}
	if c.server != nil || c.pending == nil || c.pending.Digest != pr.Digest {
		t.Fatal("failed bind changed runtime/preview")
	}
	port := availableProxyPort(t)
	if err = startProxyRPC(t, c, port); err != nil {
		t.Fatal(err)
	}
	status, _ := c.handle("proxy.status", nil)
	address := status.(map[string]any)["address"].(string)
	if address != fmt.Sprintf("127.0.0.1:%d", port) {
		t.Fatalf("wrong effective address %s", address)
	}
	if _, err = c.handle("agent.apply", json.RawMessage(fmt.Sprintf(`{"digest":%q}`, pr.Digest))); err == nil {
		t.Fatal("old-port preview applied")
	}
	req, _ := http.NewRequest("GET", "http://"+address+"/v1/models", nil)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("configured key was not forwarded")
	}
	req.Header.Set("Authorization", "Bearer "+c.localKey)
	response, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 || calls != 2 {
		t.Fatal("custom port did not forward exactly once")
	}
	c.close()
	if err = c.startProxy("nn_" + strings.Repeat("x", 40)); err != nil {
		t.Fatal(err)
	}
	status, _ = c.handle("proxy.status", nil)
	if status.(map[string]any)["address"] != address {
		t.Fatal("omitted port did not reuse session setting")
	}
}
func TestProxyPortRejectsInvalidRPCAndKeepsDefault(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	for _, raw := range []string{`null`, `0`, `-1`, `80`, `65536`, `18080.5`, `"18080"`, `true`, `[]`, `{}`} {
		if err := startProxyRaw(c, fmt.Sprintf(`{"secret":%q,"port":%s}`, "nn_"+strings.Repeat("x", 40), raw)); err == nil {
			c.close()
			t.Fatalf("invalid port accepted: %s", raw)
		}
	}
	if err := startProxyRaw(c, `{"secret":"nn_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","host":"0.0.0.0"}`); err == nil {
		c.close()
		t.Fatal("host control accepted")
	}
	status, _ := c.handle("proxy.status", nil)
	if status.(map[string]any)["address"] != "0.0.0.0:18222" || c.server != nil {
		t.Fatal("invalid input changed effective state")
	}
}

func TestProxyEndpointsShowOnlyLANAddresses(t *testing.T) {
	c := &core{proxyPort: 18222, proxyLAN: true}
	got := c.proxyEndpoints()
	if len(got) > 1 {
		t.Fatalf("expected at most one LAN endpoint, got %v", got)
	}
	if len(got) == 1 && (strings.Contains(got[0], "127.0.0.1") || strings.Contains(got[0], "://172.17.") || strings.Contains(got[0], "://172.18.") || strings.Contains(got[0], "://172.19.") || strings.Contains(got[0], "://172.20.") || strings.Contains(got[0], "://172.22.") || strings.Contains(got[0], "://172.23.")) {
		t.Fatalf("non-LAN endpoint exposed: %s", got[0])
	}
	c.proxyLAN = false
	got = c.proxyEndpoints()
	if len(got) != 1 || got[0] != "http://127.0.0.1:18222/v1" {
		t.Fatalf("loopback endpoint = %v", got)
	}
}

func startProxyRaw(c *core, raw string) error {
	_, err := c.handle("proxy.start", json.RawMessage(raw))
	return err
}

func TestProxyStopRetainsReservedListenerUntilShutdown(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	defer shutdownProxyTestCore(c)
	port := availableProxyPort(t)
	if err := c.bindStartupPort(port); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		c.close()
		probe, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			probe.Close()
			t.Fatal("stopping inference released the Core port")
		}
		if err := startProxyRPC(t, c, port); err != nil {
			t.Fatalf("restart %d: %v", i, err)
		}
	}
	c.shutdown()
	probe, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("process shutdown retained the port: %v", err)
	}
	probe.Close()
}

func shutdownProxyTestCore(c *core) {
	c.shutdown()
	if c.history != nil && c.history.db != nil {
		c.history.db.Close()
	}
}
func TestProxyPortBindsAllAdaptersAndRequiresRestoreBeforeChange(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli", "opencode", "openclaw", "pi-agent", "hermes-agent"} {
		t.Run(agent, func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			defer c.close()
			port := availableProxyPort(t)
			if err := startProxyRPC(t, c, port); err != nil {
				t.Fatal(err)
			}
			pr, err := c.preview(agent, "deployment-id")
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(pr.Content, fmt.Sprintf("http://127.0.0.1:%d", port)) || strings.Contains(pr.Content, "127.0.0.1:18222") {
				t.Fatalf("adapter used wrong address: %s", pr.Content)
			}
			if err = c.apply(pr.Digest, pr.Content); err != nil {
				t.Fatal(err)
			}
			original, err := os.ReadFile(pr.Path)
			if err != nil {
				t.Fatal(err)
			}
			c.close()
			if err = startProxyRPC(t, c, availableProxyPort(t)); err == nil {
				t.Fatal("changed port with applied configuration")
			}
			raw, _ := os.ReadFile(pr.Path)
			if string(raw) != string(original) {
				t.Fatal("port change modified agent configuration")
			}
			if err = startProxyRPC(t, c, port); err != nil {
				t.Fatal("same-port restart blocked", err)
			}
			c.close()
			// A new core may retain journals when startup restoration could not finish.
			reopened := newCore(c.home, c.gateway)
			if err = reopened.startProxy("nn_" + strings.Repeat("x", 40)); err == nil {
				reopened.close()
				t.Fatal("restart forgot installed custom port")
			}
			if err = startProxyRPC(t, reopened, port); err != nil {
				t.Fatal("recorded-port restart blocked", err)
			}
			reopened.close()
			if err = c.restoreAgent(agent); err != nil {
				t.Fatal(err)
			}
			if err = startProxyRPC(t, c, availableProxyPort(t)); err != nil {
				t.Fatal("port change blocked after restore", err)
			}
			if _, err = os.Stat(filepath.Join(c.home, ".newnoa", "proxy-settings.json")); !os.IsNotExist(err) {
				t.Fatal("port secret/config was unexpectedly persisted")
			}
		})
	}
}

func TestLANOpenAIServiceAcceptsEachCallerKey(t *testing.T) {
	first := "nn_" + strings.Repeat("x", 40)
	second := "nn_" + strings.Repeat("y", 40)
	var seen []string
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("X-Api-Key"))
		io.WriteString(w, `{"data":[]}`)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	defer c.close()
	port := availableProxyPort(t)
	if err := c.startProxyOn(first, port, true); err != nil {
		t.Fatal(err)
	}
	if c.proxyListener.Addr().(*net.TCPAddr).IP.String() != "::" && c.proxyListener.Addr().(*net.TCPAddr).IP.String() != "0.0.0.0" {
		t.Fatal("not listening on LAN", c.proxyListener.Addr())
	}
	for _, key := range []string{first, second, c.localKey} {
		req, _ := http.NewRequest("GET", c.proxyBaseURL()+"/v1/models", nil)
		req.Header.Set("Authorization", "Bearer "+key)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatal(res.StatusCode)
		}
	}
	if strings.Join(seen, ",") != first+","+second+","+first {
		t.Fatal("caller scopes changed", seen)
	}
}

func TestProxyForwardsCredentialsAndServerRejectsUnknownKeys(t *testing.T) {
	invalid := newCore(t.TempDir(), "http://127.0.0.1:1")
	if _, err := invalid.handle("proxy.start", json.RawMessage(`{"secret":null}`)); err == nil {
		t.Fatal("null secret accepted")
	}
	invalid.close()
	first := "nn_" + strings.Repeat("a", 40)
	second := "nn_" + strings.Repeat("b", 40)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Api-Key")
		if key != first && key != second {
			w.WriteHeader(401)
			return
		}
		io.WriteString(w, `{"data":[{"id":"`+key[len(key)-1:]+`"}]}`)
	}))
	defer gateway.Close()
	for _, lan := range []bool{false, true} {
		c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
		port := availableProxyPort(t)
		_, err := c.handle("proxy.start", json.RawMessage(fmt.Sprintf(`{"port":%d,"allowLAN":%t}`, port, lan)))
		if err != nil {
			t.Fatal(err)
		}
		wantHost := "127.0.0.1"
		if lan {
			wantHost = "0.0.0.0"
		}
		status, _ := c.handle("proxy.status", nil)
		if status.(map[string]any)["address"] != fmt.Sprintf("%s:%d", wantHost, port) {
			t.Fatal(status)
		}
		for _, key := range []string{"", c.localKey, first, second} {
			req, _ := http.NewRequest("GET", c.proxyBaseURL()+"/v1/models", nil)
			if key != "" {
				req.Header.Set("Authorization", "Bearer "+key)
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(res.Body)
			res.Body.Close()
			want := 200
			if key == "" || key == c.localKey {
				want = 401
			}
			if res.StatusCode != want {
				t.Fatalf("status %d wanted %d", res.StatusCode, want)
			}
			if want == 200 && !strings.Contains(string(data), key[len(key)-1:]) {
				t.Fatal("caller scope lost")
			}
		}
		c.close()
	}
}
