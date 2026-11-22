package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestProxyLANDefaultsAndSavedPreferences(t *testing.T) {
	for _, mode := range []string{"desktop", "docker"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("NEWNOA_RUNTIME_MODE", mode)
			for _, setting := range []string{"", `{"ip":"192.168.1.8"}`, `{"ip":"","allowLAN":false}`} {
				t.Run(setting, func(t *testing.T) {
					home := t.TempDir()
					if setting != "" {
						file := filepath.Join(home, ".newnoa", "proxy-endpoint.json")
						if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(file, []byte(setting), 0600); err != nil {
							t.Fatal(err)
						}
					}
					c := newCore(home, "https://server.example.test")
					defer c.shutdown()
					want := mode == "docker" || setting != `{"ip":"","allowLAN":false}`
					if c.proxyLAN != want {
						t.Fatalf("LAN = %t, want %t", c.proxyLAN, want)
					}
					port := availableProxyPort(t)
					if _, err := c.handle("proxy.start", json.RawMessage(fmt.Sprintf(`{"port":%d}`, port))); err != nil {
						t.Fatal(err)
					}
					if c.proxyLAN != want || c.proxyListener.Addr().(*net.TCPAddr).IP.IsUnspecified() != want {
						t.Fatal("listener did not honor LAN preference")
					}
					if want {
						response, err := http.Get(c.proxyBaseURL() + "/v1/models")
						if err != nil {
							t.Fatal(err)
						}
						response.Body.Close()
						if response.StatusCode != http.StatusUnauthorized {
							t.Fatalf("missing key: %d", response.StatusCode)
						}
					}
				})
			}
		})
	}
}

func TestProxyLANExplicitDisablePersistsWithoutEndpoint(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "")
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	if _, err := c.handle("proxy.start", json.RawMessage(fmt.Sprintf(`{"port":%d,"allowLAN":false}`, availableProxyPort(t)))); err != nil {
		t.Fatal(err)
	}
	c.close()
	restarted := newCore(c.home, c.gateway)
	defer restarted.shutdown()
	if restarted.proxyLAN {
		t.Fatal("explicit LAN disable was lost on restart")
	}
	if err := restarted.startProxyAt("", availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	if restarted.proxyListener.Addr().(*net.TCPAddr).IP.IsUnspecified() {
		t.Fatal("disabled LAN bound all interfaces")
	}
}

func TestDockerProxyCannotRestartAsLoopbackOnly(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	if err := c.startProxyOn("", availableProxyPort(t), false); err != nil {
		t.Fatal(err)
	}
	if !c.proxyLAN || !c.proxyListener.Addr().(*net.TCPAddr).IP.IsUnspecified() {
		t.Fatal("Docker proxy became loopback-only")
	}
}

func TestDesktopReservedProxyFollowsActualLANScope(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "")
	c := newCore(t.TempDir(), "https://server.example.test")
	defer shutdownProxyTestCore(c)
	port := availableProxyPort(t)
	c.proxyLAN = false
	check := func(wantLAN bool) {
		t.Helper()
		for _, listener := range []net.Listener{c.proxyListener, c.proxyServerListener} {
			if listener == nil {
				t.Fatal("missing proxy listener")
			}
			ip := listener.Addr().(*net.TCPAddr).IP
			if wantLAN && !ip.IsUnspecified() || !wantLAN && !ip.IsLoopback() {
				t.Fatalf("actual proxy binding %s does not match allowLAN=%t", ip, wantLAN)
			}
		}
	}
	if err := c.bindStartupPort(port); err != nil {
		t.Fatal(err)
	}
	check(false)
	for _, allow := range []bool{true, false} {
		c.close()
		if err := c.startProxyOn("", port, allow); err != nil {
			t.Fatal(err)
		}
		check(allow)
	}
}
