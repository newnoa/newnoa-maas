package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestEndpointIPRejectsNonLANAndAcceptsPrivateIPv4(t *testing.T) {
	for _, value := range []string{"127.0.0.2", "8.8.8.8", "0.0.0.0", "localhost", "192.168.1.1:80", "192.168.1.1/path", "169.254.1.1", "::1"} {
		if validEndpointIP(value) {
			t.Fatalf("accepted %q", value)
		}
	}
	for _, value := range []string{"127.0.0.1", "192.168.1.8", "10.219.77.14", "172.16.0.2"} {
		if !validEndpointIP(value) {
			t.Fatalf("rejected %q", value)
		}
	}
}

func TestStartupProxyRequiresAPIKeyBeforeLogin(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	if err := c.bindStartupPort(availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(c.proxyBaseURL() + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized || response.Header.Get("X-Newnoa-Proxy") != hash([]byte(c.localKey)) {
		t.Fatal("startup LAN proxy did not require an API key before login")
	}
}

func TestEndpointProbeIdentifiesThisProxyAndPersistsAddress(t *testing.T) {
	addrs, _ := net.InterfaceAddrs()
	ip := ""
	for _, addr := range addrs {
		candidate, _, _ := net.ParseCIDR(addr.String())
		if candidate != nil && candidate.To4() != nil && candidate.IsPrivate() && !candidate.IsLoopback() {
			ip = candidate.String()
			break
		}
	}
	if ip == "" {
		t.Skip("no LAN IPv4")
	}
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	if err := c.startProxyOn("", availableProxyPort(t), true); err != nil {
		t.Fatal(err)
	}
	if _, err := c.configureProxyEndpoint(ip, false); err != nil {
		t.Fatal(err)
	}
	if c.endpointIP != ip {
		t.Fatal("not saved")
	}
	restarted := newCore(c.home, c.gateway)
	defer restarted.shutdown()
	if restarted.endpointIP != ip {
		t.Fatal("endpoint not restored")
	}
	if _, err := c.configureProxyEndpoint("10.255.255.254", true); err == nil {
		t.Fatal("unreachable endpoint accepted")
	}
	if c.endpointIP != ip {
		t.Fatal("failed test changed endpoint")
	}
}

func TestAccountCleanupRestartsGenericProxyAfterClearingOldRuntime(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	if err := c.bindStartupPort(availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	account := &browserAccount{core: c}
	if err := account.stopRuntime(); err != nil {
		t.Fatal(err)
	}
	if c.server == nil {
		t.Fatal("account cleanup left the startup proxy stopped")
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(c.proxyBaseURL() + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatal("proxy accepted a request without an API key after logout")
	}
}

func TestDisablingLANRemainsDisabledAfterRestartWithSavedEndpoint(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	c.endpointIP = "192.168.1.8"
	c.proxyLAN = true
	if err := c.saveProxyEndpoint(c.endpointIP); err != nil {
		t.Fatal(err)
	}
	var command = json.RawMessage(fmt.Sprintf(`{"port":%d,"allowLAN":false}`, availableProxyPort(t)))
	if _, err := c.handle("proxy.start", command); err != nil {
		t.Fatal(err)
	}
	restarted := newCore(c.home, c.gateway)
	defer restarted.shutdown()
	if restarted.proxyLAN {
		t.Fatal("explicitly disabled LAN access was restored as enabled")
	}
	if restarted.endpointIP != "192.168.1.8" {
		t.Fatal("endpoint preference lost")
	}
}

func TestEndpointRejectsNullAndMissingSettings(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	for _, raw := range []string{`{}`, `{"ip":null}`, `{"ip":"","testOnly":null}`, `{"ip":"","extra":true}`} {
		if _, err := c.handle("proxy.endpoint", json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}
