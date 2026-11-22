package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestProxyStartAppliesEndpointAndRestoresIt(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	command := json.RawMessage(fmt.Sprintf(`{"port":%d,"allowLAN":false,"endpointIP":"127.0.0.1"}`, availableProxyPort(t)))
	if _, err := c.handle("proxy.start", command); err != nil {
		t.Fatal(err)
	}
	if c.endpointIP != "127.0.0.1" {
		t.Fatal("endpoint not applied")
	}
	restarted := newCore(c.home, c.gateway)
	defer restarted.shutdown()
	if restarted.endpointIP != "127.0.0.1" {
		t.Fatal("endpoint not restored")
	}
}

func TestProxyStartRejectsInvalidEndpointBeforeChangingListener(t *testing.T) {
	for _, value := range []string{`null`, `42`, `"8.8.8.8"`, `"localhost"`, `"127.0.0.1:18222"`} {
		c := newCore(t.TempDir(), "https://server.example.test")
		c.endpointIP = "192.168.1.9"
		_, err := c.handle("proxy.start", json.RawMessage(`{"endpointIP":`+value+`}`))
		if err == nil || c.server != nil || c.endpointIP != "192.168.1.9" {
			t.Fatalf("invalid endpoint changed runtime: %s", value)
		}
		c.shutdown()
	}
}

func TestDockerProxyStartRejectsLoopbackEndpoint(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	if _, _, err := c.proxyStartParams(json.RawMessage(`{"endpointIP":"127.0.0.1","allowLAN":true}`)); err == nil {
		t.Fatal("Docker advertised container loopback")
	}
}

func TestProxyStartRejectsUnreachableEndpointAndRestoresPreviousSetting(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	c.endpointIP = "127.0.0.1"
	raw := json.RawMessage(fmt.Sprintf(`{"port":%d,"allowLAN":true,"endpointIP":"10.255.255.254"}`, availableProxyPort(t)))
	if _, err := c.handle("proxy.start", raw); err == nil {
		t.Fatal("unreachable endpoint persisted")
	}
	if c.server != nil || c.endpointIP != "127.0.0.1" || c.proxyPort != 0 || !c.proxyLAN {
		t.Fatal("failed start left runtime changes")
	}
}
