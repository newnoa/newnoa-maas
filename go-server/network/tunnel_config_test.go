package network

import (
	"github.com/pelletier/go-toml/v2"
	"strings"
	"testing"
)

func TestPeerTunnelConfigSeparatesConsumerCredentials(t *testing.T) {
	route := peerRoute{NodeID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Epoch: 4, Tunnel: &peerTunnel{
		ServerAddr: "coordinator.example", ServerPort: 7000, ServerName: "coordinator.example", TrustedCA: "-----BEGIN CERTIFICATE----- fixture",
		ProxyName: "node_fixture_peer", RelayName: "node_fixture_relay", Secret: strings.Repeat("a", 64),
	}}
	config, err := peerVisitorConfig(route, "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", "custom_consumer_key_123", 19090, "/private/ca.pem")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := toml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err = toml.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	metas := config["metadatas"].(map[string]any)
	if metas["nodeId"] != route.NodeID || metas["epoch"] != "4" || metas["consumerKey"] != "custom_consumer_key_123" || metas["credential"] != nil || metas["deploymentId"] != "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee" {
		t.Fatalf("wrong metadata %v", metas)
	}
	visitors := config["visitors"].([]any)
	if len(visitors) != 1 {
		t.Fatal("unexpected visitor")
	}
	relay := visitors[0].(map[string]any)
	if relay["type"] != "stcp" || relay["bindPort"] != 19090 || relay["bindAddr"] != "127.0.0.1" || relay["serverName"] != route.Tunnel.RelayName {
		t.Fatal("unsafe visitor config")
	}
	if config["proxies"] != nil {
		t.Fatal("consumer became provider")
	}
	for _, secret := range []string{"", strings.Repeat("g", 64), strings.Repeat("a", 63)} {
		route.Tunnel.Secret = secret
		if _, err := peerVisitorConfig(route, "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", "custom_consumer_key_123", 19090, "/private/ca.pem"); err == nil {
			t.Fatal("accepted invalid peer secret")
		}
	}
}
