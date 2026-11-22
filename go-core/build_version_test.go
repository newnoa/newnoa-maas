package main

import (
	"encoding/json"
	"testing"
)

func TestHandshakeReportsInjectedBuildWithoutChangingProtocol(t *testing.T) {
	original := buildVersion
	buildVersion = "0.1.127"
	t.Cleanup(func() { buildVersion = original })
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	result, err := c.handle("handshake", json.RawMessage(`{"nonce":"version-fixture"}`))
	if err != nil {
		t.Fatal(err)
	}
	hello := result.(map[string]any)
	if hello["version"] != 1 || hello["nonce"] != "version-fixture" || hello["build"] != buildVersion {
		t.Fatalf("unexpected handshake: %#v", hello)
	}
}
