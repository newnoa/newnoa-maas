package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestHandshakeCapabilitiesFollowRuntimeBoundary(t *testing.T) {
	for _, mode := range []string{"dev", "docker", "public"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("NEWNOA_RUNTIME_MODE", mode)
			c := &core{publicStateless: mode == "public"}
			result, err := c.handle("handshake", json.RawMessage(`{"nonce":"runtime-boundary"}`))
			if err != nil {
				t.Fatal(err)
			}
			capabilities := result.(map[string]any)["capabilities"].([]string)
			seen := map[string]bool{}
			for _, capability := range capabilities {
				seen[capability] = true
				if mode != "dev" && (strings.HasPrefix(capability, "agent.") || strings.HasPrefix(capability, "remote.")) {
					t.Errorf("%s advertised unavailable capability %s", mode, capability)
				}
			}
			for _, capability := range []string{"proxy.start", "proxy.stop", "proxy.status", "proxy.diagnose", "connector.ensure", "connector.start", "connector.stop", "connector.status", "deployment.context_probe", "model.list", "model.service.check"} {
				if !seen[capability] {
					t.Errorf("%s lost MaaS capability %s", mode, capability)
				}
			}
			if mode == "dev" {
				for _, capability := range []string{"agent.detect", "agent.version", "agent.preview", "agent.apply", "agent.restore"} {
					if !seen[capability] {
						t.Errorf("development lost capability %s", capability)
					}
				}
			}
		})
	}
}
