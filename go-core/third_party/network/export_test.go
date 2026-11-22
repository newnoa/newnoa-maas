package network

import (
	"context"
	"strings"
	"testing"
)

func TestNetworkContractDoesNotRequireAControlPlaneOrLocalState(t *testing.T) {
	manager := NewManager()
	defer manager.Reset()
	if manager.Status()["state"] != "idle" {
		t.Fatal("new manager has unexpected state")
	}
	if _, err := Transport(Route{URL: "http://unsafe.example", Pin: strings.Repeat("a", 64)}); err == nil {
		t.Fatal("accepted an unpinned HTTP peer")
	}
	if _, err := StartProvider(context.Background(), "invalid", 1, strings.Repeat("a", 64), 0); err == nil {
		t.Fatal("accepted invalid provider identity")
	}
}
