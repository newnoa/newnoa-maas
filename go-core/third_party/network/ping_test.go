package network

import (
	"context"
	"testing"
)

func TestEndpointPingRejectsUntrustedURLParts(t *testing.T) {
	for _, endpoint := range []string{"", "127.0.0.1", "file:///tmp/model", "http://user:secret@127.0.0.1", "http://127.0.0.1?target=8.8.8.8", "http://127.0.0.1/#target"} {
		if got := PingEndpoint(context.Background(), endpoint); got.Status != "unavailable" || got.Milliseconds != 0 {
			t.Fatalf("invalid endpoint accepted: %q %+v", endpoint, got)
		}
	}
}

func TestEndpointPingUsesActualLoopbackICMP(t *testing.T) {
	got := PingEndpoint(context.Background(), "http://127.0.0.1:1/v1")
	if got.Status != "succeeded" || got.Milliseconds <= 0 {
		t.Fatalf("loopback ICMP failed: %+v", got)
	}
}
