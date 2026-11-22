package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCancelledPeerCapacityDoesNotStartAVisitorOrLeakASlot(t *testing.T) {
	for i := 0; i < cap(peerTunnelSlots); i++ {
		peerTunnelSlots <- struct{}{}
	}
	defer func() {
		for len(peerTunnelSlots) > 0 {
			<-peerTunnelSlots
		}
	}()
	provider := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer provider.Close()
	digest := sha256.Sum256(provider.Certificate().Raw)
	route := Route{URL: provider.URL, Pin: hex.EncodeToString(digest[:]), NodeID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Epoch: 1, Tunnel: &Tunnel{Secret: strings.Repeat("a", 64)}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if connection, err := Connect(ctx, route, "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", "custom_consumer_key_123", func(*Fallback) error { return nil }, nil); err == nil || connection != nil {
		t.Fatal("cancelled capacity wait succeeded")
	}
	if len(peerTunnelSlots) != cap(peerTunnelSlots) {
		t.Fatal("cancelled waiter changed occupied slots")
	}
}
