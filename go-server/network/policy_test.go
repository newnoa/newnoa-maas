package network

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExplicitPoliciesNeverProbeLANOrFallBackToIt(t *testing.T) {
	calls := 0
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Write([]byte(`{"nodeId":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","epoch":1}`))
	}))
	defer peer.Close()
	pin := sha256.Sum256(peer.Certificate().Raw)
	route := Route{URL: peer.URL, Pin: hex.EncodeToString(pin[:]), NodeID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Epoch: 1}
	if c, err := ConnectEasyTier(context.Background(), route, route.NodeID, "nn_fixture", nil); err == nil {
		c.Close()
		t.Fatal("EasyTier used LAN")
	}
	if c, err := ConnectFRP(context.Background(), route, route.NodeID, "nn_fixture", nil); err == nil {
		c.Close()
		t.Fatal("FRP used LAN")
	}
	if calls != 0 {
		t.Fatalf("policy probed forbidden LAN %d times", calls)
	}
}
