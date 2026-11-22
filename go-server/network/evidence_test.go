package network

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestFallbackEvidenceRedactsSplitLogsAndBoundsOutput(t *testing.T) {
	tail := &peerLogTail{secrets: []string{"sensitive-custom-value"}}
	tail.Write([]byte(strings.Repeat("x", 9000) + "\nconsumerKey=nn_"))
	tail.Write([]byte("private secretKey=" + strings.Repeat("a", 64) + " sensitive-custom-value"))
	got := tail.snapshot()
	if strings.Contains(got, "nn_private") || strings.Contains(got, strings.Repeat("a", 64)) || strings.Contains(got, "sensitive-custom-value") || len([]rune(got)) > 2048 {
		t.Fatal("unsafe log snapshot")
	}
	if !strings.Contains(got, "[redacted]") {
		t.Fatal("redaction marker missing")
	}
}

func TestEasyTierFailureEvidenceDistinguishesSetupAndPeerState(t *testing.T) {
	m := newPeerOverlayManager()
	defer m.reset()
	_, err := m.prepare(peerRoute{}, "deployment", "key")
	if err == nil || !strings.Contains(err.Error(), "advertise") {
		t.Fatal("missing provider capability reason")
	}
	t.Setenv("NEWNOA_EASYTIER", "off")
	_, err = m.prepare(peerRoute{Overlay: true, Tunnel: &peerTunnel{}}, "deployment", "key")
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatal("missing local disable reason")
	}
	summary := overlayPeerSummary([]byte(`[{"route":{"hostname":"newnoa-provider","cost":2,"peer_id":2,"next_hop_peer_id":1},"peer":{"conns":[{"tunnel":{"tunnel_type":"ws"}}]}}]`))
	var result map[string]any
	if json.Unmarshal([]byte(summary), &result) != nil || result["udpDirect"] != false || result["providerPresent"] != true || result["nextHopMatches"] != false {
		t.Fatal(summary)
	}
	s := &overlaySession{state: "preparing"}
	s.observe("status_probe_failed", "context deadline exceeded")
	if got := s.diagnostic(); len(got) != 1 || got[0].Code != "status_probe_failed" || got[0].At.After(time.Now()) {
		t.Fatal(got)
	}
}
