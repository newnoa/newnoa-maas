package main

import (
	"errors"
	"net/http"
	"net/netip"
	"sync"
	"time"
)

const (
	frpPendingHandshakes     = 32
	frpStartsPerSecond       = 64
	frpConnectionsPerPeer    = 128
	frpConnectionsPerGateway = 1024
)

var errFRPBusy = errors.New("FRP handshake capacity reached")

// Counts local sockets; the owner plugin and API remain responsible for lease authorization.
type frpAdmission struct {
	mu     sync.Mutex
	active int
	peers  map[netip.Addr]int
	starts [frpStartsPerSecond]time.Time
	next   int
}

func (g *frpAdmission) accept(peer netip.Addr, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active >= frpConnectionsPerGateway || g.peers[peer] >= frpConnectionsPerPeer || now.Sub(g.starts[g.next]) < time.Second {
		return false
	}
	if g.peers == nil {
		g.peers = make(map[netip.Addr]int)
	}
	g.active++
	g.peers[peer]++
	g.starts[g.next] = now
	g.next = (g.next + 1) % len(g.starts)
	return true
}
func (g *frpAdmission) release(peer netip.Addr) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if g.peers[peer] == 1 {
		delete(g.peers, peer)
	} else {
		g.peers[peer]--
	}
}

// RoundTrip ends at the upgrade response; established tunnels no longer occupy a handshake slot.
type frpHandshakeTransport struct {
	upstream http.RoundTripper
	pending  chan struct{}
}

func (t *frpHandshakeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	select {
	case t.pending <- struct{}{}:
		defer func() { <-t.pending }()
		return t.upstream.RoundTrip(r)
	default:
		return nil, errFRPBusy
	}
}
func frpBusy(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "1")
	fail(w, http.StatusTooManyRequests, "FRP gateway is busy")
}
