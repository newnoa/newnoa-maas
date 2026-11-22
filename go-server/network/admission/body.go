// Package admission bounds request-body reception and retained proxy buffers.
package admission

import (
	"crypto/sha256"
	"net/netip"
	"sync"
)

type usage struct {
	requests int
	bytes    int64
}

type Budget struct {
	mu         sync.Mutex
	total      usage
	uploads    int
	executions int
	peers      map[netip.Addr]usage
	keys       map[[sha256.Size]byte]int
}

type Reservation struct {
	budget    *Budget
	ip        netip.Addr
	key       [sha256.Size]byte
	bytes     int64
	keyed     bool
	execution bool
	released  bool
}

// Reserve charges chunked uploads at their route maximum and rejects without queuing.
// The returned function releases the reservation when forwarding completes.
func (b *Budget) Reserve(remote string, bytes int64) func() {
	r := b.reserve(remote, [sha256.Size]byte{}, bytes)
	if r == nil {
		return nil
	}
	return r.Release
}

// ReserveFor also limits one caller key across source IPs without retaining the key itself.
func (b *Budget) ReserveFor(remote, callerKey string, bytes int64) *Reservation {
	return b.reserve(remote, sha256.Sum256([]byte(callerKey)), bytes)
}

func (b *Budget) reserve(remote string, callerKey [sha256.Size]byte, bytes int64) *Reservation {
	peer, _ := netip.ParseAddrPort(remote)
	ip := peer.Addr().Unmap()
	b.mu.Lock()
	defer b.mu.Unlock()
	used := b.peers[ip]
	keyed := callerKey != ([sha256.Size]byte{})
	keyRequests := b.keys[callerKey]
	if bytes < 0 || bytes > 32<<20 || b.uploads >= 64 || used.requests >= 16 ||
		keyed && keyRequests >= 16 ||
		b.total.bytes+bytes > 64<<20 || used.bytes+bytes > 32<<20 {
		return nil
	}
	if b.peers == nil {
		b.peers = make(map[netip.Addr]usage)
	}
	b.total.requests++
	b.uploads++
	b.total.bytes += bytes
	used.requests++
	used.bytes += bytes
	b.peers[ip] = used
	if keyed {
		if b.keys == nil {
			b.keys = make(map[[sha256.Size]byte]int)
		}
		b.keys[callerKey] = keyRequests + 1
	}
	return &Reservation{budget: b, ip: ip, key: callerKey, bytes: bytes, keyed: keyed}
}

// Promote releases the upload slot and acquires a separate in-flight execution slot.
func (r *Reservation) Promote() bool {
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.released {
		return false
	}
	if r.execution {
		return true
	}
	if b.executions >= 64 {
		return false
	}
	b.uploads--
	b.executions++
	r.execution = true
	return true
}

func (r *Reservation) Release() {
	b := r.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.released {
		return
	}
	r.released = true
	b.total.requests--
	b.total.bytes -= r.bytes
	if r.execution {
		b.executions--
	} else {
		b.uploads--
	}
	used := b.peers[r.ip]
	used.requests--
	used.bytes -= r.bytes
	if used.requests == 0 {
		delete(b.peers, r.ip)
	} else {
		b.peers[r.ip] = used
	}
	if r.keyed {
		keyRequests := b.keys[r.key] - 1
		if keyRequests == 0 {
			delete(b.keys, r.key)
		} else {
			b.keys[r.key] = keyRequests
		}
	}
}

// BodyLimit also prevents an inaccurate Content-Length from exceeding its reservation.
func BodyLimit(declared, maximum int64) int64 {
	if declared < 0 || declared > maximum {
		return maximum
	}
	return declared
}
