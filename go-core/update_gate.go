package main

import (
	"errors"
	"net/http"
	"sync"
)

// Preparing installation fences new work atomically; it never cancels active work.
type runtimeUpdateGate struct {
	mu      sync.Mutex
	active  int
	pending bool
}

func (g *runtimeUpdateGate) begin() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pending {
		return false
	}
	g.active++
	return true
}
func (g *runtimeUpdateGate) end() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.active--
	g.mu.Unlock()
}
func (g *runtimeUpdateGate) prepare() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active > 0 {
		return errors.New("Requests or local tasks are active; retry installation when idle")
	}
	g.pending = true
	return nil
}
func (g *runtimeUpdateGate) cancel() { g.mu.Lock(); g.pending = false; g.mu.Unlock() }
func (c *core) updateGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !c.updateGate.begin() {
			http.Error(w, "Client update installation is starting", http.StatusServiceUnavailable)
			return
		}
		defer c.updateGate.end()
		next.ServeHTTP(w, r)
	})
}
