package main

import (
	"net"
	"net/http"
)

func (g *gateway) connectionState(c net.Conn, state http.ConnState) {
	g.connectionsMu.Lock()
	defer g.connectionsMu.Unlock()
	if g.connections == nil {
		g.connections = make(map[net.Conn]http.ConnState)
	}
	if state == http.StateClosed || state == http.StateHijacked {
		delete(g.connections, c)
	} else {
		g.connections[c] = state
	}
}
func (g *gateway) connectionCounts() (active, idle int) {
	g.connectionsMu.Lock()
	defer g.connectionsMu.Unlock()
	for _, state := range g.connections {
		if state == http.StateActive {
			active++
		} else if state == http.StateIdle {
			idle++
		}
	}
	return
}
