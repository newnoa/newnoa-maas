package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConnectionLoadTracksTCPStatesWithoutDoubleCounting(t *testing.T) {
	g := &gateway{}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	g.connectionState(a, http.StateNew)
	g.connectionState(a, http.StateActive)
	g.connectionState(a, http.StateActive)
	active, idle := g.connectionCounts()
	if active != 1 || idle != 0 {
		t.Fatalf("active=%d idle=%d", active, idle)
	}
	g.connectionState(a, http.StateIdle)
	active, idle = g.connectionCounts()
	if active != 0 || idle != 1 {
		t.Fatal("idle not tracked")
	}
	g.connectionState(a, http.StateClosed)
	active, idle = g.connectionCounts()
	if active != 0 || idle != 0 {
		t.Fatal("closed leaked")
	}
}
func TestHealthRequestsNeverEnterModelLoad(t *testing.T) {
	g := &gateway{}
	g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/not-a-model", nil))
	if g.modelActive.Load() != 0 {
		t.Fatal("health or unknown route counted")
	}
}
