package main

import (
	"net"
	"strings"
	"testing"
)

func TestReserveProxyListenerRejectsPortOwnedByAnotherCore(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	port := occupied.Addr().(*net.TCPAddr).Port
	listener, err := reserveProxyListener(port, true)
	if err == nil {
		listener.Close()
		t.Fatal("second Core reserved a port already owned by another process")
	}
	if !strings.Contains(err.Error(), "local proxy port") {
		t.Fatalf("unexpected reservation error: %v", err)
	}
}
