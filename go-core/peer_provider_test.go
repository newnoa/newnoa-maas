package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestProviderPeerPinAndReadiness(t *testing.T) {
	port, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := port.Addr().String()
	port.Close()
	n := &connector{ctx: context.Background(), nodeID: "fixture", epoch: 2, state: "online", purpose: "serving"}
	stop, pin, err := n.startPeer(address, "https://"+address)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	transport, err := peerTransport(peerRoute{URL: "https://" + address, Pin: pin})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	response, err := client.Get("https://" + address + "/peer/ready")
	if err != nil {
		t.Fatal(err)
	}
	var ready peerRoute
	json.NewDecoder(response.Body).Decode(&ready)
	response.Body.Close()
	if response.StatusCode != 200 || ready.Epoch != 2 || ready.NodeID != "fixture" {
		t.Fatalf("readiness %+v", ready)
	}
	n.mu.Lock()
	n.stopping = true
	n.mu.Unlock()
	response, err = client.Get("https://" + address + "/peer/ready")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatal("draining peer advertised ready")
	}
}

func TestProviderPeerAutoAddressAndEphemeralSecret(t *testing.T) {
	n := &connector{ctx: context.Background(), nodeID: "fixture", epoch: 3, state: "online", purpose: "serving"}
	stop, pin, err := n.startPeer("127.0.0.1:0", "")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if n.peerURL == "" || n.peerAddress == "" || len(n.tunnelSecret) != 64 || n.peerPin != pin {
		t.Fatal("automatic peer endpoint or secret missing")
	}
	firstSecret := n.tunnelSecret
	transport, err := peerTransport(peerRoute{URL: n.peerURL, Pin: pin})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport}).Get(n.peerURL + "/peer/ready")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	other := &connector{ctx: context.Background()}
	stopOther, _, err := other.startPeer("127.0.0.1:0", "")
	if err != nil {
		t.Fatal(err)
	}
	defer stopOther()
	if other.tunnelSecret == firstSecret || other.peerPin == pin {
		t.Fatal("peer process identity reused")
	}
}

func TestProviderPeerDrainBeforeTunnelStop(t *testing.T) {
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	n := &connector{server: &http.Server{}, stopPeer: func() { close(started); <-release }}
	go func() { n.finishStop(); close(done) }()
	select {
	case <-started:
	case <-done:
		t.Fatal("connector drain returned without draining peer listener")
	case <-time.After(time.Second):
		t.Fatal("peer drain not started")
	}
	select {
	case <-done:
		t.Fatal("FRP can stop while peer request is draining")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("peer drain did not finish")
	}
}
