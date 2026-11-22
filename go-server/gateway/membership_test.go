package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"
)

type membershipLog chan string

func (messages membershipLog) Write(data []byte) (int, error) {
	messages <- string(data)
	return len(data), nil
}

func TestMembershipHeartbeatReportsHTTPFailures(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte("private-response-data"))
			}))
			defer api.Close()
			messages := make(membershipLog, 10)
			previous := log.Writer()
			log.SetOutput(messages)
			defer log.SetOutput(previous)
			g := newGateway(api.URL, "internal-test", "gateway-rejected", nil)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); g.reportMembership(ctx, "172.27.42.19:18091") }()
			defer func() { cancel(); <-done }()
			select {
			case message := <-messages:
				if !strings.Contains(message, "gateway-rejected") || !strings.Contains(message, fmt.Sprintf("status %d", status)) {
					t.Fatalf("heartbeat failure omitted node or HTTP status: %s", message)
				}
				if strings.Contains(message, "internal-test") || strings.Contains(message, "private-response-data") {
					t.Fatalf("heartbeat failure exposed private data: %s", message)
				}
			case <-time.After(time.Second):
				t.Fatal("rejected heartbeat was silently ignored")
			}
		})
	}
}

func TestMembershipHeartbeatRetriesAfterHTTPFailure(t *testing.T) {
	got := make(chan int, 2)
	attempts := 0
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		got <- attempts
		if attempts == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal-test", "gateway-retry", nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); g.reportMembership(ctx, "172.27.42.19:18091") }()
	defer func() { cancel(); <-done }()
	for want := 1; want <= 2; want++ {
		select {
		case attempt := <-got:
			if attempt != want {
				t.Fatalf("heartbeat attempt=%d, want %d", attempt, want)
			}
		case <-time.After(12 * time.Second):
			t.Fatalf("heartbeat did not reach attempt %d after API recovery", want)
		}
	}
}

func TestAdvertiseRequiresTLSForPublicGateways(t *testing.T) {
	for _, address := range []string{"172.27.42.19:18091", "[::1]:18091", "https://gateway.example.test:443"} {
		if !validAdvertiseAddress(address) {
			t.Errorf("valid address rejected: %s", address)
		}
	}
	for _, address := range []string{"203.0.113.1:18091", "http://gateway.example.test", "https://user:secret@host", "https://host/path", "https://host?token=x", "https://host:0"} {
		if validAdvertiseAddress(address) {
			t.Errorf("invalid address accepted: %s", address)
		}
	}
}

func TestMembershipHeartbeatReportsIdentityAddressLoadAndHealth(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-fixture")
	got := make(chan map[string]any, 1)
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/internal/gateways/gateway-242/heartbeat" || r.Header.Get("Authorization") != "Bearer internal-test" {
			t.Errorf("unexpected heartbeat request: %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		got <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal-test", "gateway-242", nil)
	g.active.Store(99)
	g.modelActive.Store(7)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.reportMembership(ctx, "172.27.42.19:18091"); close(done) }()
	select {
	case body := <-got:
		if body["address"] != "172.27.42.19:18091" || body["activeRequests"] != float64(7) || body["healthy"] != true || body["ownerId"] != "owner-fixture" {
			t.Fatalf("unexpected heartbeat body: %#v", body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("membership heartbeat was not sent")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not stop")
	}
}
