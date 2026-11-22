package main

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// This sampler belongs to the public Core process; API hosts never supply its measurements.
type publicResourceSampler struct {
	mu               sync.Mutex
	at               time.Time
	previous         map[string]uint64
	latest           map[string]any
	requests, models atomic.Uint64
	connections      map[net.Conn]http.ConnState
}

func publicCoreIdentity(host, port string) string {
	digest := sha256.Sum256([]byte(host + "\x00" + port))
	return "core-" + hex.EncodeToString(digest[:16])
}

func (s *publicResourceSampler) connectionState(c net.Conn, state http.ConnState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.connections == nil {
		s.connections = make(map[net.Conn]http.ConnState)
	}
	if state == http.StateClosed || state == http.StateHijacked {
		delete(s.connections, c)
	} else {
		s.connections[c] = state
	}
}

func (s *publicResourceSampler) measure(now time.Time, counters map[string]uint64, values map[string]any) map[string]any {
	counters["requests"], counters["models"] = s.requests.Load(), s.models.Load()
	for source, target := range map[string]string{"requests": "requestsPerSecond", "models": "modelRequestsPerSecond", "rx": "networkReceiveBytesPerSecond", "tx": "networkTransmitBytesPerSecond", "read": "diskReadBytesPerSecond", "write": "diskWriteBytesPerSecond"} {
		values[target] = nil
		current, ok := counters[source]
		previous, had := s.previous[source]
		if elapsed := now.Sub(s.at).Seconds(); !s.at.IsZero() && elapsed > 0 && ok && had && current >= previous {
			values[target] = float64(current-previous) / elapsed
		}
	}
	values["cpuPercent"] = nil
	if total, ok := counters["cpu"]; ok && s.previous != nil && s.previous["cpu"] > 0 && total > s.previous["cpu"] && counters["idle"] >= s.previous["idle"] {
		busy := float64(total-s.previous["cpu"]) - float64(counters["idle"]-s.previous["idle"])
		if busy >= 0 {
			values["cpuPercent"] = 100 * busy / float64(total-s.previous["cpu"])
		}
	}
	s.at, s.previous, s.latest = now, counters, values
	return values
}

func (s *publicResourceSampler) snapshot(localAddress string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.at.IsZero() || now.Sub(s.at) >= time.Second {
		counters, values := publicOSResources()
		s.measure(now, counters, values)
	}
	host := strings.TrimSpace(os.Getenv("NEWNOA_MONITOR_HOSTNAME"))
	if host == "" {
		host, _ = os.Hostname()
	}
	address := strings.TrimSpace(os.Getenv("NEWNOA_MONITOR_ADDRESS"))
	if address == "" {
		address, _, _ = net.SplitHostPort(localAddress)
	}
	listen := os.Getenv("NEWNOA_PUBLIC_PROXY_LISTEN")
	if listen == "" {
		listen = "127.0.0.1:18223"
	}
	_, port, _ := net.SplitHostPort(listen)
	values := make(map[string]any, len(s.latest))
	for k, v := range s.latest {
		values[k] = v
	}
	active, idle := 0, 0
	for _, state := range s.connections {
		if state == http.StateActive {
			active++
		}
		if state == http.StateIdle {
			idle++
		}
	}
	values["tcpActiveConnections"], values["tcpIdleConnections"] = active, idle
	return map[string]any{"service": "website-go-core", "status": "ready", "instanceId": publicCoreIdentity(host, port), "hostname": host, "address": address, "observedAt": s.at.UTC().Format(time.RFC3339Nano), "resourceScope": "service-os", "resources": values}
}
