package main

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var monitoredRequests atomic.Uint64
var modelRequests atomic.Uint64

type resourceSampler struct {
	at            time.Time
	requests      uint64
	modelRequests uint64
	counters      map[string]uint64
}

func resourceRate(current, previous uint64, elapsed time.Duration) any {
	if elapsed <= 0 || current < previous {
		return nil
	}
	return float64(current-previous) / elapsed.Seconds()
}

func (s *resourceSampler) sample() (string, map[string]any) {
	now := time.Now()
	current, values := osResources()
	count := monitoredRequests.Load()
	values["requestsPerSecond"] = nil
	values["modelRequestsPerSecond"] = nil
	if !s.at.IsZero() {
		values["requestsPerSecond"] = resourceRate(count, s.requests, now.Sub(s.at))
		values["modelRequestsPerSecond"] = resourceRate(modelRequests.Load(), s.modelRequests, now.Sub(s.at))
		for _, pair := range [][2]string{{"networkReceiveBytes", "networkReceiveBytesPerSecond"}, {"networkTransmitBytes", "networkTransmitBytesPerSecond"}, {"diskReadBytes", "diskReadBytesPerSecond"}, {"diskWriteBytes", "diskWriteBytesPerSecond"}} {
			next, exists := current[pair[0]]
			prev, had := s.counters[pair[0]]
			if exists && had {
				values[pair[1]] = resourceRate(next, prev, now.Sub(s.at))
			}
		}
		total, ok := current["cpuTotal"]
		prev, had := s.counters["cpuTotal"]
		if ok && had && total > prev && current["cpuIdle"] >= s.counters["cpuIdle"] {
			busy := float64(total-prev) - float64(current["cpuIdle"]-s.counters["cpuIdle"])
			if busy >= 0 {
				values["cpuPercent"] = 100 * busy / float64(total-prev)
			}
		}
	}
	s.at = now
	s.requests = count
	s.modelRequests = modelRequests.Load()
	s.counters = current
	hostname := strings.TrimSpace(os.Getenv("NEWNOA_MONITOR_HOSTNAME"))
	if hostname == "" {
		hostname, _ = os.Hostname()
	}
	if hostname == "" {
		hostname = "server-local"
	}
	return hostname, values
}

func number(value string) uint64 { n, _ := strconv.ParseUint(value, 10, 64); return n }
