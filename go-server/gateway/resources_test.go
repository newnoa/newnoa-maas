package main

import (
	"testing"
	"time"
)

func TestResourceRatesRequireTwoMonotonicSamples(t *testing.T) {
	if resourceRate(20, 10, 4*time.Second) != 2.5 {
		t.Fatal("incorrect sampled rate")
	}
	if resourceRate(10, 20, time.Second) != nil || resourceRate(20, 10, 0) != nil {
		t.Fatal("invented counter reset/initial rate")
	}
}

func TestLocalResourceSnapshotUsesActualHost(t *testing.T) {
	s := &resourceSampler{}
	hostname, first := s.sample()
	if hostname == "" {
		t.Fatal("missing hostname")
	}
	if first["requestsPerSecond"] != nil {
		t.Fatal("first request rate must be unknown")
	}
	if first["memoryTotalBytes"] == nil || first["diskTotalBytes"] == nil {
		t.Fatal("missing actual memory/disk")
	}
	time.Sleep(time.Millisecond)
	_, next := s.sample()
	if next["requestsPerSecond"] == nil {
		t.Fatal("second sample needs measured rate")
	}
}
