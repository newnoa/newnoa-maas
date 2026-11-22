package main

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestPublicLinuxSamplerReadsCountersAndRejectsMissingOrMalformedMetrics(t *testing.T) {
	fixture := map[string]string{
		"/proc/stat":      "cpu 10 20 30 40 5 6 7 8\n",
		"/proc/meminfo":   "MemTotal: 100 kB\nMemAvailable: 25 kB\n",
		"/proc/net/dev":   "lo: 999 0 0 0 0 0 0 0 999\neth0: 100 0 0 0 0 0 0 0 200\nveth123: 999 0 0 0 0 0 0 0 999\n",
		"/proc/diskstats": "8 0 sda 0 0 10 0 0 0 20 0\n8 1 sda1 0 0 99 0 0 0 99 0\n",
	}
	read := func(name string) ([]byte, error) {
		value, ok := fixture[name]
		if !ok {
			return nil, os.ErrNotExist
		}
		return []byte(value), nil
	}
	counters, values := readPublicOSResources(read, func(name string) bool { return name == "sda" }, []uint64{1000, 400})
	if counters["cpu"] != 126 || counters["idle"] != 45 || counters["rx"] != 100 || counters["tx"] != 200 || counters["read"] != 5120 || counters["write"] != 10240 {
		t.Fatal("incorrect real OS counter decoding", counters)
	}
	if values["memoryTotalBytes"] != uint64(102400) || values["memoryUsedBytes"] != uint64(76800) || values["diskUsedBytes"] != uint64(400) {
		t.Fatal("incorrect OS capacities", values)
	}
	fixture["/proc/stat"] = "cpu broken 0 0 0\n"
	fixture["/proc/meminfo"] = "MemTotal: 100 kB\nMemAvailable: broken kB\n"
	fixture["/proc/net/dev"] = "lo: 1 0 0 0 0 0 0 0 2\neth0: broken 0 0 0 0 0 0 0 2\n"
	fixture["/proc/diskstats"] = "8 0 sda 0 0 broken 0 0 0 20 0\n"
	counters, values = readPublicOSResources(read, func(string) bool { return true }, nil)
	if len(counters) != 0 || values["memoryUsedBytes"] != nil || values["diskTotalBytes"] != nil {
		t.Fatal("missing or malformed counters became fabricated zero", counters, values)
	}
	delete(fixture, "/proc/meminfo")
	counters, values = readPublicOSResources(func(string) ([]byte, error) { return nil, os.ErrPermission }, func(string) bool { return false }, nil)
	if len(counters) != 0 || len(values) != 0 {
		t.Fatal("unreadable OS files fabricated resources")
	}
}

func TestPublicLinuxSamplerFallsBackToActualFreeMemoryAndIgnoresInvalidCapacity(t *testing.T) {
	_, values := readPublicOSResources(func(name string) ([]byte, error) {
		if name == "/proc/meminfo" {
			return []byte("MemTotal: 100 kB\nMemFree: 20 kB\n"), nil
		}
		return nil, os.ErrNotExist
	}, func(string) bool { return false }, []uint64{10, 20})
	if values["memoryUsedBytes"] != uint64(81920) || values["diskUsedBytes"] != nil {
		t.Fatal("free memory fallback or capacity bound incorrect", values)
	}
}

func TestPublicCoreLinuxActualSample(t *testing.T) {
	sampler := &publicResourceSampler{}
	first := sampler.snapshot("127.0.0.1:18223")
	if first["hostname"] == "" || first["instanceId"] == "" {
		t.Fatal("real identity unavailable")
	}
	resource := first["resources"].(map[string]any)
	if resource["memoryTotalBytes"] == nil || resource["diskTotalBytes"] == nil || resource["cpuPercent"] != nil {
		t.Fatal("actual Linux capacity or first sample invalid", resource)
	}
	time.Sleep(1050 * time.Millisecond)
	second := sampler.snapshot("127.0.0.1:18223")
	resource = second["resources"].(map[string]any)
	cpu, ok := resource["cpuPercent"].(float64)
	if !ok || cpu < 0 || cpu > 100 {
		t.Fatal("actual CPU delta unavailable", resource)
	}
	encoded, err := json.Marshal(second)
	if err != nil || len(encoded) > 4096 {
		t.Fatal("health resource payload exceeds API bound")
	}
	t.Log(string(encoded))
}
