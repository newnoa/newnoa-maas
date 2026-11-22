package main

import "testing"

func TestHostedMeterClosureOmitsRawCountersAndLeavesEvidenceToAPI(t *testing.T) {
	for _, report := range []usageReport{{Status: "unknown", MeterSequence: 1}, {Status: "cancelled", MeterSequence: 7, Input: ptrUsage(12), Output: ptrUsage(3), Cached: ptrUsage(0), Write: ptrUsage(0)}} {
		payload := hostedUsagePayload(report)
		if payload["meterSequence"] != report.MeterSequence || payload["executionClosed"] != true || payload["status"] != report.Status {
			t.Fatal("lifecycle closure lost last ACK/state", payload)
		}
		for _, field := range []string{"inputTokens", "outputTokens", "cacheWriteTokens", "cachedInputTokens", "usageSource", "pendingMeter"} {
			if _, ok := payload[field]; ok {
				t.Fatalf("meter closure sent raw/stale field %s=%v", field, payload[field])
			}
		}
	}
	legacy := hostedUsagePayload(usageReport{Status: "succeeded", Input: ptrUsage(12), Output: ptrUsage(3)})
	if legacy["inputTokens"] == nil || legacy["outputTokens"] == nil {
		t.Fatal("legacy no-meter usage changed")
	}
	if _, ok := legacy["meterSequence"]; ok {
		t.Fatal("legacy reporter pretended metering")
	}
}
