package metering

import (
	"encoding/json"
	"testing"
)

func TestSnapshotWireContract(t *testing.T) {
	input, output := int64(12), int64(3)
	snapshot := Snapshot{Estimator: "tiktoken-visible-v1", Encoding: "cl100k_base", EstimatedInputTokens: &input, EstimatedOutputTokens: &output, ResponseStarted: true, Reported: &Usage{InputTokens: &input, OutputTokens: &output}, TerminalUsage: true}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"estimator":"tiktoken-visible-v1","encoding":"cl100k_base","estimatedInputTokens":12,"estimatedOutputTokens":3,"responseStarted":true,"reported":{"inputTokens":12,"outputTokens":3},"terminalUsage":true}`
	if string(raw) != want {
		t.Fatalf("wire shape %s want %s", raw, want)
	}
	raw, err = json.Marshal(Snapshot{Estimator: "reference-o200k-v1", Encoding: "unknown"})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"estimator":"reference-o200k-v1","encoding":"unknown","responseStarted":false,"terminalUsage":false}` {
		t.Fatalf("unavailable became zero: %s", raw)
	}
}
