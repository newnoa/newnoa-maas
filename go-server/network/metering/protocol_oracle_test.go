package metering

import (
	"encoding/json"
	"math/rand"
	"os"
	"testing"
)

// Expected counts come from official Python tiktoken, not this Go implementation.
func TestIndependentProtocolOracle(t *testing.T) {
	data, err := os.ReadFile("testdata/protocol-golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			ID, Protocol, Request, Wire string
			ExpectedOutputTokens        map[string]int64
		}
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) != 7 {
		t.Fatal("unexpected independent protocol corpus")
	}
	for _, row := range fixture.Cases {
		for _, model := range []struct{ id, encoding string }{{"gpt-4", "cl100k_base"}, {"gpt-4o", "o200k_base"}, {"custom-alias", "o200k_base"}} {
			t.Run(row.ID+"/"+model.id, func(t *testing.T) {
				for seed := int64(0); seed < 12; seed++ {
					m, err := New(model.id, row.Protocol, []byte(row.Request))
					if err != nil {
						t.Fatal(err)
					}
					rng := rand.New(rand.NewSource(seed))
					remaining := []byte(row.Wire)
					for len(remaining) > 0 {
						n := min(1+rng.Intn(97), len(remaining))
						if seed == 0 {
							n = 1
						}
						if _, err := m.Feed(remaining[:n]); err != nil {
							t.Fatalf("seed %d: %v", seed, err)
						}
						remaining = remaining[n:]
					}
					if _, err := m.Finish(); err != nil {
						t.Fatalf("seed %d finish: %v", seed, err)
					}
					snapshot := m.Snapshot()
					want := row.ExpectedOutputTokens[model.encoding]
					if snapshot.EstimatedOutputTokens == nil || *snapshot.EstimatedOutputTokens != want {
						t.Fatalf("seed %d output %v, independent oracle wants %d", seed, snapshot.EstimatedOutputTokens, want)
					}
				}
			})
		}
	}
}
