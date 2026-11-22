package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

type hostedFixtureEvidence struct {
	mu       sync.Mutex
	sequence int64
	snapshot map[string]any
}

var hostedFixtureSnapshots sync.Map // testing.T -> request-scoped API fixture evidence

func hostedFixtureState(t *testing.T) *sync.Map {
	t.Helper()
	state := &sync.Map{}
	actual, loaded := hostedFixtureSnapshots.LoadOrStore(t, state)
	if !loaded {
		t.Cleanup(func() { hostedFixtureSnapshots.Delete(t) })
	}
	return actual.(*sync.Map)
}

// This fixture models the API's lifecycle-only closure contract: first assert
// the real wire omits every raw counter, then resolve its own last durable meter
// snapshot for existing business/usage assertions. It never changes the HTTP
// request or inserts fields into the production caller's payload.
func decodeHostedFixtureClosure(t *testing.T, r *http.Request, result any) error {
	t.Helper()
	var closure map[string]any
	if err := json.NewDecoder(r.Body).Decode(&closure); err != nil {
		return err
	}
	if sequence, ok := closure["meterSequence"].(float64); ok {
		for _, field := range []string{"inputTokens", "outputTokens", "cacheWriteTokens", "cachedInputTokens", "usageSource"} {
			if _, present := closure[field]; present {
				t.Errorf("lifecycle-only closure sent %s", field)
			}
		}
		if closure["executionClosed"] != true {
			t.Error("meter closure did not close execution")
		}
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/client/hosted/dispatches/"), "/finish")
		value, ok := hostedFixtureState(t).Load(id)
		if !ok {
			t.Error("closure has no durable fixture checkpoint")
		} else {
			e := value.(*hostedFixtureEvidence)
			e.mu.Lock()
			if sequence <= 0 || sequence > float64(e.sequence) {
				t.Error("invalid future/zero closure sequence")
			}
			if reported, ok := e.snapshot["reported"].(map[string]any); ok {
				for _, field := range []string{"inputTokens", "outputTokens", "cacheWriteTokens", "cachedInputTokens"} {
					if count, ok := reported[field]; ok {
						closure[field] = count
					}
				}
			}
			e.mu.Unlock()
		}
	}
	raw, err := json.Marshal(closure)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, result)
}

// Existing hosted regression fixtures now acknowledge the additional persistence
// boundary. Dedicated meter tests still exercise withheld, lost and bad acks.
func hostedMeterFixtureAck(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	t.Helper()
	if strings.HasSuffix(r.URL.Path, "/dispatch-attempts/abort") {
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || !nodePattern.MatchString(historyText(body["attemptId"])) || !nodePattern.MatchString(historyText(body["model"])) || !historyInferencePath(historyText(body["inferencePath"])) || len(historyText(body["bodyDigest"])) != 64 {
			t.Error("invalid hosted attempt compensation fixture")
			w.WriteHeader(400)
			return true
		}
		for field := range body {
			switch field {
			case "attemptId", "model", "inferencePath", "bodyDigest", "secret", "session":
			default:
				t.Errorf("unexpected compensation field %s", field)
			}
		}
		io.WriteString(w, `{"accepted":true}`)
		return true
	}
	if !strings.HasSuffix(r.URL.Path, "/meter") {
		return false
	}
	const prefix = "/api/v1/client/hosted/dispatches/"
	id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, prefix), "/meter")
	var v struct {
		Sequence int64           `json:"sequence"`
		Snapshot json.RawMessage `json:"snapshot"`
	}
	if !strings.HasPrefix(r.URL.Path, prefix) || !nodePattern.MatchString(id) || r.Header.Get("Authorization") != "Bearer "+hostedFixtureReportToken || r.Header.Get("X-Api-Key") != "" || json.NewDecoder(r.Body).Decode(&v) != nil || v.Sequence < 1 || !json.Valid(v.Snapshot) {
		t.Error("hosted checkpoint fixture trust/payload invalid")
		w.WriteHeader(400)
		return true
	}
	var snapshot map[string]any
	json.Unmarshal(v.Snapshot, &snapshot)
	state := hostedFixtureState(t)
	actual, _ := state.LoadOrStore(id, &hostedFixtureEvidence{})
	e := actual.(*hostedFixtureEvidence)
	e.mu.Lock()
	e.sequence = v.Sequence
	e.snapshot = snapshot
	e.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": id, "sequence": v.Sequence})
	return true
}
