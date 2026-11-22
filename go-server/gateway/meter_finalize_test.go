package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Mirror Dispatch.finishMeter's counter equality check before lifecycle closure.
// A failed newer checkpoint must not leave an otherwise closed pool lease occupied.
func TestMeterOutageFinalizesOnlyAcknowledgedUsageAndClosesLease(t *testing.T) {
	for _, mode := range []string{"start-only", "previous-reported-usage", "committed-ack-lost"} {
		previousUsage := mode != "start-only"
		name := mode
		t.Run(name, func(t *testing.T) {
			var sequence int64
			var reported map[string]any
			var final map[string]any
			var rejected, finalizeCalls, upstreamCalls int
			var selected map[string]any
			leaseOccupied := true
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"gpt-4"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					io.WriteString(w, `{"requestId":"outage-finalize","permit":"one","endpoint":"http://127.0.0.1:1"}`)
				case strings.HasSuffix(r.URL.Path, "/meter"):
					var b struct {
						Sequence int64          `json:"sequence"`
						Snapshot map[string]any `json:"snapshot"`
					}
					_ = json.NewDecoder(r.Body).Decode(&b)
					limit := int64(1)
					if previousUsage {
						limit = 2
					}
					if b.Sequence > limit {
						rejected++
						if mode == "committed-ack-lost" {
							sequence = b.Sequence
							reported, _ = b.Snapshot["reported"].(map[string]any)
							// Commit succeeded, but the ACK is truncated on every retry.
							io.WriteString(w, `{`)
							return
						}
						http.Error(w, "checkpoint unavailable", 503)
						return
					}
					sequence = b.Sequence
					reported, _ = b.Snapshot["reported"].(map[string]any)
					_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": "outage-finalize", "sequence": sequence})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					finalizeCalls++
					_ = json.NewDecoder(r.Body).Decode(&final)
					hint, ok := final["meterSequence"].(float64)
					if !ok || hint < 1 || hint > float64(sequence) || final["executionClosed"] != true {
						http.Error(w, "invalid closure hint", 409)
						return
					}
					for _, key := range []string{"inputTokens", "outputTokens", "cacheWriteTokens", "cachedInputTokens"} {
						if _, present := final[key]; present {
							http.Error(w, "raw counter KEY present", 409)
							return
						}
					}
					selected = reported // API owns latest persisted evidence, never the stale hint's counters.
					leaseOccupied = false
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			body := &meteredFragments{}
			if previousUsage {
				body.parts = append(body.parts, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":2}}\n\n")
			}
			body.parts = append(body.parts, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":9,\"prompt_tokens_details\":{\"cached_tokens\":4,\"cache_write_tokens\":3}}}\n\ndata: [DONE]\n\n")
			g := newGateway(api.URL, "internal", "fixture", nil)
			g.egress = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				upstreamCalls++
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: body}, nil
			})}
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hello"}],"stream":true}`))
			r.Header.Set("X-Api-Key", "fixture")
			r.Header.Set("Authorization", "Bearer ses_fixture")
			serveGatewayCapturingAbort(g, httptest.NewRecorder(), r)
			if rejected < 2 {
				t.Fatalf("missing bounded closure checkpoint retry: %d", rejected)
			}
			if leaseOccupied || finalizeCalls != 1 {
				t.Fatalf("closure rejected, lease held: final=%v calls=%d", final, finalizeCalls)
			}
			if final["inputTokens"] == float64(20) || final["outputTokens"] == float64(9) || final["cacheWriteTokens"] == float64(3) || final["cachedInputTokens"] == float64(4) {
				t.Fatalf("unacknowledged usage escaped: %v", final)
			}
			if mode == "committed-ack-lost" && (final["meterSequence"] != float64(2) || sequence != 3 || selected["outputTokens"] != float64(9)) {
				t.Fatalf("latest committed evidence not selected: hint=%v latest=%d selected=%v", final, sequence, selected)
			}
			if mode == "previous-reported-usage" && selected["outputTokens"] != float64(2) {
				t.Fatalf("persisted evidence changed: %v", selected)
			}
			if upstreamCalls != 1 || !body.closed.Load() {
				t.Fatal("producer replayed or left open")
			}
		})
	}
}
