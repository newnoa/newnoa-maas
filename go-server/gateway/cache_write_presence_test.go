package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFinalizePreservesCacheWritePresence(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		want        any
	}{
		{"missing", `"prompt_tokens":12,"completion_tokens":3`, nil},
		{"zero", `"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_write_tokens":0}`, float64(0)},
		{"positive", `"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_write_tokens":2}`, float64(2)},
	} {
		for _, format := range []string{"json", "sse"} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if format == "sse" {
						w.Header().Set("Content-Type", "text/event-stream")
						io.WriteString(w, "data: {\"choices\":[],\"usage\":{"+tc.usage+"}}\n\ndata: [DONE]\n\n")
					} else {
						io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{`+tc.usage+`}}`)
					}
				}))
				defer upstream.Close()
				var finalized map[string]any
				api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case strings.HasSuffix(r.URL.Path, "/authorize"):
						io.WriteString(w, `{"model":"upstream-fixture"}`)
					case strings.HasSuffix(r.URL.Path, "/dispatches"):
						json.NewEncoder(w).Encode(map[string]any{"requestId": "[TEST] request", "permit": "[TEST] permit", "endpoint": upstream.URL})
					case strings.HasSuffix(r.URL.Path, "/finalize"):
						json.NewDecoder(r.Body).Decode(&finalized)
						finalized = meterFixtureSelectedEvidence(r, finalized)
						io.WriteString(w, `{}`)
					default:
						io.WriteString(w, `{}`)
					}
				}))
				defer api.Close()
				gateway := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
				stream := "false"
				if format == "sse" {
					stream = "true"
				}
				r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"[TEST] message"}],"max_tokens":16,"stream":`+stream+`}`))
				r.Header.Set("X-Api-Key", "nn_fixture")
				r.Header.Set("Authorization", "Bearer ses_fixture")
				w := httptest.NewRecorder()
				gateway.ServeHTTP(w, r)
				if w.Code != 200 || finalized["status"] != "succeeded" || finalized["cacheWriteTokens"] != tc.want {
					t.Fatal("finalize fabricated/lost write presence", w.Code, finalized)
				}
				if finalized["inputTokens"] != float64(12) || finalized["outputTokens"] != float64(3) {
					t.Fatal("cache-write presence changed main token evidence", finalized)
				}
			})
		}
	}
}
func TestMessagesMissingWriteRemainsUnknown(t *testing.T) {
	var e evidence
	if !e.messagesUsage(&messagesUsage{Input: cacheNumber(2), Output: cacheNumber(3)}, true) || e.write != nil {
		t.Fatal("messages fabricated missing cache write", e.write)
	}
}
