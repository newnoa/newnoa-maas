package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func TestMessagesNativeSystemRolePreservesBoundStream(t *testing.T) {
	for _, content := range []string{`[{"type":"text","text":"mid-conversation instruction"}]`, `"mid-conversation instruction"`} {
		t.Run(content, func(t *testing.T) {
			// Deidentified shape from the failed request, not a replay of its private text.
			const prefix = `{"context_management":{"edits":[]},"max_tokens":16,"metadata":{"user_id":"fixture"},"model":"deployment","output_config":{"effort":"low"},"stream":true,"system":[{"type":"text","text":"top-level instruction"}],"thinking":{"type":"disabled"},"tools":[],"messages":[{"role":"user","content":[{"type":"text","text":"fixture"}]},{"role":"system","content":`
			raw := prefix + content + `}]}`
			beta := claude2325Betas[0]
			wantOriginal := sha256.Sum256([]byte("POST /v1/messages\nanthropic-beta:" + beta + "\n" + raw))
			var expectedBody map[string]any
			if err := json.Unmarshal([]byte(raw), &expectedBody); err != nil {
				t.Fatal(err)
			}
			expectedBody["model"] = "upstream-model"
			var calls, authorized, dispatched, consumed atomic.Int32
			var execution atomic.Value
			finalized := make(chan map[string]any, 1)
			response := nativeSSE(nativeStart,
				`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"NEWNOA_REMOTE_OK"}}`,
				`{"type":"content_block_stop","index":0}`, nativeDelta, `{"type":"message_stop"}`)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				sum := sha256.Sum256(append([]byte("POST /v1/messages\nanthropic-beta:"+beta+"\n"), body...))
				if execution.Load() != hex.EncodeToString(sum[:]) {
					t.Error("execution digest does not bind actual upstream header/body")
				}
				if r.URL.Path != "/v1/messages" || r.Header.Get("Anthropic-Beta") != beta || r.Header.Get("Anthropic-Version") != "2023-06-01" || r.Header.Get("X-Api-Key") != "upstream-fixture" || r.Header.Get("Authorization") != "" {
					t.Error("native protocol/header/credential boundary changed")
				}
				var actual map[string]any
				if json.Unmarshal(body, &actual) != nil || !reflect.DeepEqual(actual, expectedBody) {
					t.Error("native system position, top-level system, or other request fields changed")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, response)
			}))
			defer upstream.Close()
			control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					authorized.Add(1)
					io.WriteString(w, `{"model":"upstream-model","inferencePath":"/v1/messages"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					dispatched.Add(1)
					if body["inferencePath"] != "/v1/messages" || body["bodyDigest"] != hex.EncodeToString(wantOriginal[:]) {
						t.Error("original request path/header/body not bound")
					}
					execution.Store(body["executionDigest"].(string))
					json.NewEncoder(w).Encode(map[string]any{"requestId": "system-role-fixture", "permit": "permit-fixture", "endpoint": upstream.URL, "apiKey": "upstream-fixture"})
				case strings.HasSuffix(r.URL.Path, "/consume"):
					consumed.Add(1)
					io.WriteString(w, `{}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					finalized <- meterFixtureSelectedEvidence(r, body)
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer control.Close()
			gateway := httptest.NewServer(newGateway(control.URL, "internal-fixture", "gateway-fixture", []string{"127.0.0.1"}))
			defer gateway.Close()
			req, _ := http.NewRequest("POST", gateway.URL+"/v1/messages", strings.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Api-Key", "consumer-fixture")
			req.Header.Set("Authorization", "Bearer ses_fixture")
			req.Header.Set("Anthropic-Beta", beta)
			res, err := gateway.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil || res.StatusCode != 200 || string(data) != response {
				t.Fatalf("native system stream: code=%d readErr=%v responseChanged=%v", res.StatusCode, err, string(data) != response)
			}
			if calls.Load() != 1 || authorized.Load() != 1 || dispatched.Load() != 1 || consumed.Load() != 1 {
				t.Fatalf("native execution counts: upstream=%d authorize=%d dispatch=%d consume=%d", calls.Load(), authorized.Load(), dispatched.Load(), consumed.Load())
			}
			select {
			case evidence := <-finalized:
				if evidence["status"] != "succeeded" || evidence["inputTokens"] != float64(10) || evidence["outputTokens"] != float64(7) || evidence["cachedInputTokens"] != float64(5) || evidence["cacheWriteTokens"] != float64(3) {
					t.Fatal("native system stream lost reported metering", evidence)
				}
			default:
				t.Fatal("native execution did not finalize")
			}
		})
	}
}

func TestMessagesSystemRoleCannotBeAdaptedToOpenAI(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			var authorized, dispatched, upstreamCalls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { upstreamCalls.Add(1) }))
			defer upstream.Close()
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					authorized.Add(1)
					json.NewEncoder(w).Encode(map[string]any{"model": "upstream-model", "inferencePath": path})
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					dispatched.Add(1)
					json.NewEncoder(w).Encode(map[string]any{"requestId": "forbidden-fixture", "endpoint": upstream.URL})
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer control.Close()
			gateway := httptest.NewServer(newGateway(control.URL, "internal-fixture", "gateway-fixture", []string{"127.0.0.1"}))
			defer gateway.Close()
			const raw = `{"model":"deployment","max_tokens":16,"stream":true,"system":"top-level instruction","messages":[{"role":"user","content":"fixture"},{"role":"system","content":"mid-conversation instruction"}]}`
			req, _ := http.NewRequest("POST", gateway.URL+"/v1/messages", strings.NewReader(raw))
			req.Header.Set("X-Api-Key", "consumer-fixture")
			req.Header.Set("Authorization", "Bearer ses_fixture")
			req.Header.Set("Anthropic-Beta", claude2325Betas[0])
			res, err := gateway.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != 400 || !strings.Contains(string(data), "Unsupported Messages feature for configured OpenAI protocol") || authorized.Load() != 1 || dispatched.Load() != 0 || upstreamCalls.Load() != 0 {
				t.Fatalf("system role was translated or rejected before protocol selection: code=%d authorize=%d dispatch=%d upstream=%d", res.StatusCode, authorized.Load(), dispatched.Load(), upstreamCalls.Load())
			}
		})
	}
}

func TestMessagesUnknownRolesAndInvalidSystemContentStayRejected(t *testing.T) {
	for _, message := range []string{`{"role":"developer","content":"fixture"}`, `{"role":"tool","content":"fixture"}`, `{"role":"unknown","content":"fixture"}`, `{"role":"system","content":42}`} {
		t.Run(message, func(t *testing.T) {
			var authorized, dispatched atomic.Int32
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/authorize") {
					authorized.Add(1)
				}
				if strings.HasSuffix(r.URL.Path, "/dispatches") {
					dispatched.Add(1)
				}
				io.WriteString(w, `{}`)
			}))
			defer control.Close()
			gateway := httptest.NewServer(newGateway(control.URL, "internal-fixture", "gateway-fixture", []string{"127.0.0.1"}))
			defer gateway.Close()
			req, _ := http.NewRequest("POST", gateway.URL+"/v1/messages", strings.NewReader(`{"model":"deployment","max_tokens":16,"messages":[`+message+`]} `))
			req.Header.Set("X-Api-Key", "consumer-fixture")
			req.Header.Set("Authorization", "Bearer ses_fixture")
			res, err := gateway.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != 400 || authorized.Load() != 0 || dispatched.Load() != 0 {
				t.Fatalf("invalid native role/content escaped validation: code=%d authorize=%d dispatch=%d", res.StatusCode, authorized.Load(), dispatched.Load())
			}
		})
	}
}
