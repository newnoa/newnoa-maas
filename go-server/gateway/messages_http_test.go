package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMessagesNativeExecutionAndUnknownStream(t *testing.T) {
	good := `{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"cache_creation_input_tokens":3,"cache_read_input_tokens":5,"output_tokens":7}}`
	for _, tc := range []struct {
		name, response, state string
		stream                bool
		code                  int
	}{
		{"json", good, "succeeded", false, 200},
		{"beta", good, "succeeded", false, 200},
		{"malformed", `{"choices":[]}`, "unknown", false, 502},
		{"stream", nativeSSE(nativeStart, nativeDelta, `{"type":"message_stop"}`), "succeeded", true, 200},
		{"truncated", nativeSSE(nativeStart, nativeDelta), "unknown", true, 200},
		{"open-tool-block", nativeSSE(nativeStart, `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"tool_1","name":"Read","input":{}}}`, nativeDelta, `{"type":"message_stop"}`), "unknown", true, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beta := ""
			if tc.name == "beta" {
				beta = claude2325Betas[0]
			}
			calls := 0
			var finalized map[string]any
			execution := ""
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				raw, _ := io.ReadAll(r.Body)
				sum := sha256.Sum256(append([]byte("POST /v1/messages\n"+messagesBinding(beta)), raw...))
				if r.Header.Get("Anthropic-Beta") != beta {
					t.Error("beta lost")
				}
				if execution != hex.EncodeToString(sum[:]) || r.URL.Path != "/v1/messages" || r.Header.Get("Anthropic-Version") != "2023-06-01" || r.Header.Get("X-Api-Key") != "upstream-private" || r.Header.Get("Authorization") != "" {
					t.Error("protocol, credential or digest boundary changed")
				}
				var body map[string]any
				json.Unmarshal(raw, &body)
				if body["model"] != "upstream-model" {
					t.Error("model not rewritten")
				}
				if tc.stream {
					w.Header().Set("Content-Type", "text/event-stream")
				}
				io.WriteString(w, tc.response)
			}))
			defer upstream.Close()
			raw := `{"model":"deployment","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"stream":` + map[bool]string{false: "false", true: "true"}[tc.stream] + `}`
			control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					if body["secret"] != "nn_consumer" {
						t.Error("wrong consumer key")
					}
					io.WriteString(w, `{"model":"upstream-model"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					sum := sha256.Sum256([]byte("POST /v1/messages\n" + messagesBinding(beta) + raw))
					if body["inferencePath"] != "/v1/messages" || body["bodyDigest"] != hex.EncodeToString(sum[:]) {
						t.Error("request path not bound")
					}
					execution = body["executionDigest"].(string)
					json.NewEncoder(w).Encode(map[string]any{"requestId": "native-test", "permit": "fixture", "endpoint": upstream.URL, "apiKey": "upstream-private"})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					finalized = meterFixtureSelectedEvidence(r, body)
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer control.Close()
			g := newGateway(control.URL, "internal", "native", []string{"127.0.0.1"})
			r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(raw))
			r.Header.Set("X-Api-Key", "nn_consumer")
			r.Header.Set("Authorization", "Bearer ses_client_login")
			if beta != "" {
				r.Header.Set("Anthropic-Beta", beta)
			}
			w := httptest.NewRecorder()
			if aborted := serveGatewayCapturingAbort(g, w, r); aborted != (tc.stream && tc.state == "unknown" && tc.code == 200) {
				t.Fatalf("incorrect stream termination: aborted=%v", aborted)
			}
			if calls != 1 || w.Code != tc.code || finalized["status"] != tc.state {
				t.Fatalf("calls=%d code=%d finalize=%v", calls, w.Code, finalized)
			}
			if tc.state == "succeeded" && (finalized["inputTokens"] != float64(10) || finalized["outputTokens"] != float64(7) || finalized["cachedInputTokens"] != float64(5) || finalized["cacheWriteTokens"] != float64(3)) {
				t.Fatal("native usage wrong", finalized)
			}
		})
	}
}
func TestMessagesRejectUnsupportedHeadersBeforeAuthority(t *testing.T) {
	calls := 0
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/inference-protection/events") {
			w.Write([]byte(`{}`))
			return
		}
		calls++
		w.WriteHeader(500)
	}))
	defer control.Close()
	g := newGateway(control.URL, "internal", "native", nil)
	for _, headers := range []map[string]string{{}, {"Authorization": "Bearer a", "X-Api-Key": "b"}, {"X-Api-Key": "b", "Anthropic-Version": "future"}, {"X-Api-Key": "b", "Anthropic-Beta": "unknown"}} {
		r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{}`))
		for key, value := range headers {
			r.Header.Set(key, value)
		}
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != 400 && w.Code != 401 {
			t.Fatal("invalid request accepted", w.Code)
		}
		var result map[string]any
		json.Unmarshal(w.Body.Bytes(), &result)
		if result["type"] != "error" {
			t.Fatal("non-native error envelope")
		}
	}
	if calls != 0 {
		t.Fatal("rejected request reached authority")
	}
}
func TestMessagesMalformedRequestsDoNotReachAuthority(t *testing.T) {
	calls := 0
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/inference-protection/events") {
			w.Write([]byte(`{}`))
			return
		}
		calls++
		w.WriteHeader(500)
	}))
	defer control.Close()
	g := newGateway(control.URL, "internal", "native", nil)
	for _, body := range []string{
		`{"model":"x","messages":[{"role":"developer","content":"x"}],"max_tokens":16}`,
		`{"model":"x","messages":[{"role":"user","content":{}}],"max_tokens":16}`,
		`{"model":"x","messages":[{"role":"user","content":"x"}],"max_completion_tokens":16}`,
		`{"model":"x","messages":[{"role":"user","content":"x"}],"max_tokens":16,"stream":null}`,
	} {
		r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(body))
		r.Header.Set("X-Api-Key", "fixture")
		r.Header.Set("Authorization", "Bearer ses_client_login")
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatal("invalid request accepted", w.Code, body)
		}
	}
	if calls != 0 {
		t.Fatal("invalid requests reached authority")
	}
}
