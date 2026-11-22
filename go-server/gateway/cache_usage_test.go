package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEvidenceScansOnlyNewPendingBytes(t *testing.T) {
	var e evidence
	for range 60000 {
		e.feed([]byte("x"))
	}
	if e.invalid || e.pendingScanned != 60000 {
		t.Fatalf("pending scan offset = %d, invalid = %t", e.pendingScanned, e.invalid)
	}
}

func TestCacheInputSubsetEvidence(t *testing.T) {
	for _, tc := range []struct {
		body string
		want *int64
	}{
		{`{"usage":{"prompt_tokens":12,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":5}}}`, cacheNumber(5)},
		{`{"usage":{"input_tokens":12,"output_tokens":2,"input_tokens_details":{"cached_tokens":0}}}`, cacheNumber(0)},
		{`{"usage":{"prompt_tokens":12,"completion_tokens":2}}`, nil},
		{`{"usage":{"prompt_tokens":12,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":13}}}`, nil},
		{`{"usage":{"prompt_tokens":12,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":-1}}}`, nil},
		{`{"usage":{"prompt_tokens":12,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":"5"}}}`, nil},
		{`{"usage":{"prompt_tokens":12,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":1.5}}}`, nil},
	} {
		var e evidence
		e.usage([]byte(tc.body))
		if e.input == nil || *e.input != 12 || e.output == nil || *e.output != 2 {
			t.Fatalf("lost valid main usage: %s", tc.body)
		}
		if (e.cached == nil) != (tc.want == nil) || e.cached != nil && *e.cached != *tc.want {
			t.Fatalf("cache mismatch %s: %v", tc.body, e.cached)
		}
	}
}
func cacheNumber(n int64) *int64 { return &n }
func TestCacheDoesNotLeakAcrossUsageSnapshots(t *testing.T) {
	var e evidence
	e.usage([]byte(`{"usage":{"prompt_tokens":12,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":5}}}`))
	e.usage([]byte(`{"usage":{}}`))
	if e.cached == nil || *e.cached != 5 {
		t.Fatal("partial event lost known evidence")
	}
	e.usage([]byte(`{"usage":{"prompt_tokens":12,"completion_tokens":3}}`))
	if e.cached != nil {
		t.Fatal("cache leaked across complete snapshots")
	}
}
func TestMessagesCacheReadIsSubsetOfNormalizedInput(t *testing.T) {
	var e evidence
	if !e.messagesUsage(&messagesUsage{Input: cacheNumber(2), Output: cacheNumber(7), Creation: cacheNumber(3), Read: cacheNumber(5)}, true) || *e.input != 10 || *e.cached != 5 {
		t.Fatal("cache read should be 5 of total 10, not include creation")
	}
	if !e.messagesUsage(&messagesUsage{Output: cacheNumber(8)}, false) || *e.cached != 5 {
		t.Fatal("delta lost initial cache evidence")
	}
	if !e.messagesUsage(&messagesUsage{Input: cacheNumber(2), Output: cacheNumber(0)}, true) || e.cached != nil {
		t.Fatal("missing cache must remain unknown")
	}
}

func TestChatStreamRequiresUsageRegardlessOfClientOptions(t *testing.T) {
	for _, options := range []string{"", `,"stream_options":{"include_usage":false}`, `,"stream_options":{"include_usage":true}`} {
		t.Run(options, func(t *testing.T) {
			raw := []byte(`{"model":"fixture","messages":[{"role":"user","content":"test"}],"stream":true,"max_tokens":16` + options + `}`)
			var forwarded, finalized, reserved map[string]any
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&forwarded)
				w.Header().Set("Content-Type", "text/event-stream")
				usage := `{"choices":[{"delta":{},"finish_reason":"stop"}]}`
				if settings, ok := forwarded["stream_options"].(map[string]any); ok && settings["include_usage"] == true {
					usage = `{"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":2,"prompt_tokens_details":{"cache_write_tokens":2,"cached_tokens":5}}}`
				}
				_, _ = io.WriteString(w, "data: "+usage+"\n\ndata: [DONE]\n\n")
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					_, _ = io.WriteString(w, `{"model":"upstream-fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					_ = json.NewDecoder(r.Body).Decode(&reserved)
					_ = json.NewEncoder(w).Encode(map[string]any{"requestId": "cache-test", "permit": "permit", "endpoint": upstream.URL})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					_ = json.NewDecoder(r.Body).Decode(&finalized)
					finalized = meterFixtureSelectedEvidence(r, finalized)
					_, _ = io.WriteString(w, `{}`)
				default:
					_, _ = io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			gateway := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(raw))
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_client_login")
			result := httptest.NewRecorder()
			gateway.ServeHTTP(result, req)
			if result.Code != 200 {
				t.Fatalf("status=%d body=%s", result.Code, result.Body.String())
			}
			settings, ok := forwarded["stream_options"].(map[string]any)
			if !ok || settings["include_usage"] != true {
				t.Fatalf("client disabled trusted stream usage: %#v", forwarded)
			}
			if finalized["inputTokens"] != float64(12) || finalized["outputTokens"] != float64(2) || finalized["cachedInputTokens"] != float64(5) || finalized["cacheWriteTokens"] != float64(2) {
				t.Fatalf("lost final cache evidence: %#v", finalized)
			}
			digest := sha256.Sum256(append([]byte("POST /v1/chat/completions\n"), raw...))
			if reserved["inferencePath"] != "/v1/chat/completions" {
				t.Fatalf("actual inference path omitted: %#v", reserved)
			}
			if reserved["bodyDigest"] != hex.EncodeToString(digest[:]) || reserved["executionDigest"] == reserved["bodyDigest"] {
				t.Fatalf("raw/execution bindings lost: %#v", reserved)
			}
		})
	}
}

func TestMalformedChatStreamOptionsFailBeforeAuthority(t *testing.T) {
	for _, options := range []string{`[]`, `false`, `"disabled"`, `{"include_usage":"false"}`} {
		called := false
		api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/inference-protection/events") {
				w.Write([]byte(`{}`))
				return
			}
			called = true
			http.Error(w, "unexpected", 500)
		}))
		gateway := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"test"}],"stream":true,"max_tokens":16,"stream_options":`+options+`}`))
		req.Header.Set("X-Api-Key", "nn_fixture")
		req.Header.Set("Authorization", "Bearer ses_client_login")
		result := httptest.NewRecorder()
		gateway.ServeHTTP(result, req)
		api.Close()
		if result.Code != 400 || called {
			t.Fatalf("malformed settings reached authority: %s status=%d called=%v", options, result.Code, called)
		}
	}
}
