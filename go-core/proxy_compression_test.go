package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func compressionFixture() string {
	var rows []map[string]any
	for i := 0; i < 80; i++ {
		rows = append(rows, map[string]any{"request_id": fmt.Sprintf("req_%03d", i), "status": "ok", "latency_ms": i, "detail": "所有原始记录必须保留"})
	}
	b, _ := json.MarshalIndent(map[string]any{"rows": rows}, "", "  ")
	return string(b)
}
func compressionRequest(path, text string) []byte {
	var v any
	switch path {
	case "/v1/chat/completions":
		v = map[string]any{"model": "[TEST]", "messages": []any{map[string]any{"role": "system", "content": text}, map[string]any{"role": "user", "content": text}, map[string]any{"role": "tool", "tool_call_id": "call_1", "content": text}}, "stream": true}
	case "/v1/responses":
		v = map[string]any{"model": "[TEST]", "instructions": text, "input": []any{map[string]any{"type": "function_call", "call_id": "call_1", "name": "read_file", "arguments": text}, map[string]any{"type": "function_call_output", "call_id": "call_1", "output": text}}, "stream": true}
	case "/v1/messages":
		v = map[string]any{"model": "[TEST]", "system": text, "messages": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": text}, map[string]any{"type": "tool_result", "tool_use_id": "call_1", "content": text}}}}, "stream": true}
	}
	b, _ := json.Marshal(v)
	return b
}
func TestProxyCompressionPolicyPersistenceAndFailures(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	status := c.compressionStatus()
	if !status.Enabled || !status.Available {
		t.Fatal(status)
	}
	if _, err := c.configureCompression(json.RawMessage(`{"enabled":false}`)); err != nil {
		t.Fatal(err)
	}
	next := newCore(c.home, c.gateway)
	defer next.shutdown()
	if next.compressionStatus().Enabled {
		t.Fatal("disabled setting lost")
	}
	for _, raw := range []string{`{}`, `{"enabled":null}`, `{"enabled":1}`, `{"enabled":true,"other":1}`, `{"enabled":true} {}`, `{"enabled":false,"enabled":true}`} {
		if _, err := c.configureCompression(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	if err := os.WriteFile(c.compressionPath(), []byte(`broken`), 0600); err != nil {
		t.Fatal(err)
	}
	broken := newCore(c.home, c.gateway)
	defer broken.shutdown()
	if broken.compressionStatus().Available {
		t.Fatal("corrupt settings claimed available")
	}
	if err := os.Remove(c.compressionPath()); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(c.compressionPath(), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := c.configureCompression(json.RawMessage(`{"enabled":true}`)); err == nil {
		t.Fatal("ignored save failure")
	}
	if c.compressionStatus().Enabled {
		t.Fatal("failed save changed policy")
	}
}
func TestProxyCompressionChangesOnlyToolJSONAcrossProtocols(t *testing.T) {
	text := compressionFixture()
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			raw := compressionRequest(path, text)
			out := compressProxyPayload(path, raw)
			if bytes.Equal(out, raw) || len(out) >= len(raw) {
				t.Fatal("tool JSON not compressed")
			}
			// Only one string value may change: the user/system/arguments are byte-identical.
			quoted, _ := json.Marshal(text)
			if bytes.Count(out, quoted) != bytes.Count(raw, quoted)-1 {
				t.Fatal("protected content changed")
			}
			if !bytes.Contains(out, []byte(`"call_1"`)) || !bytes.Contains(out, []byte(`"stream":true`)) {
				t.Fatal("protocol fields changed")
			}
			if !bytes.Equal(out, compressProxyPayload(path, raw)) {
				t.Fatal("unstable compression")
			}
		})
	}
}
func TestProxyCompressionPassThrough(t *testing.T) {
	good := compressionRequest("/v1/chat/completions", compressionFixture())
	for _, tc := range []struct {
		path string
		raw  []byte
	}{
		{"/v1/embeddings", good}, {"/v1/messages/count_tokens", good}, {"/v1/responses", []byte(`{"input":"leave me alone"}`)},
		{"/v1/chat/completions", []byte(`{"messages":[{"role":"tool","content":"small"}]}`)},
		{"/v1/chat/completions", []byte(`{"messages":[],"messages":[]}`)},
		{"/v1/chat/completions", append(good, []byte(`{}`)...)},
		{"/v1/chat/completions", compressionRequest("/v1/chat/completions", strings.Repeat("x", (256<<10)+1))},
		{"/v1/messages", bytes.Replace(compressionRequest("/v1/messages", compressionFixture()), []byte(`"stream":true`), []byte(`"stream":true,"cache_control":{"type":"ephemeral"}`), 1)},
	} {
		if !bytes.Equal(compressProxyPayload(tc.path, tc.raw), tc.raw) {
			t.Fatalf("changed protected input %s", tc.path)
		}
	}
}
func TestProxyCompressionMiddlewareSnapshotsAndPreservesStream(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	original := compressionRequest("/v1/chat/completions", compressionFixture())
	calls := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		raw, _ := io.ReadAll(r.Body)
		if bytes.Equal(raw, original) {
			t.Error("request unchanged")
		}
		if r.ContentLength != int64(len(raw)) {
			t.Error("wrong length")
		}
		if r.Header.Get("Authorization") != "Bearer [TEST]" {
			t.Error("identity changed")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"usage\":{\"input_tokens\":42}}\n\ndata: [DONE]\n\n")
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(original))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer [TEST]")
	w := httptest.NewRecorder()
	c.compressInference(next).ServeHTTP(w, req)
	if calls != 1 || !strings.Contains(w.Body.String(), `"input_tokens":42`) {
		t.Fatal("stream/retry changed")
	}
	_, err := c.configureCompression(json.RawMessage(`{"enabled":false}`))
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(original))
	req.Header.Set("Content-Type", "application/json")
	c.compressInference(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if !bytes.Equal(raw, original) {
			t.Error("disabled compression changed body")
		}
	})).ServeHTTP(httptest.NewRecorder(), req)
	info, err := os.Stat(filepath.Join(c.home, ".newnoa", "proxy-compression.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("unsafe persistence", err)
	}
}

func TestProxyCompressionRejectsIncompletePolicyAndEscapedCacheBoundaries(t *testing.T) {
	for _, raw := range []string{`{"revision":1}`, `{"enabled":null,"revision":1}`, `{"enabled":true,"revision":1,"unknown":1}`} {
		home := t.TempDir()
		path := filepath.Join(home, ".newnoa", "proxy-compression.json")
		if err := atomicFile(path, []byte(raw)); err != nil {
			t.Fatal(err)
		}
		c := newCore(home, "https://server.example.test")
		if c.compressionStatus().Available {
			t.Errorf("invalid policy available: %s", raw)
		}
		c.shutdown()
	}
	raw := bytes.Replace(compressionRequest("/v1/messages", compressionFixture()), []byte(`"stream":true`), []byte(`"stream":true,"cache_\u0063ontrol":{"type":"ephemeral"}`), 1)
	if !bytes.Equal(compressProxyPayload("/v1/messages", raw), raw) {
		t.Fatal("escaped cache boundary changed")
	}
}
func TestProxyCompressionPreservesOpaqueSignedRequests(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	original := compressionRequest("/v1/chat/completions", compressionFixture())
	for _, header := range []string{"Digest", "Content-Digest", "Content-MD5"} {
		req := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(original))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(header, "[TEST] signed body")
		c.compressInference(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			raw, _ := io.ReadAll(r.Body)
			if !bytes.Equal(raw, original) {
				t.Error("signed body changed", header)
			}
		})).ServeHTTP(httptest.NewRecorder(), req)
	}
}

func TestProxyCompressionRealListenerKeepsHistoryOriginalAndDispatchesOnce(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/enabled=%t", path, enabled), func(t *testing.T) {
				testCompressionThroughListener(t, path, enabled, false)
			})
		}
	}
	t.Run("messages/cache-bypass", func(t *testing.T) {
		testCompressionThroughListener(t, "/v1/messages", true, true)
	})
}

func testCompressionThroughListener(t *testing.T, path string, enabled, bypass bool) {
	t.Helper()
	original := compressionRequest(path, compressionFixture())
	if bypass {
		original = bytes.Replace(original, []byte(`"stream":true`), []byte(`"stream":true,"cache_control":{"type":"ephemeral"}`), 1)
	}
	wantForward := original
	if enabled && !bypass {
		wantForward = compressProxyPayload(path, original)
		if len(wantForward) >= len(original) {
			t.Fatal("fixture did not compress")
		}
	}
	terminal := map[string]string{
		"/v1/chat/completions": "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}],\"usage\":{\"prompt_tokens\":42,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n",
		"/v1/responses":        "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":42,\"output_tokens\":1}}}\n\n",
		"/v1/messages": "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":42}}}\n\n" +
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n" +
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
	}[path]
	calls := 0
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		raw, _ := io.ReadAll(r.Body)
		if r.URL.Path != path || !bytes.Equal(raw, wantForward) {
			t.Error("forwarded request did not match effective compression policy")
		}
		if r.Header.Get("X-Api-Key") != "nn_"+strings.Repeat("x", 40) {
			t.Error("caller key changed")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, terminal)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	configureTestHistorySQLite(t, c.home, "00000000-0000-0000-0000-000000000001")
	defer c.shutdown()
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	historyPolicyFixture(t, h, "local")
	c.control = h.control
	if status, err := c.configureCompression(json.RawMessage(fmt.Sprintf(`{"enabled":%t}`, enabled))); err != nil || !status.Available || status.Enabled != enabled {
		t.Fatal(status, err)
	}
	if err := c.startProxyAt("nn_"+strings.Repeat("x", 40), availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest("POST", c.proxyBaseURL()+path, bytes.NewReader(original))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	stream, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if calls != 1 || res.StatusCode != 200 || !bytes.Equal(stream, []byte(terminal)) {
		t.Fatalf("dispatch=%d status=%d", calls, res.StatusCode)
	}
	account := "00000000-0000-0000-0000-000000000001"
	manager, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := manager.store.Query(context.Background(), account, historyQuery{})
	// SSE EOF can precede the capture commit. Drain via the same lock as capture and sync.
	deadline := time.Now().Add(2 * time.Second)
	for err == nil && rows.Total == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		manager.flushPending(context.Background())
		rows, err = manager.store.Query(context.Background(), account, historyQuery{})
	}
	if err != nil || rows.Total != 1 {
		t.Fatal(rows, err)
	}
	row, err := manager.store.Get(context.Background(), account, "own", rows.Items[0]["id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	var saved any
	var want any
	json.Unmarshal([]byte(row["request"].(string)), &saved)
	json.Unmarshal(original, &want)
	savedRaw, _ := json.Marshal(saved)
	wantRaw, _ := json.Marshal(want)
	if !bytes.Equal(savedRaw, wantRaw) {
		t.Fatal("original call record changed")
	}
}
func TestProxyCompressionConcurrencyAndPinnedPolicy(t *testing.T) {
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	raw := compressionRequest("/v1/responses", compressionFixture())
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			if i%2 == 0 {
				_, err := c.configureCompression(json.RawMessage(`{"enabled":false}`))
				if err != nil {
					t.Error(err)
				}
			} else {
				_ = compressProxyPayload("/v1/responses", raw)
				_ = c.compressionStatus()
			}
		}(i)
	}
	group.Wait()
	req := httptest.NewRequest("POST", "/v1/responses", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(context.WithValue(req.Context(), compressionPolicyContext{}, proxyCompressionStatus{Enabled: true, Available: true}))
	c.compressInference(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		out, _ := io.ReadAll(r.Body)
		if bytes.Equal(out, raw) {
			t.Error("policy was not pinned")
		}
	})).ServeHTTP(httptest.NewRecorder(), req)
}

func TestProxyCompressionBlockBudgetPassesWholeRequest(t *testing.T) {
	message := map[string]any{"role": "tool", "tool_call_id": "call", "content": compressionFixture()}
	items := make([]any, 33)
	for i := range items {
		items[i] = message
	}
	raw, _ := json.Marshal(map[string]any{"messages": items})
	if !bytes.Equal(compressProxyPayload("/v1/chat/completions", raw), raw) {
		t.Fatal("over-budget request partially compressed")
	}
}

func TestProxyCompressionRejectsLoneSurrogateEscapes(t *testing.T) {
	text := strings.Repeat(`{"value":"\ud800"},`, 90)
	text = "[" + strings.TrimSuffix(text, ",") + "]"
	if !json.Valid([]byte(text)) {
		t.Fatal("invalid surrogate fixture")
	}
	raw := compressionRequest("/v1/responses", text)
	if !bytes.Equal(compressProxyPayload("/v1/responses", raw), raw) {
		t.Fatal("invalid surrogate changed")
	}
}

func TestProxyCompressionRejectsCaseFoldedPersistedKeys(t *testing.T) {
	home := t.TempDir()
	if err := atomicFile(filepath.Join(home, ".newnoa", "proxy-compression.json"), []byte(`{"enabled":false,"Enabled":true,"revision":1}`)); err != nil {
		t.Fatal(err)
	}
	c := newCore(home, "https://server.example.test")
	defer c.shutdown()
	if c.compressionStatus().Available {
		t.Fatal("ambiguous policy accepted")
	}
}

func TestProxyCompressionRPCBrowserAndPublicBoundaries(t *testing.T) {
	account := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/me" {
			io.WriteString(w, `{"id":"00000000-0000-0000-0000-000000000001"}`)
			return
		}
		http.NotFound(w, r)
	}))
	defer account.Close()
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	for _, mode := range []string{"dev", "docker"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("NEWNOA_RUNTIME_MODE", mode)
			req := httptest.NewRequest("POST", "http://127.0.0.1/api/local/core", strings.NewReader(`{"method":"proxy.compression.set","params":{"enabled":false}}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer [TEST]")
			w := httptest.NewRecorder()
			browserLocal(c, account.URL).ServeHTTP(w, req)
			if w.Code != 200 || c.compressionStatus().Enabled {
				t.Fatalf("RPC %d %s", w.Code, w.Body.String())
			}
		})
	}
	status, err := c.handle("proxy.status", nil)
	if err != nil || status.(map[string]any)["compression"] == nil {
		t.Fatal("missing status")
	}
}
