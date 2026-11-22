package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientCancellationStopsUpstreamPromptlyWithoutReplay(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/audio/transcriptions"} {
		t.Run(path, func(t *testing.T) {
			var calls atomic.Int32
			upstreamCancelled := make(chan struct{}, 1)
			stop := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				data := `{"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":5}}}`
				if path == "/v1/responses" {
					data = `{"type":"response.output_text.delta","delta":"test","usage":{"input_tokens":12,"output_tokens":2,"input_tokens_details":{"cached_tokens":5}}}`
				}
				if path == "/v1/audio/transcriptions" {
					data = `{"type":"transcript.text.delta","delta":"test"}`
				}
				_, _ = w.Write([]byte("data: " + data + "\n\n"))
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					upstreamCancelled <- struct{}{}
				case <-stop:
				}
			}))
			defer upstream.Close()
			finalized := make(chan map[string]any, 2)
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					var authorization map[string]any
					if err := json.NewDecoder(r.Body).Decode(&authorization); err != nil || authorization["gatewayId"] != "fixture" {
						t.Error("authorization omitted the Go Server identity")
					}
					_, _ = w.Write([]byte(`{"model":"fixture"}`))
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					_ = json.NewEncoder(w).Encode(map[string]any{"requestId": "cancel-test", "permit": "permit", "endpoint": upstream.URL})
				case strings.HasSuffix(r.URL.Path, "/consume"):
					_ = json.NewEncoder(w).Encode(map[string]any{"executionDeadlineAt": time.Now().Add(5 * time.Minute).Format(time.RFC3339Nano)})
				case strings.HasSuffix(r.URL.Path, "/transport"):
					w.Write([]byte(`{}`))
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					var body map[string]any
					_ = json.NewDecoder(r.Body).Decode(&body)
					body = meterFixtureSelectedEvidence(r, body)
					finalized <- body
					_, _ = w.Write([]byte(`{}`))
				default:
					_, _ = w.Write([]byte(`{}`))
				}
			}))
			defer api.Close()
			handler := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				deadlineCtx, deadlineCancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer deadlineCancel()
				handler.ServeHTTP(w, r.WithContext(deadlineCtx))
			}))
			defer func() { close(stop); gatewayServer.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, "POST", gatewayServer.URL+path, strings.NewReader(`{"model":"fixture","max_tokens":16,"max_output_tokens":16,"messages":[{"role":"user","content":"test"}],"input":"test","stream":true}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_client_login")
			if path == "/v1/audio/transcriptions" {
				audio := audioRequest(t, "true", 10)
				req.Body, req.ContentLength = audio.Body, audio.ContentLength
				req.Header.Set("Content-Type", audio.Header.Get("Content-Type"))
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			line, err := bufio.NewReader(response.Body).ReadString('\n')
			if err != nil || !strings.HasPrefix(line, "data: ") {
				t.Fatalf("missing first event: %q, %v", line, err)
			}
			cancel()
			select {
			case <-upstreamCancelled:
			case <-time.After(300 * time.Millisecond):
				t.Fatal("client cancellation did not promptly stop upstream; execution must not drain for usage")
			}
			select {
			case body := <-finalized:
				if path == "/v1/audio/transcriptions" {
					if body["status"] != "cancelled" || body["inputTokens"] != nil || body["outputTokens"] != nil || body["cachedInputTokens"] != nil {
						t.Fatalf("invalid cancelled ASR evidence: %#v", body)
					}
					break
				}
				if body["status"] != "cancelled" || body["inputTokens"] != float64(12) || body["outputTokens"] != float64(2) || body["cachedInputTokens"] != float64(5) {
					t.Fatalf("lost cancellation evidence: %#v", body)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancelled request was not finalized")
			}
			if calls.Load() != 1 {
				t.Fatalf("upstream replayed %d times", calls.Load())
			}
		})
	}
}

func TestEgressDNSStopsWhenRequestIsCancelled(t *testing.T) {
	original := net.DefaultResolver
	release := make(chan struct{})
	started := make(chan struct{}, 8)
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return nil, errors.New("fixture stopped")
		}
	}}
	defer func() { net.DefaultResolver = original; close(release) }()
	g := newGateway("http://127.0.0.1", "fixture", "fixture", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		request, _ := http.NewRequestWithContext(ctx, "GET", "https://cancel.fixture.invalid", nil)
		_, err := g.egress.Do(request)
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("DNS fixture was not reached")
	}
	cancel()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("cancelled DNS dial succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("request cancellation did not stop DNS")
	}
}

func TestSlowConsumerWriteHasBoundedDeadline(t *testing.T) {
	finished := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		finished <- writeDownstream(r.Context(), w, bytes.Repeat([]byte("x"), 8*maxBody), 50*time.Millisecond)
	}))
	defer server.Close()
	address, err := net.ResolveTCPAddr("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	connection, err := net.DialTCP("tcp", nil, address)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	// Never read: the response must stop once the kernel send buffer fills.
	select {
	case err := <-finished:
		var networkError net.Error
		if !errors.As(err, &networkError) || !networkError.Timeout() {
			t.Fatalf("expected write timeout, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("slow consumer retained handler")
	}
}

type failedFlushWriter struct{ *httptest.ResponseRecorder }

func (w failedFlushWriter) FlushError() error { return errors.New("consumer disconnected") }

func TestFlushFailureCannotBecomeSuccessfulCompletion(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer upstream.Close()
	state := ""
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			_, _ = w.Write([]byte(`{"model":"fixture"}`))
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			_ = json.NewEncoder(w).Encode(map[string]any{"requestId": "flush-test", "permit": "permit", "endpoint": upstream.URL})
		case strings.HasSuffix(r.URL.Path, "/consume"):
			_, _ = w.Write([]byte(`{}`))
		case strings.HasSuffix(r.URL.Path, "/transport"):
			io.WriteString(w, `{}`)
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			body = meterFixtureSelectedEvidence(r, body)
			state = body["status"].(string)
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","max_tokens":16,"messages":[{"role":"user","content":"test"}],"stream":true}`))
	req.Header.Set("X-Api-Key", "nn_fixture")
	req.Header.Set("Authorization", "Bearer ses_client_login")
	g.ServeHTTP(failedFlushWriter{httptest.NewRecorder()}, req)
	if state != "unknown" {
		t.Fatalf("flush failure recorded as %q", state)
	}
}

func TestGatewayPropagatesTraceToControlButNotModelEgress(t *testing.T) {
	installTelemetryTestProvider(t)
	var controlTrace, modelTrace string
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		modelTrace = r.Header.Get("traceparent")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer model.Close()
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if value := r.Header.Get("traceparent"); value != "" {
			controlTrace = value
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			_, _ = w.Write([]byte(`{"model":"fixture"}`))
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": model.URL, "model": "fixture", "requestId": "r1", "permit": "p1", "apiKey": "model-key"})
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
	h := instrumentHTTP(g, "newnoa-gateway", gatewayRoute)
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","max_tokens":16,"messages":[{"role":"user","content":"test"}]}`))
	r.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	r.Header.Set("X-Api-Key", "user-key")
	r.Header.Set("Authorization", "Bearer ses_client_login")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("request status = %d: %s", w.Code, w.Body.String())
	}
	if controlTrace == "" {
		t.Fatal("trace context was not propagated to trusted control service")
	}
	if modelTrace != "" {
		t.Fatalf("trace context escaped to model endpoint: %q", modelTrace)
	}
}

func TestStreamEvidence(t *testing.T) {
	for _, tc := range []struct {
		data string
		done bool
	}{{"data: {\"choices\":[]}\n\n", false}, {"data: [DONE]\n\n", true}, {"data: [DONE]", false}, {"data: [DONE]\n", false}, {"data: {\"error\":{\"message\":\"failed\"}}\n\ndata: [DONE]\n\n", false}, {"data: {\"message\":\"[DONE]\"}\n\n", false}} {
		var e evidence
		for _, b := range []byte(tc.data) {
			e.feed([]byte{b})
		}
		if e.done != tc.done {
			t.Fatalf("done=%v for %q", e.done, tc.data)
		}
	}
}
func TestGatewayDoesNotReplayAndRecordsUnknown(t *testing.T) {
	for _, tc := range []struct{ name, path, data, state string }{
		{"missing-terminal", "/v1/chat/completions", "data: {\"choices\":[]}\n\n", "unknown"},
		{"truncated-after-terminal", "/v1/chat/completions", "data: [DONE]\n\ndata: {", "unknown"},
		{"responses-wrong-terminal", "/v1/responses", "data: [DONE]\n\n", "unknown"},
		{"chat-complete", "/v1/chat/completions", "data: [DONE]\n\n", "succeeded"},
		{"responses-complete", "/v1/responses", "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n", "succeeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			calls := 0
			state := ""
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				calls++
				mu.Unlock()
				w.Header().Set("Content-Type", "text/event-stream")
				w.Write([]byte(tc.data))
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/api/v1/internal/authorize":
					json.NewEncoder(w).Encode(map[string]any{"model": "upstream-model"})
				case r.URL.Path == "/api/v1/internal/dispatches":
					json.NewEncoder(w).Encode(map[string]any{"requestId": "r1", "permit": "p1", "endpoint": upstream.URL, "apiKey": "key", "model": "mock"})
				case strings.HasSuffix(r.URL.Path, "/consume"):
					w.Write([]byte(`{"consumed":true}`))
				case strings.HasSuffix(r.URL.Path, "/transport"):
					io.WriteString(w, `{}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					var b map[string]any
					json.NewDecoder(r.Body).Decode(&b)
					b = meterFixtureSelectedEvidence(r, b)
					mu.Lock()
					state = b["status"].(string)
					mu.Unlock()
					w.Write([]byte(`{}`))
				default:
					t.Errorf("unexpected %s", r.URL.Path)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal-secret", "g-test", []string{"127.0.0.1"})
			req := httptest.NewRequest("POST", tc.path, strings.NewReader(`{"model":"deployment","max_tokens":16,"max_output_tokens":16,"messages":[{"role":"user","content":"test"}],"input":"test","stream":true}`))
			req.Header.Set("X-Api-Key", "nn_test")
			req.Header.Set("Authorization", "Bearer ses_client_login")
			out := httptest.NewRecorder()
			if aborted := serveGatewayCapturingAbort(g, out, req); aborted != (tc.state == "unknown") {
				t.Fatalf("incorrect stream termination: aborted=%v", aborted)
			}
			if calls != 1 || state != tc.state {
				t.Fatalf("calls=%d state=%s body=%s", calls, state, out.Body.String())
			}
		})
	}
}
func TestRejectPrivateEgress(t *testing.T) {
	if _, err := safeAddress(context.Background(), "127.0.0.1:8080", nil); err == nil {
		t.Fatal("private egress accepted")
	}
	if _, err := safeAddress(context.Background(), "127.0.0.1:8080", []string{"127.0.0.1"}); err != nil {
		t.Fatal(err)
	}
}

func TestRejectSpecialPurposeEgress(t *testing.T) {
	for _, host := range []string{"100.64.0.1", "100.100.100.200", "100.127.255.255", "198.18.0.1", "198.19.255.255", "240.0.0.1", "255.255.255.254", "::ffff:100.100.100.200"} {
		address := net.JoinHostPort(host, "8080")
		if _, err := safeAddress(context.Background(), address, nil); err == nil {
			t.Errorf("special-purpose address %s accepted", address)
		}
		if _, err := safeAddress(context.Background(), address, []string{address}); err != nil {
			t.Errorf("explicitly allowed address %s rejected: %v", address, err)
		}
	}
	for _, host := range []string{"8.8.8.8", "100.63.255.255", "100.128.0.1", "198.17.255.255", "198.20.0.1"} {
		if _, err := safeAddress(context.Background(), net.JoinHostPort(host, "8080"), nil); err != nil {
			t.Errorf("public address %s rejected: %v", host, err)
		}
	}
}

func TestPrivateEgressCanBeRestrictedToAnExactModelPort(t *testing.T) {
	allow := []string{"127.0.0.1:8081", "127.0.0.1:8082"}
	for _, address := range []string{"127.0.0.1:8081", "127.0.0.1:8082"} {
		if _, err := safeAddress(context.Background(), address, allow); err != nil {
			t.Fatalf("approved model address %s rejected: %v", address, err)
		}
	}
	for _, address := range []string{"127.0.0.1:5432", "127.0.0.1:18101", "127.0.0.1:8083", "127.0.0.2:8081"} {
		if _, err := safeAddress(context.Background(), address, allow); err == nil {
			t.Fatalf("unapproved address %s accepted", address)
		}
	}
}

func TestResponsesEvidence(t *testing.T) {
	e := evidence{responses: true}
	e.feed([]byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":3}}}\n\n"))
	if !e.done || e.input == nil || *e.input != 2 || e.output == nil || *e.output != 3 {
		t.Fatal("Responses completion was not recognized")
	}
}

func TestStreamTerminationIsProtocolBoundAndFrameComplete(t *testing.T) {
	for _, tc := range []struct {
		data                string
		responses, complete bool
	}{
		{"data: [DONE]\n\n", false, true},
		{"data: [DONE]\n\n", true, false},
		{"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n", true, true},
		{"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n", false, false},
		{"data: [DONE]\n\ndata: {", false, false},
		{"data: [DONE]\n\ndata: {}\n", false, false},
		{"data: [DONE]\n\n: heartbeat\n\n", false, true},
		{"data: [DONE]\n\ndata: {\"error\":{}}\n\n", false, false},
	} {
		e := evidence{responses: tc.responses}
		for _, b := range []byte(tc.data) {
			e.feed([]byte{b})
		}
		if e.complete() != tc.complete {
			t.Fatalf("responses=%v completion for %q", tc.responses, tc.data)
		}
	}
}

func TestUsageRequiresCompleteIntegerEvidence(t *testing.T) {
	for _, data := range []string{
		`{"usage":{}}`, `{"usage":{"prompt_tokens":2}}`,
		`{"usage":{"completion_tokens":3}}`,
		`{"usage":{"prompt_tokens":null,"completion_tokens":3}}`,
		`{"usage":{"prompt_tokens":1.5,"completion_tokens":3}}`,
		`{"usage":{"prompt_tokens":-1,"completion_tokens":3}}`,
		`{"usage":{"prompt_tokens":1000000001,"completion_tokens":3}}`,
	} {
		var e evidence
		e.usage([]byte(data))
		if e.input != nil || e.output != nil {
			t.Fatalf("invented usage for %s", data)
		}
	}
	for _, data := range []string{`{"usage":{"prompt_tokens":0,"completion_tokens":3}}`, `{"usage":{"input_tokens":0,"output_tokens":3}}`} {
		var e evidence
		e.usage([]byte(data))
		if e.input == nil || *e.input != 0 || e.output == nil || *e.output != 3 {
			t.Fatalf("lost explicit usage for %s", data)
		}
		e.usage([]byte(`{"usage":{}}`))
		if *e.output != 3 {
			t.Fatal("partial later event overwrote known usage")
		}
	}
}

func TestNonstreamCompletionShape(t *testing.T) {
	for _, tc := range []struct {
		body             string
		responses, valid bool
	}{
		{`{"choices":[]}`, false, false},
		{`{"choices":"invalid"}`, false, false},
		{`{"choices":[{"message":{"content":"OK"}}]}`, false, false},
		{`{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}]}`, false, true},
		{`{"choices":[{"message":{"tool_calls":[]},"finish_reason":"tool_calls"}]}`, false, true},
		{`{"choices":[{"message":{"content":"partial"},"finish_reason":"length"}]}`, false, true},
		{`{"choices":[{"message":{},"finish_reason":"stop"},null]}`, false, false},
		{`{"choices":[{"message":{},"finish_reason":"stop"}],"error":{}}`, false, false},
		{`{"status":"completed","output":[]}`, true, true},
		{`{"status":"completed","output":"invalid"}`, true, false},
		{`{"status":"incomplete","output":[]}`, true, false},
		{`{"status":"completed","output":[],"error":{"message":"failed"}}`, true, false},
	} {
		if validCompletion([]byte(tc.body), tc.responses) != tc.valid {
			t.Fatalf("completion validity for %s", tc.body)
		}
	}
}

func TestInvalidCompletionRemainsUnknownWithoutReplay(t *testing.T) {
	for _, finish := range []string{"", "stop", "length", "tool_calls"} {
		t.Run(finish, func(t *testing.T) {
			calls := 0
			var finalized map[string]any
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"content": "reply"}, "finish_reason": finish}}, "usage": map[string]int{"prompt_tokens": 2, "completion_tokens": 3}})
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					json.NewEncoder(w).Encode(map[string]any{"model": "fixture"})
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": "fixture", "permit": "fixture", "endpoint": upstream.URL})
				case strings.HasSuffix(r.URL.Path, "/transport"):
					io.WriteString(w, `{}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					json.NewDecoder(r.Body).Decode(&finalized)
					finalized = meterFixtureSelectedEvidence(r, finalized)
					w.Write([]byte(`{}`))
				default:
					w.Write([]byte(`{"consumed":true}`))
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "fixture", "fixture", []string{"127.0.0.1"})
			req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","max_tokens":16,"messages":[{"role":"user","content":"test"}]}`))
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_client_login")
			out := httptest.NewRecorder()
			g.ServeHTTP(out, req)
			status, state := 200, "succeeded"
			if finish == "" {
				status, state = 502, "unknown"
			}
			if calls != 1 || out.Code != status || finalized["status"] != state || finalized["inputTokens"] != float64(2) || finalized["outputTokens"] != float64(3) {
				t.Fatalf("calls=%d HTTP=%d evidence=%v", calls, out.Code, finalized)
			}
		})
	}
}

func TestChatAllowsEncodedTenMiBImage(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		calls++
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
	}))
	defer upstream.Close()
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			io.WriteString(w, `{"model":"fixture"}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(map[string]any{"requestId": "image-test", "permit": "permit", "endpoint": upstream.URL})
		default:
			io.WriteString(w, `{}`)
		}
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
	body := `{"model":"fixture","max_tokens":65535,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,` + strings.Repeat("A", (10*1024*1024+2)/3*4) + `"}}]}]}`
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("X-Api-Key", "nn_fixture")
	req.Header.Set("Authorization", "Bearer ses_client_login")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, req)
	if w.Code != 200 || calls != 1 {
		t.Fatalf("image blocked: status=%d upstream=%d body=%s", w.Code, calls, w.Body.String())
	}
}

func TestResponsesProviderMetadataDoesNotSupplyUsageOrTerminal(t *testing.T) {
	var e evidence
	e.responses = true
	e.feed([]byte("data: {\"type\":\"response.metadata\",\"usage\":{\"input_tokens\":999,\"output_tokens\":999},\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":999,\"output_tokens\":999}}}\n\n"))
	if e.complete() || e.invalid || e.input != nil || e.output != nil {
		t.Fatal("metadata must not act as completion or usage evidence")
	}
	e.feed([]byte("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":12,\"output_tokens\":5}}}\n\n"))
	if !e.complete() || e.input == nil || *e.input != 12 || e.output == nil || *e.output != 5 {
		t.Fatal("valid Responses terminal did not supply exact usage")
	}
}
