package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Recorders have no HTTP server to recover the deliberate connection abort.
func serveGatewayCapturingAbort(handler http.Handler, w http.ResponseWriter, r *http.Request) (aborted bool) {
	defer func() {
		if value := recover(); value != nil {
			if value != http.ErrAbortHandler {
				panic(value)
			}
			aborted = true
		}
	}()
	handler.ServeHTTP(w, r)
	return
}

func TestGatewaySSETerminationAcrossRealHTTP(t *testing.T) {
	const done = "data: {\"type\":\"transcript.text.done\",\"text\":\"hello\",\"usage\":{\"input_tokens\":171,\"output_tokens\":30}}\n\n"
	const delta = "data: {\"type\":\"transcript.text.delta\",\"delta\":\"hello\"}\n\n"
	const chat = "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":171,\"completion_tokens\":30}}\n\ndata: [DONE]\n\n"
	for _, tc := range []struct {
		name, first, tail             string
		abort, success, chat, noUsage bool
	}{
		{name: "typed done clean EOF", first: done, success: true},
		{name: "typed done optional sentinel", first: done, tail: "data: [DONE]\n\n", success: true},
		{name: "typed done upstream abort", first: done, abort: true},
		{name: "typed done error event", first: done, tail: "data: {\"type\":\"error\",\"error\":\"failed\"}\n\n"},
		{name: "typed done malformed event", first: done, tail: "data: not-json\n\n"},
		{name: "typed done incomplete trailing frame", first: done, tail: "data: {"},
		{name: "delta incomplete clean EOF", first: delta, noUsage: true},
		{name: "chat complete clean EOF", first: chat, success: true, chat: true},
		{name: "chat complete upstream abort", first: chat, abort: true, chat: true},
		{name: "chat data after terminal", first: chat, tail: "data: {\"choices\":[{\"delta\":{\"content\":\"late\"}}]}\n\n", chat: true},
		{name: "chat usage after terminal", first: chat, tail: "data: {\"usage\":{\"prompt_tokens\":999,\"completion_tokens\":999}}\n\n", chat: true},
		{name: "chat duplicate terminal", first: chat, tail: "data: [DONE]\n\n", chat: true},
		{name: "chat keepalive after terminal", first: chat, tail: ": keepalive\n\n", success: true, chat: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finish := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(finish) }) }
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, tc.first)
				w.(http.Flusher).Flush()
				select {
				case <-finish:
				case <-r.Context().Done():
					return
				}
				if tc.abort {
					panic(http.ErrAbortHandler)
				}
				io.WriteString(w, tc.tail)
			}))
			defer upstream.Close()
			finalized := make(chan map[string]any, 1)
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"upstream-asr"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": "stream-test", "permit": "fixture", "endpoint": upstream.URL})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					var final map[string]any
					if err := json.NewDecoder(r.Body).Decode(&final); err != nil {
						t.Error(err)
					}
					final = meterFixtureSelectedEvidence(r, final)
					finalized <- final
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			server := httptest.NewServer(newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"}))
			defer func() { release(); server.Close() }()
			source := audioRequest(t, "true", 10)
			path := "/v1/audio/transcriptions"
			if tc.chat {
				source = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"deployment","messages":[{"role":"user","content":"hello"}],"max_tokens":16,"stream":true}`))
				source.Header.Set("X-Api-Key", "nn_fixture")
				source.Header.Set("Authorization", "Bearer ses_client_login")
				path = "/v1/chat/completions"
			}
			request, err := http.NewRequest("POST", server.URL+path, source.Body)
			if err != nil {
				t.Fatal(err)
			}
			request.Header = source.Header.Clone()
			client := server.Client()
			client.Timeout = 3 * time.Second
			response, err := client.Do(request)
			if err != nil {
				t.Fatal("first event was not forwarded", err)
			}
			defer response.Body.Close()
			first := make([]byte, len(tc.first))
			if _, err = io.ReadFull(response.Body, first); err != nil || string(first) != tc.first {
				t.Fatal("first event changed", string(first), err)
			}
			release()
			tail, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if tc.success && (readErr != nil || string(tail) != tc.tail) {
				t.Errorf("valid completion changed: tail=%q err=%v", tail, readErr)
			}
			if !tc.success && readErr == nil {
				t.Errorf("Gateway converted abnormal stream to clean EOF: tail=%q", tail)
			}
			select {
			case final := <-finalized:
				state := "unknown"
				if tc.success {
					state = "succeeded"
				}
				if final["status"] != state {
					t.Errorf("wrong settlement state: %v", final)
				}
				if tc.noUsage {
					if final["inputTokens"] != nil || final["outputTokens"] != nil {
						t.Error("invented usage", final)
					}
				} else if final["inputTokens"] != float64(171) || final["outputTokens"] != float64(30) {
					t.Error("lost observed usage", final)
				}
			case <-time.After(2 * time.Second):
				t.Error("request was not finalized")
			}
			if calls.Load() != 1 {
				t.Error("request replayed", calls.Load())
			}
		})
	}
}

type gatewayRoundTripFunc func(*http.Request) (*http.Response, error)

func (f gatewayRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type callerCancelledBody struct {
	cancel context.CancelFunc
	closed bool
}

func (b *callerCancelledBody) Read([]byte) (int, error) { b.cancel(); return 0, context.Canceled }
func (b *callerCancelledBody) Close() error             { b.closed = true; return nil }
func TestGatewayCallerCancelledStreamReturnsWithoutPanic(t *testing.T) {
	finalized := make(chan map[string]any, 1)
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			io.WriteString(w, `{"model":"fixture"}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			io.WriteString(w, `{"requestId":"cancelled-stream","permit":"fixture","endpoint":"https://upstream.example.test"}`)
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			var final map[string]any
			json.NewDecoder(r.Body).Decode(&final)
			final = meterFixtureSelectedEvidence(r, final)
			finalized <- final
			io.WriteString(w, `{}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	defer api.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &callerCancelledBody{cancel: cancel}
	g := newGateway(api.URL, "internal", "fixture", nil)
	g.egress.Transport = gatewayRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
	})
	request := audioRequest(t, "true", 10).WithContext(ctx)
	g.ServeHTTP(httptest.NewRecorder(), request)
	if !body.closed || ctx.Err() != context.Canceled {
		t.Fatal("upstream did not close after caller cancellation")
	}
	select {
	case final := <-finalized:
		if final["status"] != "cancelled" {
			t.Fatal(final)
		}
	default:
		t.Fatal("request was not finalized")
	}
}

func TestGatewayCallerCancellationDoesNotOverrideUpstreamUnknown(t *testing.T) {
	for _, transportError := range []bool{false, true} {
		t.Run(fmt.Sprint(transportError), func(t *testing.T) {
			finalized := make(chan map[string]any, 1)
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					io.WriteString(w, `{"requestId":"unknown-stream","permit":"fixture","endpoint":"https://upstream.example.test"}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					var final map[string]any
					json.NewDecoder(r.Body).Decode(&final)
					final = meterFixtureSelectedEvidence(r, final)
					finalized <- final
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g := newGateway(api.URL, "internal", "fixture", nil)
			g.egress.Transport = gatewayRoundTripFunc(func(*http.Request) (*http.Response, error) {
				cancel()
				if transportError {
					return nil, context.Canceled
				}
				return &http.Response{StatusCode: 502, Header: http.Header{"X-Newnoa-Outcome": {"unknown"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
			})
			g.ServeHTTP(httptest.NewRecorder(), audioRequest(t, "true", 10).WithContext(ctx))
			if final := <-finalized; final["status"] != "unknown" {
				t.Fatal("caller stop hid upstream uncertainty", final)
			}
		})
	}
}

type cancelledWriteWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
	err    error
}

func (w cancelledWriteWriter) Write([]byte) (int, error) { w.cancel(); return 0, w.err }

func TestGatewayCancelledDownstreamWritesPreserveTimeoutUncertainty(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, writeErr := range []error{syscall.EPIPE, syscall.ECONNRESET, status.Error(codes.Canceled, "caller stopped"), context.DeadlineExceeded, status.Error(codes.DeadlineExceeded, "write timed out")} {
			t.Run(fmt.Sprint(stream, writeErr), func(t *testing.T) {
				finalized := make(chan map[string]any, 1)
				api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case strings.HasSuffix(r.URL.Path, "/authorize"):
						io.WriteString(w, `{"model":"fixture"}`)
					case strings.HasSuffix(r.URL.Path, "/dispatches"):
						io.WriteString(w, `{"requestId":"write-stop","permit":"fixture","endpoint":"https://upstream.example.test"}`)
					case strings.HasSuffix(r.URL.Path, "/finalize"):
						var final map[string]any
						json.NewDecoder(r.Body).Decode(&final)
						final = meterFixtureSelectedEvidence(r, final)
						finalized <- final
						io.WriteString(w, `{}`)
					default:
						io.WriteString(w, `{}`)
					}
				}))
				defer api.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				g := newGateway(api.URL, "internal", "fixture", nil)
				g.egress.Transport = gatewayRoundTripFunc(func(*http.Request) (*http.Response, error) {
					body, contentType := `{"status":"completed","output":[]}`, "application/json"
					if stream {
						body, contentType = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n", "text/event-stream"
					}
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(body))}, nil
				})
				req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"fixture","input":"hi","stream":%t}`, stream))).WithContext(ctx)
				req.Header.Set("X-Api-Key", "nn_fixture")
				req.Header.Set("Authorization", "Bearer ses_client_login")
				g.ServeHTTP(cancelledWriteWriter{httptest.NewRecorder(), cancel, writeErr}, req)
				want := "cancelled"
				if writeErr == context.DeadlineExceeded || status.Code(writeErr) == codes.DeadlineExceeded {
					want = "unknown"
				}
				if final := <-finalized; final["status"] != want {
					t.Fatalf("final=%v want=%s", final, want)
				}
			})
		}
	}
}
