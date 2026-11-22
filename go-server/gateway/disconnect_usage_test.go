package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type disconnectWriter struct {
	header http.Header
	cancel context.CancelFunc
	writes int
}

func (w *disconnectWriter) Header() http.Header { return w.header }
func (w *disconnectWriter) WriteHeader(int)     {}
func (w *disconnectWriter) Write(p []byte) (int, error) {
	w.writes++
	w.cancel()
	return 0, context.Canceled
}

func TestDisconnectedStreamStopsProducerAndPreservesObservedUsage(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/messages", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			closed := make(chan struct{}, 1)
			release := make(chan struct{})
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				switch path {
				case "/v1/messages":
					io.WriteString(w, nativeSSE(nativeStart))
				case "/v1/responses":
					io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\",\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}\n\n")
				default:
					io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\n")
				}
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					closed <- struct{}{}
				case <-release:
				}
			}))
			defer upstream.Close()
			defer close(release)
			finalized := make(chan map[string]any, 1)
			control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					json.NewEncoder(w).Encode(map[string]any{"model": "upstream", "inferencePath": path})
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": "disconnect", "permit": "one", "endpoint": upstream.URL, "apiKey": "private"})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					var final map[string]any
					_ = json.NewDecoder(r.Body).Decode(&final)
					final = meterFixtureSelectedEvidence(r, final)
					finalized <- final
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer control.Close()
			g := newGateway(control.URL, "internal", "g", []string{"127.0.0.1"})
			r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"deployment","messages":[{"role":"user","content":"hi"}],"max_tokens":20,"stream":true}`)).WithContext(ctx)
			r.Header.Set("X-Api-Key", "consumer")
			r.Header.Set("Authorization", "Bearer ses_client_login")
			w := &disconnectWriter{header: http.Header{}, cancel: cancel}
			started := time.Now()
			g.ServeHTTP(w, r)
			if time.Since(started) > time.Second {
				t.Fatal("write failure held execution for usage")
			}
			select {
			case <-closed:
			case <-time.After(300 * time.Millisecond):
				t.Fatal("failed downstream write did not promptly close producer")
			}
			final := <-finalized
			output := float64(2)
			if path == "/v1/messages" {
				output = 0
			}
			if calls.Load() != 1 || w.writes != 1 || final["status"] != "cancelled" || final["executionClosed"] != true || final["inputTokens"] != float64(10) || final["outputTokens"] != output {
				t.Fatalf("lost observed-only evidence: calls=%d writes=%d final=%v", calls.Load(), w.writes, final)
			}
		})
	}
}
func TestFailedResponsesRetainsReportedUsage(t *testing.T) {
	ev := evidence{responses: true}
	ev.feed([]byte("data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":3}}}\n\n"))
	if !ev.invalid || ev.complete() || ev.input == nil || *ev.input != 10 || ev.output == nil || *ev.output != 3 {
		t.Fatalf("lost observed usage %+v", ev)
	}
}

type stopOnCloseBody struct {
	first  []byte
	closed chan struct{}
	once   sync.Once
	reads  atomic.Int32
}

func (b *stopOnCloseBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	if len(b.first) > 0 {
		n := copy(p, b.first)
		b.first = b.first[n:]
		return n, nil
	}
	<-b.closed
	return 0, io.ErrClosedPipe
}
func (b *stopOnCloseBody) Close() error { b.once.Do(func() { close(b.closed) }); return nil }

type failedDownstreamWriter struct {
	header http.Header
	err    error
}

func (w *failedDownstreamWriter) Header() http.Header       { return w.header }
func (w *failedDownstreamWriter) WriteHeader(int)           {}
func (w *failedDownstreamWriter) Write([]byte) (int, error) { return 0, w.err }

func TestDownstreamWriteFailureWithoutCallerCancellationStopsProducer(t *testing.T) {
	for _, writeErr := range []error{syscall.EPIPE, context.DeadlineExceeded} {
		t.Run(writeErr.Error(), func(t *testing.T) {
			finalized := make(chan map[string]any, 1)
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					io.WriteString(w, `{"requestId":"write-failure","permit":"permit","endpoint":"http://127.0.0.1:1"}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					var final map[string]any
					_ = json.NewDecoder(r.Body).Decode(&final)
					final = meterFixtureSelectedEvidence(r, final)
					finalized <- final
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			body := &stopOnCloseBody{closed: make(chan struct{}), first: []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":2}}\n\n")}
			g := newGateway(api.URL, "internal", "fixture", nil)
			g.egress = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: body}, nil
			})}
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hi"}],"stream":true}`))
			r.Header.Set("X-Api-Key", "fixture")
			r.Header.Set("Authorization", "Bearer ses_fixture")
			done := make(chan struct{})
			go func() {
				defer close(done)
				g.ServeHTTP(&failedDownstreamWriter{header: http.Header{}, err: writeErr}, r)
			}()
			select {
			case <-done:
			case <-time.After(300 * time.Millisecond):
				body.Close()
				<-done
				t.Fatal("failed downstream kept reading upstream for usage")
			}
			if r.Context().Err() != nil || body.reads.Load() != 1 {
				t.Fatalf("required caller cancellation or drained extra reads=%d", body.reads.Load())
			}
			select {
			case <-body.closed:
			default:
				t.Fatal("producer body not closed")
			}
			final := <-finalized
			if final["inputTokens"] != float64(12) || final["outputTokens"] != float64(2) || final["executionClosed"] != true {
				t.Fatalf("observed evidence lost %v", final)
			}
		})
	}
}
