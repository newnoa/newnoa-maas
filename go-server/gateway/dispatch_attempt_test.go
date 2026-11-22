package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLostDispatchDeliveryCompensatesWithoutInferenceReplay(t *testing.T) {
	for _, mode := range []string{"committed drop", "malformed JSON", "empty JSON", "null JSON", "missing permit", "caller cancelled late commit", "abort ack missing", "abort ack false", "abort transport lost", "explicit rejection", "normal admission"} {
		t.Run(mode, func(t *testing.T) {
			const model = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
			var reservations, aborts, upstream, consumes atomic.Int32
			var mu sync.Mutex
			var original map[string]any
			committed, tombstone := false, false
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), proxySessionContext{}, "ses_caller"))
			defer cancel()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					fmt.Fprintf(w, `{"model":"actual","deploymentId":%q}`, model)
				case r.URL.Path == "/api/v1/internal/dispatches":
					reservations.Add(1)
					mu.Lock()
					original = body
					mu.Unlock()
					if mode == "explicit rejection" {
						http.Error(w, "unavailable", 503)
						return
					}
					if mode == "normal admission" {
						fmt.Fprint(w, `{"requestId":"normal","permit":"permit","endpoint":"http://127.0.0.1:1"}`)
						return
					}
					mu.Lock()
					committed = mode != "caller cancelled late commit"
					mu.Unlock()
					if mode == "caller cancelled late commit" {
						cancel()
						<-r.Context().Done()
						return
					}
					if mode == "empty JSON" {
						fmt.Fprint(w, `{}`)
						return
					}
					if mode == "null JSON" {
						fmt.Fprint(w, `null`)
						return
					}
					if mode == "missing permit" {
						fmt.Fprint(w, `{"requestId":"missing"}`)
						return
					}
					if mode == "malformed JSON" {
						fmt.Fprint(w, `{"requestId":`)
						return
					}
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
				case r.URL.Path == "/api/v1/internal/dispatch-attempts/abort":
					aborts.Add(1)
					if r.Header.Get("Authorization") != "Bearer internal-fixture" {
						t.Error("abort lost internal authentication")
					}
					mu.Lock()
					if !reflect.DeepEqual(body, original) {
						t.Errorf("abort changed original binding body=%v original=%v", body, original)
					}
					if mode == "caller cancelled late commit" {
						tombstone = true
						committed = !tombstone
					} else {
						committed = false
						tombstone = true
					}
					mu.Unlock()
					switch mode {
					case "abort ack missing":
						fmt.Fprint(w, `{}`)
					case "abort ack false":
						fmt.Fprint(w, `{"accepted":false}`)
					case "abort transport lost":
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = conn.Close()
					default:
						fmt.Fprint(w, `{"accepted":true}`)
					}
				case strings.HasSuffix(r.URL.Path, "/consume"):
					consumes.Add(1)
					fmt.Fprint(w, `{}`)
				default:
					fmt.Fprint(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal-fixture", "fixture", nil)
			g.egress = &http.Client{Transport: executionRoundTripper(func(r *http.Request) (*http.Response, error) {
				upstream.Add(1)
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))}, nil
			})}
			callerAttempt := uuid.NewString()
			req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"alias","attemptId":"`+callerAttempt+`","messages":[{"role":"user","content":"hello"}]}`)).WithContext(ctx)
			req.Header.Set("Authorization", "Bearer ses_caller")
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Idempotency-Key", "caller-idem")
			out := httptest.NewRecorder()
			g.ServeHTTP(out, req)
			mu.Lock()
			defer mu.Unlock()
			attempt, _ := original["attemptId"].(string)
			if uuid.Validate(attempt) != nil || attempt == callerAttempt {
				t.Fatalf("attempt missing, not UUID or caller controlled: %q", attempt)
			}
			if original["model"] != model || original["session"] != "ses_caller" || original["secret"] != "nn_fixture" || original["gatewayId"] != "fixture" || original["inferencePath"] != "/v1/chat/completions" || original["idempotencyKey"] != "caller-idem" {
				t.Fatalf("admission binding changed: %v", original)
			}
			for _, key := range []string{"bodyDigest", "executionDigest"} {
				if value, ok := original[key].(string); !ok || len(value) != 64 {
					t.Fatalf("missing digest %s", key)
				}
			}
			wantAbort := int32(1)
			if mode == "explicit rejection" || mode == "normal admission" {
				wantAbort = 0
			}
			if reservations.Load() != 1 || aborts.Load() != wantAbort {
				t.Fatalf("admission=%d aborts=%d wantAbort=%d", reservations.Load(), aborts.Load(), wantAbort)
			}
			if mode == "normal admission" {
				if upstream.Load() != 1 || consumes.Load() != 1 {
					t.Fatal("normal inference blocked")
				}
			} else if upstream.Load() != 0 || consumes.Load() != 0 {
				t.Fatalf("ambiguous admission executed/replayed: upstream=%d consumes=%d", upstream.Load(), consumes.Load())
			}
			if wantAbort == 1 && (!tombstone || committed) {
				t.Fatalf("late admission escaped abort fixture committed=%v tombstone=%v", committed, tombstone)
			}
		})
	}
}

func TestDispatchAbortIndependentBoundAndAcknowledgement(t *testing.T) {
	// Inspect the real cleanup request context without sleeping8sec.
	var calls atomic.Int32
	g := newGateway("http://fixture.invalid", "internal", "fixture", nil)
	parent, stop := context.WithCancel(context.WithValue(context.Background(), proxySessionContext{}, "ses_caller"))
	defer stop()
	g.client = &http.Client{Transport: executionRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if strings.HasSuffix(r.URL.Path, "/dispatches") {
			stop()
			return nil, io.ErrUnexpectedEOF
		}
		if r.URL.Path != "/api/v1/internal/dispatch-attempts/abort" {
			t.Fatalf("wrong path %s", r.URL.Path)
		}
		if r.Context().Err() != nil {
			t.Fatal("abort inherited cancelled admission")
		}
		deadline, ok := r.Context().Deadline()
		left := time.Until(deadline)
		if !ok || left <= 0 || left > 8*time.Second {
			t.Fatalf("cleanup unbounded: %v", left)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["session"] != "ses_caller" {
			t.Fatal("cleanup lost context identity")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"accepted":true}`))}, nil
	})}
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hello"}]}`)).WithContext(parent)
	// Authorize also uses control; inject a response for it before the admission-failure transport.
	original := g.client.Transport
	g.client.Transport = executionRoundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/authorize") {
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"model":"fixture"}`))}, nil
		}
		return original.RoundTrip(r)
	})
	req.Header.Set("Authorization", "Bearer ses_caller")
	req.Header.Set("X-Api-Key", "fixture")
	g.ServeHTTP(httptest.NewRecorder(), req)
	if calls.Load() != 2 {
		t.Fatalf("admission/abort calls=%d", calls.Load())
	}
}

func TestDispatchAbortRequiresBooleanAcceptance(t *testing.T) {
	for _, body := range []string{`{"accepted":true}`, `{}`, `null`, `{"accepted":null}`, `{"accepted":false}`, `{"accepted":"true"}`, `{"accepted":1}`, `{"accepted":`} {
		t.Run(body, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", nil)
			err := g.abortDispatchAttempt(context.Background(), map[string]any{"attemptId": uuid.NewString()})
			if (err == nil) != (body == `{"accepted":true}`) {
				t.Fatalf("ack=%s err=%v", body, err)
			}
		})
	}
}
