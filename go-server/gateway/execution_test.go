package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecutionTimeoutMetadata(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want time.Duration
	}{
		{"", 5 * time.Minute}, {"1800", 30 * time.Minute}, {"1", time.Second},
		{"null", 0}, {"0", 0}, {"-1", 0}, {"1801", 0}, {"1.0", 0}, {"1e3", 0}, {`"1800"`, 0}, {"9223372036854775808", 0}, {"true", 0},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := executionTimeout(json.RawMessage(tc.raw))
			if (err != nil) != (tc.want == 0) || got != tc.want {
				t.Fatalf("got %v %v want %v", got, err, tc.want)
			}
		})
	}
}

func TestExecutionDeadlineMetadata(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		raw  string
		want time.Duration
	}{
		{"", 5 * time.Minute}, {`"2026-11-14T00:30:00Z"`, 30 * time.Minute}, {`"2026-11-14T08:00:00.010+08:00"`, 10 * time.Millisecond},
		{"null", 0}, {"123", 0}, {`"nonsense"`, 0}, {`"2026-11-14T00:00:00Z"`, 0}, {`"2026-11-14T00:30:00.500Z"`, 30*time.Minute + 500*time.Millisecond}, {`"2026-11-14T00:30:01Z"`, 30*time.Minute + time.Second}, {`"2026-11-14T00:30:04.900Z"`, 30*time.Minute + 4900*time.Millisecond}, {`"2026-11-14T00:30:05.100Z"`, 0}, {`"2026-11-14T00:31:00Z"`, 0},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := executionDeadline(json.RawMessage(tc.raw), 30*time.Minute, now)
			if (err != nil) != (tc.want == 0) || (err == nil && got.Sub(now) != tc.want) {
				t.Fatalf("got %v %v want %v", got, err, tc.want)
			}
		})
	}
	// A missing consume deadline may never extend a smaller admitted budget.
	got, err := executionDeadline(nil, time.Second, now)
	if err != nil || got.Sub(now) != time.Second {
		t.Fatalf("legacy cap %v %v", got, err)
	}
}

func TestGatewayExecutionAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, admission, promotion, consume string
		node, queued                        bool
		want                                int
		budget                              time.Duration
	}{
		{name: "direct authoritative", admission: `1800`, consume: `"SHORT"`, want: 200, budget: time.Second},
		{name: "legacy missing", want: 200, budget: 5 * time.Minute},
		{name: "malformed admission", admission: `1800.0`, want: 503},
		{name: "malformed consume", admission: `1800`, consume: `null`, want: 502},
		{name: "consume exceeds admission", admission: `1`, consume: `"FAR"`, want: 502},
		{name: "device fresh budget after queue", admission: `1`, promotion: `1`, node: true, queued: true, want: 200, budget: time.Second},
		{name: "device legacy promotion", admission: `1800`, node: true, queued: true, want: 200, budget: 5 * time.Minute},
		{name: "direct fresh budget after queue", admission: `1`, promotion: `1800`, consume: `"FAR"`, queued: true, want: 200, budget: time.Minute},
		{name: "promotion malformed", admission: `1800`, promotion: `1801`, node: true, queued: true, want: 503},
		{name: "promotion replaces admission", admission: `1`, promotion: `1800`, node: true, queued: true, want: 200, budget: 30 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					fmt.Fprint(w, `{"model":"fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					if _, ok := body["executionTimeoutSeconds"]; ok {
						t.Error("caller execution timeout forwarded to control")
					}
					if _, ok := body["executionDeadlineAt"]; ok {
						t.Error("caller deadline forwarded to control")
					}
					value := map[string]any{"requestId": "execution-fixture", "permit": "permit", "endpoint": "http://127.0.0.1:1"}
					if tc.admission != "" {
						value["executionTimeoutSeconds"] = json.RawMessage(tc.admission)
					}
					if tc.node {
						value["nodeId"] = "node"
						value["ownerId"] = "owner"
						value["route"] = "http://127.0.0.1:18101/tunnels/node/1"
					}
					if tc.queued {
						value["admissionStatus"] = "queued"
						value["retryAfterMillis"] = 100
						value["queueDeadlineAt"] = time.Now().Add(time.Second)
					}
					_ = json.NewEncoder(w).Encode(value)
				case strings.HasSuffix(r.URL.Path, "/poll"):
					value := map[string]any{"admissionStatus": "reserved"}
					if tc.promotion != "" {
						value["executionTimeoutSeconds"] = json.RawMessage(tc.promotion)
					}
					_ = json.NewEncoder(w).Encode(value)
				case strings.HasSuffix(r.URL.Path, "/consume"):
					raw := tc.consume
					raw = strings.ReplaceAll(raw, "SHORT", time.Now().Add(time.Second).Format(time.RFC3339Nano))
					raw = strings.ReplaceAll(raw, "FAR", time.Now().Add(time.Minute).Format(time.RFC3339Nano))
					if raw == "" {
						fmt.Fprint(w, `{}`)
					} else {
						fmt.Fprintf(w, `{"executionDeadlineAt":%s}`, raw)
					}
				default:
					fmt.Fprint(w, `{}`)
				}
			}))
			defer api.Close()
			t.Setenv("NEWNOA_FRP_OWNER_ID", "owner")
			t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", "http://127.0.0.1:18101")
			g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			transport := executionRoundTripper(func(r *http.Request) (*http.Response, error) {
				calls.Add(1)
				deadline, ok := r.Context().Deadline()
				remaining := time.Until(deadline)
				want := tc.budget
				if tc.node {
					want += 30 * time.Second
				}
				if !ok || remaining < want-80*time.Millisecond || remaining > want {
					t.Errorf("execution budget %v want %v", remaining, want)
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))}, nil
			})
			g.egress = &http.Client{Transport: transport}
			g.owner = g.egress
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hello"}],"executionTimeoutSeconds":999999,"executionDeadlineAt":"2099-01-01T00:00:00Z"}`))
			r.Header.Set("X-Api-Key", "fixture")
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("X-Newnoa-Execution-Deadline", "2099-01-01T00:00:00Z")
			ctx, cancel := context.WithTimeout(r.Context(), time.Hour)
			defer cancel()
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r.WithContext(ctx))
			if w.Code != tc.want {
				t.Fatalf("status %d want %d body %s", w.Code, tc.want, w.Body.String())
			}
			if (calls.Load() == 1) != (tc.want == 200) {
				t.Fatalf("upstream calls %d", calls.Load())
			}
		})
	}
}

type executionRoundTripper func(*http.Request) (*http.Response, error)

func (f executionRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestPeerLifecycleExecutionHandoff(t *testing.T) {
	for _, handoff := range []bool{false, true} {
		t.Run(fmt.Sprint(handoff), func(t *testing.T) {
			admission, cancel := context.WithCancel(context.Background())
			defer cancel()
			peer, detach, closePeer := peerLifecycle(admission)
			defer closePeer()
			if handoff {
				detach()
			}
			cancel()
			if handoff {
				select {
				case <-peer.Done():
					t.Fatal("execution peer died on admission/client cancellation")
				default:
				}
			} else {
				select {
				case <-peer.Done():
				case <-time.After(time.Second):
					t.Fatal("admission cancellation leaked peer")
				}
			}
			closePeer()
			if peer.Err() == nil {
				t.Fatal("peer lifetime cleanup missing")
			}
		})
	}
}

func TestExecutionHTTPFirstByteTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(30 * time.Millisecond):
		}
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	for _, execution := range []bool{false, true} {
		t.Run(fmt.Sprint(execution), func(t *testing.T) {
			client := &http.Client{Transport: withImageResponseTimeout(&http.Transport{ResponseHeaderTimeout: 5 * time.Millisecond})}
			defer client.CloseIdleConnections()
			req, _ := http.NewRequest("POST", server.URL+"/v1/chat/completions", nil)
			req.Header.Set("X-Newnoa-Execution-Deadline", "2099-01-01T00:00:00Z")
			if execution {
				req = req.WithContext(context.WithValue(req.Context(), executionContextKey{}, true))
			}
			res, err := client.Do(req)
			if res != nil {
				res.Body.Close()
			}
			if (err == nil) != execution {
				t.Fatalf("execution %v error %v", execution, err)
			}
		})
	}
	base := &http.Transport{ResponseHeaderTimeout: 5 * time.Millisecond}
	client := &http.Client{Transport: base}
	long := inferenceHTTPClient(client)
	defer long.CloseIdleConnections()
	if base.ResponseHeaderTimeout != 5*time.Millisecond || long.Transport.(*http.Transport).ResponseHeaderTimeout != 31*time.Minute {
		t.Fatal("peer timeout mutated or unbounded")
	}
}

func TestQuietInferenceStopsAtConsumeDeadlineWithoutReplay(t *testing.T) {
	var calls atomic.Int32
	closed := make(chan struct{}, 1)
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
			closed <- struct{}{}
		case <-release:
		}
	}))
	defer upstream.Close()
	defer close(release)
	finalized := make(chan map[string]any, 1)
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			fmt.Fprint(w, `{"model":"fixture"}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			_ = json.NewEncoder(w).Encode(map[string]any{"requestId": "quiet-fixture", "permit": "permit", "endpoint": upstream.URL, "executionTimeoutSeconds": 1800})
		case strings.HasSuffix(r.URL.Path, "/consume"):
			_ = json.NewEncoder(w).Encode(map[string]any{"executionDeadlineAt": time.Now().Add(60 * time.Millisecond).Format(time.RFC3339Nano)})
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			var final map[string]any
			_ = json.NewDecoder(r.Body).Decode(&final)
			finalized <- final
			fmt.Fprint(w, `{}`)
		default:
			fmt.Fprint(w, `{}`)
		}
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
	defer g.egress.CloseIdleConnections()
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hello"}],"executionDeadlineAt":"2099-01-01T00:00:00Z","executionTimeoutSeconds":999999}`))
	r.Header.Set("X-Api-Key", "fixture")
	r.Header.Set("Authorization", "Bearer ses_fixture")
	w := httptest.NewRecorder()
	started := time.Now()
	g.ServeHTTP(w, r)
	if w.Code != 502 || time.Since(started) > time.Second || calls.Load() != 1 {
		t.Fatalf("status=%d calls=%d elapsed=%v", w.Code, calls.Load(), time.Since(started))
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("quiet upstream transport not closed")
	}
	final := <-finalized
	if final["status"] != "unknown" || final["executionClosed"] != true {
		t.Fatalf("lost unknown evidence: %#v", final)
	}
}

func TestDeviceQueueAndDelayedConsumePreserveExecutionBudget(t *testing.T) {
	for _, budget := range []int{0, 1, 1800} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			t.Setenv("NEWNOA_FRP_OWNER_ID", "owner")
			t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", "http://127.0.0.1:18101")
			var directConsumes atomic.Int32
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					fmt.Fprint(w, `{"model":"fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					value := map[string]any{"requestId": "device-delay", "permit": "permit", "nodeId": "node", "ownerId": "owner", "route": "http://127.0.0.1:18101/tunnels/node/1", "admissionStatus": "queued", "retryAfterMillis": 100, "queueDeadlineAt": time.Now().Add(time.Second)}
					if budget != 0 {
						value["executionTimeoutSeconds"] = budget
					}
					_ = json.NewEncoder(w).Encode(value)
				case strings.HasSuffix(r.URL.Path, "/poll"):
					value := map[string]any{"admissionStatus": "reserved"}
					if budget != 0 {
						value["executionTimeoutSeconds"] = budget
					}
					_ = json.NewEncoder(w).Encode(value)
				case strings.HasSuffix(r.URL.Path, "/consume"):
					directConsumes.Add(1)
					fmt.Fprint(w, `{}`)
				default:
					fmt.Fprint(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", nil)
			g.owner = &http.Client{Transport: executionRoundTripper(func(r *http.Request) (*http.Response, error) {
				wrapper, ok := r.Context().Deadline()
				want := time.Duration(budget) * time.Second
				if budget == 0 {
					want = 5 * time.Minute
				}
				left := time.Until(wrapper)
				if !ok || left > want+30*time.Second || left < want+29*time.Second {
					t.Fatalf("device wrapper=%v expected execution=%v + bounded30s", left, want)
				}
				// Simulate real owner/transit work before Core receives and consumes the permit.
				select {
				case <-r.Context().Done():
					t.Fatal("pre-consume wrapper ended")
				case <-time.After(80 * time.Millisecond):
				}
				consumedAt := time.Now()
				coreDeadline := consumedAt.Add(want)
				if wrapper.Before(coreDeadline) {
					t.Fatalf("wrapper truncated newly consumed execution by %v", coreDeadline.Sub(wrapper))
				}
				// Core remains responsible for the API timestamp, even when the wrapper is longer.
				coreCtx, stop := context.WithDeadline(r.Context(), consumedAt.Add(20*time.Millisecond))
				defer stop()
				<-coreCtx.Done()
				if r.Context().Err() != nil {
					t.Fatal("Core expiry and outer forwarding were conflated")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))}, nil
			})}
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hello"}],"executionTimeoutSeconds":999999}`))
			r.Header.Set("X-Api-Key", "fixture")
			r.Header.Set("Authorization", "Bearer ses_fixture")
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			if w.Code != 200 || directConsumes.Load() != 0 {
				t.Fatalf("status=%d Gateway improperly consumed device permit=%d", w.Code, directConsumes.Load())
			}
		})
	}
}
