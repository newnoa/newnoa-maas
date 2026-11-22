package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectorExecutionDeadlineAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, wire     string
		budget         time.Duration
		reject, expire bool
	}{
		{name: "API long", budget: 20 * time.Minute},
		{name: "legacy missing", budget: 5 * time.Minute},
		{name: "short expiry", budget: 80 * time.Millisecond, expire: true},
		{name: "null", wire: `null`, reject: true},
		{name: "empty", wire: `""`, reject: true},
		{name: "non string", wire: `1800`, reject: true},
		{name: "malformed", wire: `"garbage"`, reject: true},
		{name: "past", budget: -time.Second, reject: true},
		{name: "too far", budget: 31 * time.Minute, reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wanted time.Time
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hostedMeterFixtureAck(t, w, r) {
					return
				}
				b := map[string]any{"endpoint": "http://127.0.0.1:1", "model": "approved"}
				if tc.name != "legacy missing" {
					if tc.wire != "" {
						b["executionDeadlineAt"] = json.RawMessage(tc.wire)
					} else {
						wanted = time.Now().Add(tc.budget)
						b["executionDeadlineAt"] = wanted.Format(time.RFC3339Nano)
					}
				}
				json.NewEncoder(w).Encode(b)
			}))
			defer api.Close()
			var calls int
			n := &connector{purpose: "serving", control: api.URL, credential: "node_fixture", client: api.Client(), upstream: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				if tc.reject {
					t.Error("invalid API deadline executed upstream")
					return nil, errors.New("unexpected execution")
				}
				deadline, ok := r.Context().Deadline()
				if !ok {
					t.Error("missing execution deadline")
				} else if tc.name == "legacy missing" {
					if left := time.Until(deadline); left < 4*time.Minute || left > 5*time.Minute {
						t.Errorf("legacy budget %v", left)
					}
				} else if !deadline.Equal(wanted) {
					t.Errorf("deadline changed: got %v want %v", deadline, wanted)
				}
				if tc.expire {
					select {
					case <-r.Context().Done():
						return nil, r.Context().Err()
					case <-time.After(time.Second):
						t.Error("API execution deadline did not stop request")
						return nil, errors.New("test wait expired")
					}
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"choices":[]}`))}, nil
			})}}
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"approved","executionDeadlineAt":"2099-01-01T00:00:00Z"}`))
			r.Header.Set("X-Newnoa-Request", hostedFixtureID)
			r.Header.Set("X-Newnoa-Permit", "permit_fixture")
			r.Header.Set("X-Newnoa-Gateway", "fixture")
			r.Header.Set("X-Newnoa-Epoch", "1")
			r.Header.Set("X-Newnoa-Execution-Deadline-At", "2099-01-01T00:00:00Z")
			out := httptest.NewRecorder()
			n.ServeHTTP(out, r)
			if tc.reject {
				if calls != 0 || out.Code != 502 {
					t.Fatalf("invalid metadata not closed: calls=%d HTTP=%d", calls, out.Code)
				}
			} else if calls != 1 {
				t.Fatalf("upstream calls %d", calls)
			}
			if tc.expire && (out.Code != 502 || out.Header().Get("X-Newnoa-Outcome") != "unknown") {
				t.Fatalf("timeout evidence changed: HTTP=%d outcome=%q", out.Code, out.Header().Get("X-Newnoa-Outcome"))
			}
		})
	}
}

func TestHostedExecutionDeadlineStopsBeforeOrDuringUpstream(t *testing.T) {
	for _, mode := range []string{"invalid", "null", "past", "too far", "short"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				select {
				case <-r.Context().Done():
				case <-time.After(time.Second):
					io.WriteString(w, `{"choices":[]}`)
				}
			}))
			defer upstream.Close()
			reports := make(chan usageReport, 1)
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hostedMeterFixtureAck(t, w, r) {
					return
				}
				switch {
				case r.URL.Path == "/api/v1/me":
					io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					b := map[string]any{"accountId": hostedFixtureID, "keyId": hostedFixtureID, "requestId": hostedFixtureID, "reportToken": hostedFixtureReportToken, "endpoint": "http://upstream.test/v1", "model": "actual", "revision": 1, "apiKey": "fixture"}
					switch mode {
					case "invalid":
						b["executionDeadlineAt"] = "bad"
					case "null":
						b["executionDeadlineAt"] = nil
					case "past":
						b["executionDeadlineAt"] = time.Now().Add(-time.Second).Format(time.RFC3339Nano)
					case "too far":
						b["executionDeadlineAt"] = time.Now().Add(time.Hour).Format(time.RFC3339Nano)
					case "short":
						b["executionDeadlineAt"] = time.Now().Add(100 * time.Millisecond).Format(time.RFC3339Nano)
					}
					json.NewEncoder(w).Encode(b)
				case strings.HasSuffix(r.URL.Path, "/finish"):
					var report usageReport
					decodeHostedFixtureClosure(t, r, &report)
					reports <- report
					io.WriteString(w, `{"finalized":true}`)
				default:
					t.Error("unexpected API route", r.URL.Path)
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control, c.onlineSupply = api.URL, fixturePublicPolicy(upstream)
			defer c.shutdown()
			raw := []byte(`{"model":"` + hostedFixtureID + `","stream":true}`)
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw)))
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("X-Api-Key", "nn_fixture")
			r.Header.Set("Content-Type", "application/json")
			out := httptest.NewRecorder()
			c.executeHosted(out, r, hostedFixtureID, raw)
			var report usageReport
			select {
			case report = <-reports:
			case <-time.After(time.Second):
				t.Fatal("admitted request was not finalized")
			}
			wantCalls, wantStatus := int32(0), "failed"
			if mode == "short" {
				wantCalls, wantStatus = 1, "unknown"
			}
			if calls.Load() != wantCalls || out.Code != 502 || report.Status != wantStatus || report.TransportConfirmed {
				t.Fatalf("mode=%s calls=%d HTTP=%d report=%+v", mode, calls.Load(), out.Code, report)
			}
		})
	}
}

func TestExecutionContextPreservesParentCancellation(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	stop()
	// Exercise the real connector path; no upstream bytes may be sent after cancellation.
	n := &connector{purpose: "serving", control: "http://127.0.0.1:1", credential: "fixture", client: connectorControlClient()}
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"approved"}`)).WithContext(parent)
	r.Header.Set("X-Newnoa-Request", hostedFixtureID)
	r.Header.Set("X-Newnoa-Permit", "permit_fixture")
	r.Header.Set("X-Newnoa-Gateway", "fixture")
	r.Header.Set("X-Newnoa-Epoch", "1")
	out := httptest.NewRecorder()
	n.ServeHTTP(out, r)
	if out.Code != 409 {
		t.Fatal(out.Code)
	}
}

func TestExecutionContextValidationBoundaries(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		raw    json.RawMessage
		want   time.Time
		reject bool
	}{
		{name: "legacy", want: now.Add(5 * time.Minute)},
		{name: "API absolute", raw: json.RawMessage(`"2026-11-14T00:30:00Z"`), want: now.Add(30 * time.Minute)},
		{name: "clock tolerance", raw: json.RawMessage(`"2026-11-14T00:30:05Z"`), want: now.Add(30*time.Minute + 5*time.Second)},
		{name: "beyond tolerance", raw: json.RawMessage(`"2026-11-14T00:30:05.000000001Z"`), reject: true},
		{name: "exact now", raw: json.RawMessage(`"2026-11-14T00:00:00Z"`), reject: true},
		{name: "null", raw: json.RawMessage(`null`), reject: true},
		{name: "array", raw: json.RawMessage(`[]`), reject: true},
		{name: "invalid JSON", raw: json.RawMessage(`"2026`), reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel, err := executionContext(context.Background(), tc.raw, now)
			if tc.reject {
				if err == nil {
					cancel()
					t.Fatal("invalid metadata accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer cancel()
			got, _ := ctx.Deadline()
			if !got.Equal(tc.want) {
				t.Fatalf("deadline %v want %v", got, tc.want)
			}
		})
	}
	// Earlier caller cancellation/deadlines remain stricter than the API grant.
	parent, stop := context.WithDeadline(context.Background(), now.Add(time.Minute))
	defer stop()
	ctx, cancel, err := executionContext(parent, json.RawMessage(`"2026-11-14T00:30:00Z"`), now)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	got, _ := ctx.Deadline()
	if !got.Equal(now.Add(time.Minute)) {
		t.Fatal("caller deadline extended")
	}
}

func TestAPIExecutionHeadersWaitForDeadlineNotLegacyHeaderCap(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(50 * time.Millisecond):
			io.WriteString(w, `{"data":[]}`)
		}
	}))
	defer upstream.Close()
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.ResponseHeaderTimeout = 5 * time.Millisecond
	tr := withImageResponseTimeout(base)
	client := &http.Client{Transport: tr}
	defer client.CloseIdleConnections()
	for _, path := range []string{"/v1/chat/completions", "/custom/v1/messages", "/custom/v1/images/generations", imageGenerationPath} {
		ctx, cancel, err := executionContext(context.Background(), json.RawMessage(`"`+time.Now().Add(time.Second).Format(time.RFC3339Nano)+`"`), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequestWithContext(ctx, "POST", upstream.URL+path, nil)
		res, err := client.Do(req)
		cancel()
		if err != nil {
			t.Fatalf("API authorized header wait rejected %s: %v", path, err)
		}
		res.Body.Close()
	}
	req, _ := http.NewRequest("POST", upstream.URL+"/v1/chat/completions", nil)
	req.Header.Set("X-Newnoa-Execution-Deadline-At", time.Now().Add(time.Hour).Format(time.RFC3339Nano))
	if res, err := client.Do(req); err == nil {
		res.Body.Close()
		t.Fatal("caller header relaxed ordinary header limit")
	}
	// Context remains the hard limit even before response headers, including Images.
	ctx, cancel, err := executionContext(context.Background(), json.RawMessage(`"`+time.Now().Add(10*time.Millisecond).Format(time.RFC3339Nano)+`"`), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	req, _ = http.NewRequestWithContext(ctx, "POST", upstream.URL+imageGenerationPath, nil)
	if res, err := client.Do(req); err == nil {
		res.Body.Close()
		t.Fatal("image headers escaped API deadline")
	}
}

func TestAuthorizedInferenceDoesNotRelaxImageDownloadHeaderTimeout(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(50 * time.Millisecond):
			io.WriteString(w, "image")
		}
	}))
	defer upstream.Close()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = 5 * time.Millisecond
	client := &http.Client{Transport: withImageResponseTimeout(tr)}
	defer client.CloseIdleConnections()
	ctx, cancel, err := executionContext(context.Background(), json.RawMessage(`"`+time.Now().Add(time.Second).Format(time.RFC3339Nano)+`"`), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", upstream.URL+"/generated.png", nil)
	if res, err := client.Do(req); err == nil {
		res.Body.Close()
		t.Fatal("execution marker extended image download first-header limit")
	}
}

func TestHostedDeadlineDuringStreamRetainsTransportAndPartialUsage(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer upstream.Close()
	reports := make(chan usageReport, 1)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		switch {
		case r.URL.Path == "/api/v1/me":
			io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(map[string]any{"accountId": hostedFixtureID, "keyId": hostedFixtureID, "requestId": hostedFixtureID, "reportToken": hostedFixtureReportToken, "endpoint": "http://upstream.test/v1", "model": "actual", "revision": 1, "apiKey": "fixture", "executionDeadlineAt": time.Now().Add(2 * time.Second).Format(time.RFC3339Nano)})
		case strings.HasSuffix(r.URL.Path, "/finish"):
			var report usageReport
			decodeHostedFixtureClosure(t, r, &report)
			reports <- report
			io.WriteString(w, `{"finalized":true}`)
		default:
			t.Error("unexpected route", r.URL.Path)
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control, c.onlineSupply = api.URL, fixturePublicPolicy(upstream)
	defer c.shutdown()
	raw := []byte(`{"model":"` + hostedFixtureID + `","stream":true}`)
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(string(raw)))
	r.Header.Set("Authorization", "Bearer ses_fixture")
	r.Header.Set("X-Api-Key", "nn_fixture")
	r.Header.Set("Content-Type", "application/json")
	out := httptest.NewRecorder()
	c.executeHosted(out, r, hostedFixtureID, raw)
	report := <-reports
	if report.Status != "unknown" || !report.TransportConfirmed || report.Input == nil || *report.Input != 12 || report.Output == nil || *report.Output != 3 || !strings.Contains(out.Body.String(), "partial") {
		t.Fatalf("deadline lost stream evidence: HTTP=%d report=%+v body=%s", out.Code, report, out.Body.String())
	}
}

func TestConnectorExecutionBudgetStartsAfterConsumption(t *testing.T) {
	var deadline time.Time
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		time.Sleep(80 * time.Millisecond)
		deadline = time.Now().Add(150 * time.Millisecond)
		json.NewEncoder(w).Encode(map[string]any{"endpoint": "http://127.0.0.1:1", "model": "approved", "executionDeadlineAt": deadline.Format(time.RFC3339Nano)})
	}))
	defer api.Close()
	n := &connector{purpose: "serving", control: api.URL, credential: "fixture", client: api.Client(), upstream: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got, ok := r.Context().Deadline()
		if !ok || !got.Equal(deadline) || time.Until(got) < 100*time.Millisecond {
			t.Errorf("admission consumed execution budget: %v", time.Until(got))
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}}
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"approved"}`))
	r.Header.Set("X-Newnoa-Request", hostedFixtureID)
	r.Header.Set("X-Newnoa-Permit", "permit_fixture")
	r.Header.Set("X-Newnoa-Gateway", "fixture")
	r.Header.Set("X-Newnoa-Epoch", "1")
	out := httptest.NewRecorder()
	n.ServeHTTP(out, r)
	if out.Code != 200 {
		t.Fatal(out.Code, out.Body.String())
	}
}
