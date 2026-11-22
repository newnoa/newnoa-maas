package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueDeadlineDuringPollReturnsTimeout(t *testing.T) {
	release := make(chan struct{})
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer api.Close()
	defer close(release)
	g := newGateway(api.URL, "internal", "fixture", nil)
	dispatch := &dispatchResult{ID: "queue", Permit: "permit", admissionState: admissionState{AdmissionStatus: "queued", RetryAfterMillis: 1, QueueDeadlineAt: time.Now().Add(30 * time.Millisecond)}}
	err := g.waitForAdmission(context.Background(), dispatch)
	var failure *controlError
	if !errors.As(err, &failure) || failure.status != 408 {
		t.Fatalf("deadline returned %v, want408", err)
	}
}

func TestPoolQueuePromotionInstallsCredentialBeforeSingleUpstreamCall(t *testing.T) {
	var calls, authorizations atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-promoted-key" {
			t.Error("promotion credential was not installed")
		}
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	defer upstream.Close()
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			authorizations.Add(1)
			fmt.Fprint(w, `{"model":"fixture"}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(map[string]any{"requestId": "pool-queue", "permit": "permit_fixture", "admissionStatus": "queued", "pooledCredential": true, "retryAfterMillis": 1, "queueDeadlineAt": time.Now().Add(time.Minute)})
		case strings.HasSuffix(r.URL.Path, "/poll"):
			json.NewEncoder(w).Encode(map[string]any{"admissionStatus": "reserved", "pooledCredential": true, "endpoint": upstream.URL, "apiKey": "fixture-promoted-key"})
		default:
			fmt.Fprint(w, `{}`)
		}
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hello"}],"max_tokens":16}`))
	r.Header.Set("X-Api-Key", "fixture")
	r.Header.Set("Authorization", "Bearer ses_client_login")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 200 || calls.Load() != 1 || authorizations.Load() != 1 {
		t.Fatalf("status=%d upstream=%d authorizations=%d body=%s", w.Code, calls.Load(), authorizations.Load(), w.Body.String())
	}
}

func TestPoolQueueRejectsPromotionWithoutCredential(t *testing.T) {
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"admissionStatus":"reserved"}`) }))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", nil)
	dispatch := &dispatchResult{ID: "queue", Permit: "permit", PooledCredential: true, admissionState: admissionState{AdmissionStatus: "queued", RetryAfterMillis: 1, QueueDeadlineAt: time.Now().Add(time.Minute)}}
	if err := g.waitForAdmission(context.Background(), dispatch); err == nil {
		t.Fatal("pool promotion accepted without a credential")
	}
}

func TestBandwidthFailsClosedOnMissingGrant(t *testing.T) {
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{}`) }))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", nil)
	reader := newBandwidthReader(io.NopCloser(strings.NewReader("payload")), context.Background(), g, "limited")
	data, err := io.ReadAll(reader)
	if err == nil || len(data) != 0 {
		t.Fatalf("invalid grant forwarded %q error=%v", data, err)
	}
}

func TestBandwidthReaderBatchesAvailableBytesWithinGrant(t *testing.T) {
	data := strings.Repeat("x", 70000)
	var calls atomic.Int32
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ Bytes, BatchBytes, BandwidthProtocol int }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Bytes < 1 || request.Bytes > bandwidthReadSize || request.BatchBytes > bandwidthBatchSize || request.BandwidthProtocol != 2 {
			t.Errorf("grant bytes = %d", request.Bytes)
		}
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]int{"bandwidthProtocol": 2, "waitMillis": 0, "grantMillis": 1, "grantedBytes": request.BatchBytes})
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", nil)
	reader := newBandwidthReader(io.NopCloser(strings.NewReader(data)), context.Background(), g, "limited")
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != data || calls.Load() != 1 {
		t.Fatalf("bytes=%d grant calls=%d", len(got), calls.Load())
	}
}

func TestBandwidthReaderPacesBytesAcrossReservedGrant(t *testing.T) {
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ BatchBytes int }
		json.NewDecoder(r.Body).Decode(&request)
		json.NewEncoder(w).Encode(map[string]int{"bandwidthProtocol": 2, "grantedBytes": request.BatchBytes, "waitMillis": 0, "grantMillis": 40})
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", nil)
	reader := newBandwidthReader(io.NopCloser(strings.NewReader(strings.Repeat("x", 64<<10))), context.Background(), g, "limited")
	start := time.Now()
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 35*time.Millisecond {
		t.Fatalf("reserved bytes were delivered too quickly: %s", elapsed)
	}
}

func TestBandwidthReaderStopsBeforeDeliveringAfterQueueWaitCancellation(t *testing.T) {
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ BatchBytes int }
		json.NewDecoder(r.Body).Decode(&request)
		json.NewEncoder(w).Encode(map[string]int{"bandwidthProtocol": 2, "grantedBytes": request.BatchBytes, "waitMillis": 500, "grantMillis": 100})
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", nil)
	ctx, cancel := context.WithCancel(context.Background())
	reader := newBandwidthReader(io.NopCloser(strings.NewReader("reserved")), ctx, g, "limited")
	time.AfterFunc(20*time.Millisecond, cancel)
	buffer := make([]byte, 8)
	n, err := reader.Read(buffer)
	if n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("delivered %d bytes after cancellation, error=%v", n, err)
	}
	_ = reader.Close()
}

func TestGatewayWaitsForCharityAdmission(t *testing.T) {
	for _, scenario := range []struct {
		name                   string
		pollStatus, wantStatus int
		cancel                 bool
	}{
		{"promoted", 200, 200, false},
		{"expired", 408, 408, false},
		{"control unavailable", 503, 503, false},
		{"transient contention", 429, 200, false},
		{"caller cancelled", 200, 503, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var polls, executions atomic.Int32
			var finalized string
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				executions.Add(1)
				if polls.Load() < 2 {
					t.Error("queued request reached model before promotion")
				}
				fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
			}))
			defer upstream.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					fmt.Fprint(w, `{"model":"fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					var input map[string]any
					json.NewDecoder(r.Body).Decode(&input)
					if _, spoofed := input["transportPath"]; spoofed {
						t.Error("trusted caller-supplied direct path")
					}
					json.NewEncoder(w).Encode(map[string]any{"requestId": "queue-fixture", "permit": "permit_fixture", "endpoint": upstream.URL, "admissionStatus": "queued", "retryAfterMillis": 1, "queueDeadlineAt": time.Now().Add(time.Minute)})
				case strings.HasSuffix(r.URL.Path, "/poll"):
					n := polls.Add(1)
					if scenario.cancel {
						cancel()
						return
					}
					if scenario.pollStatus != 200 && !(scenario.pollStatus == 429 && n > 1) {
						w.WriteHeader(scenario.pollStatus)
						return
					}
					status := "queued"
					if n >= 2 {
						status = "reserved"
					}
					json.NewEncoder(w).Encode(map[string]any{"admissionStatus": status, "retryAfterMillis": 1})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					var input struct{ Status string }
					json.NewDecoder(r.Body).Decode(&input)
					finalized = input.Status
					fmt.Fprint(w, `{}`)
				default:
					fmt.Fprint(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hello"}],"max_tokens":16}`)).WithContext(ctx)
			r.Header.Set("X-Api-Key", "fixture")
			r.Header.Set("Authorization", "Bearer ses_client_login")
			r.Header.Set("X-Newnoa-Path", "direct")
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			if w.Code != scenario.wantStatus {
				t.Fatalf("status %d want %d: %s", w.Code, scenario.wantStatus, w.Body.String())
			}
			if scenario.wantStatus == http.StatusRequestTimeout && !strings.Contains(w.Body.String(), "Request timed out; unable to connect") {
				t.Fatalf("timeout must explain connection failure: %s", w.Body.String())
			}
			if scenario.wantStatus == 200 {
				if executions.Load() != 1 || finalized != "succeeded" {
					t.Fatalf("executions=%d finalized=%s", executions.Load(), finalized)
				}
			} else if executions.Load() != 0 || finalized != "cancelled" {
				t.Fatalf("queue failure executed=%d finalized=%s", executions.Load(), finalized)
			}
		})
	}
}

func TestGatewayPacesFRPRequestAndResponse(t *testing.T) {
	for _, limited := range []bool{false, true} {
		t.Run(fmt.Sprint(limited), func(t *testing.T) {
			var charged atomic.Int64
			var grantCalls atomic.Int32
			response := `{"choices":[{"message":{"role":"assistant","content":"` + strings.Repeat("x", 70000) + `"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, response) }))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					fmt.Fprint(w, `{"model":"fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": "bandwidth-fixture", "permit": "permit_fixture", "endpoint": upstream.URL, "membership": map[string]any{"applies": limited}})
				case strings.HasSuffix(r.URL.Path, "/bandwidth"):
					var input struct {
						Bytes, BatchBytes, BandwidthProtocol int
						GatewayID                            string
					}
					if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
						t.Error(err)
					}
					if input.Bytes < 1 || input.Bytes > bandwidthReadSize || input.BatchBytes > bandwidthBatchSize || input.BandwidthProtocol != 2 || input.GatewayID != "fixture" {
						t.Errorf("invalid byte grant: %+v", input)
					}
					grantCalls.Add(1)
					charged.Add(int64(input.BatchBytes))
					json.NewEncoder(w).Encode(map[string]int{"bandwidthProtocol": 2, "grantedBytes": input.BatchBytes, "waitMillis": 1, "grantMillis": 1})
				default:
					fmt.Fprint(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			raw := `{"model":"fixture","messages":[{"role":"user","content":"hello"}],"max_tokens":16}`
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(raw))
			r.Header.Set("X-Api-Key", "fixture")
			r.Header.Set("Authorization", "Bearer ses_client_login")
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatalf("status%d: %s", w.Code, w.Body.String())
			}
			if limited && charged.Load() != int64(len(raw)+len(response)) {
				t.Fatalf("charged %d want request+response=%d", charged.Load(), len(raw)+len(response))
			}
			if limited && grantCalls.Load() > 4 {
				t.Fatalf("grant call count did not batch stream data: %d", grantCalls.Load())
			}
			if !limited && charged.Load() != 0 {
				t.Fatal("unlimited direct/charity request charged bandwidth")
			}
		})
	}
}
