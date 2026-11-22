package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMeterCheckpointRequiresExactAcknowledgement(t *testing.T) {
	for _, ack := range []string{`{"accepted":true,"requestId":"request","sequence":1}`, `{}`, `null`, `{"accepted":false,"requestId":"request","sequence":1}`, `{"accepted":"true","requestId":"request","sequence":1}`, `{"accepted":true,"requestId":"other","sequence":1}`, `{"accepted":true,"requestId":"request","sequence":2}`, `{"accepted":true,"requestId":"request","sequence":1.0}`, `{"accepted":true,"requestId":"request"}`} {
		t.Run(ack, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/internal/requests/request/meter" || r.Header.Get("Authorization") != "Bearer internal" {
					t.Fatal("checkpoint route/auth wrong")
				}
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				if body["gatewayId"] != "gateway" || body["permit"] != "one-use" || body["sequence"] != float64(1) {
					t.Errorf("wrong envelope %v", body)
				}
				snapshot := body["snapshot"].(map[string]any)
				if snapshot["responseStarted"] != false || snapshot["estimatedInputTokens"] != float64(12) {
					t.Errorf("start checkpoint claimed execution %v", snapshot)
				}
				for _, key := range []string{"secret", "prompt", "messages", "output", "apiKey"} {
					if body[key] != nil || snapshot[key] != nil {
						t.Errorf("plaintext or credentials field %s", key)
					}
				}
				fmt.Fprint(w, ack)
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "gateway", nil)
			reporter := meterCheckpoint{gateway: g, request: "request", permit: "one-use"}
			err := reporter.persist(context.Background(), map[string]any{"estimator": "text-bpe-v1", "encoding": "cl100k_base", "estimatedInputTokens": 12, "responseStarted": false, "terminalUsage": false})
			valid := ack == `{"accepted":true,"requestId":"request","sequence":1}`
			if (err == nil) != valid || ((reporter.sequence == 1) != valid) {
				t.Fatalf("ack accepted=%v err=%v storedseq=%d", valid, err, reporter.sequence)
			}
		})
	}
}

func TestMeterCheckpointLostAckRetriesIdenticalSequenceAndPayload(t *testing.T) {
	var bodies [][]byte
	g := newGateway("http://fixture.invalid", "internal", "gateway", nil)
	g.client = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > 8*time.Second {
			t.Error("checkpoint unbounded")
		}
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, body)
		if len(bodies) == 1 {
			return nil, io.ErrUnexpectedEOF
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"accepted":true,"requestId":"request","sequence":1}`))}, nil
	})}
	reporter := meterCheckpoint{gateway: g, request: "request", permit: "one-use"}
	if err := reporter.persist(context.Background(), map[string]any{"estimator": "text-bpe-v1", "encoding": "o200k_base", "responseStarted": true, "terminalUsage": false}); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || !reflect.DeepEqual(bodies[0], bodies[1]) || reporter.sequence != 1 {
		t.Fatalf("retry changed checkpoint or sequence: calls=%d seq=%d", len(bodies), reporter.sequence)
	}
}

func TestMeterCheckpointCancellationAndPayloadBounds(t *testing.T) {
	for _, mode := range []string{"cancelled", "too large"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			g := newGateway("http://fixture.invalid", "internal", "gateway", nil)
			g.client = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) { calls++; return nil, errors.New("must not send") })}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			snapshot := map[string]any{"estimator": "text-bpe-v1", "encoding": "unknown", "responseStarted": false, "terminalUsage": false}
			if mode == "cancelled" {
				cancel()
			} else {
				snapshot["encoding"] = strings.Repeat("x", 4096)
			}
			reporter := meterCheckpoint{gateway: g, request: "request", permit: "one-use"}
			if err := reporter.persist(ctx, snapshot); err == nil || reporter.sequence != 0 || calls != 0 {
				t.Fatalf("invalid checkpoint sent calls=%d err=%v", calls, err)
			}
		})
	}
}
