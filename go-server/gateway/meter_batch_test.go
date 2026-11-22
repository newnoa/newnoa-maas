package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metering "github.com/newnoa/newnoa-maas/go-server/network/metering"
)

func newBatchFixture(t testing.TB) (*metering.Meter, *meterCheckpoint, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	g := newGateway("http://meter.fixture", "internal", "gateway", nil)
	g.client = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		var b struct {
			Sequence int64 `json:"sequence"`
		}
		_ = json.NewDecoder(r.Body).Decode(&b)
		raw, _ := json.Marshal(map[string]any{"accepted": true, "requestId": "batch", "sequence": b.Sequence})
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw))}, nil
	})}
	meter, err := metering.New("gpt-4", "/v1/chat/completions", []byte(`{"messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	reporter := &meterCheckpoint{gateway: g, request: "batch", permit: "permit"}
	if err = reporter.persist(context.Background(), meter.Snapshot()); err != nil {
		t.Fatal(err)
	}
	return meter, reporter, calls
}

func TestGatewayMeterBatchCoalescesReadAndSkipsUnchangedSnapshot(t *testing.T) {
	meter, reporter, calls := newBatchFixture(t)
	event := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello \"}}]}\n\n"
	raw := []byte(strings.Repeat(event, 128))
	started := time.Now()
	ready, err := checkpointMeterBatch(context.Background(), meter, reporter, raw, false)
	elapsed := time.Since(started)
	if err != nil || !bytes.Equal(ready, raw) || calls.Load() != 2 {
		t.Fatalf("read batch not coalesced: events=128 rpc=%d err=%v", calls.Load(), err)
	}
	if _, err = checkpointMeterBatch(context.Background(), meter, reporter, []byte(": keepalive\n\n"), false); err != nil || calls.Load() != 2 {
		t.Fatalf("unchanged snapshot caused RPC: %d %v", calls.Load(), err)
	}
	if _, err = checkpointMeterBatch(context.Background(), meter, reporter, []byte("data: {\"choices\":"), false); err != nil || calls.Load() != 2 {
		t.Fatalf("incomplete event checkpointed: %d %v", calls.Load(), err)
	}
	t.Logf("local128-event batch: bytes=%d elapsed=%v checkpointRPCs=1 plus start; no timer delay", len(raw), elapsed)
}

func BenchmarkGatewayMeterCheckpointReadBatch(b *testing.B) {
	event := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello \"}}]}\n\n"
	raw := []byte(strings.Repeat(event, 128))
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		meter, reporter, _ := newBatchFixture(b)
		if _, err := checkpointMeterBatch(context.Background(), meter, reporter, raw, false); err != nil {
			b.Fatal(err)
		}
	}
}

func TestCallerCancellationInterruptsBlockedMeterBeforeOutput(t *testing.T) {
	entered := make(chan struct{}, 1)
	cancelledCheckpoint := make(chan struct{}, 1)
	var attempts atomic.Int32
	final := make(chan map[string]any, 1)
	body := &stopOnCloseBody{closed: make(chan struct{}), first: []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			io.WriteString(w, `{"model":"gpt-4"}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			io.WriteString(w, `{"requestId":"cancel-meter","permit":"one","endpoint":"http://127.0.0.1:1"}`)
		case strings.HasSuffix(r.URL.Path, "/meter"):
			var b struct {
				Sequence int64 `json:"sequence"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			if b.Sequence > 1 && attempts.Add(1) == 1 {
				entered <- struct{}{}
				<-r.Context().Done()
				cancelledCheckpoint <- struct{}{}
				return
			}
			if b.Sequence > 1 {
				http.Error(w, "storage unavailable", 503)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": "cancel-meter", "sequence": b.Sequence})
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			var value map[string]any
			_ = json.NewDecoder(r.Body).Decode(&value)
			final <- value
			io.WriteString(w, `{}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", nil)
	g.egress = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: body}, nil
	})}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hi"}],"stream":true}`)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer ses_fixture")
	r.Header.Set("X-Api-Key", "fixture")
	out := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); g.ServeHTTP(out, r) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("checkpoint not reached")
	}
	stop()
	select {
	case <-body.closed:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("cancelled checkpoint held producer open")
	}
	select {
	case <-cancelledCheckpoint:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("checkpoint lost caller cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup blocked cancellation")
	}
	if strings.Contains(out.Body.String(), "hello") {
		t.Fatal("unacknowledged content delivered")
	}
	saved := <-final
	if saved["meterSequence"] != float64(1) || saved["status"] != "cancelled" {
		t.Fatalf("checkpoint sequence/closure wrong %v", saved)
	}
}
