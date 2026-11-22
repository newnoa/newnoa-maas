package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A Gateway closing its producer tunnel must cancel the provider immediately,
// without draining for final usage or adding a second ordinary billing reporter.
func TestConnectorGatewayCloseCancelsProviderWithoutSecondReporter(t *testing.T) {
	providerCancelled := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(providerCancelled)
	}))
	defer upstream.Close()
	var consumes, reports atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/consume") {
			reports.Add(1)
			w.WriteHeader(404)
			return
		}
		consumes.Add(1)
		var claim map[string]any
		json.NewDecoder(r.Body).Decode(&claim)
		if r.URL.Path != "/api/v1/connector/dispatches/"+hostedFixtureID+"/consume" || r.Header.Get("Authorization") != "Bearer node_fixture" || claim["epoch"] != float64(1) || claim["gatewayId"] != "gateway-fixture" || claim["bodyDigest"] != hash([]byte("POST /v1/chat/completions\n"+`{"model":"fixture","stream":true}`)) {
			t.Error("producer did not consume the current authenticated lease/body")
		}
		json.NewEncoder(w).Encode(map[string]any{"endpoint": upstream.URL, "model": "fixture", "usageReporting": false})
	}))
	defer api.Close()
	var bodyClosed atomic.Bool
	providerClient := upstream.Client()
	transport := providerClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	providerClient.Transport = connectorFlushTransport(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if err == nil {
			response.Body = &connectorObservedClose{ReadCloser: response.Body, closed: &bodyClosed}
		}
		return response, err
	})
	n := &connector{control: api.URL, credential: "node_fixture", client: api.Client(), upstream: providerClient}
	returned := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(returned); n.ServeHTTP(w, r) }))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"fixture","stream":true}`))
	r.Header.Set("X-Newnoa-Request", hostedFixtureID)
	r.Header.Set("X-Newnoa-Permit", "permit_fixture")
	r.Header.Set("X-Newnoa-Gateway", "gateway-fixture")
	r.Header.Set("X-Newnoa-Epoch", "1")
	response, err := server.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if _, err = response.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	cancel()
	select {
	case <-providerCancelled:
	case <-time.After(time.Second):
		t.Fatal("provider kept generating after Gateway tunnel closed")
	}
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("connector did not return after Gateway close")
	}
	if !bodyClosed.Load() {
		t.Fatal("connector returned before closing provider response body")
	}
	if consumes.Load() != 1 || reports.Load() != 0 {
		t.Fatalf("consume=%d duplicate metering reports=%d", consumes.Load(), reports.Load())
	}
}

type connectorObservedClose struct {
	io.ReadCloser
	closed *atomic.Bool
}

func (b *connectorObservedClose) Close() error {
	err := b.ReadCloser.Close()
	b.closed.Store(true)
	return err
}
