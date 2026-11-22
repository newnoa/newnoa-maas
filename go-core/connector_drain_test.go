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

func TestConnectorStopDrainsBeforeClosingActiveRequest(t *testing.T) {
	started, release, drained := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var executes, consumes, drains atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executes.Add(1)
		close(started)
		<-release
		io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/drain") {
			if drains.Add(1) == 1 {
				close(drained)
			}
			io.WriteString(w, `{}`)
			return
		}
		consumes.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"endpoint": upstream.URL, "model": "fixture"})
	}))
	defer api.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &connector{ctx: ctx, cancel: cancel, control: api.URL, credential: "node_fixture", nodeID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", epoch: 4, state: "online", purpose: "serving", client: api.Client(), upstream: upstream.Client()}
	server := httptest.NewServer(n)
	defer server.Close()
	n.server = server.Config
	request, _ := http.NewRequest("POST", server.URL+"/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[]}`))
	request.Header.Set("X-Newnoa-Request", n.nodeID)
	request.Header.Set("X-Newnoa-Permit", "permit_fixture")
	request.Header.Set("X-Newnoa-Gateway", "gateway-a")
	request.Header.Set("X-Newnoa-Epoch", "4")
	result := make(chan error, 1)
	go func() {
		response, err := server.Client().Do(request)
		if err == nil {
			defer response.Body.Close()
			data, readErr := io.ReadAll(response.Body)
			err = readErr
			if response.StatusCode != 200 || string(data) != `{"ok":true}` {
				t.Errorf("in-flight response interrupted: %d %s", response.StatusCode, data)
			}
		}
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("model did not start")
	}
	n.stop()
	n.stop()
	rejected := httptest.NewRecorder()
	n.ServeHTTP(rejected, httptest.NewRequest("POST", "/v1/chat/completions", nil))
	if rejected.Code != 503 || consumes.Load() != 1 {
		t.Fatal("new work admitted during stop")
	}
	done := make(chan struct{})
	go func() { n.finishStop(); close(done) }()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain not sent")
	}
	select {
	case <-done:
		t.Fatal("closed before active request finished")
	default:
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish")
	}
	n.finishStop()
	if drains.Load() != 1 || executes.Load() != 1 {
		t.Fatal("stop duplicated control or model requests")
	}
}

func TestConnectorStopHasBoundWhenControlUnavailable(t *testing.T) {
	release := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer api.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &connector{ctx: ctx, cancel: cancel, control: api.URL, client: api.Client(), epoch: 4, server: &http.Server{}}
	n.stop()
	start := time.Now()
	n.finishStop()
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("control drain not bounded: %v", elapsed)
	}
}

func TestConnectorStopBoundsActiveRequestWait(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-release; io.WriteString(w, "late") }))
	defer server.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &connector{ctx: ctx, cancel: cancel, server: server.Config}
	result := make(chan error, 1)
	go func() {
		response, err := server.Client().Get(server.URL)
		if err == nil {
			defer response.Body.Close()
			_, err = io.ReadAll(response.Body)
		}
		result <- err
	}()
	<-started
	n.stop()
	start := time.Now()
	n.finishStop()
	elapsed := time.Since(start)
	if elapsed < 2900*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("active request wait exceeded bound: %v", elapsed)
	}
	// run's existing deferred Close cancels unfinished HTTP after the grace window.
	n.server.Close()
	if err := <-result; err == nil {
		t.Fatal("unfinished response was reported complete")
	}
}
