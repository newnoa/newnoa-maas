package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type connectorFlushWriter struct{ *httptest.ResponseRecorder }

func (w connectorFlushWriter) FlushError() error { return errors.New("tunnel disconnected") }

type connectorCountedStream struct {
	reads  int
	closed bool
}

func (s *connectorCountedStream) Read(p []byte) (int, error) {
	s.reads++
	if s.reads == 1 {
		return copy(p, "data: first\n\n"), nil
	}
	return 0, io.EOF
}
func (s *connectorCountedStream) Close() error { s.closed = true; return nil }

type connectorFlushTransport func(*http.Request) (*http.Response, error)

func (f connectorFlushTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestConnectorStopsReadingOnFlushFailure(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"endpoint": "http://127.0.0.1:12345", "model": "fixture"})
	}))
	defer control.Close()
	body := &connectorCountedStream{}
	n := &connector{control: control.URL, credential: "node_fixture", client: connectorControlClient(), upstream: &http.Client{Transport: connectorFlushTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
	})}}
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","stream":true}`))
	r.Header.Set("X-Newnoa-Request", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	r.Header.Set("X-Newnoa-Permit", "permit_fixture")
	r.Header.Set("X-Newnoa-Gateway", "fixture")
	r.Header.Set("X-Newnoa-Epoch", "1")
	n.ServeHTTP(connectorFlushWriter{httptest.NewRecorder()}, r)
	if body.reads != 1 {
		t.Fatalf("continued reading local model after failed flush: reads=%d", body.reads)
	}
	if !body.closed {
		t.Fatal("local model body not closed")
	}
}
