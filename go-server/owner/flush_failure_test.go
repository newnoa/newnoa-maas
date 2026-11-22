package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type flushFailureWriter struct{ *httptest.ResponseRecorder }

func (w flushFailureWriter) FlushError() error { return errors.New("downstream disconnected") }

type deadlineFlushWriter struct {
	flushFailureWriter
	deadline time.Time
	lastSet  time.Time
}

func (w *deadlineFlushWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	if !deadline.IsZero() {
		w.lastSet = deadline
	}
	return nil
}

type countedStream struct {
	reads  int
	closed bool
}

func (s *countedStream) Read(p []byte) (int, error) {
	s.reads++
	if s.reads == 1 {
		return copy(p, "data: first\n\n"), nil
	}
	return 0, io.EOF
}
func (s *countedStream) Close() error { s.closed = true; return nil }

func TestOwnerStopsReadingOnFlushFailure(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int{"port": 25000})
	}))
	defer api.Close()
	body := &countedStream{}
	o := newOwner("owner-a", api.URL, "internal-test")
	o.upstream.Transport = embeddingTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
	})
	r := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/messages", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer internal-test")
	w := &deadlineFlushWriter{flushFailureWriter: flushFailureWriter{httptest.NewRecorder()}}
	o.ServeHTTP(w, r)
	if body.reads != 1 {
		t.Fatalf("continued reading upstream after failed flush: reads=%d", body.reads)
	}
	if !body.closed {
		t.Fatal("upstream body not closed")
	}
	if w.lastSet.IsZero() || !w.lastSet.After(time.Now()) || !w.deadline.IsZero() {
		t.Fatalf("downstream write deadline not refreshed or not cleared: last=%v current=%v", w.lastSet, w.deadline)
	}
}

type ownerDeadlineWriter struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

func (w *ownerDeadlineWriter) SetWriteDeadline(d time.Time) error { w.deadline = d; return nil }

type ownerReadHook struct {
	io.Reader
	onRead func()
}

func (r ownerReadHook) Read(p []byte) (int, error) { r.onRead(); return r.Reader.Read(p) }

func TestOwnerWriteDeadlineClearedWhileWaitingForUpstream(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"port":25000}`) }))
	defer api.Close()
	o := newOwner("owner-a", api.URL, "internal-test")
	w := &ownerDeadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	o.upstream.Transport = embeddingTransport(func(r *http.Request) (*http.Response, error) {
		body := ownerReadHook{Reader: strings.NewReader("chunk"), onRead: func() {
			if !w.deadline.IsZero() {
				t.Error("idle upstream read retained downstream deadline")
			}
		}}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(body)}, nil
	})
	r := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/messages", strings.NewReader("{}"))
	r.Header.Set("Authorization", "Bearer internal-test")
	o.ServeHTTP(w, r)
	if w.Body.String() != "chunk" {
		t.Fatal(w.Body.String())
	}
}
