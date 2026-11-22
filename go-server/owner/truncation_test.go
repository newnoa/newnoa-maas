package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type terminalAudioStream struct {
	data   string
	err    error
	closed bool
	cancel context.CancelFunc
}

func (s *terminalAudioStream) Read(p []byte) (int, error) {
	n := copy(p, s.data)
	s.data = s.data[n:]
	if s.data == "" {
		if s.cancel != nil {
			s.cancel()
		}
		return n, s.err
	}
	return n, nil
}

func TestOwnerCancelledReadClosesUpstreamWithoutPanic(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"port":21001}`)
	}))
	defer control.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &terminalAudioStream{err: context.Canceled, cancel: cancel}
	o := newOwner("owner", control.URL, "internal")
	o.upstream.Transport = embeddingTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
	})
	req := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/audio/transcriptions", strings.NewReader("audio")).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer internal")
	o.ServeHTTP(httptest.NewRecorder(), req)
	if ctx.Err() == nil || !body.closed {
		t.Fatalf("canceled read leaked upstream: err=%v closed=%v", ctx.Err(), body.closed)
	}
}

func (s *terminalAudioStream) Close() error { s.closed = true; return nil }

func TestOwnerPreservesTruncationAfterTranscriptDone(t *testing.T) {
	done := "data: {\"type\":\"transcript.text.done\",\"text\":\"hello\",\"usage\":{\"input_tokens\":171,\"output_tokens\":30}}\n\n"
	for _, terminal := range []error{io.EOF, io.ErrUnexpectedEOF} {
		t.Run(terminal.Error(), func(t *testing.T) {
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, `{"port":21001}`)
			}))
			defer control.Close()
			body := &terminalAudioStream{data: done, err: terminal}
			o := newOwner("owner", control.URL, "internal")
			o.upstream.Transport = embeddingTransport(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
			})
			server := httptest.NewServer(o)
			defer server.Close()
			req, _ := http.NewRequest("POST", server.URL+"/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/audio/transcriptions", strings.NewReader("audio"))
			req.Header.Set("Authorization", "Bearer internal")
			res, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			got, readErr := io.ReadAll(res.Body)
			res.Body.Close()
			server.Close()
			if string(got) != done || !body.closed {
				t.Fatalf("lost transcript or upstream not closed: body=%q closed=%v", got, body.closed)
			}
			if terminal == io.EOF && readErr != nil {
				t.Fatalf("clean EOF changed: %v", readErr)
			}
			if terminal != io.EOF && readErr == nil {
				t.Fatal("upstream truncation after done became clean EOF")
			}
		})
	}
}
