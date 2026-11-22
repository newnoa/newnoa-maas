package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type readDeadlineRecorder struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

func (w *readDeadlineRecorder) SetReadDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

func TestBodyReadTimeoutAllowsConfiguredMinimumRateAndCapsUnknownBodies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    int64
		maximum int64
		want    time.Duration
	}{
		{name: "small", body: 1, want: 16 * time.Second},
		{name: "one second", body: 64 << 10, want: 16 * time.Second},
		{name: "evaluation body", body: 4096, maximum: 4096, want: 16 * time.Second},
		{name: "large model body", body: 32 << 20, want: 527 * time.Second},
		{name: "unknown length", body: -1, want: 527 * time.Second},
		{name: "over limit", body: 64 << 20, want: 527 * time.Second},
		{name: "hard maximum", body: 64 << 30, maximum: 64 << 30, want: maximumBodyReadTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			maximum := tc.maximum
			if maximum == 0 {
				maximum = 32 << 20
			}
			if got := bodyReadTimeout(tc.body, maximum); got != tc.want {
				t.Fatalf("bodyReadTimeout(%d) = %s, want %s", tc.body, got, tc.want)
			}
		})
	}
}

func TestBodyReadDeadlineExtendsServerReadTimeoutAfterHeaders(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := setBodyReadDeadline(w, r.ContentLength, 4); err != nil {
			t.Errorf("set body deadline: %v", err)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		fmt.Fprint(w, string(body))
	}))
	server.Config.ReadTimeout = 100 * time.Millisecond
	server.Start()
	defer server.Close()

	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err = io.WriteString(conn, "POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	if _, err = io.WriteString(conn, "data"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "data" {
		t.Fatalf("response body = %q", body)
	}
}

func TestSetBodyReadDeadlineExtendsTheConnectionDeadline(t *testing.T) {
	w := &readDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	if err := setBodyReadDeadline(w, 32<<20, 32<<20); err != nil {
		t.Fatal(err)
	}
	remaining := time.Until(w.deadline)
	if remaining < 526*time.Second || remaining > 527*time.Second {
		t.Fatalf("body deadline remaining = %s", remaining)
	}
}
