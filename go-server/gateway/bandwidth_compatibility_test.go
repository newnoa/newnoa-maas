package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Some upstream readers return the final bytes together with the terminal error.
type boundedFinalRead struct {
	data     []byte
	terminal error
}

func (s *boundedFinalRead) Read(p []byte) (int, error) {
	n := copy(p[:min(len(p), 7001)], s.data)
	s.data = s.data[n:]
	if len(s.data) == 0 {
		return n, s.terminal
	}
	return n, nil
}
func (s *boundedFinalRead) Close() error { return nil }

func TestBandwidthRollingUpgradeAndRollbackPreserveEveryAuthorizedByte(t *testing.T) {
	for _, versions := range [][]int{{1}, {2}, {1, 2, 1}, {2, 1, 2}} {
		t.Run(fmt.Sprint(versions), func(t *testing.T) {
			payload := bytes.Repeat([]byte("arbitrary-offset,"), 40001)
			sourceErr := errors.New("stream terminated")
			var charged, calls atomic.Int64
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var b struct{ BandwidthProtocol, Bytes, BatchBytes int }
				if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
					t.Error(err)
					return
				}
				if b.BandwidthProtocol != 2 || b.Bytes != min(b.BatchBytes, bandwidthReadSize) || b.BatchBytes > bandwidthBatchSize {
					t.Errorf("invalid grant request %+v", b)
				}
				v := versions[(calls.Add(1)-1)%int64(len(versions))]
				if v == 1 {
					charged.Add(int64(b.Bytes))
					fmt.Fprint(w, `{"waitMillis":0}`)
					return
				}
				charged.Add(int64(b.BatchBytes))
				fmt.Fprintf(w, `{"bandwidthProtocol":2,"grantedBytes":%d,"waitMillis":0,"grantMillis":0}`, b.BatchBytes)
			}))
			defer api.Close()
			reader := newBandwidthReader(&boundedFinalRead{data: payload, terminal: sourceErr}, context.Background(), newGateway(api.URL, "internal", "fixture", nil), "limited")
			defer reader.Close()
			got, err := io.ReadAll(reader)
			if !errors.Is(err, sourceErr) || !bytes.Equal(got, payload) || charged.Load() != int64(len(payload)) {
				t.Fatalf("read=%d, charged=%d, err=%v", len(got), charged.Load(), err)
			}
			if n, err := reader.Read(make([]byte, 1)); n != 0 || err != sourceErr {
				t.Fatalf("terminal read=(%d,%v)", n, err)
			}
		})
	}
}

func TestBandwidthReserveFailureIsTerminalEvenOnNextRead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"unknown outcome", 503, "{}"},
		{"partial response", 200, `{"waitMillis":0,"grantMillis":0}`},
		{"truncated grant", 200, `{"bandwidthProtocol":2,"grantedBytes":1,"waitMillis":0,"grantMillis":10}`},
		{"negative window", 200, `{"bandwidthProtocol":2,"grantedBytes":4,"waitMillis":0,"grantMillis":-1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer api.Close()
			reader := newBandwidthReader(io.NopCloser(strings.NewReader("data")), context.Background(), newGateway(api.URL, "internal", "fixture", nil), "limited")
			defer reader.Close()
			for range 2 {
				if n, err := reader.Read(make([]byte, 4)); err == nil || n != 0 {
					t.Fatalf("failed grant delivered %d bytes err=%v", n, err)
				}
			}
			if calls.Load() != 1 {
				t.Fatal("uncertain grant was retried")
			}
		})
	}
}

func TestCloseUnblocksBandwidthSourceAndQueue(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			started := make(chan struct{})
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); fmt.Fprint(w, `{"waitMillis":60000}`) }))
			defer api.Close()
			source, sender := io.Pipe()
			defer sender.Close()
			reader := newBandwidthReader(source, context.Background(), newGateway(api.URL, "internal", "fixture", nil), "limited")
			done := make(chan error, 1)
			go func() { _, err := reader.Read(make([]byte, 8)); done <- err }()
			if queued {
				_, _ = sender.Write([]byte("data"))
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("reader failed to reserve partial batch")
				}
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("closed stream returned data")
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not unblock Read")
			}
		})
	}
}

func TestTinyBandwidthStreamAllocatesInProportionToPayload(t *testing.T) {
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"waitMillis":0}`) }))
	defer api.Close()
	reader := newBandwidthReader(io.NopCloser(strings.NewReader("x")), context.Background(), newGateway(api.URL, "internal", "fixture", nil), "limited")
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "x" || cap(reader.batch) > 4096 {
		t.Fatalf("bytes=%q capacity=%d err=%v", data, cap(reader.batch), err)
	}
}

func TestTimerFlushesSmallBandwidthFramesWhileUpstreamIsIdle(t *testing.T) {
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"waitMillis":0}`) }))
	defer api.Close()
	source, writer := io.Pipe()
	defer writer.Close()
	reader := newBandwidthReader(source, context.Background(), newGateway(api.URL, "internal", "fixture", nil), "limited")
	defer reader.Close()
	result := make(chan string, 1)
	go func() {
		buf := make([]byte, 4)
		n, err := reader.Read(buf)
		if err != nil {
			result <- err.Error()
			return
		}
		result <- string(buf[:n])
	}()
	if _, err := writer.Write([]byte("tick")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got != "tick" {
			t.Fatal(got)
		}
	case <-time.After(time.Second):
		t.Fatal("idle SSE frame waited for another source read")
	}
}
