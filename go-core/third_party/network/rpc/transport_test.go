package rpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/anypb"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestMalformedAndOversizedRequests(t *testing.T) {
	tests := []struct {
		name   string
		head   Head
		frames []*anypb.Any
		code   codes.Code
	}{
		{"private route", Head{Method: "POST", Path: "/internal/authorize"}, nil, codes.InvalidArgument},
		{"wrong method", Head{Method: "CONNECT", Path: "/ready"}, nil, codes.InvalidArgument},
		{"body limit", Head{Method: "POST", Path: "/v1/messages", ContentLength: MaxBody + 1}, nil, codes.InvalidArgument},
		{"frame limit", Head{Method: "POST", Path: "/v1/messages", ContentLength: -1}, []*anypb.Any{DataFrame(make([]byte, MaxData+1))}, codes.ResourceExhausted},
		{"duplicate head", Head{Method: "POST", Path: "/v1/messages", ContentLength: -1}, []*anypb.Any{{TypeUrl: RequestHead, Value: []byte("{}")}}, codes.InvalidArgument},
		{"length mismatch", Head{Method: "POST", Path: "/v1/messages", ContentLength: 1}, []*anypb.Any{{TypeUrl: RequestEnd, Value: []byte("null")}}, codes.InvalidArgument},
		{"extra after end", Head{Method: "POST", Path: "/v1/messages", ContentLength: 0}, []*anypb.Any{{TypeUrl: RequestEnd, Value: []byte("null")}, DataFrame([]byte("extra"))}, codes.InvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := openFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					return
				}
				w.Write([]byte("ok"))
			}))
			sendHead(t, s, tc.head)
			for _, frame := range tc.frames {
				s.Send(frame)
			}
			s.CloseSend()
			for {
				_, err := s.Recv()
				if err != nil {
					if status.Code(err) != tc.code {
						t.Fatalf("got %v want %v", err, tc.code)
					}
					break
				}
			}
		})
	}
}
func TestSlowReceiverBackpressuresHandler(t *testing.T) {
	var sent atomic.Int64
	s, cancel := openFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data := bytes.Repeat([]byte("a"), MaxData)
		for i := 0; i < 512; i++ {
			n, err := w.Write(data)
			sent.Add(int64(n))
			if err != nil {
				return
			}
		}
	}))
	sendHead(t, s, Head{Method: "GET", Path: "/ready"})
	endRequest(t, s)
	if _, err := s.Recv(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if count := sent.Load(); count >= 4<<20 {
		t.Fatalf("unbounded read-ahead: %d bytes", count)
	}
	cancel()
}

func TestBlockedReceiverWriteDeadlineCancelsUpstreamAndReleasesHandler(t *testing.T) {
	for _, fixture := range []struct {
		name string
		open func(*testing.T, http.Handler) (grpc.BidiStreamingClient[anypb.Any, anypb.Any], context.CancelFunc)
	}{{"native", openFixture}, {"TLS multiplex", openTLSFixture}} {
		t.Run(fixture.name, func(t *testing.T) { testWriteDeadline(t, fixture.open, fixture.name == "TLS multiplex") })
	}
}

func openTLSFixture(t *testing.T, handler http.Handler) (grpc.BidiStreamingClient[anypb.Any, anypb.Any], context.CancelFunc) {
	t.Helper()
	rpcServer := NewServer(handler)
	t.Cleanup(rpcServer.Stop)
	server := httptest.NewUnstartedServer(Multiplex(rpcServer, handler))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	conn, err := grpc.NewClient(server.Listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	stream, err := Open(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	return stream, cancel
}

func testWriteDeadline(t *testing.T, open func(*testing.T, http.Handler) (grpc.BidiStreamingClient[anypb.Any, anypb.Any], context.CancelFunc), allowHTTP2Reset bool) {
	upstreamCanceled := make(chan struct{})
	handlerFinished := make(chan error, 1)
	startWrite := make(chan struct{})
	s, _ := open(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		go func() { <-r.Context().Done(); close(upstreamCanceled) }()
		w.WriteHeader(http.StatusOK)
		<-startWrite
		controller := http.NewResponseController(w)
		if err := controller.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
			handlerFinished <- err
			return
		}
		_, err := w.Write(bytes.Repeat([]byte("x"), 32<<20))
		handlerFinished <- err
	}))
	sendHead(t, s, Head{Method: "GET", Path: "/ready"})
	endRequest(t, s)
	if _, err := s.Recv(); err != nil {
		t.Fatal(err)
	}
	close(startWrite)
	// Leave data unread so HTTP/2 flow control blocks the sender.
	select {
	case <-upstreamCanceled:
	case <-time.After(time.Second):
		t.Fatal("stalled send did not cancel upstream")
	}
	select {
	case err := <-handlerFinished:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("write error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stalled handler remained blocked after the RPC deadline")
	}
	for {
		_, err := s.Recv()
		if err != nil {
			if status.Code(err) != codes.DeadlineExceeded && !(allowHTTP2Reset && status.Code(err) == codes.Internal) {
				t.Fatalf("RPC error = %v", err)
			}
			break
		}
	}
}

func TestWriteDeadlineResetDoesNotCancelIdleHandler(t *testing.T) {
	s, _ := openFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		controller := http.NewResponseController(w)
		if err := controller.SetWriteDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
			t.Error(err)
		}
		if err := controller.SetWriteDeadline(time.Time{}); err != nil {
			t.Error(err)
		}
		time.Sleep(25 * time.Millisecond)
		if r.Context().Err() != nil {
			t.Error("cleared deadline canceled idle handler")
		}
		w.Write([]byte("done"))
	}))
	sendHead(t, s, Head{Method: "GET", Path: "/ready"})
	endRequest(t, s)
	for {
		_, err := s.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}
func TestInterruptedResponseDoesNotEndCleanly(t *testing.T) {
	s, _ := openFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("partial")); panic(http.ErrAbortHandler) }))
	sendHead(t, s, Head{Method: "GET", Path: "/ready"})
	endRequest(t, s)
	var body string
	for {
		frame, err := s.Recv()
		if err != nil {
			if status.Code(err) != codes.Aborted {
				t.Fatal(err)
			}
			break
		}
		if frame.TypeUrl == ResponseEnd {
			t.Fatal("aborted stream ended cleanly")
		}
		if frame.TypeUrl == Data {
			body += string(frame.Value)
		}
	}
	if body != "partial" {
		t.Fatal(body)
	}
}
func TestTLSMultiplexKeepsHTTPFallback(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/~!frp" {
			w.WriteHeader(418)
			return
		}
		w.Write([]byte("ready"))
	})
	grpcServer := NewServer(handler)
	defer grpcServer.Stop()
	server := httptest.NewUnstartedServer(Multiplex(grpcServer, handler))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	connection, err := grpc.NewClient(server.Listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, err := Open(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	sendHead(t, s, Head{Method: "GET", Path: "/ready"})
	endRequest(t, s)
	for {
		_, err = s.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, NextProtos: []string{"http/1.1"}, MinVersion: tls.VersionTLS12}}}
	response, err := client.Get(server.URL + "/~!frp")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 418 || response.ProtoMajor != 1 {
		t.Fatalf("fallback %d %s", response.StatusCode, response.Proto)
	}
}
func TestMetadataRejectsAmbiguousAndInjectedHeaders(t *testing.T) {
	for _, value := range []string{`{"method":"GET","unknown":true}`, `{} {}`, `{"headers":{"X-Value":["a\r\nb"]}}`} {
		if _, err := ParseHead(&anypb.Any{TypeUrl: RequestHead, Value: []byte(value)}); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
	if _, err := HeadFrame(RequestHead, Head{Headers: http.Header{"X-Value": {"a\r\nb"}}}); err == nil {
		t.Fatal("accepted injected header")
	}
	if _, err := ParseHead(&anypb.Any{TypeUrl: RequestHead, Value: bytes.Repeat([]byte("a"), MaxMetadata+1)}); err == nil {
		t.Fatal("accepted oversized head")
	}
	head, err := ParseHead(&anypb.Any{TypeUrl: RequestHead, Value: []byte(`{"headers":{"authorization":["Bearer session"]}}`)})
	if err != nil || head.Headers.Get("Authorization") != "Bearer session" {
		t.Fatal("header case was not normalized")
	}
}

func openFixture(t *testing.T, handler http.Handler) (grpc.BidiStreamingClient[anypb.Any, anypb.Any], context.CancelFunc) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := NewServer(handler)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///fixture", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	stream, err := Open(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)
	return stream, cancel
}
func sendHead(t *testing.T, s grpc.BidiStreamingClient[anypb.Any, anypb.Any], head Head) {
	t.Helper()
	frame, err := HeadFrame(RequestHead, head)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Send(frame); err != nil {
		t.Fatal(err)
	}
}
func endRequest(t *testing.T, s grpc.BidiStreamingClient[anypb.Any, anypb.Any]) {
	t.Helper()
	frame, _ := EndFrame(RequestEnd, nil)
	if err := s.Send(frame); err != nil {
		t.Fatal(err)
	}
	s.CloseSend()
}
func TestResponseStreamsBeforeHandlerFinishes(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	s, _ := openFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer session" {
			t.Error("lost identity")
		}
		w.Header().Add("X-Reply", "one")
		w.Header().Set("Trailer", "X-Final")
		w.WriteHeader(201)
		w.Write([]byte("first"))
		w.(http.Flusher).Flush()
		<-release
		w.Header().Set("X-Final", "done")
	}))
	sendHead(t, s, Head{Method: "POST", Path: "/v1/chat/completions", Headers: http.Header{"Authorization": {"Bearer session"}}, ContentLength: 0})
	endRequest(t, s)
	frame, err := s.Recv()
	if err != nil {
		t.Fatal(err)
	}
	head, err := ParseHead(frame)
	if err != nil || head.Status != 201 || head.Headers.Get("X-Reply") != "one" {
		t.Fatalf("head=%+v err=%v", head, err)
	}
	frame, err = s.Recv()
	if err != nil || frame.TypeUrl != Data || string(frame.Value) != "first" {
		t.Fatalf("frame=%v err=%v", frame, err)
	}
}
func TestCancelReachesHandler(t *testing.T) {
	stopped := make(chan struct{})
	s, cancel := openFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("start"))
		<-r.Context().Done()
		close(stopped)
	}))
	sendHead(t, s, Head{Method: "GET", Path: "/ready"})
	endRequest(t, s)
	if _, err := s.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("handler did not cancel")
	}
}
func TestRequestBodyAndResponseTrailers(t *testing.T) {
	s, _ := openFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil || string(data) != "payload" {
			t.Errorf("body %q %v", data, err)
		}
		w.Header().Set("Trailer", "X-Final")
		w.WriteHeader(429)
		w.Write(data)
		w.Header().Set("X-Final", "done")
	}))
	sendHead(t, s, Head{Method: "POST", Path: "/v1/messages", ContentLength: 7})
	s.Send(DataFrame([]byte("payload")))
	endRequest(t, s)
	var code int
	var data string
	var final http.Header
	for {
		frame, err := s.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		switch frame.TypeUrl {
		case ResponseHead:
			h, _ := ParseHead(frame)
			code = h.Status
		case Data:
			data += string(frame.Value)
		case ResponseEnd:
			final, err = ParseEnd(frame)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if code != 429 || data != "payload" || final.Get("X-Final") != "done" {
		t.Fatalf("code=%d body=%s trailers=%v", code, data, final)
	}
}

func TestHTTP2RPCReadDeadlineReachesTransport(t *testing.T) {
	result := make(chan error, 1)
	stream, cancel := openTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := http.NewResponseController(w).SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
			result <- err
			return
		}
		_, err := io.ReadAll(r.Body)
		if err == nil {
			t.Error("incomplete upload reached EOF without a deadline error")
		}
		result <- nil
	}))
	defer cancel()
	sendHead(t, stream, Head{Method: "POST", Path: "/v1/responses", ContentLength: 100})
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stalled gRPC upload remained blocked")
	}
}
