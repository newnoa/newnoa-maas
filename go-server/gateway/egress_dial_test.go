package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

func dualStackFixtureDNS(t *testing.T) {
	t.Helper()
	server, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, 4096)
		for {
			n, address, err := server.ReadFrom(buffer)
			if err != nil {
				return
			}
			var question dnsmessage.Message
			if question.Unpack(buffer[:n]) != nil {
				continue
			}
			answer := dnsmessage.Message{Header: dnsmessage.Header{ID: question.ID, Response: true, RecursionDesired: true, RecursionAvailable: true}, Questions: question.Questions}
			for _, q := range question.Questions {
				resource := dnsmessage.Resource{Header: dnsmessage.ResourceHeader{Name: q.Name, Type: q.Type, Class: dnsmessage.ClassINET, TTL: 60}}
				switch q.Type {
				case dnsmessage.TypeAAAA:
					resource.Body = &dnsmessage.AAAAResource{AAAA: [16]byte{15: 1}}
				case dnsmessage.TypeA:
					resource.Body = &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}}
				default:
					continue
				}
				answer.Answers = append(answer.Answers, resource)
			}
			data, err := answer.Pack()
			if err == nil {
				_, _ = server.WriteTo(data, address)
			}
		}
	}()
	original := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp4", server.LocalAddr().String())
	}}
	t.Cleanup(func() {
		net.DefaultResolver = original
		_ = server.Close()
		<-done
	})
}

func fixedEgressIPs(values ...string) func(context.Context, string) ([]net.IPAddr, error) {
	return func(context.Context, string) ([]net.IPAddr, error) {
		addresses := make([]net.IPAddr, len(values))
		for i, value := range values {
			addresses[i].IP = net.ParseIP(value)
		}
		return addresses, nil
	}
}

func fixtureDialConnection(t *testing.T) net.Conn {
	t.Helper()
	connection, peer := net.Pipe()
	t.Cleanup(func() { _ = connection.Close(); _ = peer.Close() })
	return connection
}

func TestSafeDialerRejectsEntireUnsafeAnswerBeforeConnecting(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ips   []string
		allow []string
	}{
		{"mixed loopback", []string{"8.8.8.8", "127.0.0.1"}, nil},
		{"mixed private IPv6", []string{"8.8.8.8", "fd00::1"}, nil},
		{"mixed mapped private", []string{"8.8.8.8", "::ffff:192.168.1.1"}, nil},
		{"mixed metadata range", []string{"8.8.8.8", "100.100.100.200"}, nil},
		{"invalid even when host allowed", []string{"8.8.8.8", "invalid"}, []string{"fixture.invalid:443"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dials := 0
			d := safeDialer{timeout: time.Second, allow: tc.allow, lookup: fixedEgressIPs(tc.ips...), dial: func(context.Context, string, string) (net.Conn, error) {
				dials++
				return fixtureDialConnection(t), nil
			}}
			connection, err := d.DialContext(context.Background(), "tcp", "fixture.invalid:443")
			if connection != nil || err == nil || dials != 0 {
				t.Fatalf("unsafe answer dialed: connection=%v err=%v attempts=%d", connection != nil, err, dials)
			}
		})
	}
}

func TestSafeDialerUsesOneResolutionAndOnlyPinnedIPs(t *testing.T) {
	lookups, dials := 0, 0
	connection := fixtureDialConnection(t)
	d := safeDialer{timeout: time.Second, lookup: func(context.Context, string) ([]net.IPAddr, error) {
		lookups++
		if lookups > 1 {
			return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
		}
		return []net.IPAddr{{IP: net.ParseIP("2606:4700:4700::1111")}, {IP: net.ParseIP("1.1.1.1")}}, nil
	}, dial: func(_ context.Context, network, address string) (net.Conn, error) {
		dials++
		if network != "tcp" {
			t.Errorf("network changed: %s", network)
		}
		switch address {
		case "[2606:4700:4700::1111]:443":
			return nil, errors.New("first address unreachable")
		case "1.1.1.1:443":
			return connection, nil
		default:
			t.Fatalf("dial escaped validated addresses: %s", address)
			return nil, errors.New("unexpected address")
		}
	}}
	got, err := d.DialContext(context.Background(), "tcp", "rebind.fixture.invalid:443")
	if err != nil || got != connection || lookups != 1 || dials != 2 {
		t.Fatalf("pinned fallback failed: err=%v lookups=%d dials=%d", err, lookups, dials)
	}
}

func TestSafeDialerSupportsIPv6OnlyAndIPv4Failure(t *testing.T) {
	for _, ips := range [][]string{{"2606:4700:4700::1111"}, {"1.1.1.1", "2606:4700:4700::1111"}} {
		d := safeDialer{timeout: time.Second, lookup: fixedEgressIPs(ips...), dial: func(_ context.Context, _, address string) (net.Conn, error) {
			host, _, _ := net.SplitHostPort(address)
			if net.ParseIP(host).To4() != nil {
				return nil, errors.New("IPv4 unavailable")
			}
			return fixtureDialConnection(t), nil
		}}
		connection, err := d.DialContext(context.Background(), "tcp", "fixture.invalid:443")
		if err != nil || connection == nil {
			t.Fatalf("IPv6 fallback failed for %v: %v", ips, err)
		}
	}
}

func TestSafeDialerSharesBudgetAfterFirstAddressTimesOut(t *testing.T) {
	connection := fixtureDialConnection(t)
	dials := 0
	d := safeDialer{timeout: 100 * time.Millisecond, lookup: fixedEgressIPs("2606:4700:4700::1111", "1.1.1.1"), dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		dials++
		if dials == 1 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if ctx.Err() != nil {
			t.Error("first address exhausted the whole budget")
		}
		return connection, nil
	}}
	got, err := d.DialContext(context.Background(), "tcp", "fixture.invalid:443")
	if err != nil || got != connection || dials != 2 {
		t.Fatalf("second address received no budget: err=%v attempts=%d", err, dials)
	}
}

func TestSafeDialerBoundsDNSByConnectionAndParentDeadline(t *testing.T) {
	for _, parentBudget := range []time.Duration{time.Second, 20 * time.Millisecond} {
		parent, cancel := context.WithTimeout(context.Background(), parentBudget)
		dials := 0
		d := safeDialer{timeout: 40 * time.Millisecond, lookup: func(ctx context.Context, _ string) ([]net.IPAddr, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}, dial: func(context.Context, string, string) (net.Conn, error) {
			dials++
			return nil, errors.New("unexpected dial")
		}}
		started := time.Now()
		connection, err := d.DialContext(parent, "tcp", "fixture.invalid:443")
		cancel()
		if connection != nil || !errors.Is(err, context.DeadlineExceeded) || dials != 0 || time.Since(started) > time.Second {
			t.Fatalf("DNS escaped connection deadline: err=%v attempts=%d", err, dials)
		}
	}
}

func TestSafeDialerCancellationStopsAttemptsAndClosesLateConnection(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	connection, peer := net.Pipe()
	defer peer.Close()
	dials := 0
	d := safeDialer{timeout: time.Second, lookup: fixedEgressIPs("1.1.1.1", "8.8.8.8"), dial: func(context.Context, string, string) (net.Conn, error) {
		dials++
		cancel()
		return connection, nil
	}}
	got, err := d.DialContext(parent, "tcp", "fixture.invalid:443")
	if got != nil || !errors.Is(err, context.Canceled) || dials != 1 {
		t.Fatalf("cancellation did not stop attempts: err=%v attempts=%d", err, dials)
	}
	_ = peer.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := peer.Write([]byte("closed")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("cancelled connection leaked: %v", err)
	}
}

func TestSafeDialerClosesLateAttemptBeforeFallback(t *testing.T) {
	late, peer := net.Pipe()
	defer peer.Close()
	winner := fixtureDialConnection(t)
	dials := 0
	d := safeDialer{timeout: 100 * time.Millisecond, lookup: fixedEgressIPs("1.1.1.1", "8.8.8.8"), dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		dials++
		if dials == 1 {
			<-ctx.Done()
			return late, nil
		}
		return winner, nil
	}}
	connection, err := d.DialContext(context.Background(), "tcp", "fixture.invalid:443")
	if err != nil || connection != winner || dials != 2 {
		t.Fatalf("late attempt prevented fallback: %v, attempts=%d", err, dials)
	}
	_ = peer.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := peer.Write([]byte("closed")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("late connection leaked: %v", err)
	}
}

func TestSafeDialerReturnsFailureAfterEveryValidatedAddressFails(t *testing.T) {
	want := errors.New("second connection refused")
	dials := 0
	d := safeDialer{timeout: time.Second, lookup: fixedEgressIPs("1.1.1.1", "8.8.8.8"), dial: func(context.Context, string, string) (net.Conn, error) {
		dials++
		if dials == 2 {
			return nil, want
		}
		return nil, errors.New("first connection refused")
	}}
	connection, err := d.DialContext(context.Background(), "tcp", "fixture.invalid:443")
	if connection != nil || !errors.Is(err, want) || dials != 2 {
		t.Fatalf("connection failure was lost: %v, attempts=%d", err, dials)
	}
}

func TestSafeDialerCancellationJoinsPendingWork(t *testing.T) {
	for _, stage := range []string{"DNS", "TCP"} {
		t.Run(stage, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := make(chan struct{})
			var active atomic.Int32
			wait := func(ctx context.Context) {
				active.Add(1)
				defer active.Add(-1)
				close(started)
				<-ctx.Done()
			}
			d := safeDialer{timeout: time.Second, lookup: func(ctx context.Context, _ string) ([]net.IPAddr, error) {
				if stage == "DNS" {
					wait(ctx)
					return nil, ctx.Err()
				}
				return []net.IPAddr{{IP: net.ParseIP("1.1.1.1")}, {IP: net.ParseIP("8.8.8.8")}}, nil
			}, dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				if stage == "DNS" {
					t.Error("dial after cancelled DNS")
				}
				wait(ctx)
				return nil, ctx.Err()
			}}
			finished := make(chan error, 1)
			go func() {
				_, err := d.DialContext(parent, "tcp", "fixture.invalid:443")
				finished <- err
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("fixture did not start")
			}
			cancel()
			select {
			case err := <-finished:
				if !errors.Is(err, context.Canceled) || active.Load() != 0 {
					t.Fatalf("cancelled work remains: err=%v active=%d", err, active.Load())
				}
			case <-time.After(time.Second):
				t.Fatal("cancelled dial goroutine did not finish")
			}
		})
	}
}

type countedEgressListener struct {
	net.Listener
	accepted atomic.Int32
}

func (l *countedEgressListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return connection, err
}

func dualStackHTTPServers(t *testing.T, handler http.Handler, secure bool) (string, *countedEgressListener, *countedEgressListener) {
	t.Helper()
	ipv6, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(ipv6.Addr().String())
	ipv4, err := net.Listen("tcp4", "127.0.0.1:"+port)
	if err != nil {
		_ = ipv6.Close()
		t.Fatal(err)
	}
	first, second := &countedEgressListener{Listener: ipv6}, &countedEgressListener{Listener: ipv4}
	for _, listener := range []*countedEgressListener{first, second} {
		server := httptest.NewUnstartedServer(handler)
		_ = server.Listener.Close()
		server.Listener = listener
		server.Config.ErrorLog = log.New(io.Discard, "", 0)
		if secure {
			server.StartTLS()
		} else {
			server.Start()
		}
		t.Cleanup(server.Close)
	}
	return net.JoinHostPort("dual-stack.fixture.invalid", port), first, second
}

func TestEgressDoesNotFallbackAfterTLSFailure(t *testing.T) {
	dualStackFixtureDNS(t)
	var requests atomic.Int32
	address, first, second := dualStackHTTPServers(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}), true)
	g := newGateway("http://127.0.0.1", "fixture", "fixture", []string{address})
	t.Cleanup(g.egress.CloseIdleConnections)
	request, _ := http.NewRequest("POST", "https://"+address+"/v1/messages", bytes.NewBufferString("one request"))
	request.GetBody = nil
	response, err := g.egress.Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil || first.accepted.Load() != 1 || second.accepted.Load() != 0 || requests.Load() != 0 {
		t.Fatalf("TLS failure retried another address: err=%v first=%d second=%d HTTP=%d", err, first.accepted.Load(), second.accepted.Load(), requests.Load())
	}
}

func TestEgressDoesNotFallbackAfterHTTPRequestWasSent(t *testing.T) {
	dualStackFixtureDNS(t)
	var requests atomic.Int32
	address, first, second := dualStackHTTPServers(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		if string(body) != "one request" {
			t.Error("request body changed")
		}
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	}), false)
	g := newGateway("http://127.0.0.1", "fixture", "fixture", []string{address})
	t.Cleanup(g.egress.CloseIdleConnections)
	request, _ := http.NewRequest("POST", "http://"+address+"/v1/messages", bytes.NewBufferString("one request"))
	request.GetBody = nil
	response, err := g.egress.Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil || first.accepted.Load() != 1 || second.accepted.Load() != 0 || requests.Load() != 1 {
		t.Fatalf("sent request replayed: err=%v first=%d second=%d HTTP=%d", err, first.accepted.Load(), second.accepted.Load(), requests.Load())
	}
}

func TestEgressFallbackPreservesTLSHostnameAndCertificateVerification(t *testing.T) {
	dualStackFixtureDNS(t)
	var requests atomic.Int32
	var expectedHost string
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		host, _, _ := net.SplitHostPort(r.Host)
		if host != expectedHost || r.TLS.ServerName != expectedHost {
			t.Errorf("original TLS/HTTP hostname lost: SNI=%s Host=%s", r.TLS.ServerName, r.Host)
		}
		_, _ = io.WriteString(w, "OK")
	}))
	defer upstream.Close()
	expectedHost = upstream.Certificate().DNSNames[0]
	_, port, _ := net.SplitHostPort(upstream.Listener.Addr().String())
	address := net.JoinHostPort(expectedHost, port)
	g := newGateway("http://127.0.0.1", "fixture", "fixture", []string{address})
	t.Cleanup(g.egress.CloseIdleConnections)
	roots := x509.NewCertPool()
	roots.AddCert(upstream.Certificate())
	transport := g.egress.Transport.(telemetryTransport).next.(imageResponseTransport).regular.(*http.Transport)
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	request, _ := http.NewRequest("POST", "https://"+address+"/v1/messages", bytes.NewBufferString("one request"))
	request.GetBody = nil
	response, err := g.egress.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 || requests.Load() != 1 {
		t.Fatalf("TLS fallback requests=%d status=%d", requests.Load(), response.StatusCode)
	}
}

func TestEgressConnectsSecondAddressWithoutReplayingHTTP(t *testing.T) {
	dualStackFixtureDNS(t)
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.Method != "POST" || !bytes.Equal(body, []byte("one request")) {
			t.Error("changed upstream request")
		}
		_, _ = io.WriteString(w, "OK")
	}))
	defer upstream.Close()
	_, port, _ := net.SplitHostPort(upstream.Listener.Addr().String())
	address := net.JoinHostPort("dual-stack.fixture.invalid", port)
	ips, err := net.DefaultResolver.LookupIPAddr(context.Background(), "dual-stack.fixture.invalid")
	if err != nil || len(ips) != 2 || ips[0].IP.To4() != nil {
		t.Fatalf("fixture must resolve IPv6 first: %v %v", ips, err)
	}
	g := newGateway("http://127.0.0.1", "fixture", "fixture", []string{address})
	t.Cleanup(g.egress.CloseIdleConnections)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "POST", "http://"+address+"/v1/messages", bytes.NewBufferString("one request"))
	request.GetBody = nil
	response, err := g.egress.Do(request)
	if err != nil {
		t.Fatalf("reachable second address was not used: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 200 || requests.Load() != 1 {
		t.Fatalf("HTTP requests=%d status=%d", requests.Load(), response.StatusCode)
	}
}
