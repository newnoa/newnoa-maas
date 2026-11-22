package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := l.Addr().String()
	l.Close()
	return a
}
func waitListener(t *testing.T, a string) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		c, err := net.DialTimeout("tcp", a, 20*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("listener unavailable")
}
func TestOwnerDrainsBothListeners(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		io.WriteString(w, "complete")
	})
	a, b := freeAddress(t), freeAddress(t)
	servers := []*http.Server{{Addr: a, Handler: handler}, {Addr: b, Handler: handler}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveOwner(ctx, servers, time.Second) }()
	waitListener(t, a)
	waitListener(t, b)
	responses := make(chan string, 2)
	for _, address := range []string{a, b} {
		go func() {
			r, err := http.Get("http://" + address)
			if err != nil {
				responses <- err.Error()
				return
			}
			defer r.Body.Close()
			body, _ := io.ReadAll(r.Body)
			responses <- string(body)
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("handler absent")
		}
	}
	cancel()
	for _, address := range []string{a, b} {
		until := time.Now().Add(time.Second)
		for {
			c, err := net.DialTimeout("tcp", address, 20*time.Millisecond)
			if err != nil {
				break
			}
			c.Close()
			if time.Now().After(until) {
				t.Fatal("still admitting")
			}
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case err := <-done:
		t.Fatalf("early exit: %v", err)
	default:
	}
	close(release)
	for range 2 {
		if response := <-responses; response != "complete" {
			t.Fatal(response)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestOwnerDeadlineCancelsHandler(t *testing.T) {
	entered, cancelled := make(chan struct{}), make(chan struct{})
	address := freeAddress(t)
	server := &http.Server{Addr: address, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(cancelled) })}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveOwner(ctx, []*http.Server{server}, 50*time.Millisecond) }()
	waitListener(t, address)
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		r, err := http.Get("http://" + address)
		if err == nil {
			r.Body.Close()
		}
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("unbounded shutdown")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("handler alive")
	}
	<-requestDone
}
func TestOwnerBindFailureReleasesListeners(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	a := freeAddress(t)
	if err := serveOwner(context.Background(), []*http.Server{{Addr: a}, {Addr: occupied.Addr().String()}}, time.Second); err == nil {
		t.Fatal("accepted occupied endpoint")
	}
	l, err := net.Listen("tcp", a)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
}

func TestOwnerMainSIGTERMClosesBothEndpoints(t *testing.T) {
	a, b := freeAddress(t), freeAddress(t)
	command := exec.Command(os.Args[0], "-test.run=^TestOwnerTLSStartupChild$")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "NEWNOA_") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, "NEWNOA_TEST_OWNER_STARTUP=1", "NEWNOA_FRP_OWNER_ID=shutdown-fixture", "NEWNOA_INTERNAL_TOKEN=fixture-token-01234567890123456789012345", "NEWNOA_FRP_OWNER_BIND="+a, "NEWNOA_FRP_PLUGIN_BIND="+b)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer command.Process.Kill()
	waitListener(t, a)
	waitListener(t, b)
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("SIGTERM did not exit")
	}
	for _, address := range []string{a, b} {
		c, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			c.Close()
			t.Fatal("listener survived exit")
		}
	}
}
