package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// Bind every endpoint before serving, and drain all endpoints under one deadline.
func serveOwner(ctx context.Context, servers []*http.Server, timeout time.Duration) error {
	listeners := make([]net.Listener, 0, len(servers))
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	for _, server := range servers {
		listener, err := net.Listen("tcp", server.Addr)
		if err != nil {
			return err
		}
		listeners = append(listeners, listener)
	}
	finished := make(chan error, len(servers))
	for i, server := range servers {
		listener := listeners[i]
		go func() {
			if server.TLSConfig != nil {
				finished <- server.ServeTLS(listener, "", "")
			} else {
				finished <- server.Serve(listener)
			}
		}()
	}
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-finished:
	}
	drain, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	drained := make(chan error, len(servers))
	for _, server := range servers {
		go func() { drained <- server.Shutdown(drain) }()
	}
	for range servers {
		if err := <-drained; err != nil {
			serveErr = errors.Join(serveErr, err)
		}
	}
	for _, server := range servers {
		_ = server.Close()
	}
	return serveErr
}
