package main

import (
	"context"
	"net"
	"net/http"
	"time"
)

// Stop admission first; never replay an interrupted inference on shutdown.
func serveGateway(ctx context.Context, server *http.Server, listener net.Listener, timeout time.Duration) error {
	finished := make(chan error, 1)
	go func() {
		if server.TLSConfig != nil {
			finished <- server.ServeTLS(listener, "", "")
		} else {
			finished <- server.Serve(listener)
		}
	}()
	select {
	case err := <-finished:
		return err
	case <-ctx.Done():
		drain, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := server.Shutdown(drain); err != nil {
			_ = server.Close()
			return err
		}
		return nil
	}
}
