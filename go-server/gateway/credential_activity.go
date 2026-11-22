package main

import (
	"context"
	"io"
	"time"
)

// Renew only on actual upstream reads; a timer must not keep an idle lease alive.
type credentialReader struct {
	io.ReadCloser
	ctx      context.Context
	report   func(context.Context) error
	interval time.Duration
	last     time.Time
}

func (r *credentialReader) Read(buffer []byte) (int, error) {
	n, err := r.ReadCloser.Read(buffer)
	if n > 0 && (r.last.IsZero() || time.Since(r.last) >= r.interval) {
		ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
		reportErr := r.report(ctx)
		cancel()
		if reportErr != nil {
			return n, reportErr
		}
		r.last = time.Now()
	}
	return n, err
}
