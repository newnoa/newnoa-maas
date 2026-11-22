package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	bandwidthBatchSize = 256 << 10
	bandwidthReadSize  = 32 << 10
	bandwidthBatchIdle = 20 * time.Millisecond
)

type admissionState struct {
	ExecutionTimeoutSeconds json.RawMessage `json:"executionTimeoutSeconds"`
	AdmissionStatus         string          `json:"admissionStatus"`
	RetryAfterMillis        int             `json:"retryAfterMillis"`
	QueueDeadlineAt         time.Time       `json:"queueDeadlineAt"`
}

type dispatchResult struct {
	admissionState
	ID               string  `json:"requestId"`
	Permit           string  `json:"permit"`
	Endpoint         string  `json:"endpoint"`
	Key              string  `json:"apiKey"`
	Model            string  `json:"model"`
	DeploymentID     string  `json:"deploymentId"`
	DeploymentMethod string  `json:"deploymentMethod"`
	Revision         int64   `json:"revision"`
	ModelType        string  `json:"modelType"`
	ModelCategory    string  `json:"modelCategory"`
	NodeID           *string `json:"nodeId"`
	OwnerID          string  `json:"ownerId"`
	Route            string  `json:"route"`
	Epoch            int64   `json:"epoch"`
	PooledCredential bool    `json:"pooledCredential"`
	Membership       struct {
		Applies bool `json:"applies"`
	} `json:"membership"`
}

func (g *gateway) waitForAdmission(ctx context.Context, dispatch *dispatchResult) error {
	if _, err := executionTimeout(dispatch.ExecutionTimeoutSeconds); err != nil {
		return err
	}
	if dispatch.AdmissionStatus == "" || dispatch.AdmissionStatus == "reserved" {
		return nil
	}
	if dispatch.AdmissionStatus != "queued" || dispatch.QueueDeadlineAt.IsZero() {
		return errors.New("invalid admission response")
	}
	deadline := time.Now().Add(time.Minute)
	if dispatch.QueueDeadlineAt.Before(deadline) {
		deadline = dispatch.QueueDeadlineAt
	}
	waitCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	for dispatch.AdmissionStatus == "queued" {
		delay := dispatch.RetryAfterMillis
		if delay < 1 || delay > 1000 {
			delay = 250
		}
		if err := waitMillis(waitCtx, int64(delay)); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return &controlError{status: http.StatusRequestTimeout}
			}
			return err
		}
		var next dispatchResult
		if err := g.control(waitCtx, "/dispatches/"+dispatch.ID+"/poll", map[string]any{"gatewayId": g.id, "permit": dispatch.Permit}, &next); err != nil {
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				return &controlError{status: http.StatusRequestTimeout}
			}
			var busy *controlError
			if errors.As(err, &busy) && busy.status == http.StatusTooManyRequests {
				dispatch.RetryAfterMillis = 250
				continue
			}
			return err
		}
		if _, err := executionTimeout(next.ExecutionTimeoutSeconds); err != nil {
			return err
		}
		dispatch.ExecutionTimeoutSeconds = next.ExecutionTimeoutSeconds
		if next.AdmissionStatus != "reserved" && next.AdmissionStatus != "queued" {
			return errors.New("invalid queue promotion")
		}
		if next.AdmissionStatus == "reserved" && dispatch.PooledCredential {
			if !next.PooledCredential || next.Endpoint == "" || next.Key == "" {
				return errors.New("missing pooled credential on promotion")
			}
			dispatch.Endpoint, dispatch.Key = next.Endpoint, next.Key
		}
		dispatch.AdmissionStatus, dispatch.RetryAfterMillis = next.AdmissionStatus, next.RetryAfterMillis
	}
	return nil
}

func waitMillis(ctx context.Context, delay int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay == 0 {
		return nil
	}
	timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type bandwidthReader struct {
	source    io.ReadCloser
	ctx       context.Context
	cancel    context.CancelFunc
	gateway   *gateway
	request   string
	closeOnce sync.Once
	closeErr  error

	// Only Read owns the fields below. Close cancels waits and unblocks the source.
	chunks               chan bandwidthRead
	tail                 []byte
	sourceErr            error
	failure              error
	batch                []byte
	offset               int
	grantStart, grantEnd int
	started              time.Time
	duration             time.Duration
}

type bandwidthRead struct {
	data []byte
	err  error
}

func newBandwidthReader(source io.ReadCloser, ctx context.Context, g *gateway, request string) *bandwidthReader {
	ctx, cancel := context.WithCancel(ctx)
	return &bandwidthReader{source: source, ctx: ctx, cancel: cancel, gateway: g, request: request}
}

func (r *bandwidthReader) pump() {
	size := 4096
	for r.ctx.Err() == nil {
		buf := make([]byte, size)
		n, err := r.source.Read(buf)
		if n > 0 || err != nil {
			select {
			case r.chunks <- bandwidthRead{buf[:n], err}:
			case <-r.ctx.Done():
				return
			}
		}
		if err != nil {
			return
		}
		if n == size {
			size = min(2*size, bandwidthReadSize)
		}
	}
}

func (r *bandwidthReader) fillBatch() error {
	if r.chunks == nil {
		r.chunks = make(chan bandwidthRead, 1)
		go r.pump()
	}
	r.batch = append(r.batch[:0], r.tail...)
	r.tail = nil
	r.offset, r.grantEnd = 0, 0
	var timer *time.Timer
	var deadline <-chan time.Time
	for len(r.batch) < bandwidthBatchSize && r.sourceErr == nil {
		if len(r.batch) > 0 && timer == nil {
			timer = time.NewTimer(bandwidthBatchIdle)
			defer timer.Stop()
			deadline = timer.C
		}
		select {
		case chunk := <-r.chunks:
			n := min(len(chunk.data), bandwidthBatchSize-len(r.batch))
			r.batch = append(r.batch, chunk.data[:n]...)
			r.tail = chunk.data[n:]
			r.sourceErr = chunk.err
		case <-deadline:
			return nil
		case <-r.ctx.Done():
			return r.ctx.Err()
		}
	}
	if len(r.batch) == 0 {
		return r.sourceErr
	}
	return nil
}

func (r *bandwidthReader) reserve() error {
	remaining := len(r.batch) - r.offset
	// A legacy API reads only bytes, so its response pays for this prefix only.
	prefix := min(remaining, bandwidthReadSize)
	var grant struct {
		Protocol     *int   `json:"bandwidthProtocol"`
		GrantedBytes *int   `json:"grantedBytes"`
		WaitMillis   *int64 `json:"waitMillis"`
		GrantMillis  *int64 `json:"grantMillis"`
	}
	err := r.gateway.control(r.ctx, "/requests/"+r.request+"/bandwidth", map[string]any{
		"gatewayId": r.gateway.id, "bandwidthProtocol": 2, "bytes": prefix, "batchBytes": remaining,
	}, &grant)
	if err != nil {
		return err
	}
	if grant.WaitMillis == nil || *grant.WaitMillis < 0 || *grant.WaitMillis > 360000 {
		return errors.New("invalid bandwidth wait")
	}
	r.duration = 0
	granted := prefix
	if grant.Protocol != nil || grant.GrantMillis != nil || grant.GrantedBytes != nil {
		if grant.Protocol == nil || *grant.Protocol != 2 || grant.GrantedBytes == nil || *grant.GrantedBytes != remaining ||
			grant.GrantMillis == nil || *grant.GrantMillis < 0 || *grant.GrantMillis > 360000 {
			return errors.New("invalid bandwidth window")
		}
		granted = remaining
		r.duration = time.Duration(*grant.GrantMillis) * time.Millisecond
	}
	if err := waitMillis(r.ctx, *grant.WaitMillis); err != nil {
		return err
	}
	r.started = time.Now()
	r.grantStart, r.grantEnd = r.offset, r.offset+granted
	return nil
}

func (r *bandwidthReader) Read(p []byte) (int, error) {
	if r.failure != nil {
		return 0, r.failure
	}
	if err := r.ctx.Err(); err != nil || len(p) == 0 {
		return 0, err
	}
	if r.offset == len(r.batch) {
		if err := r.fillBatch(); err != nil {
			return 0, err
		}
	}
	if r.offset == r.grantEnd {
		if err := r.reserve(); err != nil {
			// An ambiguous RPC failure may have consumed a grant. Never reserve it again.
			r.failure = err
			_ = r.Close()
			return 0, err
		}
	}
	end := r.offset + min(len(p), r.grantEnd-r.offset, bandwidthReadSize)
	target := r.started.Add(r.duration * time.Duration(end-r.grantStart) / time.Duration(r.grantEnd-r.grantStart))
	delay := max(time.Until(target), 0)
	if err := waitMillis(r.ctx, (delay + time.Millisecond - 1).Milliseconds()); err != nil {
		return 0, err
	}
	n := copy(p, r.batch[r.offset:end])
	r.offset = end
	return n, nil
}

func (r *bandwidthReader) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		r.closeErr = r.source.Close()
	})
	return r.closeErr
}
