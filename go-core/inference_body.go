package main

import (
	"bytes"
	"io"
	"net/http"

	"github.com/newnoa/newnoa-maas/go-server/network/admission"
)

var inferenceUploads admission.Budget

// A snapshot allows read-only middleware to inspect the same bytes without copying.
type inferenceBody struct {
	*bytes.Reader
	raw []byte
}

func (*inferenceBody) Close() error { return nil }

func setInferenceBody(r *http.Request, raw []byte) {
	r.Body = &inferenceBody{Reader: bytes.NewReader(raw), raw: raw}
	r.ContentLength = int64(len(raw))
	r.GetBody = nil
}

func readInferenceBody(r *http.Request) ([]byte, error) {
	limit := inferenceRequestLimit(r.URL.Path)
	if body, ok := r.Body.(*inferenceBody); ok && int64(len(body.raw)) <= limit && body.Len() == len(body.raw) {
		return body.raw, nil
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, &http.MaxBytesError{Limit: limit}
	}
	setInferenceBody(r, raw)
	return raw, nil
}

func reserveInferenceUpload(w http.ResponseWriter, r *http.Request, remote string) func() {
	limit := inferenceRequestLimit(r.URL.Path)
	if r.ContentLength > limit {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return nil
	}
	declared := r.ContentLength
	// Go callers can supply a non-empty Body with zero ContentLength to mean unknown.
	if declared == 0 && r.Body != nil && r.Body != http.NoBody {
		declared = -1
	}
	charged := admission.BodyLimit(declared, limit)
	release := inferenceUploads.Reserve(remote, charged)
	if release == nil {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "proxy is busy", http.StatusTooManyRequests)
		return nil
	}
	r.Body = http.MaxBytesReader(w, r.Body, charged)
	return release
}
