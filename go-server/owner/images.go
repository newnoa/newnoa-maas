package main

import (
	"net/http"
	"strings"
	"time"
)

type ownerImageTransport struct{ regular, images, execution *http.Transport }

type ownerExecutionContextKey struct{}

func ownerImageTimeout(regular *http.Transport) http.RoundTripper {
	images := regular.Clone()
	images.ResponseHeaderTimeout = 120 * time.Second
	execution := regular.Clone()
	execution.ResponseHeaderTimeout = 31 * time.Minute
	return ownerImageTransport{regular: regular, images: images, execution: execution}
}

func (t ownerImageTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Context().Value(ownerExecutionContextKey{}) == true {
		return t.execution.RoundTrip(r)
	}
	if strings.HasSuffix(r.URL.Path, "/images/generations") || strings.HasSuffix(r.URL.Path, "/images/edits") {
		return t.images.RoundTrip(r)
	}
	return t.regular.RoundTrip(r)
}

func (t ownerImageTransport) CloseIdleConnections() {
	t.regular.CloseIdleConnections()
	t.images.CloseIdleConnections()
	t.execution.CloseIdleConnections()
}
