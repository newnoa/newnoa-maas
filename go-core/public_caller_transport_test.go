package main

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestPublicCallerNeverFallsBackToUnpinnedTransport(t *testing.T) {
	r := httptest.NewRequest("POST", "https://example.test/v1/chat/completions", nil)
	r = r.WithContext(context.WithValue(r.Context(), publicCallerProofContext{}, publicCallerProofInput{token: publicTestSecret, bodySHA256: "ignored", callerIP: "203.0.113.9"}))
	for _, endpoint := range []*gatewayEndpoint{nil, {ID: "node", Address: "https://example.test", Transport: "grpc"}, {ID: "node", Address: "https://example.test", Transport: "http", TrustedCA: "invalid"}} {
		request := r
		if endpoint != nil {
			request = r.WithContext(context.WithValue(r.Context(), selectedGatewayContext{}, *endpoint))
		}
		if _, err := (gatewayTransport{}).RoundTrip(request); err == nil {
			t.Fatal("public API key sent without exact selected TLS gRPC trust")
		}
	}
}
