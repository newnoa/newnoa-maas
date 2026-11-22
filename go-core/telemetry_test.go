package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestCoreTelemetryUsesInjectedBuildVersion(t *testing.T) {
	original := buildVersion
	buildVersion = "0.1.127"
	t.Cleanup(func() { buildVersion = original })
	t.Setenv("NEWNOA_OTEL_ENABLED", "true")
	t.Setenv("PYROSCOPE_ENABLED", "false")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer server.Close()
	t.Setenv("NEWNOA_OTEL_TRACES_ENDPOINT", server.URL+"/v1/traces")
	previous, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	shutdown, err := initCoreTelemetry()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		shutdown()
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(previousPropagator)
	}()
	_, span := otel.Tracer("version-test").Start(context.Background(), "version")
	defer span.End()
	version, ok := span.(sdktrace.ReadOnlySpan).Resource().Set().Value(attribute.Key("service.version"))
	if !ok || version.AsString() != buildVersion {
		t.Fatalf("telemetry version=%v, want %s", version, buildVersion)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestConnectorControlClientPropagatesTraceContext(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	previous := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	})

	received := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, span := otel.Tracer("test").Start(context.Background(), "parent")
	defer span.End()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := connectorControlClient().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if got := <-received; len(got) != 55 || got[:3] != "00-" {
		t.Fatalf("traceparent = %q, want a W3C trace context", got)
	}
}

func TestTelemetryTransportStripsTraceAtUntrustedEgress(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "http://127.0.0.1/v1/chat/completions?secret=value", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")
	request.Header.Set("tracestate", "vendor=value")
	transport := telemetryTransport{next: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("traceparent") != "" || r.Header.Get("tracestate") != "" {
			t.Fatalf("trace headers reached untrusted endpoint: %v", r.Header)
		}
		return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody}, nil
	}), route: "desktop.model"}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
}

func TestCoreRouteOmitsDynamicPathAndQuery(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tasks/secret-id?token=secret", nil)
	if got := coreRoute(request); got != "GET /{unmatched}" {
		t.Fatalf("route = %q, want normalized route", got)
	}
}
