package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestTelemetryKeepsTraceWithinServiceAndRedactsHTTPAttributes(t *testing.T) {
	exporter := installTelemetryTestProvider(t)
	var trustedTraceparent string
	trusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trustedTraceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer trusted.Close()
	var modelTraceparent string
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		modelTraceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer model.Close()

	handler := instrumentHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "api_key=secret-query" {
			t.Errorf("handler did not receive original query: %q", r.URL.RawQuery)
		}
		for _, item := range []struct {
			url       string
			propagate bool
		}{{trusted.URL + "/users/secret-path?api_key=secret-query", true}, {model.URL + "/users/secret-path?api_key=secret-query", false}} {
			client := &http.Client{Transport: instrumentClient(http.DefaultTransport, item.propagate)}
			request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, item.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
		}
		w.WriteHeader(http.StatusNoContent)
	}), "newnoa-gateway")
	server := httptest.NewServer(handler)
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL+"/v1/models/private-id?api_key=secret-query", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("User-Agent", "user-device-secret")
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()

	if !strings.HasPrefix(trustedTraceparent, "00-4bf92f3577b34da6a3ce929d0e0e4736-") {
		t.Fatalf("trusted service did not receive the active trace: %q", trustedTraceparent)
	}
	if modelTraceparent != "" {
		t.Fatalf("model endpoint received trace context: %q", modelTraceparent)
	}
	for _, span := range exporter.GetSpans() {
		if span.SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Fatalf("span lost incoming trace context: %s", span.SpanContext.TraceID())
		}
		for _, attr := range span.Attributes {
			if strings.Contains(attr.Value.AsString(), "secret-query") || strings.Contains(attr.Value.AsString(), "private-id") || strings.Contains(attr.Value.AsString(), "user-device-secret") {
				t.Fatalf("sensitive request value exported as %s=%s", attr.Key, attr.Value.AsString())
			}
		}
	}
}

func TestHTTPDurationHistogramUsesSecondsBuckets(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithView(httpServerDurationView()))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	histogram, err := provider.Meter("test").Float64Histogram("http.server.request.duration", metric.WithUnit("s"))
	if err != nil {
		t.Fatal(err)
	}
	histogram.Record(context.Background(), 0.012)
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(data); !strings.Contains(got, "[0.005 0.01 0.025 0.05 0.075 0.1 0.25 0.5 0.75 1 2.5 5 7.5 10]") {
		t.Fatalf("HTTP duration histogram has unexpected buckets: %s", got)
	}
}

func TestHTTPMetricsAndStructuredLogsUseBoundedCorrelatedFields(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previousMetrics := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetMeterProvider(previousMetrics)
	})
	traceExporter := installTelemetryTestProvider(t)
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	handler := instrumentHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}), "newnoa-gateway", func(*http.Request) string { return "GET /v1/models/{id}" })
	server := httptest.NewServer(handler)
	defer server.Close()
	request, err := http.NewRequest(http.MethodGet, server.URL+"/v1/models/private?token=secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()

	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	metrics := fmt.Sprint(data)
	for _, expected := range []string{"newnoa.http.server.request.count", "http.server.request.duration", "GET /v1/models/{id}", "418"} {
		if !strings.Contains(metrics, expected) {
			t.Fatalf("metrics missing %q: %s", expected, metrics)
		}
	}
	for _, output := range []string{metrics, logs.String(), fmt.Sprint(traceExporter.GetSpans())} {
		if strings.Contains(output, "secret") || strings.Contains(output, "private") {
			t.Fatalf("request data leaked into telemetry: %s", output)
		}
	}
	if !strings.Contains(logs.String(), `"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`) || !strings.Contains(logs.String(), "http.server.request") {
		t.Fatalf("structured request log lacks trace correlation: %s", logs.String())
	}
}

func TestTelemetryExportsCorrelatedLogsOverOTLP(t *testing.T) {
	var payload []byte
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/logs" {
			payload, _ = io.ReadAll(r.Body)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", receiver.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=integration")
	shutdown, err := initTelemetry("newnoa-gateway")
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown()
	previousLogger := slog.Default()
	defer slog.SetDefault(previousLogger)
	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	server := httptest.NewServer(instrumentHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}), "newnoa-gateway", func(*http.Request) string { return "GET /v1/models" }))
	defer server.Close()
	request, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/models?token=query-secret", nil)
	request.Header.Set("traceparent", parent)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	shutdown()

	var exported collectorlogsv1.ExportLogsServiceRequest
	if err := proto.Unmarshal(payload, &exported); err != nil {
		t.Fatalf("decode OTLP logs payload: %v", err)
	}
	encoded := fmt.Sprint(&exported)
	if !strings.Contains(encoded, "newnoa-gateway") || !strings.Contains(encoded, "http.server.request") {
		t.Fatalf("OTLP logs lack resource or request event: %s", encoded)
	}
	resourceAttributes := map[string]string{}
	for _, item := range exported.ResourceLogs[0].Resource.Attributes {
		resourceAttributes[item.Key] = item.Value.GetStringValue()
	}
	for key, expected := range map[string]string{
		"service.namespace": "newnoa", "service.name": "newnoa-gateway",
		"service.version": "0.4.1", "deployment.environment.name": "integration",
	} {
		if resourceAttributes[key] != expected {
			t.Fatalf("resource %s = %q, want %q; all=%v", key, resourceAttributes[key], expected, resourceAttributes)
		}
	}
	if resourceAttributes["service.instance.id"] == "" {
		t.Fatalf("resource lacks instance identity: %v", resourceAttributes)
	}
	record := exported.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if hex.EncodeToString(record.TraceId) != "4bf92f3577b34da6a3ce929d0e0e4736" || hex.EncodeToString(record.SpanId) == "" {
		t.Fatalf("OTLP log lacks W3C trace/span correlation: %s", encoded)
	}
	if strings.Contains(encoded, "query-secret") || strings.Contains(encoded, "token=") {
		t.Fatalf("OTLP logs contain request query data: %s", encoded)
	}
}

func installTelemetryTestProvider(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := trace.NewTracerProvider(trace.WithSyncer(exporter), trace.WithSampler(trace.AlwaysSample()))
	previousProvider, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	return exporter
}
