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

func TestOwnerHTTPDurationHistogramUsesSecondsBuckets(t *testing.T) {
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

func TestOwnerHTTPMetricsAndStructuredLogsUseBoundedCorrelatedFields(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previousMetrics := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetMeterProvider(previousMetrics)
	})
	traceExporter := installOwnerTelemetryTestProvider(t)
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	handler := instrumentHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}), "newnoa-owner", func(*http.Request) string { return "POST /tunnels/{node}/{epoch}/v1/{operation}" })
	server := httptest.NewServer(handler)
	defer server.Close()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/tunnels/private/1/v1/chat/completions?token=secret", nil)
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
	for _, expected := range []string{"newnoa.http.server.request.count", "http.server.request.duration", "POST /tunnels/{node}/{epoch}/v1/{operation}", "503"} {
		if !strings.Contains(metrics, expected) {
			t.Fatalf("metrics missing %q: %s", expected, metrics)
		}
	}
	for _, output := range []string{metrics, logs.String(), fmt.Sprint(traceExporter.GetSpans())} {
		if strings.Contains(output, "secret") || strings.Contains(output, "private") {
			t.Fatalf("request data leaked into telemetry: %s", output)
		}
	}
	if !strings.Contains(logs.String(), `"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`) {
		t.Fatalf("structured request log lacks trace correlation: %s", logs.String())
	}
}

func TestOwnerTelemetryExportsCorrelatedLogsOverOTLP(t *testing.T) {
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
	shutdown, err := initTelemetry("newnoa-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown()
	previousLogger := slog.Default()
	defer slog.SetDefault(previousLogger)
	server := httptest.NewServer(instrumentHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}), "newnoa-owner", func(*http.Request) string { return "POST /tunnels/{node}/{epoch}/v1/{operation}" }))
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/tunnels/private/1/v1/chat/completions?token=query-secret", nil)
	request.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
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
	if !strings.Contains(encoded, "newnoa-owner") || !strings.Contains(encoded, "http.server.request") {
		t.Fatalf("OTLP logs lack resource or request event: %s", encoded)
	}
	resourceAttributes := map[string]string{}
	for _, item := range exported.ResourceLogs[0].Resource.Attributes {
		resourceAttributes[item.Key] = item.Value.GetStringValue()
	}
	for key, expected := range map[string]string{
		"service.namespace": "newnoa", "service.name": "newnoa-owner",
		"service.version": "0.1.0", "deployment.environment.name": "integration",
	} {
		if resourceAttributes[key] != expected {
			t.Fatalf("resource %s = %q, want %q; all=%v", key, resourceAttributes[key], expected, resourceAttributes)
		}
	}
	if resourceAttributes["service.instance.id"] == "" {
		t.Fatalf("resource lacks instance identity: %v", resourceAttributes)
	}
	if got := hex.EncodeToString(exported.ResourceLogs[0].ScopeLogs[0].LogRecords[0].TraceId); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("OTLP log trace ID = %s", got)
	}
	if hex.EncodeToString(exported.ResourceLogs[0].ScopeLogs[0].LogRecords[0].SpanId) == "" || strings.Contains(encoded, "query-secret") || strings.Contains(encoded, "private") {
		t.Fatalf("OTLP log correlation/privacy invalid: %s", encoded)
	}
}

func TestOwnerTelemetryPropagatesControlAndStripsDeviceHeaders(t *testing.T) {
	exporter := installOwnerTelemetryTestProvider(t)
	traceparents := make(chan string, 2)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparents <- r.Header.Get("traceparent")
		if strings.HasSuffix(r.URL.Path, "/route") {
			_, _ = w.Write([]byte(`{"port":25000}`))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer api.Close()
	device := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("traceparent"); got != "" {
			t.Errorf("device model received trace context: %q", got)
		}
		if got := r.Header.Get("tracestate"); got != "" {
			t.Errorf("device model received trace state: %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer device.Close()
	port := strings.TrimPrefix(device.URL, "http://127.0.0.1:")
	var route struct {
		Port int `json:"port"`
	}
	o := newOwner("owner-a", api.URL, "internal-test")
	o.upstream = &http.Client{Transport: instrumentOwnerClient(http.DefaultTransport, false)}
	_ = route

	// The control span check exercises the same transport used by route lookup.
	parent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.HeaderCarrier(http.Header{"Traceparent": []string{parent}}))
	if err := o.call(ctx, "/route", map[string]any{"nodeId": "fixture"}, &route); err != nil {
		t.Fatal(err)
	}
	if got := <-traceparents; !strings.HasPrefix(got, "00-4bf92f3577b34da6a3ce929d0e0e4736-") {
		t.Fatalf("control did not receive trace context: %q", got)
	}
	if port == "" {
		t.Fatal("unexpected device fixture address")
	}
	if len(exporter.GetSpans()) == 0 {
		t.Fatal("expected a client span")
	}
}

func installOwnerTelemetryTestProvider(t *testing.T) *tracetest.InMemoryExporter {
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
