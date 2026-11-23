package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/pyroscope-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	otellogglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func initTelemetry(service string) (func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var options []sdktrace.TracerProviderOption
	if endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); endpoint != "" || strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")) != "" {
		exporter, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		options = append(options, sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(2048), sdktrace.WithExportTimeout(5*time.Second)))
	}
	ratio := 0.1
	if raw := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); raw != "" {
		if value, err := strconv.ParseFloat(raw, 64); err == nil && value >= 0 && value <= 1 && !math.IsNaN(value) {
			ratio = value
		}
	}
	environment := os.Getenv("NEWNOA_OTEL_ENVIRONMENT")
	if environment == "" {
		environment = os.Getenv("NEWNOA_ENV")
	}
	if environment == "" {
		environment = "production"
	}
	hostname := strings.TrimSpace(os.Getenv("NEWNOA_MONITOR_HOSTNAME"))
	if hostname == "" {
		hostname, _ = os.Hostname()
	}
	envResource, err := resource.New(ctx, resource.WithFromEnv())
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.NewWithAttributes("",
		attribute.String("service.version", "0.4.7"),
		attribute.String("deployment.environment.name", environment),
	), envResource)
	if err != nil {
		return nil, err
	}
	res, err = resource.Merge(res, resource.NewWithAttributes("",
		attribute.String("service.namespace", "newnoa"),
		attribute.String("host.name", hostname),
		attribute.String("service.name", service),
		attribute.String("service.instance.id", uuid.NewString()),
	))
	if err != nil {
		return nil, err
	}
	options = append(options,
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithResource(res),
	)
	provider := sdktrace.NewTracerProvider(options...)
	metricOptions := []sdkmetric.Option{sdkmetric.WithResource(res), sdkmetric.WithView(httpServerDurationView())}
	if strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != "" || strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")) != "" {
		exporter, err := otlpmetrichttp.New(ctx)
		if err != nil {
			return nil, err
		}
		metricOptions = append(metricOptions, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)))
	}
	metrics := sdkmetric.NewMeterProvider(metricOptions...)
	var logOptions []sdklog.LoggerProviderOption
	logOptions = append(logOptions, sdklog.WithResource(res))
	if strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != "" || strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT")) != "" {
		exporter, err := otlploghttp.New(ctx)
		if err != nil {
			return nil, err
		}
		logOptions = append(logOptions, sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter, sdklog.WithMaxQueueSize(2048), sdklog.WithExportTimeout(5*time.Second))))
	}
	logs := sdklog.NewLoggerProvider(logOptions...)
	otel.SetTracerProvider(provider)
	otel.SetMeterProvider(metrics)
	otellogglobal.SetLoggerProvider(logs)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	shutdownProfiler := startPyroscope(service)
	return func() {
		shutdownProfiler()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
		_ = metrics.Shutdown(ctx)
		_ = logs.Shutdown(ctx)
	}, nil
}

func httpServerDurationView() sdkmetric.View {
	return sdkmetric.NewView(
		sdkmetric.Instrument{Name: "http.server.request.duration", Kind: sdkmetric.InstrumentKindHistogram, Unit: "s"},
		sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
			Boundaries: []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10},
			NoMinMax:   true,
		}},
	)
}

func pyroscopeConfig(service string) (pyroscope.Config, error) {
	application := strings.TrimSpace(os.Getenv("PYROSCOPE_APPLICATION_NAME"))
	if application == "" {
		application = service
	}
	if len(application) > 100 {
		return pyroscope.Config{}, fmt.Errorf("PYROSCOPE_APPLICATION_NAME is too long")
	}
	for _, r := range application {
		if !(r == '-' || r == '.' || r == '/' || r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return pyroscope.Config{}, fmt.Errorf("PYROSCOPE_APPLICATION_NAME contains an invalid character")
		}
	}
	address := strings.TrimSpace(os.Getenv("PYROSCOPE_SERVER_ADDRESS"))
	if address == "" {
		address = "http://127.0.0.1:4040"
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return pyroscope.Config{}, fmt.Errorf("PYROSCOPE_SERVER_ADDRESS must be an http(s) URL without credentials or query")
	}
	rate := 10 * time.Second
	if raw := strings.TrimSpace(os.Getenv("PYROSCOPE_UPLOAD_RATE")); raw != "" {
		rate, err = time.ParseDuration(raw)
		if err != nil || rate <= 0 {
			return pyroscope.Config{}, fmt.Errorf("PYROSCOPE_UPLOAD_RATE must be a positive duration")
		}
	}
	environment := strings.TrimSpace(os.Getenv("NEWNOA_OTEL_ENVIRONMENT"))
	if environment == "" {
		environment = strings.TrimSpace(os.Getenv("NEWNOA_ENV"))
	}
	if environment == "" {
		environment = "production"
	}
	return pyroscope.Config{
		ApplicationName:   application,
		ServerAddress:     address,
		BasicAuthUser:     os.Getenv("PYROSCOPE_BASIC_AUTH_USER"),
		BasicAuthPassword: os.Getenv("PYROSCOPE_BASIC_AUTH_PASSWORD"),
		UploadRate:        rate,
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects, pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects, pyroscope.ProfileInuseSpace,
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount, pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount, pyroscope.ProfileBlockDuration,
		},
		Tags: map[string]string{
			"service_namespace":      "newnoa",
			"deployment_environment": environment,
		},
	}, nil
}

func startPyroscope(service string) func() {
	if strings.TrimSpace(os.Getenv("PYROSCOPE_ENABLED")) != "true" {
		return func() {}
	}
	cfg, err := pyroscopeConfig(service)
	if err != nil {
		slog.Warn("Pyroscope disabled", "error", err)
		return func() {}
	}
	previousMutexRate := runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(5)
	profiler, err := pyroscope.Start(cfg)
	if err != nil {
		runtime.SetMutexProfileFraction(previousMutexRate)
		// Go exposes no getter for the block rate; restore the neutral default on failure.
		runtime.SetBlockProfileRate(0)
		slog.Warn("Pyroscope disabled", "error", err)
		return func() {}
	}
	return func() {
		_ = profiler.Stop()
		runtime.SetMutexProfileFraction(previousMutexRate)
		runtime.SetBlockProfileRate(0)
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func instrumentHandler(next http.Handler, service string) http.Handler {
	return instrumentHTTP(next, service, func(r *http.Request) string {
		return r.Method + " /{unmatched}"
	})
}

func instrumentHTTP(next http.Handler, service string, route func(*http.Request) string) http.Handler {
	meter := otel.Meter(service)
	requests, _ := meter.Int64Counter("newnoa.http.server.request.count", metric.WithUnit("{request}"))
	duration, _ := meter.Float64Histogram("http.server.request.duration", metric.WithUnit("s"))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" && r.URL.Path != "/ready" {
			monitoredRequests.Add(1)
		}
		name := route(r)
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer(service).Start(ctx, name, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("http.route", name),
		))
		defer span.End()
		started := time.Now()
		writer := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(writer, r.WithContext(ctx))
		attributes := []attribute.KeyValue{
			attribute.String("http.request.method", r.Method),
			attribute.String("http.route", name),
			attribute.Int("http.response.status_code", writer.status),
		}
		requests.Add(ctx, 1, metric.WithAttributes(attributes...))
		duration.Record(ctx, time.Since(started).Seconds(), metric.WithAttributes(attributes...))
		span.SetAttributes(attribute.Int("http.response.status_code", writer.status))
		if writer.status >= 500 {
			span.SetStatus(codes.Error, "http_error")
		}
		fields := []any{"http.request.method", r.Method, "http.route", name, "http.response.status_code", writer.status, "duration_ms", time.Since(started).Milliseconds()}
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			fields = append(fields, "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
		}
		slog.InfoContext(ctx, "http.server.request", fields...)
		var record otellog.Record
		record.SetTimestamp(time.Now())
		record.SetSeverity(otellog.SeverityInfo)
		record.SetSeverityText("INFO")
		record.SetBody(attribute.StringValue("http.server.request"))
		record.AddAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("http.route", name),
			attribute.Int("http.response.status_code", writer.status),
			attribute.Int64("duration_ms", time.Since(started).Milliseconds()),
		)
		otellogglobal.Logger(service).Emit(ctx, record)
	})
}

type telemetryTransport struct {
	next      http.RoundTripper
	propagate bool
}

func (t telemetryTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, span := otel.Tracer("newnoa-http-client").Start(r.Context(), "HTTP "+r.Method, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("http.request.method", r.Method),
	))
	defer span.End()
	request := r.Clone(ctx)
	if t.propagate {
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(request.Header))
	} else {
		request.Header.Del("traceparent")
		request.Header.Del("tracestate")
	}
	response, err := t.next.RoundTrip(request)
	if err != nil {
		span.SetStatus(codes.Error, "http_client_error")
		return nil, err
	}
	span.SetAttributes(attribute.Int("http.response.status_code", response.StatusCode))
	if response.StatusCode >= 500 {
		span.SetStatus(codes.Error, "http_error")
	}
	return response, nil
}

func instrumentClient(next http.RoundTripper, propagate bool) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	return telemetryTransport{next: next, propagate: propagate}
}

func gatewayRoute(r *http.Request) string {
	if strings.HasPrefix(r.URL.Path, "/internal/evaluations/") {
		return "POST /internal/evaluations/{id}"
	}
	for _, path := range []string{"/v1/models", "/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/embeddings", "/v1/rerank", "/v1/audio/transcriptions", "/v1/systemone", "/v1/images/generations", "/v1/images/edits", "/health", "/ready", "/internal/runtime"} {
		if r.URL.Path == path {
			return r.Method + " " + path
		}
	}
	return r.Method + " /{unmatched}"
}
