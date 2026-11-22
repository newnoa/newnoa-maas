package main

import (
	"context"
	"fmt"
	"io"
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
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func initCoreTelemetry() (func(), error) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	shutdownProfiler := startPyroscope("newnoa-desktop-core")
	noop := func() { shutdownProfiler() }
	if os.Getenv("NEWNOA_OTEL_ENABLED") != "true" {
		return noop, nil
	}
	endpoint := strings.TrimSpace(os.Getenv("NEWNOA_OTEL_TRACES_ENDPOINT"))
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || !strings.HasSuffix(parsed.Path, "/v1/traces") || !(parsed.Scheme == "https" || parsed.Scheme == "http" && (parsed.Hostname() == "localhost" || parsed.Hostname() == "127.0.0.1" || parsed.Hostname() == "::1")) {
		return noop, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	cancel()
	if err != nil {
		return noop, err
	}
	ratio := 1.0
	if raw := os.Getenv("NEWNOA_OTEL_SAMPLE_RATIO"); raw != "" {
		if value, parseErr := strconv.ParseFloat(raw, 64); parseErr == nil && !math.IsNaN(value) && value >= 0 && value <= 1 {
			ratio = value
		}
	}
	res, err := resource.New(context.Background(), resource.WithAttributes(
		attribute.String("service.namespace", "newnoa"),
		attribute.String("service.name", "newnoa-desktop-core"),
		attribute.String("service.version", buildVersion),
		attribute.String("service.instance.id", uuid.NewString()),
		attribute.String("deployment.environment.name", envOr("NEWNOA_OTEL_ENVIRONMENT", "production")),
	))
	if err != nil {
		return noop, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(128), sdktrace.WithExportTimeout(3*time.Second)),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return func() {
		shutdownProfiler()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	}, nil
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

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

type telemetryTransport struct {
	next      http.RoundTripper
	propagate bool
	route     string
}

func (t telemetryTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}
	ctx, span := otel.Tracer("newnoa-desktop-core").Start(r.Context(), "HTTP "+r.Method+" "+t.route, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(
		attribute.String("http.request.method", r.Method), attribute.String("http.route", t.route),
	))
	defer span.End()
	request := r.Clone(ctx)
	if t.propagate {
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(request.Header))
	} else {
		request.Header.Del("traceparent")
		request.Header.Del("tracestate")
	}
	response, err := next.RoundTrip(request)
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

type coreStatusWriter struct {
	http.ResponseWriter
	status               int
	written, writeFailed bool
}

func (w *coreStatusWriter) WriteHeader(status int) {
	if status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	if w.written {
		return
	}
	w.written = true
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *coreStatusWriter) Write(data []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.writeFailed = w.writeFailed || err != nil || n != len(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}

func (w *coreStatusWriter) FlushError() error {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.writeFailed = w.writeFailed || err != nil
	return err
}

type inferenceDiagnosticContext struct{}
type inferenceDiagnostic struct{ outcome, sessionID string }

func (w *coreStatusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func instrumentCoreHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		route := coreRoute(r)
		ctx, span := otel.Tracer("newnoa-desktop-core").Start(ctx, route, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(
			attribute.String("http.request.method", r.Method), attribute.String("http.route", route),
		))
		defer span.End()
		writer := &coreStatusWriter{ResponseWriter: w, status: http.StatusOK}
		diagnostic := &inferenceDiagnostic{}
		ctx = context.WithValue(ctx, inferenceDiagnosticContext{}, diagnostic)
		inference := historyInferencePath(r.URL.Path) || r.URL.Path == "/v1/models"
		started, completed, localID := time.Now(), false, ""
		if inference {
			localID = uuid.NewString()
			w.Header().Set("X-Newnoa-Local-Request", localID)
			r.Header.Set("X-Newnoa-Local-Request", localID)
		}
		defer func() {
			if !inference {
				return
			}
			interrupted := !completed || writer.writeFailed || r.Context().Err() != nil || diagnostic.outcome == "unknown" || diagnostic.outcome == "cancelled" || writer.status < 400 && diagnostic.outcome == "failed"
			fields := []any{"local_request_id", localID, "http.route", route, "http.response.status_code", writer.status, "duration_ms", time.Since(started).Milliseconds(), "interrupted", interrupted, "downstream_cancelled", r.Context().Err() != nil}
			if diagnostic.outcome != "" {
				fields = append(fields, "outcome", diagnostic.outcome)
			}
			if id := diagnostic.sessionID; id != "" {
				fields = append(fields, "codex_session_id", id)
			} else if id := r.Header.Get("session_id"); uuid.Validate(id) == nil {
				fields = append(fields, "codex_session_id", id)
			}
			if id := w.Header().Get("X-Newnoa-Request"); uuid.Validate(id) == nil {
				fields = append(fields, "request_id", id)
			}
			switch transport := w.Header().Get("X-Newnoa-Path"); transport {
			case "direct", "peer-direct", "peer-tunnel", "core-hosted-direct":
				fields = append(fields, "transport_path", transport)
			}
			if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
				fields = append(fields, "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
			}
			slog.InfoContext(ctx, "proxy.inference.completed", fields...)
		}()
		next.ServeHTTP(writer, r.WithContext(ctx))
		completed = true
		span.SetAttributes(attribute.Int("http.response.status_code", writer.status))
		if writer.status >= 500 {
			span.SetStatus(codes.Error, "http_error")
		}
	})
}

func coreRoute(r *http.Request) string {
	if historyInferencePath(r.URL.Path) || r.URL.Path == "/v1/models" {
		return r.Method + " " + r.URL.Path
	}
	switch r.URL.Path {
	case "/api/local/core":
		return r.Method + " /api/local/core"
	case "/health", "/ready":
		return r.Method + " " + r.URL.Path
	default:
		return r.Method + " /{unmatched}"
	}
}
