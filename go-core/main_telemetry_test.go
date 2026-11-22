package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestPublicProxyTelemetryProcess(t *testing.T) {
	if os.Getenv("NEWNOA_TEST_PUBLIC_TELEMETRY_CHILD") == "true" {
		main()
		return
	}
	t.Skip("subprocess entrypoint")
}

func TestPublicProxyStartsTelemetryAndStopsCleanly(t *testing.T) {
	profile, trace := make(chan struct{}, 1), make(chan struct{}, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method == http.MethodPost {
			var received chan struct{}
			switch r.URL.Path {
			case "/ingest":
				if len(body) > 0 && strings.Contains(r.URL.Query().Get("name"), "newnoa-test-public-core") {
					received = profile
				}
			case "/v1/traces":
				if publicProxyTraceReceived(body) {
					received = trace
				}
			}
			select {
			case received <- struct{}{}:
			default:
			}
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{}`)
	}))
	defer receiver.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	secret := strings.Repeat("test-public-proxy-", 3)
	for name, value := range map[string]string{
		"NEWNOA_TEST_PUBLIC_TELEMETRY_CHILD": "true",
		"NEWNOA_PUBLIC_PROXY_ENABLED":        "true",
		"NEWNOA_PUBLIC_PROXY_LISTEN":         address,
		"NEWNOA_PUBLIC_PROXY_SECRET":         secret,
		"NEWNOA_INTERNAL_TOKEN":              "test-internal-token",
		"NEWNOA_CONTROL_URL":                 receiver.URL,
		"NEWNOA_PUBLIC_GO_SERVER":            "",
		"NEWNOA_CORE_TEST_HOME":              t.TempDir(),
		"NEWNOA_OTEL_ENABLED":                "true",
		"NEWNOA_OTEL_TRACES_ENDPOINT":        receiver.URL + "/v1/traces",
		"NEWNOA_OTEL_SAMPLE_RATIO":           "1",
		"PYROSCOPE_ENABLED":                  "true",
		"PYROSCOPE_SERVER_ADDRESS":           receiver.URL,
		"PYROSCOPE_APPLICATION_NAME":         "newnoa-test-public-core",
		"PYROSCOPE_UPLOAD_RATE":              "1s",
	} {
		t.Setenv(name, value)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPublicProxyTelemetryProcess$")
	var output bytes.Buffer
	child.Stdout, child.Stderr = &output, &output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		child.Process.Signal(syscall.SIGTERM)
		if err := child.Wait(); err != nil {
			t.Errorf("public proxy did not stop cleanly: %v; output=%s", err, output.String())
		}
	}()
	client := &http.Client{Timeout: time.Second}
	ready := false
	for stop := time.Now().Add(5 * time.Second); time.Now().Before(stop); time.Sleep(20 * time.Millisecond) {
		request, _ := http.NewRequest(http.MethodGet, "http://"+address+"/internal/www/health", nil)
		request.Header.Set("X-Newnoa-Public-Proxy", secret)
		response, err := client.Do(request)
		if err == nil {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
	}
	if !ready {
		t.Fatal("public proxy did not become ready")
	}
	for name, received := range map[string]<-chan struct{}{"profile": profile, "trace": trace} {
		select {
		case <-received:
		case <-time.After(8 * time.Second):
			t.Fatalf("ready public proxy did not export %s", name)
		}
	}
}

func publicProxyTraceReceived(body []byte) bool {
	var exported collectortrace.ExportTraceServiceRequest
	if proto.Unmarshal(body, &exported) != nil {
		return false
	}
	for _, resource := range exported.ResourceSpans {
		for _, scope := range resource.ScopeSpans {
			for _, span := range scope.Spans {
				if publicProxyHealthSpan(span) {
					return true
				}
			}
		}
	}
	return false
}

func publicProxyHealthSpan(span *tracepb.Span) bool {
	if span.Kind != tracepb.Span_SPAN_KIND_SERVER || span.Name != "GET /{unmatched}" {
		return false
	}
	for _, attr := range span.Attributes {
		if attr.Key == "http.response.status_code" && attr.Value.GetIntValue() == 200 {
			return true
		}
	}
	return false
}
