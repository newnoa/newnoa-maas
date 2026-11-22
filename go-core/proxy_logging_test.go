package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestProxyLogsCorrelateWithoutPayloadOrCredentials(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	requestID := uuid.NewString()
	release := make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(raw), "private-prompt") {
			t.Error("request changed")
		}
		w.Header().Set("X-Newnoa-Request", requestID)
		w.Header().Set("X-Newnoa-Path", "direct")
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: private-output\n\n")
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer upstream.Close()
	defer func() { release <- struct{}{} }()
	c := fixedGatewayTestCore(t.TempDir(), upstream.URL)
	control := httptest.NewServer(http.NotFoundHandler())
	defer control.Close()
	c.control = control.URL
	if err := c.startProxyAt("nn_"+strings.Repeat("x", 40), availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	defer c.shutdown()
	req, _ := http.NewRequest("POST", c.proxyBaseURL()+"/v1/responses", strings.NewReader(`{"model":"fixture","input":"private-prompt","prompt_cache_key":"`+requestID+`"}`))
	req.Header.Set("Authorization", "Bearer secret-caller")
	req.Header.Set("session_id", "private-invalid-session")
	res, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(res.Body)
	if line, err := reader.ReadString('\n'); err != nil || line != "data: private-output\n" {
		t.Fatalf("first chunk was not flushed before EOF: %q %v", line, err)
	}
	release <- struct{}{}
	io.Copy(io.Discard, reader)
	res.Body.Close()
	local := res.Header.Get("X-Newnoa-Local-Request")
	if uuid.Validate(local) != nil {
		t.Fatalf("missing local correlation: %q", local)
	}
	var row map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &row); err != nil {
		t.Fatalf("invalid log %q: %v", logs.String(), err)
	}
	if row["request_id"] != requestID || row["local_request_id"] != local || row["codex_session_id"] != requestID || row["http.route"] != "POST /v1/responses" || row["http.response.status_code"] != float64(200) || row["transport_path"] != "direct" || row["interrupted"] != false {
		t.Fatalf("incomplete diagnostic: %v", row)
	}
	for _, secret := range []string{"private-prompt", "private-output", "secret-caller"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("log leaked %s", secret)
		}
	}
}

func TestProxyLogsAbortAndPreDispatchFailure(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(map[bool]string{false: "rejected", true: "aborted"}[abort], func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(previous)
			handler := instrumentCoreHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if abort {
					w.WriteHeader(200)
					panic(http.ErrAbortHandler)
				}
				http.Error(w, "private-error", 401)
			}))
			func() {
				defer func() {
					if p := recover(); p != nil && p != http.ErrAbortHandler {
						panic(p)
					}
				}()
				handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/responses?api_key=secret-query", strings.NewReader("private-body")))
			}()
			var row map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &row); err != nil {
				t.Fatalf("missing log: %q", logs.String())
			}
			if row["interrupted"] != abort || row["http.response.status_code"] != map[bool]float64{false: 401, true: 200}[abort] {
				t.Fatalf("wrong outcome: %v", row)
			}
			if strings.Contains(logs.String(), "private-") || strings.Contains(logs.String(), "secret-query") {
				t.Fatal("sensitive log")
			}
		})
	}
}

func TestProxyLogsHostedStreamWithoutTerminalEventAsInterrupted(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")
	}))
	defer upstream.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/models"):
			io.WriteString(w, `[{"id":"`+hostedFixtureID+`"}]`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "fixture", Endpoint: "http://upstream.test/v1", APIKey: "secret", Revision: 1})
		case strings.HasSuffix(r.URL.Path, "/finish"):
			io.WriteString(w, `{"finalized":true}`)
		case r.URL.Path == "/api/v1/me":
			io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
		default:
			t.Errorf("unexpected API %s", r.URL.Path)
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control, c.onlineSupply = api.URL, fixturePublicPolicy(upstream)
	defer c.shutdown()
	req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"`+hostedFixtureID+`","input":"test","stream":true}`))
	req.Header.Set("Authorization", "Bearer ses_fixture")
	req.Header.Set("X-Api-Key", "nn_fixture")
	handler := instrumentCoreHTTP(c.hostedInference(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("used Go Server") })))
	handler.ServeHTTP(httptest.NewRecorder(), req)
	var row map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &row); err != nil || row["interrupted"] != true {
		t.Fatalf("incomplete hosted stream logged as successful: %s", logs.String())
	}
}

type failingDiagnosticWriter struct {
	*httptest.ResponseRecorder
	failFlush bool
}

func (w failingDiagnosticWriter) Write(data []byte) (int, error) { return 0, io.ErrClosedPipe }
func (w failingDiagnosticWriter) FlushError() error {
	if w.failFlush {
		return io.ErrClosedPipe
	}
	return nil
}

func TestProxyLogsWriteAndFlushFailures(t *testing.T) {
	for _, flush := range []bool{false, true} {
		t.Run(map[bool]string{false: "write", true: "flush"}[flush], func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(previous)
			h := instrumentCoreHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var err error
				if flush {
					err = http.NewResponseController(w).Flush()
				} else {
					_, err = w.Write([]byte("private-output"))
				}
				if !errors.Is(err, io.ErrClosedPipe) {
					t.Errorf("failure swallowed: %v", err)
				}
			}))
			h.ServeHTTP(failingDiagnosticWriter{httptest.NewRecorder(), flush}, httptest.NewRequest("POST", "/v1/responses", nil))
			var row map[string]any
			if json.Unmarshal(logs.Bytes(), &row) != nil || row["interrupted"] != true {
				t.Fatalf("write failure not reported: %s", logs.String())
			}
		})
	}
}
