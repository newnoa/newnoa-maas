package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectorRequiresDeviceConsumptionAndForwardsOnlyBoundPayload(t *testing.T) {
	for _, purpose := range []string{"serving", "evaluation"} {
		t.Run(purpose, func(t *testing.T) { testConnectorBoundPayload(t, purpose, "/v1/chat/completions") })
	}
	t.Run("embeddings", func(t *testing.T) { testConnectorBoundPayload(t, "serving", "/v1/embeddings") })
	t.Run("rerank", func(t *testing.T) { testConnectorBoundPayload(t, "serving", "/v1/rerank") })
	t.Run("asr", func(t *testing.T) { testConnectorBoundPayload(t, "serving", "/v1/audio/transcriptions") })
	t.Run("asr-evaluation", func(t *testing.T) { testConnectorBoundPayload(t, "evaluation", "/v1/audio/transcriptions") })
	t.Run("decision", func(t *testing.T) { testConnectorBoundPayload(t, "serving", "/v1/systemone") })
	t.Run("messages", func(t *testing.T) { testConnectorBoundPayload(t, "serving", "/v1/messages") })
	t.Run("versioned-base-url", func(t *testing.T) { testConnectorBoundPayload(t, "serving", "/v1/chat/completions", "/v1") })
	t.Run("embedding-evaluation", func(t *testing.T) { testConnectorBoundPayload(t, "evaluation", "/v1/embeddings") })
	t.Run("reranking-evaluation", func(t *testing.T) { testConnectorBoundPayload(t, "evaluation", "/v1/rerank") })
	t.Run("decision-evaluation", func(t *testing.T) { testConnectorBoundPayload(t, "evaluation", "/v1/systemone") })
}

func testConnectorBoundPayload(t *testing.T, purpose string, inferencePath string, basePath ...string) {
	var executions, consumes atomic.Int32
	beta := ""
	if inferencePath == "/v1/messages" {
		beta = "claude-code-20250219,effort-2025-11-24"
	}
	contentType := "application/json"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executions.Add(1)
		if r.Header.Get("Content-Type") != contentType {
			t.Error("content type changed")
		}
		if r.URL.Path != inferencePath {
			t.Error("inference path changed")
		}
		auth := r.Header.Get("Authorization") == "Bearer upstream-secret"
		if inferencePath == "/v1/messages" {
			if r.Header.Get("Anthropic-Beta") != beta {
				t.Error("beta lost")
			}
			auth = r.Header.Get("X-Api-Key") == "upstream-secret" && r.Header.Get("Authorization") == "" && r.Header.Get("Anthropic-Version") == "2023-06-01"
		}
		if !auth || r.Header.Get("X-Newnoa-Permit") != "" {
			t.Error("credential boundary failed")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	defer upstream.Close()
	raw := `{"model":"approved-model","messages":[{"role":"user","content":"test"}]}`
	if inferencePath == "/v1/embeddings" {
		raw = `{"model":"approved-model","input":"text"}`
	}
	if inferencePath == "/v1/audio/transcriptions" {
		var buf bytes.Buffer
		form := multipart.NewWriter(&buf)
		form.WriteField("model", "approved-model")
		file, _ := form.CreateFormFile("file", "sample.wav")
		file.Write(bytes.Repeat([]byte("a"), maxFrame+1))
		form.Close()
		raw, contentType = buf.String(), form.FormDataContentType()
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := "/api/v1/connector/dispatches/"
		if purpose == "evaluation" {
			path = "/api/v1/connector/evaluations/"
		}
		if !strings.HasPrefix(r.URL.Path, path) {
			t.Error("wrong purpose consumption route")
		}
		if r.Header.Get("Authorization") != "Bearer node_test" {
			t.Error("device authentication missing")
		}
		var b map[string]any
		json.NewDecoder(r.Body).Decode(&b)
		bound := raw
		if purpose != "evaluation" || inferencePath != "/v1/chat/completions" {
			bound = "POST " + inferencePath + "\n" + raw
			if inferencePath == "/v1/messages" {
				bound = "POST " + inferencePath + "\n" + messagesBinding(beta) + raw
			}
			if inferencePath == "/v1/audio/transcriptions" {
				bound = "POST " + inferencePath + "\n" + contentType + "\n" + raw
			}
		}
		if b["bodyDigest"] != hash([]byte(bound)) || b["epoch"] != float64(4) {
			t.Error("execution binding missing")
		}
		if consumes.Add(1) != 1 {
			w.WriteHeader(409)
			return
		}
		endpoint := upstream.URL
		if len(basePath) > 0 {
			endpoint += basePath[0]
		}
		json.NewEncoder(w).Encode(map[string]any{"endpoint": endpoint, "model": "approved-model", "apiKey": "upstream-secret"})
	}))
	defer api.Close()
	n := &connector{control: api.URL, purpose: purpose, credential: "node_test", client: api.Client(), upstream: upstream.Client()}
	request := func(auth bool) *httptest.ResponseRecorder {
		path, prefix := inferencePath, "permit_"
		if purpose == "evaluation" {
			path, prefix = "/evaluation"+inferencePath, "eval_"
		}
		r := httptest.NewRequest("POST", path, strings.NewReader(raw))
		r.Header.Set("Content-Type", contentType)
		if beta != "" {
			r.Header.Set("Anthropic-Beta", beta)
		}
		if auth {
			r.Header.Set("X-Newnoa-Request", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
			r.Header.Set("X-Newnoa-Permit", prefix+"test")
			r.Header.Set("X-Newnoa-Gateway", "gateway-a")
			r.Header.Set("X-Newnoa-Epoch", "4")
		}
		w := httptest.NewRecorder()
		n.ServeHTTP(w, r)
		return w
	}
	if request(false).Code != 401 || consumes.Load() != 0 {
		t.Fatal("unauthorized request reached control")
	}
	if request(true).Code != 200 || executions.Load() != 1 {
		t.Fatal("valid request did not execute")
	}
	if request(true).Code != 409 || executions.Load() != 1 {
		t.Fatal("consumed permit replayed")
	}
}

func TestConnectorPermitCannotMoveBetweenInferencePaths(t *testing.T) {
	const raw = `{"model":"fixture","input":"test","messages":[{"role":"user","content":"test"}]}`
	var executions, consumed atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executions.Add(1)
		if r.URL.Path != "/v1/responses" {
			t.Error("permit executed on another path")
		}
		w.Write([]byte(`{}`))
	}))
	defer model.Close()
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["bodyDigest"] != hash([]byte("POST /v1/responses\n"+raw)) || !consumed.CompareAndSwap(0, 1) {
			w.WriteHeader(409)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"endpoint": model.URL, "model": "fixture"})
	}))
	defer control.Close()
	n := &connector{purpose: "serving", control: control.URL, credential: "node_fixture", client: control.Client(), upstream: model.Client()}
	for index, path := range []string{"/v1/embeddings", "/v1/responses", "/v1/responses"} {
		req := httptest.NewRequest("POST", path, strings.NewReader(raw))
		req.Header.Set("X-Newnoa-Request", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
		req.Header.Set("X-Newnoa-Permit", "permit_fixture")
		req.Header.Set("X-Newnoa-Gateway", "fixture")
		req.Header.Set("X-Newnoa-Epoch", "1")
		res := httptest.NewRecorder()
		n.ServeHTTP(res, req)
		want := 409
		if index == 1 {
			want = 200
		}
		if res.Code != want || executions.Load() != int32((index+1)/2) {
			t.Fatalf("path binding/replay failed at %d: status=%d executions=%d", index, res.Code, executions.Load())
		}
	}
}
func TestConnectorControlURLAndEnrollmentBoundaries(t *testing.T) {
	for _, value := range []string{"http://external.example", "https://user:secret@example.com", "https://example.com?token=x", "file:///etc/passwd"} {
		if _, err := controlBase(value); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	if c.startConnector("bad-id", "node_secret", "owner-a", "") == nil {
		t.Fatal("invalid enrollment accepted")
	}
}

func TestEvaluationLeaseRejectsConsumerRequestsLocally(t *testing.T) {
	n := &connector{purpose: "evaluation"}
	w := httptest.NewRecorder()
	n.ServeHTTP(w, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{}`)))
	if w.Code != 403 {
		t.Fatalf("evaluation lease accepted consumption: %d", w.Code)
	}
	if n.status()["purpose"] != "evaluation" {
		t.Fatal("connector did not expose its lease purpose")
	}
}

func TestConnectorRejectsChangedLeasePurposeBeforeStartingFRP(t *testing.T) {
	n := &connector{purpose: "evaluation"}
	if err := n.attempt(nodeLease{Purpose: "serving"}); err == nil || err.Error() != "lease purpose mismatch" {
		t.Fatalf("lease purpose mismatch accepted: %v", err)
	}
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	if err := c.startConnector("bad-id", "node_secret", "owner-a", "unknown"); err == nil || err.Error() != "invalid connector lease purpose" {
		t.Fatalf("unknown purpose accepted: %v", err)
	}
}

func TestRejectedDeviceStopsConnectorWithoutReconnect(t *testing.T) {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["purpose"] != "evaluation" {
			t.Error("requested lease purpose was not sent to authority")
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer api.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &connector{ctx: ctx, cancel: cancel, done: make(chan struct{}), nodeID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", credential: "node_test", purpose: "evaluation", control: api.URL, client: api.Client(), upstream: api.Client(), server: &http.Server{}}
	go n.run()
	select {
	case <-n.done:
	case <-time.After(300 * time.Millisecond):
		cancel()
		<-n.done
		t.Fatal("rejected device kept reconnecting")
	}
	if n.status()["failureCode"] != "device_unavailable" || n.status()["failureAt"] == "" {
		t.Fatal("rejection diagnostic missing")
	}
	if calls.Load() != 1 || n.status()["state"] != "stopped" || n.credential != "" {
		t.Fatal("device revocation did not stop and clear credential")
	}
}

func TestConnectorDiagnosticsSanitized(t *testing.T) {
	n := &connector{}
	for _, tc := range []struct {
		stage string
		err   error
		code  string
	}{
		{"lease", errDeviceUnavailable, "device_unavailable"},
		{"heartbeat", errDeviceAdmission, "admission_unavailable"},
		{"configuration", errors.New("secret configuration"), "configuration_failed"},
		{"process", errors.New("secret path"), "process_failed"},
		{"tunnel", errors.New("secret endpoint"), "tunnel_unavailable"},
		{"ready", errors.New("secret response"), "control_unavailable"},
	} {
		n.setStage(tc.stage)
		n.recordFailure(tc.err)
		status := n.status()
		if status["failureCode"] != tc.code || status["failureAt"] == "" || status["stage"] != tc.stage {
			t.Fatalf("unexpected diagnostics: %v", status)
		}
	}
	before := n.status()["failureAt"]
	n.recordFailure(context.Canceled)
	if n.status()["failureAt"] != before {
		t.Fatal("cancellation overwrote failure")
	}
}

func TestConnectorRunReportsConfigurationAndProcessFailure(t *testing.T) {
	const nodeID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	_, ca := gatewayTLSFixture(t, http.NotFoundHandler(), false)
	for _, code := range []string{"configuration_failed", "process_failed"} {
		t.Run(code, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/leases") {
					json.NewEncoder(w).Encode(map[string]any{})
					return
				}
				lease := nodeLease{Purpose: "serving", Epoch: 1, Credential: "frp_fixture"}
				if code == "process_failed" {
					lease.FRP.ServerAddr = "127.0.0.1"
					lease.FRP.ServerPort = 7000
					lease.FRP.RemotePort = 24000
					lease.FRP.ServerName = "fixture"
					lease.FRP.ProxyName = "newnoa_" + nodeID + "_1"
					lease.FRP.TrustedCA = ca
				}
				json.NewEncoder(w).Encode(lease)
			}))
			defer api.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			n := &connector{ctx: ctx, cancel: cancel, done: make(chan struct{}), nodeID: nodeID, credential: "node_fixture", purpose: "serving", control: api.URL, binary: filepath.Join(t.TempDir(), "missing-frpc"), address: "127.0.0.1:1", client: api.Client(), upstream: api.Client(), server: &http.Server{}}
			go n.run()
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) && n.status()["failureCode"] != code {
				time.Sleep(time.Millisecond)
			}
			cancel()
			<-n.done
			status := n.status()
			if status["failureCode"] != code || status["state"] != "stopped" || status["failureAt"] == "" {
				t.Fatalf("wrong stopped diagnostic: %v", status)
			}
			if n.credential != "" {
				t.Fatal("credential retained after stop")
			}
		})
	}
}

func TestFRPLeaseHostRequiresBareDNSNameOrIPAddress(t *testing.T) {
	for _, host := range []string{"frps.internal", "node-1.example.org.", "192.168.1.1", "fd00::5"} {
		if !validFRPHost(host) {
			t.Fatalf("rejected %q", host)
		}
	}
	for _, host := range []string{"", "host:9090", "https://host", "user@host", "host/path", ".example", "bad..name", "0.0.0.0", "ff02::1"} {
		if validFRPHost(host) {
			t.Fatalf("accepted %q", host)
		}
	}
}
