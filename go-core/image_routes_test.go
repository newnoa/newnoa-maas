package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	rpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
)

const imagePath = "/v1/images/generations"

type imageBodyReadConn struct {
	net.Conn
	began chan struct{}
	seen  []byte
	once  sync.Once
}

func (c *imageBodyReadConn) Read(data []byte) (int, error) {
	if bytes.Contains(c.seen, []byte(`{"data":[`)) {
		c.once.Do(func() { close(c.began) })
	}
	n, err := c.Conn.Read(data)
	if len(c.seen) < 1024 {
		c.seen = append(c.seen, data[:n]...)
	}
	return n, err
}

func TestImageProtocolRoutesPreservePayloadAndRejectInvalidRequests(t *testing.T) {
	calls := 0
	raw := `{"model":"image-model","prompt":"A red circle","n":1,"size":"1024x1024","output_format":"webp"}`
	gatewayHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			io.WriteString(w, "ready")
			return
		}
		calls++
		received, _ := io.ReadAll(r.Body)
		if r.URL.Path != imagePath || string(received) != raw {
			t.Errorf("wire changed: %s %s", r.URL.Path, received)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"data":[{"url":"https://images.example.test/result.png"}]}`)
	})
	server := rpc.NewServer(gatewayHandler)
	defer server.Stop()
	gateway, ca := gatewayTLSFixture(t, server, false)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		switch r.URL.Path {
		case "/api/v1/gateways":
			json.NewEncoder(w).Encode([]map[string]any{{"id": "image-node", "address": gateway.URL, "trustedCa": ca, "transport": "grpc"}})
		case "/api/v1/internal/public/authorize":
			io.WriteString(w, `{"hosted":false}`)
		default:
			t.Errorf("unexpected API path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()
	c := &core{control: api.URL, gateway: gateway.URL, gatewaySelector: newGatewaySelector(api.URL)}
	h, err := c.publicProxyHandler(publicTestSecret, publicTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		body string
		want int
	}{
		{raw, 200}, {`{"model":"image-model","messages":[]}`, 400},
		{`{"model":"image-model","prompt":" "}`, 400},
		{`{"model":"image-model","prompt":"test","stream":true}`, 400},
		{`{"model":"image-model","prompt":"test","n":1.5}`, 400},
		{`{"model":"image-model","prompt":"test","n":0}`, 400},
	} {
		r := httptest.NewRequest("POST", imagePath, strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer nn_user_aaaaaaaaa")
		r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
		r.Header.Set("Content-Type", "application/json")
		out := httptest.NewRecorder()
		h.ServeHTTP(out, r)
		if out.Code != tc.want {
			t.Errorf("%s: %d %s", tc.body, out.Code, out.Body.String())
		}
	}
	if calls != 1 {
		t.Fatalf("invalid image requests reached gateway: %d", calls)
	}
	if !historyInferencePath(imagePath) || historyModelType("image") != "image" {
		t.Fatal("image history classification missing")
	}
}

func TestImageAliasAndEndpointNormalization(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	r := httptest.NewRequest("POST", imagePath, strings.NewReader(`{"model":"newnoa/image-model","prompt":"test"}`))
	if err := c.resolveManagedModel(r, "Bearer nn_fixture"); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	json.NewDecoder(r.Body).Decode(&body)
	if body["model"] != "image-model" || body["prompt"] != "test" {
		t.Fatal("image alias not resolved", body)
	}
	u, err := modelListURL("http://127.0.0.1:8080/prefix/v1/images/generations")
	if err != nil || u.Path != "/prefix/v1/models" {
		t.Fatalf("model listing uses inference suffix: %v %v", u, err)
	}
}

func TestConnectorImageGenerationConsumesBoundPermit(t *testing.T) {
	raw := []byte(`{"model":"image-model","prompt":"test","n":1}`)
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		received, _ := io.ReadAll(r.Body)
		if r.URL.Path != imagePath || !bytes.Equal(raw, received) || r.Header.Get("Authorization") != "Bearer upstream-fixture" {
			t.Error("image execution changed")
		}
		io.WriteString(w, `{"data":[{"b64_json":"image-fixture"}],"usage":{"input_tokens":2,"output_tokens":3}}`)
	}))
	defer upstream.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		var claim map[string]any
		json.NewDecoder(r.Body).Decode(&claim)
		if claim["bodyDigest"] != hash(append([]byte("POST "+imagePath+"\n"), raw...)) {
			t.Error("image digest unbound")
		}
		json.NewEncoder(w).Encode(map[string]any{"endpoint": upstream.URL + "/v1/images/generations", "model": "image-model", "apiKey": "upstream-fixture", "httpMethod": "POST", "inferencePath": imagePath})
	}))
	defer api.Close()
	n := &connector{purpose: "serving", control: api.URL, credential: "node_fixture", client: api.Client(), upstream: upstream.Client()}
	r := probeRequest("POST", imagePath, raw)
	r.URL.Path = imagePath
	r.Header.Set("X-Newnoa-Permit", "permit_fixture")
	out := httptest.NewRecorder()
	n.ServeHTTP(out, r)
	if out.Code != 200 || calls != 1 {
		t.Fatalf("image route=%d %s calls=%d", out.Code, out.Body.String(), calls)
	}
}

func TestImageUsageCaptureAllowsBoundedImageBodyAndRetainsMissingUsage(t *testing.T) {
	for _, usage := range []string{`,"usage":{"input_tokens":2,"output_tokens":3}`, `,"usage":{"input_tokens":0,"output_tokens":0}`, ``} {
		raw := []byte(`{"data":[{"b64_json":"` + strings.Repeat("a", historyBodyLimit+1) + `"}]` + usage + `}`)
		capture := newUsageCapture(false)
		capture.protocol = imagePath
		capture.feed(raw)
		report := capture.counts()
		if capture.invalid || capture.overflow {
			t.Fatal("image response incorrectly uses text capture limit")
		}
		if usage == "" && (report.Input != nil || report.Output != nil) {
			t.Fatal("missing image usage fabricated")
		}
		if usage != "" && (report.Input == nil || report.Output == nil) {
			t.Fatal("image usage lost")
		}
	}
}

func TestHostedImagesPreserveResponseUsageAndCredentialBoundary(t *testing.T) {
	for _, usage := range []string{`,"usage":{"input_tokens":2,"output_tokens":3}`, `,"usage":{"input_tokens":0,"output_tokens":0}`, ``} {
		t.Run(usage, func(t *testing.T) {
			raw := []byte(`{"model":"` + hostedFixtureID + `","prompt":"A red circle","n":1,"size":"1024x1024","output_format":"webp"}`)
			result := `{"data":[{"b64_json":"` + strings.Repeat("a", historyBodyLimit+1) + `"}]` + usage + `}`
			var finish map[string]any
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				received, _ := io.ReadAll(r.Body)
				var body map[string]any
				json.Unmarshal(received, &body)
				if r.URL.Path != imagePath || body["model"] != "upstream-image" || body["prompt"] != "A red circle" || body["n"] != float64(1) || body["output_format"] != "webp" || r.Header.Get("Authorization") != "Bearer upstream-only" || r.Header.Get("X-Api-Key") != "" {
					t.Error("hosted image request/credentials changed", r.URL.Path, body)
				}
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, result)
			}))
			defer upstream.Close()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hostedMeterFixtureAck(t, w, r) {
					return
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/models"):
					io.WriteString(w, `[{"id":"`+hostedFixtureID+`","modelType":"image"}]`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "upstream-image", ModelType: "image", Endpoint: "http://upstream.test/v1/images/generations", APIKey: "upstream-only", Revision: 1})
				case strings.HasSuffix(r.URL.Path, "/finish"):
					decodeHostedFixtureClosure(t, r, &finish)
					io.WriteString(w, `{"finalized":true}`)
				case r.URL.Path == "/api/v1/me":
					io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
				default:
					t.Error("unexpected API", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control = api.URL
			c.onlineSupply = fixturePublicPolicy(upstream)
			defer c.shutdown()
			r := httptest.NewRequest("POST", imagePath, bytes.NewReader(raw))
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("X-Api-Key", "nn_fixture")
			r.Header.Set("Content-Type", "application/json")
			out := httptest.NewRecorder()
			c.hostedInference(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("hosted image fell through") })).ServeHTTP(out, r)
			if out.Code != 200 || out.Body.String() != result || calls != 1 || finish["status"] != "succeeded" {
				t.Fatalf("image forwarding/report failed status=%d calls=%d finish=%v", out.Code, calls, finish)
			}
			if usage == "" && (finish["inputTokens"] != nil || finish["outputTokens"] != nil) {
				t.Fatal("missing usage changed to zero", finish)
			}
			if usage != "" && (finish["inputTokens"] == nil || finish["outputTokens"] == nil) {
				t.Fatal("image usage missing", finish)
			}
		})
	}
}

func TestImageHistoryStoresMetadataWithoutImagePayloadOrAccessURL(t *testing.T) {
	for _, response := range []string{`{"data":[{"b64_json":"PRIVATE-BASE64","revised_prompt":"circle"}],"usage":{"input_tokens":2,"output_tokens":3}}`, `{"data":[{"url":"https://images.example.test/private?token=SECRET"}]}`, `{"data":[{"b64_json":"PRIVATE-TRUNCATED`} {
		record := map[string]any{"protocol": imagePath, "response": response, "contentType": "application/json"}
		recoverHistoryUsage(record)
		saved := record["response"].(string)
		if strings.Contains(saved, "PRIVATE") || strings.Contains(saved, "SECRET") || strings.Contains(saved, "https://") {
			t.Fatal("image content/access URL stored", saved)
		}
		if strings.Contains(response, "usage") && (record["inputTokens"] != int64(2) || record["outputTokens"] != int64(3)) {
			t.Fatal("image counts lost in history", record)
		}
		recoverHistoryUsage(record)
		if record["response"] != saved {
			t.Fatal("image metadata changed on repeated history recovery")
		}
	}
}

func TestImageResponsesStayBoundedAndNonStreaming(t *testing.T) {
	for _, tc := range []struct {
		body, contentType string
		want              bool
	}{
		{`{"data":[{"url":"https://images.example.test/result.png"}]}`, "application/json", true},
		{strings.Repeat("a", imageResponseLimit+1), "application/json", false},
		{"data: {}\n\n", "text/event-stream", false},
	} {
		response := &http.Response{Header: http.Header{"Content-Type": []string{tc.contentType}}, Body: io.NopCloser(strings.NewReader(tc.body))}
		err := boundImageResponse(response)
		if (err == nil) != tc.want {
			t.Fatalf("body length=%d content type=%s err=%v", len(tc.body), tc.contentType, err)
		}
		if tc.want {
			got, _ := io.ReadAll(response.Body)
			if string(got) != tc.body {
				t.Fatal("bounded response changed")
			}
		}
	}
}

func TestImageGatewayCancellationDoesNotReplay(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	calls := 0
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer gateway.Close()
	h, err := privateInference("http://127.0.0.1:1", gateway.URL)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := httptest.NewRequest("POST", imagePath, strings.NewReader(`{"model":"image-model","prompt":"test"}`)).WithContext(ctx)
	done := make(chan struct{})
	go func() { defer close(done); h.ServeHTTP(httptest.NewRecorder(), r) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("image request never reached server")
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("image cancellation not propagated")
	}
	<-done
	if calls != 1 {
		t.Fatalf("cancelled image replayed: %d", calls)
	}
}

func TestImageDesktopProxyExposesImageRoute(t *testing.T) {
	raw := `{"model":"image-model","prompt":"A red circle","n":1}`
	calls := 0
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		received, _ := io.ReadAll(r.Body)
		if r.URL.Path != imagePath || string(received) != raw {
			t.Error("desktop image route changed")
		}
		io.WriteString(w, `{"data":[{"url":"https://images.example.test/result.png"}]}`)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	control := httptest.NewServer(http.NotFoundHandler())
	defer control.Close()
	c.control = control.URL
	defer c.close()
	port := availableProxyPort(t)
	if err := startProxyRPC(t, c, port); err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest("POST", c.proxyBaseURL()+imagePath, strings.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || calls != 1 {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("desktop image route %d calls=%d %s", response.StatusCode, calls, body)
	}
}

func TestHostedImageCancellationWhileReadingResponseReportsCancelled(t *testing.T) {
	started := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"data":[`)
		http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}))
	defer upstream.Close()
	var finish map[string]any
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/finish") {
			decodeHostedFixtureClosure(t, r, &finish)
			io.WriteString(w, `{"finalized":true}`)
			return
		}
		json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "upstream-image", ModelType: "image", Endpoint: "http://upstream.test/v1", APIKey: "secret", Revision: 1})
	}))
	defer api.Close()
	policy := fixturePublicPolicy(upstream)
	connect := policy.connect
	policy.connect = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := connect(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &imageBodyReadConn{Conn: conn, began: started}, nil
	}
	c := &core{publicStateless: true, publicInternalToken: "internal-test", control: api.URL, onlineSupply: policy}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw := []byte(`{"model":"` + hostedFixtureID + `","prompt":"test"}`)
	r := httptest.NewRequest("POST", imagePath, bytes.NewReader(raw)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer ses_fixture")
	done := make(chan struct{})
	go func() { defer close(done); c.executeHosted(httptest.NewRecorder(), r, hostedFixtureID, raw) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("hosted image did not reach upstream")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("hosted image did not stop")
	}
	if finish["status"] != "cancelled" || finish["transportConfirmed"] != true {
		t.Fatal("cancelled image reported as failure or unexecuted", finish)
	}
}
