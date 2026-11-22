package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func probeRequest(method, path string, raw []byte) *http.Request {
	r := httptest.NewRequest(method, "/probe"+path, bytes.NewReader(raw))
	r.Header.Set("X-Newnoa-Request", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	r.Header.Set("X-Newnoa-Permit", "probe_fixture")
	r.Header.Set("X-Newnoa-Gateway", "gateway-a")
	r.Header.Set("X-Newnoa-Epoch", "4")
	return r
}

func TestConnectorProbeConsumesScopedPermitAndForwardsOnce(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/embeddings", "/v1/rerank", "/v1/systemone", "/v1/audio/transcriptions", "/v1/models"} {
		t.Run(path, func(t *testing.T) {
			method, raw, contentType := "POST", []byte("{\"model\":\"fixture\",\"input\":\"probe\"}"), "application/json"
			if path == "/v1/models" {
				method, raw = "GET", nil
			}
			if path == "/v1/audio/transcriptions" {
				var buf bytes.Buffer
				form := multipart.NewWriter(&buf)
				_ = form.WriteField("model", "fixture")
				part, _ := form.CreateFormFile("file", "probe.wav")
				_, _ = part.Write(bytes.Repeat([]byte("a"), maxFrame+1))
				_ = form.Close()
				raw, contentType = buf.Bytes(), form.FormDataContentType()
			}
			var executions, consumes atomic.Int32
			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				executions.Add(1)
				w.Header().Set("X-Newnoa-Probe-Upstream", "untrusted-model-value")
				w.Header().Set("X-Newnoa-Probe-Ping-Status", "succeeded")
				w.Header().Set("X-Newnoa-Probe-Ping-Ms", "999")
				body, _ := io.ReadAll(r.Body)
				if r.Method != method || r.URL.Path != path || !bytes.Equal(body, raw) || r.Header.Get("Content-Type") != contentType {
					t.Error("probe wire payload changed")
				}
				if r.Header.Get("X-Newnoa-Permit") != "" || r.Header.Get("X-Newnoa-Gateway") != "" {
					t.Error("scoped credentials escaped")
				}
				if path == "/v1/messages" {
					if r.Header.Get("X-Api-Key") != "upstream-fixture" || r.Header.Get("Authorization") != "" || r.Header.Get("Anthropic-Version") != "2023-06-01" || r.Header.Get("Anthropic-Beta") != "" {
						t.Error("probe Anthropic headers not fixed")
					}
				} else if r.Header.Get("Authorization") != "Bearer upstream-fixture" {
					t.Error("upstream credential missing")
				}
				_, _ = io.WriteString(w, "{}")
			}))
			defer model.Close()
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/connector/model-checks/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/consume" || r.Header.Get("Authorization") != "Bearer node_fixture" {
					t.Error("wrong consume callback")
				}
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				bound := method + " " + path + "\n"
				if path == "/v1/audio/transcriptions" {
					bound += contentType + "\n"
				}
				if body["bodyDigest"] != hash(append([]byte(bound), raw...)) || body["httpMethod"] != method || body["inferencePath"] != path || body["gatewayId"] != "gateway-a" || body["epoch"] != float64(4) || body["permit"] != "probe_fixture" {
					t.Error("probe binding missing")
				}
				if consumes.Add(1) != 1 {
					w.WriteHeader(409)
					return
				}
				endpoint := model.URL
				if path == "/v1/messages" {
					endpoint += "/v1"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": endpoint, "model": "fixture", "apiKey": "upstream-fixture", "httpMethod": method, "inferencePath": path, "deploymentId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "revision": 1})
			}))
			defer control.Close()
			n := &connector{purpose: "serving", control: control.URL, credential: "node_fixture", client: control.Client(), upstream: model.Client()}
			transport := n.upstream.Transport
			n.upstream.Transport = connectorFlushTransport(func(req *http.Request) (*http.Response, error) {
				if req.GetBody != nil || method == "GET" && (req.Body == nil || req.Body == http.NoBody) {
					t.Error("probe request remains replayable")
				}
				return transport.RoundTrip(req)
			})
			request := func(permit string) *httptest.ResponseRecorder {
				r := probeRequest(method, path, raw)
				r.Header.Set("Content-Type", contentType)
				r.Header.Set("X-Newnoa-Permit", permit)
				out := httptest.NewRecorder()
				n.ServeHTTP(out, r)
				return out
			}
			if got := request(""); got.Code != 401 || consumes.Load() != 0 {
				t.Fatalf("missing permit accepted: %d", got.Code)
			}
			if got := request("probe_fixture"); got.Code != 200 || executions.Load() != 1 || got.Header().Get("X-Newnoa-Probe-Upstream") != "1" {
				t.Fatalf("probe rejected: %d %s", got.Code, got.Body.String())
			} else if path == "/v1/models" && (got.Header().Get("X-Newnoa-Probe-Ping-Status") != "succeeded" || got.Header().Get("X-Newnoa-Probe-Ping-Ms") == "" || got.Header().Get("X-Newnoa-Probe-Ping-Ms") == "999") {
				t.Fatalf("endpoint ICMP measurement missing: %v", got.Header())
			}
			if got := request("probe_fixture"); got.Code != 409 || executions.Load() != 1 || got.Header().Get("X-Newnoa-Probe-Upstream") != "" {
				t.Fatalf("probe replay executed: %d", got.Code)
			}
			if n.assessments != nil {
				t.Error("probe refreshed assessment")
			}
		})
	}
}

func TestConnectorEndpointICMPSurvivesHTTPFailure(t *testing.T) {
	model := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	model.Close()
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": model.URL, "model": "fixture", "httpMethod": "GET", "inferencePath": "/v1/models"})
	}))
	defer control.Close()
	n := &connector{purpose: "serving", control: control.URL, credential: "node_fixture", client: control.Client(), upstream: model.Client()}
	out := httptest.NewRecorder()
	n.ServeHTTP(out, probeRequest("GET", "/v1/models", nil))
	if out.Code != 502 || out.Header().Get("X-Newnoa-Probe-Upstream") != "" || out.Header().Get("X-Newnoa-Probe-Ping-Status") != "succeeded" || out.Header().Get("X-Newnoa-Probe-Ping-Ms") == "" {
		t.Fatalf("ICMP result lost on HTTP failure: %d %v", out.Code, out.Header())
	}
}

func TestConnectorProbeRejectsWrongLeaseVerbAndScopeBeforeConsumption(t *testing.T) {
	var calls atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(409) }))
	defer control.Close()
	for _, tc := range []struct{ purpose, method, path, body, permit string }{
		{"evaluation", "POST", "/v1/chat/completions", "{\"model\":\"fixture\"}", "probe_fixture"},
		{"", "POST", "/v1/chat/completions", "{\"model\":\"fixture\"}", "probe_fixture"},
		{"serving", "GET", "/v1/chat/completions", "", "probe_fixture"},
		{"serving", "POST", "/v1/models", "{}", "probe_fixture"},
		{"serving", "GET", "/v1/models", "{}", "probe_fixture"},
		{"serving", "POST", "/v1/chat/completions", "{\"model\":\"fixture\"}", "permit_fixture"},
		{"serving", "POST", "/v1/unknown", "{\"model\":\"fixture\"}", "probe_fixture"},
	} {
		n := &connector{purpose: tc.purpose, control: control.URL, credential: "node_fixture", client: control.Client()}
		r := probeRequest(tc.method, tc.path, []byte(tc.body))
		r.Header.Set("X-Newnoa-Permit", tc.permit)
		out := httptest.NewRecorder()
		n.ServeHTTP(out, r)
		if out.Code < 400 {
			t.Fatalf("invalid probe accepted: %+v", tc)
		}
	}
	for _, header := range []string{"X-Newnoa-Request", "X-Newnoa-Gateway", "X-Newnoa-Epoch"} {
		n := &connector{purpose: "serving", control: control.URL, credential: "node_fixture", client: control.Client()}
		req := probeRequest("POST", "/v1/chat/completions", []byte("{\"model\":\"fixture\"}"))
		req.Header.Del(header)
		out := httptest.NewRecorder()
		n.ServeHTTP(out, req)
		if out.Code != 401 {
			t.Fatalf("missing %s accepted: %d", header, out.Code)
		}
	}
	n := &connector{purpose: "serving", control: control.URL, credential: "node_fixture", client: control.Client()}
	req := probeRequest("POST", "/v1/messages", []byte("{\"model\":\"fixture\"}"))
	req.Header.Set("Anthropic-Beta", "")
	req.Header.Add("Anthropic-Beta", "unbound")
	out := httptest.NewRecorder()
	n.ServeHTTP(out, req)
	if out.Code != 400 {
		t.Fatalf("unbound probe beta accepted: %d", out.Code)
	}

	if calls.Load() != 0 {
		t.Fatal("invalid probe reached consume callback")
	}
}

func TestConnectorProbeChecksReturnedMethodPathModelAndApprovedDigest(t *testing.T) {
	for _, mismatch := range []string{"method", "path", "model", "digest"} {
		t.Run(mismatch, func(t *testing.T) {
			var executions atomic.Int32
			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { executions.Add(1) }))
			defer model.Close()
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				if mismatch == "digest" && body["bodyDigest"] != hash([]byte("POST /v1/chat/completions\n{\"model\":\"fixture\",\"input\":\"approved\"}")) {
					w.WriteHeader(409)
					return
				}
				target := map[string]any{"endpoint": model.URL, "model": "fixture", "httpMethod": "POST", "inferencePath": "/v1/chat/completions"}
				switch mismatch {
				case "method":
					target["httpMethod"] = "GET"
				case "path":
					target["inferencePath"] = "/v1/responses"
				case "model":
					target["model"] = "other"
				}
				_ = json.NewEncoder(w).Encode(target)
			}))
			defer control.Close()
			n := &connector{purpose: "serving", control: control.URL, credential: "node_fixture", client: control.Client(), upstream: model.Client()}
			out := httptest.NewRecorder()
			n.ServeHTTP(out, probeRequest("POST", "/v1/chat/completions", []byte("{\"model\":\"fixture\",\"input\":\"tampered\"}")))
			if out.Code != 409 || executions.Load() != 0 || out.Header().Get("X-Newnoa-Probe-Upstream") != "" {
				t.Fatalf("%s mismatch executed: %d", mismatch, out.Code)
			}
		})
	}
}

func TestConnectorOnlyCreatesProbeMarkerAfterActualUpstreamResponse(t *testing.T) {
	for _, tc := range []struct {
		probe, closed bool
		status        int
	}{{true, false, 404}, {false, false, 200}, {true, true, 0}} {
		model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Newnoa-Probe-Upstream", "1")
			w.WriteHeader(tc.status)
			_, _ = io.WriteString(w, "{}")
		}))
		if tc.closed {
			model.Close()
		} else {
			defer model.Close()
		}
		control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": model.URL, "model": "fixture", "httpMethod": "POST", "inferencePath": "/v1/chat/completions"})
		}))
		defer control.Close()
		n := &connector{purpose: "serving", control: control.URL, credential: "node_fixture", client: control.Client(), upstream: model.Client()}
		r := probeRequest("POST", "/v1/chat/completions", []byte("{\"model\":\"fixture\"}"))
		if !tc.probe {
			r.URL.Path = "/v1/chat/completions"
			r.Header.Set("X-Newnoa-Permit", "permit_fixture")
		}
		out := httptest.NewRecorder()
		n.ServeHTTP(out, r)
		want := ""
		if tc.probe && !tc.closed {
			want = "1"
		}
		if out.Header().Get("X-Newnoa-Probe-Upstream") != want {
			t.Fatalf("probe marker leaked or missing: %+v %v", tc, out.Header())
		}
		if tc.closed && out.Code != 502 {
			t.Fatalf("closed model unexpectedly responded: %d", out.Code)
		}
	}
}
