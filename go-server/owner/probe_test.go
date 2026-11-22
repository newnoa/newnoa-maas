package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOwnerProbeKeepsServingRouteAndScopedWireRequest(t *testing.T) {
	for _, path := range []string{"chat/completions", "responses", "messages", "embeddings", "rerank", "systemone", "audio/transcriptions", "images/generations", "models"} {
		t.Run(path, func(t *testing.T) {
			method, raw, contentType := "POST", []byte("{\"model\":\"fixture\"}"), "application/json"
			if path == "models" {
				method, raw = "GET", nil
			}
			if path == "audio/transcriptions" {
				var buf bytes.Buffer
				form := multipart.NewWriter(&buf)
				_ = form.WriteField("model", "fixture")
				part, _ := form.CreateFormFile("file", "probe.wav")
				_, _ = part.Write(bytes.Repeat([]byte("a"), maxBody+1))
				_ = form.Close()
				raw, contentType = buf.Bytes(), form.FormDataContentType()
			}
			calls := 0
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				if r.URL.Path != "/api/v1/internal/frp/owner-a/route" || r.Header.Get("Authorization") != "Bearer internal-fixture" || body["purpose"] != "serving" || body["epoch"] != float64(3) {
					t.Error("probe lease authority changed")
				}
				_ = json.NewEncoder(w).Encode(map[string]int{"port": 21001})
			}))
			defer control.Close()
			o := newOwner("owner-a", control.URL, "internal-fixture")
			o.upstream.Transport = embeddingTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				body, _ := io.ReadAll(r.Body)
				if r.Method != method || r.URL.String() != "http://127.0.0.1:21001/probe/v1/"+path || !bytes.Equal(body, raw) || r.Header.Get("Content-Type") != contentType {
					t.Error("probe tunnel wire request changed")
				}
				for key, want := range map[string]string{"X-Newnoa-Request": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "X-Newnoa-Permit": "probe_fixture", "X-Newnoa-Gateway": "gateway-a", "X-Newnoa-Epoch": "3"} {
					if r.Header.Get(key) != want {
						t.Errorf("%s changed", key)
					}
				}
				if r.GetBody != nil || method == "GET" && (r.Body == nil || r.Body == http.NoBody) || r.Header.Get("Authorization") != "" || r.Header.Get("X-Api-Key") != "" {
					t.Error("probe retry or credential boundary changed")
				}
				if path == "messages" && (r.Header.Get("Anthropic-Version") != "2023-06-01" || r.Header.Get("Anthropic-Beta") != "") {
					t.Error("probe messages version not fixed")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}, "X-Newnoa-Probe-Upstream": {"1"}, "X-Newnoa-Probe-Ping-Status": {"succeeded"}, "X-Newnoa-Probe-Ping-Ms": {"0.5"}}, Body: io.NopCloser(strings.NewReader("{}"))}, nil
			})
			req := ownerProbeRequest(method, path, raw)
			req.Header.Set("Content-Type", contentType)
			out := httptest.NewRecorder()
			o.ServeHTTP(out, req)
			if out.Code != 200 || calls != 1 || out.Header().Get("X-Newnoa-Probe-Upstream") != "1" {
				t.Fatalf("valid probe failed: %d %s", out.Code, out.Body.String())
			}
			if out.Header().Get("X-Newnoa-Probe-Ping-Ms") != "0.5" || out.Header().Get("X-Newnoa-Probe-Ping-Status") != "succeeded" {
				t.Fatal("endpoint ICMP headers lost")
			}
		})
	}
}

func ownerProbeRequest(method, path string, raw []byte) *http.Request {
	r := httptest.NewRequest(method, "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/3/probe/v1/"+path, bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer internal-fixture")
	r.Header.Set("X-Newnoa-Request", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	r.Header.Set("X-Newnoa-Permit", "probe_fixture")
	r.Header.Set("X-Newnoa-Gateway", "gateway-a")
	r.Header.Set("X-Newnoa-Epoch", "untrusted-header")
	return r
}

func TestOwnerProbeRejectsWrongVerbBodyBetaAndAuthorization(t *testing.T) {
	calls := 0
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(409) }))
	defer control.Close()
	o := newOwner("owner-a", control.URL, "internal-fixture")
	for _, tc := range []struct{ method, path, body, beta, token string }{
		{"POST", "models", "{}", "", "Bearer internal-fixture"}, {"GET", "models", "{}", "", "Bearer internal-fixture"}, {"GET", "chat/completions", "", "", "Bearer internal-fixture"}, {"POST", "unknown", "{}", "", "Bearer internal-fixture"}, {"POST", "messages", "{}", "unbound", "Bearer internal-fixture"}, {"POST", "chat/completions", "{}", "", ""},
	} {
		req := ownerProbeRequest(tc.method, tc.path, []byte(tc.body))
		req.Header.Set("Authorization", tc.token)
		if tc.beta != "" {
			req.Header.Set("Anthropic-Beta", "")
			req.Header.Add("Anthropic-Beta", tc.beta)
		}
		out := httptest.NewRecorder()
		o.ServeHTTP(out, req)
		if out.Code < 400 {
			t.Fatalf("invalid probe accepted: %+v", tc)
		}
	}
	if calls != 0 {
		t.Fatal("invalid probe requested a serving route")
	}
}

func TestOwnerOnlyForwardsProbeUpstreamMarkerOnProbeRoutes(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "{\"port\":21001}") }))
	defer control.Close()
	for _, tc := range []struct {
		probe        bool
		marker, want string
	}{{true, "1", "1"}, {false, "1", ""}, {true, "untrusted", ""}, {true, "", ""}} {
		o := newOwner("owner-a", control.URL, "internal-fixture")
		o.upstream.Transport = embeddingTransport(func(r *http.Request) (*http.Response, error) {
			h := http.Header{}
			h.Set("X-Newnoa-Probe-Upstream", tc.marker)
			return &http.Response{StatusCode: 404, Header: h, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		})
		r := ownerProbeRequest("POST", "chat/completions", []byte("{\"model\":\"fixture\"}"))
		if !tc.probe {
			r.URL.Path = strings.Replace(r.URL.Path, "/probe/", "/", 1)
		}
		out := httptest.NewRecorder()
		o.ServeHTTP(out, r)
		if out.Code != 404 || out.Header().Get("X-Newnoa-Probe-Upstream") != tc.want {
			t.Fatalf("probe marker leaked or lost: %+v %d %v", tc, out.Code, out.Header())
		}
	}
}
