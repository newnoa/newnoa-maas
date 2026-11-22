package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestOwnerPreservesConnectorAdmissionRejection(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int{"port": 25000})
	}))
	defer api.Close()
	o := newOwner("owner-a", api.URL, "internal-test")
	o.upstream.Transport = embeddingTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": {"1"}, "X-Newnoa-Outcome": {"not-started"}}, Body: io.NopCloser(strings.NewReader("admission rejected"))}, nil
	})
	r := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/messages", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer internal-test")
	w := httptest.NewRecorder()
	o.ServeHTTP(w, r)
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" || w.Header().Get("X-Newnoa-Outcome") != "not-started" {
		t.Fatalf("lost admission rejection: %d %v", w.Code, w.Header())
	}
}

func TestOwnerRefusesUnauthenticatedAndUnassignedRoutes(t *testing.T) {
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		json.NewEncoder(w).Encode(map[string]any{"port": 80})
	}))
	defer api.Close()
	o := newOwner("owner-a", api.URL, "internal-test")
	for _, auth := range []bool{false, true} {
		r := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/responses", strings.NewReader(`{}`))
		if auth {
			r.Header.Set("Authorization", "Bearer internal-test")
		}
		w := httptest.NewRecorder()
		o.ServeHTTP(w, r)
		want := 401
		if auth {
			want = 409
		}
		if w.Code != want {
			t.Fatalf("status %d", w.Code)
		}
	}
	if calls != 1 {
		t.Fatal("unauthenticated request reached control")
	}
}
func TestPluginFailsClosedAndIsLoopbackOnly(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer api.Close()
	o := newOwner("owner-a", api.URL, "internal-test")
	for _, remote := range []string{"192.0.2.1:1234", "127.0.0.1:1234"} {
		r := httptest.NewRequest("POST", "/plugin", strings.NewReader(`{"op":"Login"}`))
		r.RemoteAddr = remote
		w := httptest.NewRecorder()
		o.ServeHTTP(w, r)
		if remote == "192.0.2.1:1234" {
			if w.Code != 403 {
				t.Fatal("remote plugin request accepted")
			}
		} else if !strings.Contains(w.Body.String(), `"reject":true`) {
			t.Fatal("control outage allowed FRP login")
		}
	}
}

func TestPluginSeparatesConsumerAndProviderIdentity(t *testing.T) {
	for _, op := range []string{"Login", "Ping", "NewWorkConn", "NewProxy", "NewUserConn"} {
		for _, consumer := range []bool{false, true} {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				want := "/api/v1/internal/frp/owner-a/check"
				if consumer {
					want = "/api/v1/internal/frp/owner-a/consumer-check"
				}
				if r.URL.Path != want || r.Header.Get("Authorization") != "Bearer internal-test" {
					t.Errorf("wrong identity route or auth: %s", r.URL.Path)
				}
				json.NewEncoder(w).Encode(map[string]bool{"reject": false, "unchange": true})
			}))
			metas := map[string]any{"credential": "node_fixture"}
			if consumer {
				metas["consumerKey"] = "nn_fixture"
			}
			content := map[string]any{"metas": metas}
			if op != "Login" {
				content = map[string]any{"user": content}
			}
			raw, _ := json.Marshal(map[string]any{"op": op, "content": content})
			r := httptest.NewRequest("POST", "/plugin", strings.NewReader(string(raw)))
			r.RemoteAddr = "127.0.0.1:1234"
			w := httptest.NewRecorder()
			newOwner("owner-a", api.URL, "internal-test").ServeHTTP(w, r)
			api.Close()
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"reject":false`) {
				t.Fatal("plugin did not preserve control decision")
			}
		}
	}
}
func TestSeparatePublicAndPluginRoutes(t *testing.T) {
	o := newOwner("owner-a", "http://127.0.0.1:1", "internal-test")
	for _, check := range []struct {
		handler http.HandlerFunc
		path    string
	}{{o.publicOnly, "/plugin"}, {o.pluginOnly, "/tunnels/node/1/v1/responses"}} {
		w := httptest.NewRecorder()
		check.handler(w, httptest.NewRequest("POST", check.path, strings.NewReader(`{}`)))
		if w.Code != 404 {
			t.Fatal("public and plugin listeners overlapped")
		}
	}
	if !loopbackBind("127.0.0.1:18100") || loopbackBind("0.0.0.0:18100") || loopbackBind("192.0.2.1:18100") {
		t.Fatal("plugin bind boundary failed")
	}
}

func TestEvaluationRouteRequiresEvaluationPurpose(t *testing.T) {
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["purpose"] != "evaluation" || body["epoch"] != float64(3) {
			t.Error("evaluation route scope missing")
		}
		w.WriteHeader(409)
	}))
	defer api.Close()
	o := newOwner("owner-a", api.URL, "internal-test")
	for _, path := range []string{"chat/completions", "responses", "messages", "embeddings", "rerank", "audio/transcriptions", "systemone"} {
		r := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/3/evaluation/v1/"+path, strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer internal-test")
		w := httptest.NewRecorder()
		o.ServeHTTP(w, r)
		want := 409
		if path != "chat/completions" && path != "embeddings" && path != "rerank" && path != "systemone" && path != "audio/transcriptions" {
			want = 404
		}
		if w.Code != want {
			t.Fatalf("route %s: %d", path, w.Code)
		}
	}
	if calls != 5 {
		t.Fatal("unsupported evaluation protocol reached authority")
	}
}

func TestOwnerPropagatesTraceToControlButNotDeviceUpstream(t *testing.T) {
	installOwnerTelemetryTestProvider(t)
	var controlTrace, deviceTrace string
	var listener net.Listener
	for port := 20000; port < 30000; port++ {
		var err error
		listener, err = net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err == nil {
			break
		}
	}
	if listener == nil {
		t.Fatal("no available fixture port in the owner route range")
	}
	device := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deviceTrace = r.Header.Get("traceparent")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	device.Listener = listener
	device.Start()
	defer device.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		controlTrace = r.Header.Get("traceparent")
		_ = json.NewEncoder(w).Encode(map[string]any{"port": port})
	}))
	defer api.Close()
	o := newOwner("owner-a", api.URL, "internal-test")
	h := instrumentHTTP(o, "newnoa-owner", ownerRoute)
	r := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/responses", strings.NewReader(`{}`))
	r.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	r.Header.Set("Authorization", "Bearer internal-test")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("request status = %d: %s", w.Code, w.Body.String())
	}
	if controlTrace == "" {
		t.Fatal("trace context was not propagated to trusted control service")
	}
	if deviceTrace != "" {
		t.Fatalf("trace context escaped to device endpoint: %q", deviceTrace)
	}
}

func TestOwnerProtectedRouteChecksIdentityBeforeMethod(t *testing.T) {
	o := newOwner("owner", "http://127.0.0.1:1", "secret")
	for _, target := range []string{"/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/messages", "/other"} {
		r := httptest.NewRequest("GET", target, nil)
		w := httptest.NewRecorder()
		o.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Errorf("unidentified %s returned %d", target, w.Code)
		}
	}
	r := httptest.NewRequest("GET", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/messages", nil)
	r.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	o.ServeHTTP(w, r)
	if w.Code != 405 {
		t.Fatalf("authorized invalid method returned %d", w.Code)
	}
}
