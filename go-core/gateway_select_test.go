package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGatewayAddressesRequireTLSOutsidePrivateNetworks(t *testing.T) {
	for _, address := range []string{"https://gateway.example.test", "https://203.0.113.1:8443", "127.0.0.1:18091", "172.27.42.19:18091"} {
		if !validGatewayEndpoint(gatewayEndpoint{ID: "node", Address: address}) {
			t.Errorf("valid gateway rejected: %s", address)
		}
	}
	for _, address := range []string{"203.0.113.1:18091", "http://gateway.example.test", "https://user:secret@host", "https://host/path", "https://host?secret=x", "https://host#x", "https://host:0"} {
		if validGatewayEndpoint(gatewayEndpoint{ID: "node", Address: address}) {
			t.Errorf("unsafe gateway accepted: %s", address)
		}
	}
}

func TestNamedModelIsPreservedForAuthorizedServerSelection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) }))
	defer server.Close()
	selected := ""
	selector := newGatewaySelector(server.URL)
	selector.discover = func(_ context.Context, _ string, model string) ([]gatewayEndpoint, error) {
		selected = model
		return []gatewayEndpoint{{ID: "fixture", Address: server.Listener.Addr().String()}}, nil
	}
	selector.probe = func(context.Context, gatewayEndpoint) (time.Duration, error) { return time.Millisecond, nil }
	handler, err := privateInferenceWithSelector(server.URL, server.URL, selector)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"gpt-6-astra","input":"hello"}`))
	request.Header.Set("Authorization", "Bearer ses_fixture")
	request.Header.Set("X-Api-Key", "nn_"+strings.Repeat("k", 40))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if selected != "gpt-6-astra" || response.Code != 200 {
		t.Fatalf("selected model %q, status %d", selected, response.Code)
	}
}

func TestGatewaySelectorRoutesWithUserAuthorizationAndNoRegistrySecret(t *testing.T) {
	key := "nn_" + strings.Repeat("k", 40)
	var selected, fallback int
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.Write([]byte(`{"status":"up"}`))
			return
		}
		selected++
		if r.Header.Get("Authorization") != "Bearer "+key {
			t.Error("user authorization not forwarded")
		}
		w.Write([]byte(`{"data":[]}`))
	}))
	defer node.Close()
	address := node.Listener.Addr().(*net.TCPAddr).String()
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/gateways" || r.Header.Get("Authorization") != "Bearer "+key {
			t.Errorf("discovery request: %s %s", r.URL.Path, r.Header.Get("Authorization"))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode([]gatewayEndpoint{{ID: "go-242", Address: address}})
	}))
	defer control.Close()
	selector := newGatewaySelector(control.URL)
	fallbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fallback++ }))
	defer fallbackServer.Close()
	handler, err := privateInferenceWithSelector(control.URL, fallbackServer.URL, selector)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer "+key)
	handler.ServeHTTP(httptest.NewRecorder(), request)
	if selected != 1 || fallback != 0 {
		t.Fatalf("selected=%d fallback=%d", selected, fallback)
	}
}

func TestGatewaySelectorUsesLatencyThenLoadWithinNearLatencyBand(t *testing.T) {
	selected := chooseGateway([]gatewayProbeResult{
		{gatewayEndpoint: gatewayEndpoint{ID: "slow", ConnectionCount: 0, LatencyTolerancePercent: 10}, latency: 100 * time.Millisecond},
		{gatewayEndpoint: gatewayEndpoint{ID: "loaded", ConnectionCount: 8, LatencyTolerancePercent: 10}, latency: 50 * time.Millisecond},
		{gatewayEndpoint: gatewayEndpoint{ID: "light", ConnectionCount: 1, LatencyTolerancePercent: 10}, latency: 54 * time.Millisecond},
	}, "")
	if selected.ID != "light" {
		t.Fatalf("selected %q; near-latency nodes should prefer lower active load", selected.ID)
	}
}

func TestGatewaySelectorUsesAPIConnectionCountAndLatencyTolerance(t *testing.T) {
	for _, tolerance := range []int{5, 10} {
		var endpoints []gatewayEndpoint
		if err := json.Unmarshal([]byte(fmt.Sprintf(`[{"id":"fast","address":"127.0.0.1:18091","activeRequests":0,"connectionCount":8,"latencyTolerancePercent":%d},{"id":"light","address":"127.0.0.1:18092","activeRequests":20,"connectionCount":1,"latencyTolerancePercent":%d}]`, tolerance, tolerance)), &endpoints); err != nil {
			t.Fatal(err)
		}
		selected := chooseGateway([]gatewayProbeResult{{gatewayEndpoint: endpoints[0], latency: 50 * time.Millisecond}, {gatewayEndpoint: endpoints[1], latency: 54 * time.Millisecond}}, "")
		want := "fast"
		if tolerance == 10 {
			want = "light"
		}
		if selected.ID != want {
			t.Fatalf("API tolerance %d: selected %s; want %s by connections", tolerance, selected.ID, want)
		}
	}
}

func TestGatewaySelectionFailureNeverUsesConfiguredServer(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			fallback := 0
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { fallback++ }))
			defer server.Close()
			selector := newGatewaySelector(server.URL)
			selector.discover = func(context.Context, string, string) ([]gatewayEndpoint, error) {
				return nil, errors.New("bound server unavailable")
			}
			handler, err := privateInferenceWithSelector(server.URL, server.URL, selector)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(method, "/v1/chat/completions", strings.NewReader(`{"model":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("X-Api-Key", "nn_"+strings.Repeat("k", 40))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != http.StatusServiceUnavailable || fallback != 0 {
				t.Fatalf("status=%d configuredServerCalls=%d", w.Code, fallback)
			}
		})
	}
}

func TestPrivateLocalInferenceAlwaysUsesBoundServer(t *testing.T) {
	key := "nn_" + strings.Repeat("k", 40)
	model := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	for _, paired := range []bool{false, true} {
		t.Run(fmt.Sprint(paired), func(t *testing.T) {
			privateCalls, executions := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/private/") {
					privateCalls++
					io.WriteString(w, `{}`)
					return
				}
				executions++
				io.WriteString(w, `{"choices":[]}`)
			}))
			defer server.Close()
			selector := newGatewaySelector(server.URL)
			selector.discover = func(ctx context.Context, gotKey, gotModel string) ([]gatewayEndpoint, error) {
				if gotKey != key || gotModel != model {
					t.Errorf("discovery key=%q model=%q", gotKey, gotModel)
				}
				return []gatewayEndpoint{{ID: "bound", Address: strings.TrimPrefix(server.URL, "http://")}}, nil
			}
			selector.probe = func(context.Context, gatewayEndpoint) (time.Duration, error) { return time.Millisecond, nil }
			handler, err := privateInferenceWithSelector(server.URL, server.URL, selector)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"`+model+`"}`))
			r.Header.Set("Authorization", "Bearer "+key)
			if paired {
				r.Header.Set("Authorization", "Bearer ses_fixture")
				r.Header.Set("X-Api-Key", key)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != 200 || executions != 1 || privateCalls != 0 {
				t.Fatalf("status=%d executions=%d bypassCalls=%d", w.Code, executions, privateCalls)
			}
		})
	}
}

func TestGatewayHTTPRegistryPreservesFixedAndPublisherBindings(t *testing.T) {
	key := "nn_" + strings.Repeat("k", 40)
	models := []string{"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", "cccccccc-bbbb-cccc-dddd-eeeeeeeeeeee", "dddddddd-bbbb-cccc-dddd-eeeeeeeeeeee", "eeeeeeee-bbbb-cccc-dddd-eeeeeeeeeeee"}
	servers := map[string]*httptest.Server{}
	for _, id := range []string{"bound", "fast", "light", "fallback"} {
		servers[id] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/private/") {
				t.Error("private model bypassed Server execution")
			}
			io.WriteString(w, id)
		}))
		defer servers[id].Close()
	}
	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer ses_fixture" || r.Header.Get("X-Api-Key") != key {
			t.Error("registry credentials changed")
		}
		ids := []string{"bound"}
		switch r.URL.Query().Get("model") {
		case models[2]:
			ids = []string{"fast", "light"}
		case models[3]:
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var endpoints []map[string]any
		for _, id := range ids {
			connections := 1
			if id == "fast" {
				connections = 8
			}
			endpoints = append(endpoints, map[string]any{"id": id, "address": strings.TrimPrefix(servers[id].URL, "http://"), "connectionCount": connections, "latencyTolerancePercent": 10})
		}
		json.NewEncoder(w).Encode(endpoints)
	}))
	defer registry.Close()
	selector := newGatewaySelector(registry.URL)
	selector.probe = func(_ context.Context, endpoint gatewayEndpoint) (time.Duration, error) {
		if endpoint.ID == "bound" {
			return 80 * time.Millisecond, nil
		}
		if endpoint.ID == "fast" {
			return 50 * time.Millisecond, nil
		}
		return 54 * time.Millisecond, nil
	}
	for index, model := range models {
		if index == 4 {
			selector.probe = func(context.Context, gatewayEndpoint) (time.Duration, error) {
				return 0, errors.New("bound Server offline")
			}
		}
		handler, err := privateInferenceWithSelector(registry.URL, servers["fallback"].URL, selector)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"`+model+`"}`))
		r.Header.Set("Authorization", "Bearer ses_fixture")
		r.Header.Set("X-Api-Key", key)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if index >= 3 {
			if w.Code != 503 || strings.Contains(w.Body.String(), "fallback") {
				t.Fatalf("unavailable binding status=%d body=%s", w.Code, w.Body.String())
			}
			continue
		}
		want := "bound"
		if index == 2 {
			want = "light"
		}
		if w.Code != 200 || w.Body.String() != want {
			t.Fatalf("model=%s status=%d selected=%s want=%s", model, w.Code, w.Body.String(), want)
		}
	}
}

func TestGatewayDiscoveryAndCacheAreScopedToTheRequestedModel(t *testing.T) {
	first := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	second := "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
	calls := map[string]int{}
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		model := r.URL.Query().Get("model")
		calls[model]++
		id := "fixed-a"
		if model == second {
			id = "fixed-b"
		}
		json.NewEncoder(w).Encode([]gatewayEndpoint{{ID: id, Address: "127.0.0.1:18091"}})
	}))
	defer control.Close()
	selector := newGatewaySelector(control.URL)
	selector.probe = func(context.Context, gatewayEndpoint) (time.Duration, error) { return time.Millisecond, nil }
	key := "nn_" + strings.Repeat("k", 40)
	for _, model := range []string{first, second, first} {
		endpoint, ok := selector.selectGatewayForModel(context.Background(), key, model)
		expected := "fixed-a"
		if model == second {
			expected = "fixed-b"
		}
		if !ok || endpoint.ID != expected {
			t.Fatalf("model %s selected %v %v", model, endpoint, ok)
		}
	}
	if calls[first] != 1 || calls[second] != 1 {
		t.Fatalf("model-scoped cache/discovery: %v", calls)
	}
}

func TestGatewaySelectorDoesNotPinCurrentNodeAgainstLatencyAndLoadPolicy(t *testing.T) {
	results := []gatewayProbeResult{
		{gatewayEndpoint: gatewayEndpoint{ID: "current", ConnectionCount: 20, LatencyTolerancePercent: 10}, latency: 100 * time.Millisecond},
		{gatewayEndpoint: gatewayEndpoint{ID: "candidate", ConnectionCount: 0, LatencyTolerancePercent: 10}, latency: 85 * time.Millisecond},
	}
	if got := chooseGateway(results, "current"); got.ID != "candidate" {
		t.Fatalf("current node overrode lowest-latency band: %q", got.ID)
	}
	for i := range results {
		if results[i].ID == "candidate" {
			results[i].latency = 79 * time.Millisecond
		}
	}
	if got := chooseGateway(results, "current"); got.ID != "candidate" {
		t.Fatalf("large latency improvement did not switch: %q", got.ID)
	}
	if got := chooseGateway([]gatewayProbeResult{
		{gatewayEndpoint: gatewayEndpoint{ID: "current", ConnectionCount: 20, LatencyTolerancePercent: 10}, latency: 50 * time.Millisecond},
		{gatewayEndpoint: gatewayEndpoint{ID: "light", ConnectionCount: 1, LatencyTolerancePercent: 10}, latency: 54 * time.Millisecond},
	}, "current"); got.ID != "light" {
		t.Fatalf("current node overrode least-load rule: %q", got.ID)
	}
}

func TestGatewaySelectorSkipsUnreachableAndFallsBackWhenDiscoveryFails(t *testing.T) {
	now := time.Now()
	var probed atomic.Int32
	selector := gatewaySelector{
		now: func() time.Time { return now },
		discover: func(context.Context, string, string) ([]gatewayEndpoint, error) {
			return []gatewayEndpoint{{ID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Address: "10.219.77.14:18091"}, {ID: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", Address: "10.219.77.28:18091"}}, nil
		},
		probe: func(_ context.Context, endpoint gatewayEndpoint) (time.Duration, error) {
			probed.Add(1)
			if endpoint.Address == "10.219.77.14:18091" {
				return 0, errors.New("unreachable")
			}
			return 10 * time.Millisecond, nil
		},
	}
	key := "nn_" + strings.Repeat("u", 40)
	got, ok := selector.selectGateway(context.Background(), key)
	if !ok || got.ID != "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee" {
		t.Fatalf("unreachable node was not skipped: %#v, %v", got, ok)
	}
	if probed.Load() != 2 {
		t.Fatalf("probed %d registered nodes; expected both", probed.Load())
	}
	selector.discover = func(context.Context, string, string) ([]gatewayEndpoint, error) {
		return nil, errors.New("stale registry")
	}
	now = now.Add(gatewayCacheTTL)
	if _, ok = selector.selectGateway(context.Background(), key); ok {
		t.Fatal("discovery failure must fall back to configured gateway")
	}
}

func TestValidGatewayEndpointRejectsPublicOrMalformedAddresses(t *testing.T) {
	for _, endpoint := range []gatewayEndpoint{
		{ID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Address: "203.0.113.10:18091"},
		{ID: "gateway/invalid", Address: "10.219.77.28:18091"},
		{ID: strings.Repeat("a", 81), Address: "10.219.77.28:18091"},
		{ID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Address: "gateway.example:18091"},
		{ID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Address: "10.219.77.28:0"},
	} {
		if validGatewayEndpoint(endpoint) {
			t.Fatalf("accepted unsafe endpoint %#v", endpoint)
		}
	}
}

func TestGatewaySelectorDiscoveryUsesCurrentUserKeyAndCachesProbeResult(t *testing.T) {
	discoveryCalls, probes := 0, 0
	selector := gatewaySelector{
		discover: func(_ context.Context, key string, _ string) ([]gatewayEndpoint, error) {
			discoveryCalls++
			if key != "nn_"+strings.Repeat("x", 40) {
				t.Fatalf("discovery got unexpected authorization key %q", key)
			}
			return []gatewayEndpoint{{ID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Address: "10.219.77.14:18091"}}, nil
		},
		probe: func(context.Context, gatewayEndpoint) (time.Duration, error) {
			probes++
			return time.Millisecond, nil
		},
	}
	for i := 0; i < 2; i++ {
		if _, ok := selector.selectGateway(context.Background(), "nn_"+strings.Repeat("x", 40)); !ok {
			t.Fatal("expected selected node")
		}
	}
	if discoveryCalls != 1 || probes != 1 {
		t.Fatalf("calls: discovery=%d probes=%d, expected short cache", discoveryCalls, probes)
	}
}
