package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestPublicHealthReportsCoreIdentityAndActualResources(t *testing.T) {
	c := &core{control: "http://127.0.0.1:1", gateway: "http://127.0.0.1:1"}
	h, err := c.publicProxyHandler(publicTestSecret, publicTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	defer c.publicSVGCancel()
	s := httptest.NewUnstartedServer(h)
	s.Config.ConnState = c.publicResources.connectionState
	s.Start()
	defer s.Close()
	for _, auth := range []bool{false, true} {
		r, _ := http.NewRequest("GET", s.URL+"/internal/www/health", nil)
		if auth {
			r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
		}
		response, err := s.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]any
		json.NewDecoder(response.Body).Decode(&value)
		response.Body.Close()
		if !auth {
			if response.StatusCode != 401 || value["resources"] != nil {
				t.Fatal("health resources exposed to user")
			}
			continue
		}
		if response.StatusCode != 200 || value["instanceId"] == "" || value["hostname"] == "" || value["resourceScope"] != "service-os" {
			t.Fatal("missing Core identity", value)
		}
		resources := value["resources"].(map[string]any)
		if (runtime.GOOS == "linux" && (resources["memoryTotalBytes"] == nil || resources["diskTotalBytes"] == nil)) || resources["tcpActiveConnections"].(float64) < 1 {
			t.Fatal("missing real host/connection sample", resources)
		}
		if _, err := time.Parse(time.RFC3339Nano, value["observedAt"].(string)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublicResourceRatesAndIdentityAreStable(t *testing.T) {
	s := &publicResourceSampler{}
	now := time.Unix(2000000000, 0)
	first := s.measure(now, map[string]uint64{"cpu": 100, "idle": 40, "rx": 100, "tx": 200}, map[string]any{})
	if first["cpuPercent"] != nil || first["requestsPerSecond"] != nil {
		t.Fatal("initial rates fabricated")
	}
	s.requests.Add(4)
	s.models.Add(2)
	next := s.measure(now.Add(2*time.Second), map[string]uint64{"cpu": 120, "idle": 45, "rx": 200, "tx": 240}, map[string]any{})
	if next["cpuPercent"] != float64(75) || next["networkReceiveBytesPerSecond"] != float64(50) || next["requestsPerSecond"] != float64(2) || next["modelRequestsPerSecond"] != float64(1) {
		t.Fatal("incorrect measured rates", next)
	}
	reset := s.measure(now.Add(3*time.Second), map[string]uint64{"cpu": 1, "idle": 0, "rx": 1}, map[string]any{})
	if reset["cpuPercent"] != nil || reset["networkReceiveBytesPerSecond"] != nil {
		t.Fatal("counter reset fabricated rates", reset)
	}
	if publicCoreIdentity("core-41", "18223") != "core-799dc37055d1686415209293738547b3" || publicCoreIdentity("core-41", "18223") == publicCoreIdentity("core-57", "18223") || publicCoreIdentity("core-41", "18223") == publicCoreIdentity("core-41", "18224") {
		t.Fatal("unstable/merged identity")
	}
}

func TestPublicResourceConnectionStatesStayCurrentDuringOSCache(t *testing.T) {
	s := &publicResourceSampler{}
	first := s.snapshot("127.0.0.1:18223")
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	s.connectionState(left, http.StateNew)
	s.connectionState(left, http.StateActive)
	active := s.snapshot("127.0.0.1:18223")
	if active["observedAt"] != first["observedAt"] || active["resources"].(map[string]any)["tcpActiveConnections"] != 1 {
		t.Fatal("cached host sample hid active TCP state", active)
	}
	s.connectionState(left, http.StateIdle)
	idle := s.snapshot("127.0.0.1:18223")
	if idle["resources"].(map[string]any)["tcpIdleConnections"] != 1 {
		t.Fatal("idle connection omitted")
	}
	s.connectionState(left, http.StateHijacked)
	if s.snapshot("")["resources"].(map[string]any)["tcpIdleConnections"] != 0 {
		t.Fatal("hijacked connection leaked")
	}
	s.connectionState(left, http.StateClosed)
}

func TestPublicResourceModelCountsTrackAdmittedInferenceOnly(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/internal/public/models" {
			io.WriteString(w, `[]`)
			return
		}
		io.WriteString(w, `{"hosted":false}`)
	}))
	defer control.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"ok":true}`) }))
	defer upstream.Close()
	c := &core{control: control.URL, gateway: upstream.URL}
	handler, err := c.publicProxyHandler(publicTestSecret, publicTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	defer c.publicSVGCancel()
	for _, tc := range []struct {
		method, path, body string
		want               int
	}{{"GET", "/internal/www/health", "", 200}, {"GET", "/v1/models", "", 200}, {"POST", "/v1/chat/completions", `{}`, 400}, {"POST", "/v1/chat/completions", `{"model":"fixture"}`, 200}} {
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		request.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
		request.Header.Set("Authorization", "Bearer ses_fixture_monitor_request")
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != tc.want {
			t.Fatalf("%s expected%d got%d %s", tc.path, tc.want, response.Code, response.Body.String())
		}
	}
	if c.publicResources.requests.Load() != 3 || c.publicResources.models.Load() != 1 {
		t.Fatal("health/discovery/invalid-body counted as model inference")
	}
}

func TestPublicHealthTwoAPIClientsShareIdentityAndRefreshRealRates(t *testing.T) {
	c := &core{control: "http://127.0.0.1:1", gateway: "http://127.0.0.1:1"}
	handler, err := c.publicProxyHandler(publicTestSecret, publicTestSecret)
	if err != nil {
		t.Fatal(err)
	}
	defer c.publicSVGCancel()
	server := httptest.NewUnstartedServer(handler)
	server.Config.ConnState = c.publicResources.connectionState
	server.Start()
	defer server.Close()
	transports := []*http.Transport{{}, {}}
	for _, transport := range transports {
		defer transport.CloseIdleConnections()
	}
	probe := func(transport *http.Transport) map[string]any {
		t.Helper()
		request, err := http.NewRequest("GET", server.URL+"/internal/www/health", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
		response, err := (&http.Client{Transport: transport, Timeout: 5 * time.Second}).Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var sample map[string]any
		if err := json.NewDecoder(response.Body).Decode(&sample); err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("health sample unavailable: status=%d error=%v", response.StatusCode, err)
		}
		return sample
	}
	first := probe(transports[0])
	if resources := first["resources"].(map[string]any); resources["requestsPerSecond"] != nil || resources["modelRequestsPerSecond"] != nil || resources["cpuPercent"] != nil {
		t.Fatal("first probe fabricated rates", resources)
	}
	time.Sleep(1050 * time.Millisecond)
	second := probe(transports[1])
	third := probe(transports[0])
	if first["instanceId"] != second["instanceId"] || second["instanceId"] != third["instanceId"] || first["observedAt"] == second["observedAt"] || second["observedAt"] != third["observedAt"] {
		t.Fatal("API probes split Core identity or did not refresh/share cached OS sample")
	}
	for _, sample := range []map[string]any{second, third} {
		resources := sample["resources"].(map[string]any)
		if resources["requestsPerSecond"] != float64(0) || resources["modelRequestsPerSecond"] != float64(0) || resources["tcpActiveConnections"].(float64) < 1 {
			t.Fatal("health polling counted traffic or hid current TCP state", resources)
		}
		if runtime.GOOS == "linux" {
			if cpu, ok := resources["cpuPercent"].(float64); !ok || cpu < 0 || cpu > 100 {
				t.Fatal("periodic health probes did not populate actual CPU rate", resources)
			}
		}
	}
}
