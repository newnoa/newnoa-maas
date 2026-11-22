package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectorProbeOwnerBoundedFixture(t *testing.T) {
	ownerBinary, gatewayBinary := os.Getenv("NEWNOA_PROBE_OWNER_BINARY"), os.Getenv("NEWNOA_PROBE_GATEWAY_BINARY")
	if ownerBinary == "" {
		t.Skip("set NEWNOA_PROBE_OWNER_BINARY; also set NEWNOA_PROBE_GATEWAY_BINARY for the full worker fixture")
	}
	const node = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const latencyJob = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef"
	const token = "owner-fixture-internal-token-32-characters"
	reserveAddress := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		_ = listener.Close()
		return address
	}
	ownerAddress := reserveAddress()
	var executions atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executions.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-upstream" || r.Header.Get("X-Newnoa-Permit") != "" || r.Header.Get("X-Newnoa-Gateway") != "" || r.Header.Get("X-Api-Key") != "" {
			t.Error("credential boundary failed across Gateway/Owner/Core")
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" && r.URL.Path == "/v1/models" {
			_, _ = io.WriteString(w, "{\"data\":[{\"id\":\"fixture\"}]}")
			return
		}
		if r.Method != "POST" || r.URL.Path != "/v1/chat/completions" {
			t.Error("unexpected upstream request method/path")
		}
		_, _ = io.WriteString(w, "{\"choices\":[{\"message\":{\"content\":\"OK\"},\"finish_reason\":\"stop\"}]}")
	}))
	defer model.Close()
	var listener net.Listener
	for port := 20000; port < 30000; port++ {
		candidate, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err == nil {
			listener = candidate
			break
		}
	}
	if listener == nil {
		t.Fatal("no bounded connector port")
	}
	connectorPort := listener.Addr().(*net.TCPAddr).Port
	var mu sync.Mutex
	consumed, prepared, reports := map[string]bool{}, map[string]map[string]any{}, map[string]map[string]any{}
	claimed := false
	completed := make(chan string, 2)
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		internal := strings.HasPrefix(r.URL.Path, "/api/v1/internal/")
		if internal && r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("internal API identity missing")
			w.WriteHeader(403)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/route") {
			if body["nodeId"] != node || body["purpose"] != "serving" || body["epoch"] != float64(4) {
				t.Error("Owner route scope missing")
			}
			_ = json.NewEncoder(w).Encode(map[string]int{"port": connectorPort})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/heartbeat") {
			_, _ = io.WriteString(w, "{}")
			return
		}
		if r.URL.Path == "/api/v1/internal/model-checks/claim" {
			if body["gatewayId"] != "gateway-a" {
				t.Error("claim gateway missing")
			}
			mu.Lock()
			used := claimed
			claimed = true
			mu.Unlock()
			jobs := []map[string]any{}
			if !used {
				for _, job := range []struct{ id, kind string }{{node, "health"}, {latencyJob, "latency"}} {
					jobs = append(jobs, map[string]any{"id": job.id, "kind": job.kind, "nodeId": node, "epoch": 4, "probeRoute": "http://" + ownerAddress + "/tunnels/" + node + "/4/probe", "model": "fixture", "inferencePath": "/v1/chat/completions", "credential": "must-never-reach-model"})
				}
			}
			_ = json.NewEncoder(w).Encode(jobs)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/v1/internal/model-checks/") {
			tail := strings.TrimPrefix(r.URL.Path, "/api/v1/internal/model-checks/")
			parts := strings.Split(tail, "/")
			if len(parts) != 2 || (parts[0] != node && parts[0] != latencyJob) {
				t.Error("unexpected model-check job")
				w.WriteHeader(404)
				return
			}
			job := parts[0]
			if body["gatewayId"] != "gateway-a" {
				t.Error("prepare/result gateway missing")
				w.WriteHeader(403)
				return
			}
			switch parts[1] {
			case "prepare":
				method, path := "POST", "/v1/chat/completions"
				if job == latencyJob {
					method, path = "GET", "/v1/models"
				}
				digest, ok := body["bodyDigest"].(string)
				if !ok || len(digest) != 64 || body["httpMethod"] != method || body["inferencePath"] != path {
					t.Error("Gateway prepare binding missing")
					w.WriteHeader(409)
					return
				}
				mu.Lock()
				prepared[job] = body
				mu.Unlock()
				_ = json.NewEncoder(w).Encode(map[string]string{"permit": "probe_fixture"})
				return
			case "result":
				mu.Lock()
				duplicate := reports[job] != nil
				reports[job] = body
				mu.Unlock()
				if duplicate {
					t.Error("duplicate result report")
				} else {
					completed <- job
				}
				_, _ = io.WriteString(w, "{}")
				return
			}
		}
		if !strings.HasPrefix(r.URL.Path, "/api/v1/connector/model-checks/") || r.Header.Get("Authorization") != "Bearer node_fixture" {
			t.Error("Core consume identity missing")
			w.WriteHeader(403)
			return
		}
		job := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/connector/model-checks/"), "/consume")
		method, path, raw := "POST", "/v1/chat/completions", "{\"model\":\"fixture\",\"input\":\"probe\"}"
		if job == latencyJob {
			method, path, raw = "GET", "/v1/models", ""
		} else if job != node {
			t.Error("wrong consume job")
			w.WriteHeader(409)
			return
		}
		mu.Lock()
		approval := prepared[job]
		used := consumed[job]
		expected := hash([]byte(method + " " + path + "\n" + raw))
		if gatewayBinary != "" {
			if approval == nil {
				mu.Unlock()
				t.Error("consume preceded Gateway prepare")
				w.WriteHeader(409)
				return
			}
			expected, _ = approval["bodyDigest"].(string)
		}
		if body["httpMethod"] != method || body["inferencePath"] != path || body["permit"] != "probe_fixture" || body["gatewayId"] != "gateway-a" || body["epoch"] != float64(4) || body["bodyDigest"] != expected {
			mu.Unlock()
			t.Error("Gateway/Core method/path/digest scope changed")
			w.WriteHeader(409)
			return
		}
		consumed[job] = true
		mu.Unlock()
		if used {
			w.WriteHeader(409)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"endpoint": model.URL, "model": "fixture", "apiKey": "fixture-upstream", "httpMethod": method, "inferencePath": path})
	}))
	defer control.Close()
	n := &connector{purpose: "serving", nodeID: node, control: control.URL, credential: "node_fixture", client: control.Client(), upstream: model.Client()}
	connectorServer := &http.Server{Handler: n}
	go connectorServer.Serve(listener)
	defer connectorServer.Close()
	commonEnv := append(os.Environ(), "NEWNOA_INTERNAL_TOKEN="+token, "NEWNOA_CONTROL_URL="+control.URL, "OTEL_EXPORTER_OTLP_ENDPOINT=", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=", "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=")
	launch := func(binary string, environment ...string) *exec.Cmd {
		command := exec.Command(binary)
		command.Env = append(append([]string{}, commonEnv...), environment...)
		var logs bytes.Buffer
		command.Stdout = &logs
		command.Stderr = &logs
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
		return command
	}
	launch(ownerBinary, "NEWNOA_FRP_OWNER_ID=owner-fixture", "NEWNOA_FRP_OWNER_BIND="+ownerAddress, "NEWNOA_EXPECTED_OWNER_SHA256=", "NEWNOA_FRP_OWNER_TLS_CERT=", "NEWNOA_FRP_OWNER_TLS_KEY=", "NEWNOA_FRP_PLUGIN_BIND=")
	client := &http.Client{Timeout: 3 * time.Second}
	ready := false
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); {
		response, err := client.Get("http://" + ownerAddress + "/health")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !ready {
		t.Fatal("Owner fixture did not start")
	}
	if gatewayBinary != "" {
		address := reserveAddress()
		launch(gatewayBinary, "NEWNOA_GATEWAY_ID=gateway-a", "NEWNOA_GATEWAY_BIND="+address, "NEWNOA_GATEWAY_ADVERTISE_ADDR="+address, "NEWNOA_EXPECTED_GATEWAY_SHA256=", "NEWNOA_GATEWAY_TLS_CERT_FILE=", "NEWNOA_GATEWAY_TLS_KEY_FILE=")
		deadline := time.NewTimer(15 * time.Second)
		defer deadline.Stop()
		for done := 0; done < 2; done++ {
			select {
			case <-completed:
			case <-deadline.C:
				t.Fatal("Gateway health/latency fixture did not finish in 15 seconds")
			}
		}
		mu.Lock()
		for _, job := range []string{node, latencyJob} {
			if reports[job]["status"] != "succeeded" || !consumed[job] || prepared[job] == nil {
				t.Errorf("Gateway job %s did not prepare, consume and succeed: %+v", job, reports[job])
			}
		}
		mu.Unlock()
	} else {
		for _, tc := range []struct{ method, path, job, raw string }{{"POST", "/v1/chat/completions", node, "{\"model\":\"fixture\",\"input\":\"probe\"}"}, {"GET", "/v1/models", latencyJob, ""}} {
			for attempt := 0; attempt < 2; attempt++ {
				request, _ := http.NewRequest(tc.method, "http://"+ownerAddress+"/tunnels/"+node+"/4/probe"+tc.path, strings.NewReader(tc.raw))
				request.Header.Set("Authorization", "Bearer "+token)
				request.Header.Set("X-Newnoa-Request", tc.job)
				request.Header.Set("X-Newnoa-Permit", "probe_fixture")
				request.Header.Set("X-Newnoa-Gateway", "gateway-a")
				response, err := client.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				data, _ := io.ReadAll(response.Body)
				_ = response.Body.Close()
				want := 200
				if attempt == 1 {
					want = 409
				}
				if response.StatusCode != want {
					t.Fatalf("%s probe attempt %d: %d %s", tc.method, attempt, response.StatusCode, data)
				}
				marker := response.Header.Get("X-Newnoa-Probe-Upstream")
				if attempt == 0 && marker != "1" || attempt == 1 && marker != "" {
					t.Fatalf("%s probe attempt %d has incorrect upstream marker %q", tc.method, attempt, marker)
				}
			}
		}
	}
	if executions.Load() != 2 {
		t.Fatalf("bounded Gateway/Owner/Core fixture executions=%d", executions.Load())
	}
}
