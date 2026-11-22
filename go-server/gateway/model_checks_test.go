package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	peernet "github.com/newnoa/newnoa-maas/go-server/network"
)

func TestModelHealthValidatesModelAndDoesNotLeakCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != "Bearer upstream-only" {
			t.Errorf("invalid probe request")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":[{"id":"available"}]}`))
	}))
	defer server.Close()
	g := newGateway("", "internal", "gateway", []string{"127.0.0.1"})
	good := g.checkModel(context.Background(), modelCheck{Endpoint: server.URL + "/v1", Model: "available", Credential: "upstream-only", Kind: "latency"})
	if good.Status != "succeeded" || good.NetworkLatencyMs <= 0 || good.EndpointPingStatus != "succeeded" || good.EndpointPingMs <= 0 {
		t.Fatalf("health: %#v", good)
	}
	bad := g.checkModel(context.Background(), modelCheck{Endpoint: server.URL + "/v1", Model: "missing", Credential: "upstream-only", Kind: "latency"})
	if bad.Status != "succeeded" {
		t.Fatal("reachable HTTP endpoint must provide latency")
	}
}

func TestNodeModelProbeNeverUsesAnotherServer(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "local-owner")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; io.WriteString(w, `{"permit":"probe_fixture"}`) }))
	defer server.Close()
	g := newGateway(server.URL, "internal-platform", "probe-gateway", nil)
	for _, owner := range []string{"", "other-owner"} {
		result := g.checkModel(context.Background(), modelCheck{ID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", Kind: "latency", NodeID: "bbbbbbbb-cccc-dddd-eeee-ffffffffffff", OwnerID: owner, Epoch: 1, ProbeRoute: server.URL + "/tunnels/bbbbbbbb-cccc-dddd-eeee-ffffffffffff/1/probe"})
		if result.Status != "failed" || calls != 0 {
			t.Fatalf("foreign probe status=%s calls=%d", result.Status, calls)
		}
	}
}
func TestModelBenchmarkRequiresCompletedStreamAndUsage(t *testing.T) {
	for _, tc := range []struct {
		body string
		ok   bool
	}{{"data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":4096,\"completion_tokens\":12},\"timings\":{\"prompt_per_second\":100,\"predicted_per_second\":20}}\n\ndata: [DONE]\n\n", true}, {"data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\n", false}} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Write([]byte(tc.body))
		}))
		g := newGateway("", "internal", "gateway", []string{"127.0.0.1"})
		result := g.checkModel(context.Background(), modelCheck{Endpoint: s.URL + "/v1", Model: "fixture", Kind: "benchmark"})
		s.Close()
		if (result.Status == "succeeded") != tc.ok {
			t.Fatalf("%#v", result)
		}
		if tc.ok && (result.InputTokens != 4096 || result.PrefillTokensPerSecond != 100 || result.GenerationTokensPerSecond != 20 || result.TtftMs <= 0) {
			t.Fatalf("missing timings: %#v", result)
		}
	}
}

func TestModelHealthOnlyReadsCatalogForEveryInferenceProtocol(t *testing.T) {
	for _, path := range []string{"", "/v1/chat/completions", "/v1/messages", "/v1/responses", "/v1/images/generations", "/v1/embeddings", "/v1/rerank", "/v1/systemone", "/v1/audio/transcriptions"} {
		t.Run(path, func(t *testing.T) {
			for _, success := range []bool{true, false} {
				s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, _ := io.ReadAll(r.Body)
					if r.URL.Path != "/v1/models" || r.Method != "GET" || len(raw) != 0 || r.Header.Get("Authorization") != "Bearer catalog-only" || r.Header.Get("X-Api-Key") != "" {
						t.Error("health must only read the authenticated catalog without inference")
					}
					if !success {
						w.WriteHeader(503)
						return
					}
					w.Write([]byte(`{"data":[{"id":"fixture"}]}`))
				}))
				g := newGateway("", "internal", "gateway", []string{"127.0.0.1"})
				result := g.checkModel(context.Background(), modelCheck{Endpoint: s.URL + "/v1", Model: "fixture", Credential: "catalog-only", Kind: "health", Path: path})
				s.Close()
				if (result.Status == "succeeded") != success {
					t.Fatalf("%#v", result)
				}
			}
		})
	}
}

func TestModelHealthReadsNativeStartluxCatalog(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		status int
		valid  bool
	}{
		{"native", `{"models":[{"name":"startlux-decision-4B-Instant","release_date":"2026-11-17"}]}`, 200, true},
		{"openai", `{"data":[{"id":"startlux-decision-4B-Instant"}]}`, 200, true},
		{"missing", `{"models":[{"name":"other"}]}`, 200, false},
		{"empty", `{"models":[]}`, 200, false},
		{"wrong-field", `{"models":[{"id":"startlux-decision-4B-Instant"}]}`, 200, false},
		{"wrong-type", `{"models":{"name":"startlux-decision-4B-Instant"}}`, 200, false},
		{"error", `{"models":[{"name":"startlux-decision-4B-Instant"}],"error":{"message":"unavailable"}}`, 200, false},
		{"invalid-json", `<html>unavailable</html>`, 200, false},
		{"unauthorized", `{"models":[{"name":"startlux-decision-4B-Instant"}]}`, 401, false},
		{"unavailable", `{"models":[{"name":"startlux-decision-4B-Instant"}]}`, 503, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				raw, _ := io.ReadAll(r.Body)
				if r.Method != http.MethodGet || r.URL.Path != "/v1/models" || len(raw) != 0 || r.Header.Get("Authorization") != "Bearer catalog-only" {
					t.Error("health must only read the authenticated catalog without inference")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer s.Close()
			g := newGateway("", "internal", "gateway", []string{"127.0.0.1"})
			result := g.checkModel(context.Background(), modelCheck{Endpoint: s.URL + "/v1", Model: "startlux-decision-4B-Instant", Credential: "catalog-only", Kind: "health", Path: "/v1/models"})
			if (result.Status == "succeeded") != tc.valid || calls != 1 {
				t.Fatalf("health=%#v calls=%d", result, calls)
			}
		})
	}
}

func TestDecisionHealthSuppliesRequiredQuestionInstructions(t *testing.T) {
	_, body := modelHealthBody(modelCheck{Model: "decision", Path: "/v1/systemone"})
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		t.Fatal("invalid health request")
	}
	questions, ok := payload["questions"].(map[string]any)
	if !ok {
		t.Fatal("missing questions")
	}
	question, ok := questions["health"].(map[string]any)
	if !ok || question["instructions"] == nil || question["instructions"] == "" {
		t.Fatal("systemone requires explicit instructions")
	}
}

func TestNodeModelProbeBindsPermitBeforeOwnerForwarding(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-fixture")
	const jobID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const nodeID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	var prepares, executions int
	var binding string
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executions++
		if r.URL.Path != "/tunnels/"+nodeID+"/3/probe/v1/models" || r.Method != "GET" || r.Header.Get("Authorization") != "Bearer internal-platform" || r.Header.Get("X-Newnoa-Permit") != "probe_fixture" || r.Header.Get("X-Newnoa-Request") != jobID || r.Header.Get("X-Newnoa-Gateway") != "probe-gateway" {
			t.Error("unscoped owner request")
		}
		raw, _ := io.ReadAll(r.Body)
		sum := sha256.Sum256(append([]byte("GET /v1/models\n"), raw...))
		if binding != hex.EncodeToString(sum[:]) {
			t.Error("body changed after permit binding")
		}
		w.Write([]byte(`{"data":[{"id":"fixture"}]}`))
	}))
	defer owner.Close()
	t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", owner.URL)
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prepares++
		if r.URL.Path != "/api/v1/internal/model-checks/"+jobID+"/prepare" {
			t.Error("wrong prepare path")
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["httpMethod"] != "GET" || body["inferencePath"] != "/v1/models" {
			t.Error("wrong execution scope")
		}
		binding, _ = body["bodyDigest"].(string)
		w.Write([]byte(`{"permit":"probe_fixture"}`))
	}))
	defer control.Close()
	g := newGateway(control.URL, "internal-platform", "probe-gateway", nil)
	result := g.checkModel(context.Background(), modelCheck{ID: jobID, Kind: "health", Model: "fixture", Path: "/v1/chat/completions", NodeID: nodeID, OwnerID: "owner-fixture", Epoch: 3, ProbeRoute: owner.URL + "/tunnels/" + nodeID + "/3/probe", Credential: "must-not-reach-owner"})
	if result.Status != "succeeded" || prepares != 1 || executions != 1 {
		t.Fatalf("node probe failed: %#v calls=%d/%d", result, prepares, executions)
	}
}

func TestNodeLatencyRequestHasNoTransportReplayBody(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-fixture")
	t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", "http://127.0.0.1:1234")
	const jobID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const nodeID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"permit":"probe_fixture"}`)) }))
	defer control.Close()
	g := newGateway(control.URL, "internal-platform", "probe-gateway", nil)
	g.owner.Transport = gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != "GET" || r.Body == nil || r.Body == http.NoBody || r.GetBody != nil {
			t.Error("GET probe can be replayed by Transport")
		}
		raw, _ := io.ReadAll(r.Body)
		if len(raw) != 0 {
			t.Error("latency payload is not empty")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Newnoa-Probe-Upstream": []string{"1"}}, Body: io.NopCloser(strings.NewReader(`{"data":[]}`))}, nil
	})
	result := g.checkModel(context.Background(), modelCheck{ID: jobID, Kind: "latency", NodeID: nodeID, OwnerID: "owner-fixture", Epoch: 1, ProbeRoute: "http://127.0.0.1:1234/tunnels/" + nodeID + "/1/probe"})
	if result.Status != "succeeded" {
		t.Fatalf("latency failed: %#v", result)
	}
}

func TestNodeLatencyDoesNotCountOwnerOrPermitFailuresAsModelRTT(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-fixture")
	t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", "http://127.0.0.1:1234")
	const jobID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const nodeID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"permit":"probe_fixture"}`)) }))
	defer control.Close()
	for _, tc := range []struct {
		marker string
		ok     bool
	}{{"", false}, {"1", true}} {
		g := newGateway(control.URL, "internal-platform", "probe-gateway", nil)
		g.owner.Transport = gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 409, Header: http.Header{"X-Newnoa-Probe-Upstream": []string{tc.marker}}, Body: io.NopCloser(strings.NewReader("unavailable"))}, nil
		})
		result := g.checkModel(context.Background(), modelCheck{ID: jobID, Kind: "latency", NodeID: nodeID, OwnerID: "owner-fixture", Epoch: 1, ProbeRoute: "http://127.0.0.1:1234/tunnels/" + nodeID + "/1/probe"})
		if (result.Status == "succeeded") != tc.ok {
			t.Fatalf("marker=%q result=%#v", tc.marker, result)
		}
	}
}

func TestNodeLatencySubtractsOnlyBoundedEndpointPingWait(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-fixture")
	t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", "http://127.0.0.1:1234")
	const jobID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const nodeID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"permit":"probe_fixture"}`) }))
	defer control.Close()
	for _, wait := range []string{"8", "NaN", "-100", "99999"} {
		g := newGateway(control.URL, "internal-platform", "probe-gateway", nil)
		var elapsed time.Duration
		g.owner.Transport = gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			started := time.Now()
			time.Sleep(10 * time.Millisecond)
			elapsed = time.Since(started)
			return &http.Response{StatusCode: 200, Header: http.Header{"X-Newnoa-Probe-Upstream": {"1"}, "X-Newnoa-Probe-Ping-Elapsed-Ms": {wait}, "X-Newnoa-Probe-Ping-Status": {"permission-denied"}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		})
		result := g.checkModel(context.Background(), modelCheck{ID: jobID, Kind: "latency", NodeID: nodeID, OwnerID: "owner-fixture", Epoch: 1, ProbeRoute: "http://127.0.0.1:1234/tunnels/" + nodeID + "/1/probe"})
		if result.Status != "succeeded" || result.EndpointPingStatus != "permission-denied" || result.EndpointPingMs != 0 {
			t.Fatalf("independent ICMP failure lost: %+v", result)
		}
		minimum := float64(elapsed) / float64(time.Millisecond)
		if wait == "8" {
			minimum -= 8
		}
		if result.NetworkLatencyMs < minimum || wait == "8" && result.NetworkLatencyMs >= float64(elapsed)/float64(time.Millisecond) {
			t.Fatalf("ICMP wait subtraction incorrect: wait=%s http=%v result=%+v", wait, elapsed, result)
		}
	}
}

func TestEndpointPingUsesEgressPolicyAndPinnedIP(t *testing.T) {
	for _, allowed := range []bool{false, true} {
		g := &gateway{}
		if allowed {
			g.allow = []string{"127.0.0.1"}
		}
		calls := 0
		result := g.pingModelEndpoint(context.Background(), "http://127.0.0.1:1234/v1", func(ctx context.Context, address net.IPAddr) peernet.EndpointPing {
			calls++
			if !address.IP.Equal(net.ParseIP("127.0.0.1")) {
				t.Fatalf("unpinned destination: %v", address)
			}
			return peernet.EndpointPing{Status: "succeeded", Milliseconds: 1}
		})
		if allowed && (calls != 1 || result.Status != "succeeded") || !allowed && (calls != 0 || result.Status != "unavailable") {
			t.Fatalf("egress bypass allowed=%t calls=%d result=%+v", allowed, calls, result)
		}
	}
	g := &gateway{}
	calls := 0
	result := g.pingModelEndpoint(context.Background(), "http://localhost:1234/v1", func(context.Context, net.IPAddr) peernet.EndpointPing {
		calls++
		return peernet.EndpointPing{Status: "succeeded", Milliseconds: 1}
	})
	if calls != 0 || result.Status != "unavailable" {
		t.Fatalf("private DNS target reached ICMP sender: calls=%d result=%+v", calls, result)
	}
}
