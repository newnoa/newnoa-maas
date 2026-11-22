package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublicationProbeRequiresItsOwnRouteAndOneUsePermit(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-fixture")
	const jobID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const nodeID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	prepared, called := false, false
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/internal/publication-checks/"+jobID+"/prepare" {
			t.Errorf("wrong publication prepare: %s", r.URL.Path)
		}
		var binding map[string]any
		_ = json.NewDecoder(r.Body).Decode(&binding)
		if binding["inferencePath"] != "/v1/images/generations" || binding["httpMethod"] != "POST" || binding["bodyDigest"] == nil {
			t.Error("missing execution binding")
		}
		prepared = true
		_, _ = io.WriteString(w, `{"permit":"pubprobe_fixture"}`)
	}))
	defer api.Close()
	owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if !prepared || r.URL.Path != "/tunnels/"+nodeID+"/3/publication-probe/v1/images/generations" || r.Header.Get("X-Newnoa-Permit") != "pubprobe_fixture" || r.Header.Get("Authorization") != "Bearer internal-platform" {
			t.Error("publication permit or scope lost")
		}
		if r.Header.Get("X-Api-Key") != "" {
			t.Error("upstream key reached owner")
		}
		w.Header().Set("X-Newnoa-Probe-Upstream", "1")
		_, _ = io.WriteString(w, `{"data":[{"url":"https://images.example/result.png"}]}`)
	}))
	defer owner.Close()
	t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", owner.URL)
	g := newGateway(api.URL, "internal-platform", "fixture", nil)
	job := modelCheck{ID: jobID, Kind: "publication", Path: "/v1/images/generations", Model: "image", NodeID: nodeID, OwnerID: "owner-fixture", Epoch: 3, ProbeRoute: owner.URL + "/tunnels/" + nodeID + "/3/publication-probe", Credential: "must-not-reach-owner"}
	result := g.checkModel(context.Background(), job)
	if result.Status != "succeeded" || !called {
		t.Fatalf("publication failed: %#v called=%v", result, called)
	}
	job.Kind = "health"
	if _, err := modelProbeURL(job, "/v1/images/generations"); err == nil {
		t.Error("health probe accepted publication route")
	}
}

func TestImagePublicationUsesSupplierDefaultSize(t *testing.T) {
	_, data := modelHealthBody(modelCheck{Kind: "publication", Path: "/v1/images/generations", Model: "image"})
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil || body["size"] != nil || body["n"] != float64(1) || body["prompt"] == nil {
		t.Fatal("publication must request one real image at the supplier default size")
	}
}

func TestPublicationFailureUsesSafeStableReason(t *testing.T) {
	for _, tc := range []struct {
		status     int
		body, want string
	}{
		{401, "secret rejected", "Protocol probe authentication failed"},
		{403, "private upstream URL", "Protocol probe authentication failed"},
		{429, "rate limited", "Protocol probe rate limited"},
		{404, "route absent", "Protocol probe unsupported"},
		{200, `{"unexpected":true}`, "Protocol probe returned invalid response"},
	} {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			_, _ = io.WriteString(w, tc.body)
		}))
		g := newGateway("", "internal", "fixture", []string{"127.0.0.1"})
		result := g.checkModel(context.Background(), modelCheck{Kind: "publication", Path: "/v1/images/generations", Model: "image", Endpoint: upstream.URL})
		upstream.Close()
		if result.Status != "failed" || result.Error != tc.want {
			t.Fatalf("status %d got %#v", tc.status, result)
		}
	}
}

func TestPublicationResponsesAcceptOnlyBoundedOutputLimitIncomplete(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"OK"}]}]}`, true},
		{`{"status":"incomplete","output":[],"incomplete_details":{"reason":"max_output_tokens"}}`, true},
		{`{"status":"incomplete","output":[],"incomplete_details":{"reason":"content_filter"}}`, false},
		{`{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}`, false},
		{`{"status":"incomplete","output":{},"incomplete_details":{"reason":"max_output_tokens"}}`, false},
		{`{"status":"incomplete","output":[],"incomplete_details":{"reason":"max_output_tokens"},"error":{"message":"failed"}}`, false},
		{`{"status":"failed","output":[]}`, false},
	} {
		if got := validModelHealth(modelCheck{Kind: "publication", Path: "/v1/responses"}, []byte(tc.raw)); got != tc.valid {
			t.Fatalf("publication validation %s got %v", tc.raw, got)
		}
		if strings.Contains(tc.raw, `"status":"incomplete"`) && validModelHealth(modelCheck{Kind: "health", Path: "/v1/responses"}, []byte(tc.raw)) {
			t.Fatal("protocol probe loosened ordinary model health")
		}
	}
	_, raw := modelHealthBody(modelCheck{Kind: "publication", Path: "/v1/messages", Model: "native"})
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	if body["max_tokens"] != float64(64) {
		t.Fatal("native Messages publication output must remain bounded", string(raw))
	}
}

func TestNativeMessagesPublicationProbeUsesOnlyItsOwnCredentialHeader(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Api-Key") != "fixture-native-key" || r.Header.Get("Anthropic-Version") != "2023-06-01" {
			t.Error("native Messages received conflicting credentials")
		}
		_, _ = io.WriteString(w, `{"id":"msg_fixture","type":"message","role":"assistant","model":"native","content":[{"type":"text","text":"OK"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)
	}))
	defer s.Close()
	g := newGateway("", "internal", "fixture", []string{"127.0.0.1"})
	result := g.checkModel(context.Background(), modelCheck{Kind: "publication", Path: "/v1/messages", Model: "native", Endpoint: s.URL, Credential: "fixture-native-key"})
	if result.Status != "succeeded" {
		t.Fatalf("native Messages probe failed %#v", result)
	}
}

func TestPublicationWorkerClaimsAndReportsWithoutReturningGeneratedContent(t *testing.T) {
	const jobID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var claimed atomic.Bool
	var reportAttempts atomic.Int32
	var upstreamCalls atomic.Int32
	reported := make(chan map[string]any, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		_, _ = io.WriteString(w, `{"data":[{"url":"https://images.example/private-result.png?token=secret"}]}`)
	}))
	defer upstream.Close()
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/internal/model-checks/claim":
			_, _ = io.WriteString(w, `[]`)
		case "/api/v1/internal/publication-checks/claim":
			if claimed.CompareAndSwap(false, true) {
				_ = json.NewEncoder(w).Encode([]modelCheck{{ID: jobID, Kind: "publication", Path: "/v1/images/generations", Model: "image", Endpoint: upstream.URL}})
			} else {
				_, _ = io.WriteString(w, `[]`)
			}
		case "/api/v1/internal/publication-checks/" + jobID + "/result":
			if reportAttempts.Add(1) == 1 {
				http.Error(w, "temporary", 503)
				return
			}
			raw, _ := io.ReadAll(r.Body)
			if strings.Contains(string(raw), "private-result") || strings.Contains(string(raw), "token=secret") {
				t.Error("generated content leaked in report")
			}
			var value map[string]any
			_ = json.Unmarshal(raw, &value)
			reported <- value
			_, _ = io.WriteString(w, `{}`)
		default:
			t.Errorf("unexpected control route %s", r.URL.Path)
			http.Error(w, "unexpected", 404)
		}
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal-platform", "fixture", []string{"127.0.0.1"})
	done := make(chan struct{})
	go func() { defer close(done); g.runModelChecks(ctx) }()
	select {
	case report := <-reported:
		if report["gatewayId"] != "fixture" || report["status"] != "succeeded" {
			t.Fatalf("invalid report %v", report)
		}
		if reportAttempts.Load() != 2 || upstreamCalls.Load() != 1 {
			t.Fatalf("report retry regenerated image: reports=%d upstream=%d", reportAttempts.Load(), upstreamCalls.Load())
		}
	case <-time.After(3 * time.Second):
		t.Error("publication job not executed")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}
