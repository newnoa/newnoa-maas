package main

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProxyAssessmentReportsOnlyEndpointSignalsAndUncertainty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-secret" {
			t.Error("missing upstream credential")
		}
		w.Header().Set("Server", "openresty/1.27.1 secret-do-not-copy")
		w.Header().Set("Set-Cookie", "sensitive-cookie")
		w.Header().Set("Via", "1.1 secret-proxy.example")
		w.Write([]byte(`{"data":[]}`))
	}))
	defer server.Close()
	result, err := assessSupplyEndpoint(supplyPolicy{edition: "domestic"}, server.URL+"/v1", "fixture-secret")
	if err != nil {
		t.Fatal(err)
	}
	if result.Assessment.Status != "suspected" || len(result.Assessment.Products) != 1 || result.Assessment.Products[0] != "openresty" {
		t.Fatal(result)
	}
	raw, _ := json.Marshal(result)
	for _, secret := range []string{"fixture-secret", "secret-do-not-copy", "sensitive-cookie", "secret-proxy.example"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("unsafe evidence", string(raw))
		}
	}
	if len(result.Assessment.Limitations) == 0 || len(result.Assessment.Signals) < 2 {
		t.Fatal("lost uncertainty/evidence", result)
	}
}
func TestAssessmentNeverCertifiesLocalOwnershipAndDockerCannotInspectHostProcesses(t *testing.T) {
	t.Setenv("NEWNOA_RUNTIME_MODE", "docker")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer server.Close()
	result, err := assessSupplyEndpoint(supplyPolicy{edition: "domestic"}, server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Assessment.Status != "no-obvious-signal" || result.Assessment.RuntimeMode != "docker" {
		t.Fatal(result)
	}
	if !strings.Contains(strings.Join(result.Assessment.Limitations, " "), "host process") {
		t.Fatal(result.Assessment.Limitations)
	}
}
func TestDockerHostMappingRejectsUserClaimsAndAmbiguity(t *testing.T) {
	for _, hosts := range []string{"172.20.0.1 gateway", "172.20.0.1 host.docker.internal\n172.20.0.2 host.docker.internal"} {
		if ip, err := dockerHostMapping(hosts, "172.20.0.1"); err == nil || ip != nil {
			t.Fatal(hosts, ip, err)
		}
	}
	if ip, err := dockerHostMapping("172.20.0.1 host.docker.internal # fixed host gateway", "172.20.0.1"); err != nil || ip.String() != "172.20.0.1" {
		t.Fatal(ip, err)
	}
}

func TestRuntimeAssessmentCoalescesAndReportsOnlyChangedEvidence(t *testing.T) {
	var probes, reports atomic.Int32
	var product atomic.Value
	product.Store("nginx/1.26.0")
	entered, release := make(chan struct{}, 4), make(chan struct{}, 4)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		entered <- struct{}{}
		<-release
		w.Header().Set("Server", product.Load().(string))
		w.Write([]byte(`{}`))
	}))
	defer model.Close()
	defer close(release)
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer node_fixture" {
			t.Error("not using device identity")
		}
		reports.Add(1)
		w.Write([]byte(`{}`))
	}))
	defer control.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n := &connector{ctx: ctx, nodeID: uuid.NewString(), credential: "node_fixture", control: control.URL, client: control.Client()}
	deployment := uuid.NewString()
	policy := supplyPolicy{edition: "domestic"}
	waitDone := func() {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			n.assessmentMu.Lock()
			busy := n.assessing
			n.assessmentMu.Unlock()
			if !busy {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("assessment did not finish")
	}
	probe := func() {
		n.refreshAssessment(policy, deployment, 1, model.URL, "")
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("probe not started")
		}
	}
	probe()
	for i := 0; i < 10; i++ {
		n.refreshAssessment(policy, deployment, 1, model.URL, "")
	}
	if probes.Load() != 1 {
		t.Fatal("duplicated concurrent probes")
	}
	release <- struct{}{}
	waitDone()
	if reports.Load() != 1 {
		t.Fatal("missing initial report")
	}
	n.refreshAssessment(policy, deployment, 1, model.URL, "")
	waitDone()
	if probes.Load() != 1 {
		t.Fatal("ignored five-minute TTL")
	}
	expire := func() {
		n.assessmentMu.Lock()
		for id, current := range n.assessments {
			current.checked = time.Now().Add(-6 * time.Minute)
			n.assessments[id] = current
		}
		n.assessmentMu.Unlock()
	}
	expire()
	probe()
	release <- struct{}{}
	waitDone()
	if reports.Load() != 1 {
		t.Fatal("uploaded unchanged evidence")
	}
	product.Store("openresty/1.27.1")
	expire()
	probe()
	release <- struct{}{}
	waitDone()
	if reports.Load() != 2 {
		t.Fatal("did not report changed evidence")
	}
	cancel()
	expire()
	n.refreshAssessment(policy, deployment, 1, model.URL, "")
	waitDone()
	if reports.Load() != 2 {
		t.Fatal("reported after connector cancellation")
	}
}
