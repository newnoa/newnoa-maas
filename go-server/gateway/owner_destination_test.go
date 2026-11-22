package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestOwnerRoutesRejectAnotherOriginWithValidTLS(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-local")
	t.Setenv("NEWNOA_EVALUATOR_TOKEN", strings.Repeat("e", 32))
	var trustedCalls, untrustedCalls atomic.Int32
	response := `{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":2}}`
	trusted := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trustedCalls.Add(1)
		w.Header().Set("X-Newnoa-Probe-Upstream", "1")
		if r.Header.Get("Authorization") != "Bearer internal-fixture" {
			t.Error("trusted owner lost its service credential")
		}
		if strings.HasSuffix(r.URL.Path, "/v1/models") {
			io.WriteString(w, `{"data":[{"id":"fixture"}]}`)
			return
		}
		io.WriteString(w, response)
	}))
	defer trusted.Close()
	untrusted := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/certificate-check" {
			untrustedCalls.Add(1)
		}
		io.WriteString(w, response)
	}))
	defer untrusted.Close()
	// Both origins have a valid certificate under the same fixture trust roots.
	verified, err := trusted.Client().Get(untrusted.URL + "/certificate-check")
	if err != nil {
		t.Fatalf("attacker fixture must have valid TLS: %v", err)
	}
	verified.Body.Close()
	t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", trusted.URL)
	fixed := `{"model":"fixture","messages":[{"role":"user","content":"Reply with exactly OK."}],"max_tokens":16}`
	digest := sha256.Sum256([]byte(fixed))
	for _, kind := range []string{"inference", "evaluation", "health", "publication"} {
		for _, allowed := range []bool{true, false} {
			name := kind + "/untrusted"
			origin := untrusted.URL
			if allowed {
				name, origin = kind+"/trusted", trusted.URL
			}
			t.Run(name, func(t *testing.T) {
				control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case strings.HasSuffix(r.URL.Path, "/authorize"):
						json.NewEncoder(w).Encode(map[string]any{"model": "fixture", "nodeId": ownerProxyNode, "ownerId": "owner-local", "epoch": 3})
					case strings.HasSuffix(r.URL.Path, "/dispatches"):
						json.NewEncoder(w).Encode(map[string]any{"requestId": evalRequest, "permit": "fixture", "nodeId": ownerProxyNode, "ownerId": "owner-local", "epoch": 3, "route": origin + "/tunnels/" + ownerProxyNode + "/3"})
					case strings.HasSuffix(r.URL.Path, "/dispatch"):
						json.NewEncoder(w).Encode(map[string]any{"requestId": evalRequest, "attemptId": evalAttempt, "purpose": "evaluation", "permit": "fixture", "nodeId": ownerProxyNode, "ownerId": "owner-local", "epoch": 3, "route": origin + "/tunnels/" + ownerProxyNode + "/3/evaluation", "body": fixed, "bodyDigest": hex.EncodeToString(digest[:]), "reserveTokens": 128})
					case strings.HasSuffix(r.URL.Path, "/prepare"):
						permit := "probe_fixture"
						if kind == "publication" {
							permit = "pubprobe_fixture"
						}
						json.NewEncoder(w).Encode(map[string]string{"permit": permit})
					default:
						io.WriteString(w, `{"resultDigest":"fixture"}`)
					}
				}))
				defer control.Close()
				g := newGateway(control.URL, "internal-fixture", "gateway-local", nil)
				g.owner.Transport = trusted.Client().Transport
				before := trustedCalls.Load()
				if kind == "health" || kind == "publication" {
					probe := "probe"
					if kind == "publication" {
						probe = "publication-probe"
					}
					result := g.checkModel(context.Background(), modelCheck{ID: ownerProxyNode, NodeID: ownerProxyNode, OwnerID: "owner-local", Epoch: 3, Kind: kind, Model: "fixture", Path: "/v1/chat/completions", ProbeRoute: origin + "/tunnels/" + ownerProxyNode + "/3/" + probe})
					if (result.Status == "succeeded") != allowed {
						t.Fatalf("incorrect owner trust result: %+v", result)
					}
				} else {
					path, body := "/v1/chat/completions", fixed
					if kind == "evaluation" {
						path = "/internal/evaluations/" + ownerProxyNode + "/greeting"
						body = `{"workerId":"fixture","fencingToken":1,"requestId":"` + evalRequest + `","attemptId":"` + evalAttempt + `"}`
					}
					req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
					req.Header.Set("Authorization", "Bearer ses_fixture")
					req.Header.Set("X-Api-Key", "fixture-key")
					if kind == "evaluation" {
						req.Header.Set("Authorization", "Bearer "+strings.Repeat("e", 32))
					}
					out := httptest.NewRecorder()
					g.ServeHTTP(out, req)
					if allowed && out.Code != http.StatusOK || !allowed && out.Code != http.StatusBadGateway {
						t.Fatalf("unexpected HTTP status %d: %s", out.Code, out.Body.String())
					}
				}
				if allowed && trustedCalls.Load() != before+1 || !allowed && trustedCalls.Load() != before || untrustedCalls.Load() != 0 {
					t.Fatalf("unexpected owner traffic: trusted=%d before=%d untrusted=%d", trustedCalls.Load(), before, untrustedCalls.Load())
				}
			})
		}
	}
}

func TestOwnerOriginIsExactAndFailsClosed(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-local")
	for _, tc := range []struct {
		configured, destination, owner string
		allowed                        bool
	}{
		{"", "http://127.0.0.1:18101/tunnels/fixture", "owner-local", true},
		{"", "http://127.0.0.1:18102/tunnels/fixture", "owner-local", false},
		{"https://owner.example", "https://OWNER.example:443/tunnels/fixture", "owner-local", true},
		{"https://owner.example", "https://owner.example:444/tunnels/fixture", "owner-local", false},
		{"https://owner.example", "https://other.example/tunnels/fixture", "owner-local", false},
		{"https://owner.example", "https://owner.example/tunnels/fixture", "owner-other", false},
		{"https://owner.example", "https://owner.example/tunnels/fixture", "", false},
		{"http://[::1]:18101", "http://[::1]:18101/tunnels/fixture", "owner-local", true},
		{"http://localhost:18101", "http://localhost:18101/tunnels/fixture", "owner-local", true},
		{"http://localhost:18101", "http://127.0.0.1:18101/tunnels/fixture", "owner-local", false},
		{"https://owner.example/path", "https://owner.example/tunnels/fixture", "owner-local", false},
		{"https://user:password@owner.example", "https://owner.example/tunnels/fixture", "owner-local", false},
		{"https://owner.example?", "https://owner.example/tunnels/fixture", "owner-local", false},
		{"https://owner.example#fragment", "https://owner.example/tunnels/fixture", "owner-local", false},
		{"https://owner.example:65536", "https://owner.example/tunnels/fixture", "owner-local", false},
		{"not an origin", "https://owner.example/tunnels/fixture", "owner-local", false},
		{"http://203.0.113.10", "http://203.0.113.10/tunnels/fixture", "owner-local", false},
	} {
		t.Run(tc.configured+"/"+tc.destination+"/"+tc.owner, func(t *testing.T) {
			t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", tc.configured)
			g := newGateway("", "internal-fixture", "gateway-local", nil)
			request := httptest.NewRequest(http.MethodPost, tc.destination, nil)
			err := g.authorizeOwnerRequest(request, tc.owner)
			if (err == nil) != tc.allowed || (request.Header.Get("Authorization") == "Bearer internal-fixture") != tc.allowed {
				t.Fatalf("allowed=%v error=%v credential attached=%v", tc.allowed, err, request.Header.Get("Authorization") != "")
			}
		})
	}
}

func TestOwnerOriginDerivesFromLocalDeploymentConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, owners, alternate, advertise, bind, explicit, want string
		allowed                                                  bool
	}{
		{name: "public-server", advertise: "https://203.0.113.10:6666", want: "https://203.0.113.10:6666", allowed: true},
		{name: "configured-owner", owners: `[{"id":"other","gatewayUrl":"https://attacker.example"},{"id":"owner-local","gatewayUrl":"https://203.0.113.10:6666/_newnoa/owner"}]`, want: "https://203.0.113.10:6666", allowed: true},
		{name: "existing-config-alias", alternate: `[{"id":"owner-local","gatewayUrl":"http://127.0.0.1:18193"}]`, want: "http://127.0.0.1:18193", allowed: true},
		{name: "explicit-loopback", advertise: "https://203.0.113.10:6666", explicit: "http://127.0.0.1:18101", want: "http://127.0.0.1:18101", allowed: true},
		{name: "sit-bind", advertise: "172.27.42.19:6666", bind: "172.27.42.19:18101", want: "https://172.27.42.19:18101", allowed: true},
		{name: "duplicate-owner", owners: `[{"id":"owner-local","gatewayUrl":"https://one.example"},{"id":"owner-local","gatewayUrl":"https://two.example"}]`, advertise: "https://203.0.113.10:6666"},
		{name: "missing-owner", owners: `[{"id":"other","gatewayUrl":"https://attacker.example"}]`, advertise: "https://203.0.113.10:6666"},
		{name: "invalid-json", owners: "private-invalid-value", advertise: "https://203.0.113.10:6666"},
		{name: "wrong-envelope", owners: `{"owners":[{"id":"owner-local","gatewayUrl":"https://one.example"}]}`, advertise: "https://203.0.113.10:6666"},
		{name: "wrong-url-type", owners: `[{"id":"owner-local","gatewayUrl":123}]`},
		{name: "hostname-port", owners: `[{"id":"owner-local","gatewayUrl":"https://local-owner.example:7443/_newnoa/owner"}]`, want: "https://local-owner.example:7443", allowed: true},
		{name: "local-bind", bind: "127.0.0.1:18193", want: "http://127.0.0.1:18193", allowed: true},
		{name: "public-http", owners: `[{"id":"owner-local","gatewayUrl":"http://203.0.113.10:18101"}]`},
		{name: "owner-userinfo", owners: `[{"id":"owner-local","gatewayUrl":"https://user:private@203.0.113.10:6666"}]`},
		{name: "conflicting-aliases", owners: `[{"id":"owner-local","gatewayUrl":"https://one.example"}]`, alternate: `[{"id":"owner-local","gatewayUrl":"https://two.example"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-local")
			t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", tc.explicit)
			t.Setenv("NEWNOA_FRP_OWNERS_JSON", tc.owners)
			t.Setenv("NEWNOA_FRP_OWNER_CONFIG", tc.alternate)
			t.Setenv("NEWNOA_GATEWAY_ADVERTISE_ADDR", tc.advertise)
			t.Setenv("NEWNOA_FRP_OWNER_BIND", tc.bind)
			g := newGateway("", "internal-fixture", "gateway-local", nil)
			destination := tc.want
			if destination == "" {
				destination = "https://203.0.113.10:6666"
			}
			request := httptest.NewRequest(http.MethodPost, destination+"/tunnels/fixture", nil)
			err := g.authorizeOwnerRequest(request, "owner-local")
			if (err == nil) != tc.allowed {
				t.Fatalf("allowed=%v err=%v", tc.allowed, err)
			}
			if err != nil && (strings.Contains(err.Error(), "private") || request.Header.Get("Authorization") != "") {
				t.Fatal("invalid config leaked its contents or attached credentials")
			}
		})
	}
}

func TestOwnerProbeRejectsPublicHTTP(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-local")
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"permit":"probe_fixture"}`)
	}))
	defer control.Close()
	for _, configured := range []string{"https://203.0.113.10:18101", "http://203.0.113.10:18101"} {
		t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", configured)
		g := newGateway(control.URL, "internal-fixture", "gateway-local", nil)
		g.owner.Transport = gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			t.Error("public HTTP probe reached the transport")
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"data":[]}`))}, nil
		})
		result := g.checkModel(context.Background(), modelCheck{ID: ownerProxyNode, NodeID: ownerProxyNode, OwnerID: "owner-local", Epoch: 3, Kind: "latency", ProbeRoute: "http://203.0.113.10:18101/tunnels/" + ownerProxyNode + "/3/probe"})
		if result.Status == "succeeded" {
			t.Fatal("public HTTP owner accepted")
		}
	}
}

func TestTrustedLoopbackOwnerDoesNotRedirectToken(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-local")
	var calls atomic.Int32
	untrusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer untrusted.Close()
	trusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer internal-fixture" {
			t.Error("trusted loopback owner lost credential")
		}
		http.Redirect(w, r, untrusted.URL+"/capture", http.StatusTemporaryRedirect)
	}))
	defer trusted.Close()
	t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", strings.Replace(trusted.URL, "127.0.0.1", "localhost", 1))
	g := newGateway("", "internal-fixture", "gateway-local", nil)
	defer g.owner.CloseIdleConnections()
	request, err := http.NewRequest(http.MethodPost, g.ownerOrigin+"/tunnels/fixture", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.authorizeOwnerRequest(request, "owner-local"); err != nil {
		t.Fatal(err)
	}
	response, err := g.owner.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || calls.Load() != 0 {
		t.Fatalf("redirect leaked owner token: status=%d calls=%d", response.StatusCode, calls.Load())
	}
}
