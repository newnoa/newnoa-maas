package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	rpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
)

func TestPublicGoServerConfiguration(t *testing.T) {
	if endpoint, err := parsePublicGoServer(""); endpoint != nil || err != nil {
		t.Fatal("empty configuration should preserve the default route")
	}
	for _, address := range []string{"10.0.0.1:6666", "127.0.0.1:6666", "[fd00::1]:6666", "[::1]:6666"} {
		raw := `{"id":"node","address":"https://203.0.113.1:6666","internalAddress":"` + address + `","transport":"grpc"}`
		endpoint, err := parsePublicGoServer(raw)
		if err != nil || endpoint.dialAddress != address {
			t.Fatalf("address %s: endpoint=%+v error=%v", address, endpoint, err)
		}
	}
	for _, raw := range []string{
		`null`, `{}`, `[]`,
		`{"id":"node","address":"https://203.0.113.1:6666","internalAddress":"8.8.8.8:6666"}`,
		`{"id":"node","address":"https://203.0.113.1:6666","internalAddress":"localhost:6666"}`,
		`{"id":"node","address":"https://203.0.113.1:6666","internalAddress":"10.0.0.1:0"}`,
		`{"id":"node","address":"https://203.0.113.1:6666","internalAddress":"10.0.0.1:65536"}`,
		`{"id":"node","address":"http://10.0.0.1:6666","internalAddress":"10.0.0.1:6666"}`,
		`{"id":"node","address":"https://203.0.113.1:6666/path","internalAddress":"10.0.0.1:6666"}`,
		`{"id":"node","address":"https://203.0.113.1:6666","internalAddress":"10.0.0.1:6666","trustedCa":"bad"}`,
		`{"id":"node","address":"https://203.0.113.1:6666","internalAddress":"10.0.0.1:6666","transport":"udp"}`,
		`{"id":"node","address":"https://203.0.113.1:6666","internalAddress":"10.0.0.1:6666","unknown":true}`,
		`{} {}`, strings.Repeat(" ", 32769),
	} {
		if _, err := parsePublicGoServer(raw); err == nil {
			t.Fatalf("accepted invalid configuration %q", raw)
		}
	}
}

func TestPublicGoServerRetriesInternalAfterFallbackCacheExpires(t *testing.T) {
	selector := newGatewaySelector("http://127.0.0.1:1")
	selector.preferred = &gatewayEndpoint{ID: "internal", Address: "https://203.0.113.1:6666", dialAddress: "10.0.0.1:6666"}
	now := time.Now()
	selector.now = func() time.Time { return now }
	reachable, discoveries := false, 0
	selector.discover = func(context.Context, string, string) ([]gatewayEndpoint, error) {
		discoveries++
		return []gatewayEndpoint{{ID: "competition"}}, nil
	}
	selector.probe = func(_ context.Context, endpoint gatewayEndpoint) (time.Duration, error) {
		if endpoint.ID == "internal" && !reachable {
			return 0, errors.New("unreachable")
		}
		return time.Millisecond, nil
	}
	for _, want := range []string{"competition", "competition", "internal"} {
		got, ok := selector.selectGatewayForModel(context.Background(), "nn_fixture_key_123", "model")
		if !ok || got.ID != want || discoveries != 1 {
			t.Fatalf("want=%s got=%s discoveries=%d", want, got.ID, discoveries)
		}
		if reachable {
			now = now.Add(gatewayCacheTTL)
		}
		reachable = true
	}
}

func TestPublicGoServerPreferredBeforeDiscoveryAndFallsBack(t *testing.T) {
	for _, reachable := range []bool{true, false} {
		t.Run(map[bool]string{true: "internal", false: "competition"}[reachable], func(t *testing.T) {
			preferred := gatewayEndpoint{ID: "internal", Address: "https://203.0.113.1:6666", dialAddress: "10.0.0.1:6666"}
			selector := newGatewaySelector("http://127.0.0.1:1")
			selector.preferred = &preferred
			discoveries, probes := 0, 0
			selector.discover = func(_ context.Context, key, model string) ([]gatewayEndpoint, error) {
				discoveries++
				if probes != 1 || key != "nn_fixture_key_123" || model != "model" {
					t.Error("discovery must follow failed internal probe with original identity/model")
				}
				return []gatewayEndpoint{{ID: "competition", Address: "https://203.0.113.2:6666"}}, nil
			}
			selector.probe = func(_ context.Context, endpoint gatewayEndpoint) (time.Duration, error) {
				if endpoint.ID == "internal" {
					probes++
					if !reachable {
						return 0, errors.New("unreachable")
					}
				}
				return time.Millisecond, nil
			}
			got, ok := selector.selectGatewayForModel(context.Background(), "nn_fixture_key_123", "model")
			want, calls := "internal", 0
			if !reachable {
				want, calls = "competition", 1
			}
			if !ok || got.ID != want || discoveries != calls {
				t.Fatalf("selected=%s ok=%v discovery=%d", got.ID, ok, discoveries)
			}
		})
	}
}

func TestPublicGoServerInternalDialStillVerifiesTLSIdentity(t *testing.T) {
	gateway, ca := gatewayTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ready")
	}), false)
	_, otherCA := gatewayTLSFixture(t, http.NotFoundHandler(), false)
	for _, protocol := range []string{"http", "grpc"} {
		for _, tc := range []struct{ name, address, ca string }{
			{"wrong-name", "https://203.0.113.1:6666", ca},
			{"wrong-ca", "https://127.0.0.1:1", otherCA},
		} {
			t.Run(protocol+"/"+tc.name, func(t *testing.T) {
				endpoint := gatewayEndpoint{ID: "node", Address: tc.address, TrustedCA: tc.ca, Transport: protocol, dialAddress: gateway.Listener.Addr().String()}
				if _, err := probeGatewayReady(context.Background(), endpoint); err == nil {
					t.Fatal("internal connection bypassed certificate verification")
				}
			})
		}
	}
}

func TestPublicGoServerInvalidConfigurationRejectsHandler(t *testing.T) {
	t.Setenv("NEWNOA_PUBLIC_GO_SERVER", `{"internalAddress":"8.8.8.8:6666"}`)
	c := &core{control: "http://127.0.0.1:1", gateway: "http://127.0.0.1:1"}
	if _, err := c.publicProxyHandler(publicTestSecret, "internal-test"); err == nil {
		t.Fatal("invalid configuration must be reported before serving")
	}
}

func TestPublicGoServerTLSKeepsRegisteredIdentityAndDoesNotReplay(t *testing.T) {
	for _, protocol := range []string{"http", "grpc"} {
		t.Run(protocol, func(t *testing.T) {
			calls := 0
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/ready" {
					io.WriteString(w, "ready")
					return
				}
				calls++
				if r.Header.Get("Authorization") != "Bearer nn_fixture_key_123" {
					t.Error("caller credential changed")
				}
				w.WriteHeader(http.StatusConflict)
			})
			var serverHandler http.Handler = handler
			if protocol == "grpc" {
				server := rpc.NewServer(handler)
				defer server.Stop()
				serverHandler = server
			}
			gateway, ca := gatewayTLSFixture(t, serverHandler, false)
			// The URL port is deliberately unreachable; only the configured dial address works.
			raw, _ := json.Marshal(map[string]string{"id": "node", "address": "https://127.0.0.1:1", "internalAddress": gateway.Listener.Addr().String(), "trustedCa": ca, "transport": protocol})
			t.Setenv("NEWNOA_PUBLIC_GO_SERVER", string(raw))
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/internal/public/authorize" {
					t.Errorf("unexpected discovery or retry %s", r.URL.Path)
				}
				io.WriteString(w, `{"hosted":false}`)
			}))
			defer api.Close()
			original := newGatewaySelector(api.URL)
			c := &core{control: api.URL, gateway: api.URL, gatewaySelector: original}
			proxy, err := c.publicProxyHandler(publicTestSecret, publicTestSecret)
			if err != nil {
				t.Fatal(err)
			}
			if original.preferred != nil {
				t.Fatal("website configuration changed the local selector")
			}
			request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"model"}`))
			request.Header.Set("Authorization", "Bearer nn_fixture_key_123")
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
			out := httptest.NewRecorder()
			proxy.ServeHTTP(out, request)
			wantStatus, wantCalls := http.StatusConflict, 1
			if protocol == "http" {
				wantStatus, wantCalls = http.StatusBadGateway, 0
			}
			if out.Code != wantStatus || calls != wantCalls {
				t.Fatalf("status=%d calls=%d body=%s", out.Code, calls, out.Body.String())
			}
		})
	}
}
