package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSupplyEndpointPolicyNativeDockerAndOverseas(t *testing.T) {
	domestic := supplyPolicy{edition: "domestic"}
	for _, endpoint := range []string{"http://127.0.0.1:8080/v1", "http://0.0.0.0:8080/v1", "https://[::1]:443/v1", "http://192.168.50.22:8080/v1", "http://10.1.2.3:8080", "https://172.16.0.1/v1", "http://[fd12::1]:8080", "http://[::ffff:192.168.1.2]:8080"} {
		u, err := domestic.endpoint(endpoint)
		if err != nil {
			t.Fatal(endpoint, err)
		}
		if u.Hostname() == "0.0.0.0" {
			t.Fatal("unspecified address was not normalized")
		}
	}
	for _, endpoint := range []string{"http://8.8.8.8:8080", "http://172.32.0.1", "http://169.254.169.254", "https://example.com/v1", "http://localhost:8080", "http://localhost.example.com:8080", "http://127.0.0.1@evil.test", "http://host.docker.internal:8080", "http://[fe80::1]:8080", "http://[2001:4860:4860::8888]"} {
		if _, err := domestic.endpoint(endpoint); err == nil {
			t.Fatal("accepted remote supply", endpoint)
		}
	}
	docker := supplyPolicy{edition: "domestic", dockerHost: net.ParseIP("172.22.0.1")}
	for _, endpoint := range []string{"http://host.docker.internal:8080", "http://172.22.0.1:8080"} {
		if _, err := docker.endpoint(endpoint); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := docker.endpoint("http://172.22.0.2:8080"); err != nil {
		t.Fatal("rejected another LAN machine", err)
	}
	overseas := supplyPolicy{edition: "overseas"}
	if _, err := overseas.endpoint("https://remote.example.test/v1"); err == nil {
		t.Fatal("local supply accepted a domain in overseas edition")
	}
	if _, err := (supplyPolicy{edition: "forged"}).endpoint("https://remote.example.test"); err == nil {
		t.Fatal("unknown edition enabled remote access")
	}
}
func TestSupplyDialRejectsSpoofedResolutionAndDisablesEnvironmentProxy(t *testing.T) {
	policy := supplyPolicy{edition: "domestic"}
	if !policy.allows(net.ParseIP("192.168.1.2")) || policy.allows(net.ParseIP("8.8.8.8")) {
		t.Fatal("private IP policy mismatch")
	}
	if !policy.allows(net.ParseIP("::ffff:127.0.0.1")) {
		t.Fatal("mapped loopback rejected")
	}
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer local.Close()
	client := policy.client(0)
	defer client.CloseIdleConnections()
	response, err := client.Get(local.URL)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if _, err := policy.dial(context.Background(), "tcp", "8.8.8.8:8080"); err == nil {
		t.Fatal("final dial escaped private IP policy")
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "http://192.168.20.5:8080", 302) }))
	defer redirect.Close()
	response, err = client.Get(redirect.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 302 {
		t.Fatal("followed supply redirect")
	}
}
