package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOnlineModelListPublicHTTPAndDNSPolicy(t *testing.T) {
	p := supplyPolicy{edition: "online"}
	for _, endpoint := range []string{"http://8.8.8.8:8080/custom", "https://example.com/custom/v1/", "https://[2606:4700::1111]"} {
		u, err := modelListURLWithPolicy(endpoint, p)
		if err != nil || !strings.HasSuffix(u.Path, "/v1/models") {
			t.Fatalf("%s: %v %v", endpoint, u, err)
		}
	}
	for _, endpoint := range []string{"http://127.0.0.1", "http://192.0.2.14", "http://169.254.169.254", "http://[::]", "http://100.64.0.1", "http://[::ffff:127.0.0.1]", "http://[::127.0.0.1]", "http://[fec0::1]", "http://[100::1]", "http://[64:ff9b::a00:1]", "http://example.com?key=secret", "http://u:p@example.com"} {
		if _, err := p.endpoint(endpoint); err == nil {
			t.Fatalf("accepted %s", endpoint)
		}
	}
}
func TestOnlineDialPinsPublicDNSAndRejectsRedirect(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			w.Write([]byte(`{"data":[{"id":"fixture"}]}`))
			return
		}
		http.Redirect(w, r, "http://127.0.0.1/secret", 302)
	}))
	defer upstream.Close()
	destination := strings.TrimPrefix(upstream.URL, "http://")
	dials := 0
	p := supplyPolicy{edition: "online", lookup: func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
	}, connect: func(ctx context.Context, network, address string) (net.Conn, error) {
		dials++
		if address != "8.8.8.8:80" {
			t.Errorf("unvalidated dial %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, destination)
	}}
	models, err := listModelsWithPolicy("http://example.com", "secret", p)
	if err != nil || len(models) != 1 || dials != 1 {
		t.Fatalf("models=%v err=%v dials=%d", models, err, dials)
	}
	if _, err = listModelsWithPolicy("http://example.com/custom", "secret", p); err == nil {
		t.Fatal("followed redirect")
	}
	p.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}, {IP: net.ParseIP("127.0.0.1")}}, nil
	}
	before := dials
	if _, err = listModelsWithPolicy("http://example.com", "secret", p); err == nil || dials != before {
		t.Fatal("mixed DNS escaped public policy")
	}
}
