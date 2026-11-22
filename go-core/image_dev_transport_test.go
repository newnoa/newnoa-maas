package main

import "testing"

func TestPublicImageDevTransportIsOnlyExplicitDevLoopback(t *testing.T) {
	for _, tc := range []struct {
		environment, address string
		want                 bool
	}{{"dev", "http://127.0.0.1:6666", true}, {"dev", "127.0.0.1:6666", true}, {"prd", "127.0.0.1:6666", false}, {"prd", "http://127.0.0.1:6666", false}, {"dev", "http://192.168.1.2:6666", false}, {"dev", "http://example.com:6666", false}} {
		t.Setenv("NEWNOA_ENV", tc.environment)
		if got := devLoopbackGateway(gatewayEndpoint{Address: tc.address}); got != tc.want {
			t.Fatalf("%s %s =%v", tc.environment, tc.address, got)
		}
	}
}
