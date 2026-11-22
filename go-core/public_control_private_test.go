package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func privateControlTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range addresses {
		ip, _, err := net.ParseCIDR(address.String())
		if err != nil || ip.To4() == nil || !ip.IsPrivate() || ip.IsLoopback() {
			continue
		}
		listener, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
		if err != nil {
			continue
		}
		server := httptest.NewUnstartedServer(handler)
		server.Listener = listener
		server.Start()
		return server
	}
	t.Skip("no bindable private IPv4 for public Core control integration test")
	return nil
}

func TestDesktopRemoteHelpersStillRejectPrivateHTTPControl(t *testing.T) {
	calls := 0
	api := privateControlTestServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer api.Close()
	if historyRemote(context.Background(), api.URL, "/me", "Bearer desktop-session", nil, nil) == nil {
		t.Error("desktop history helper accepted private HTTP")
	}
	if hostedControl(context.Background(), api.URL, "/models", "Bearer desktop-session", "", nil, nil) == nil {
		t.Error("desktop hosted helper accepted private HTTP")
	}
	if calls != 0 {
		t.Fatal("desktop caller credential reached disallowed private origin")
	}
}

func TestPublicControlRemoteHelpersRejectInvalidOriginAfterConstruction(t *testing.T) {
	for _, origin := range []string{"http://8.8.8.8:8090", "http://api.invalid:8090", "http://169.254.1.2:8090", "http://[fe80::2]:8090", "http://10.219.77.28:8090?", "http://user@10.219.77.28:8090"} {
		c := &core{control: origin, publicStateless: true}
		if c.controlRemote(context.Background(), "/me", "Bearer caller", nil, nil) == nil {
			t.Fatalf("public user control accepted unsafe origin %s", origin)
		}
		if c.hostedRemote(context.Background(), "/dispatches/id/finish", "Bearer report", "", map[string]string{"status": "succeeded"}, nil) == nil {
			t.Fatalf("public hosted completion accepted unsafe origin %s", origin)
		}
	}
}
