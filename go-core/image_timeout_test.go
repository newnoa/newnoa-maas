package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSupplyImageTimeoutLeavesOrdinaryHeaderLimitUnchanged(t *testing.T) {
	client := (supplyPolicy{edition: "domestic"}).client(5 * time.Minute)
	defer client.CloseIdleConnections()
	transport, ok := client.Transport.(imageResponseTransport)
	if !ok {
		t.Fatal("supply client has no path-specific Images header timeout")
	}
	if transport.regular.(*http.Transport).ResponseHeaderTimeout != 30*time.Second || transport.images.(*http.Transport).ResponseHeaderTimeout != 120*time.Second || client.Timeout != 5*time.Minute {
		t.Fatal("ordinary/image/header request timeout changed")
	}
}

func TestImageHeaderWaitDoesNotRelaxOtherProtocols(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(40 * time.Millisecond):
		}
		io.WriteString(w, `{"data":[]}`)
	}))
	defer upstream.Close()
	ordinary := http.DefaultTransport.(*http.Transport).Clone()
	ordinary.ResponseHeaderTimeout = 5 * time.Millisecond
	client := &http.Client{Transport: withImageResponseTimeout(ordinary), Timeout: time.Second}
	defer client.CloseIdleConnections()
	if response, err := client.Get(upstream.URL + "/v1/chat/completions"); err == nil {
		response.Body.Close()
		t.Fatal("ordinary request escaped its short header timeout")
	}
	response, err := client.Get(upstream.URL + "/prefix" + imageGenerationPath)
	if err != nil {
		t.Fatal("image generation inherited ordinary header timeout", err)
	}
	response.Body.Close()
}
