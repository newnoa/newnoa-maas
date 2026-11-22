package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOwnerPublicationImageProbeUsesServingLeaseAndForwardsOnlyBoundHeaders(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if r.URL.Path != "/api/v1/internal/frp/owner-a/route" || body["purpose"] != "serving" {
			t.Error("publication probe widened node lease purpose")
		}
		_, _ = io.WriteString(w, `{"port":21001}`)
	}))
	defer api.Close()
	o := newOwner("owner-a", api.URL, "internal-fixture")
	calls := 0
	o.upstream.Transport = embeddingTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Path != "/publication-probe/v1/images/generations" || r.Header.Get("X-Newnoa-Permit") != "pubprobe_fixture" || r.Header.Get("Authorization") != "" || r.Header.Get("X-Api-Key") != "" || r.GetBody != nil {
			t.Error("publication scope/credentials/replay changed")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}, "X-Newnoa-Probe-Upstream": {"1"}}, Body: io.NopCloser(strings.NewReader(`{"data":[{"url":"https://images.example/result.png"}]}`))}, nil
	})
	r := ownerProbeRequest("POST", "images/generations", []byte(`{"model":"image","prompt":"circle"}`))
	r.URL.Path = strings.Replace(r.URL.Path, "/probe/", "/publication-probe/", 1)
	r.Header.Set("X-Newnoa-Permit", "pubprobe_fixture")
	out := httptest.NewRecorder()
	o.ServeHTTP(out, r)
	if out.Code != 200 || calls != 1 || out.Header().Get("X-Newnoa-Probe-Upstream") != "1" {
		t.Fatalf("code %d calls %d", out.Code, calls)
	}
	for _, path := range []string{"/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/3/publication-probe/v1/models", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/3/evaluation/v1/images/generations"} {
		r := httptest.NewRequest("POST", path, strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer internal-fixture")
		out := httptest.NewRecorder()
		o.ServeHTTP(out, r)
		if out.Code != 404 {
			t.Fatalf("unrequested route accepted %s: %d", path, out.Code)
		}
	}
}

func TestOwnerImagesWaitBeyondRegularResponseHeaderDeadline(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(30 * time.Millisecond)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer s.Close()
	base := &http.Transport{ResponseHeaderTimeout: 5 * time.Millisecond}
	client := &http.Client{Transport: ownerImageTimeout(base)}
	defer client.CloseIdleConnections()
	response, err := client.Get(s.URL + "/publication-probe/v1/images/generations")
	if err != nil {
		t.Fatal("images used short generic timeout", err)
	}
	response.Body.Close()
	if response, err = client.Get(s.URL + "/v1/chat/completions"); err == nil {
		response.Body.Close()
		t.Fatal("regular model timeout widened")
	}
}
