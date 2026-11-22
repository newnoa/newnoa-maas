package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type embeddingTransport func(*http.Request) (*http.Response, error)

func (f embeddingTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestServingEmbeddingRouteKeepsPurposeAndPermit(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(raw), `"purpose":"serving"`) {
			t.Error("wrong lease purpose")
		}
		w.Write([]byte(`{"port":21001}`))
	}))
	defer control.Close()
	owner := newOwner("owner-a", control.URL, "internal-fixture")
	calls := 0
	owner.upstream = &http.Client{Transport: embeddingTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != "http://127.0.0.1:21001/v1/embeddings" || r.Header.Get("X-Newnoa-Permit") != "permit_fixture" || r.Header.Get("X-Newnoa-Epoch") != "3" {
			t.Error("embedding route or permit changed")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"object":"list","data":[]}`))}, nil
	})}
	req := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/3/v1/embeddings", strings.NewReader(`{"model":"fixture","input":"text"}`))
	req.Header.Set("Authorization", "Bearer internal-fixture")
	req.Header.Set("X-Newnoa-Permit", "permit_fixture")
	response := httptest.NewRecorder()
	owner.ServeHTTP(response, req)
	if response.Code != 200 || calls != 1 {
		t.Fatalf("status=%d calls=%d", response.Code, calls)
	}
}
