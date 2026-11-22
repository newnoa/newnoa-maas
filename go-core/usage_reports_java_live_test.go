package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"testing"
)

func TestPrivateUsageCloudJavaLive(t *testing.T) {
	api := os.Getenv("NEWNOA_PRIVATE_USAGE_LIVE_API")
	if api == "" {
		t.Skip("run root tests/peer-gateway-live.mjs")
	}
	target, _ := url.Parse(api)
	proxy := httputil.NewSingleHostReverseProxy(target)
	director := proxy.Director
	proxy.Director = func(r *http.Request) { director(r); r.Header.Set("Accept-Encoding", "identity") }
	proxy.ModifyResponse = func(r *http.Response) error {
		if r.StatusCode != 200 {
			data, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
			r.Body.Close()
			t.Logf("cloud rejection %d: %s", r.StatusCode, data)
			r.Body = io.NopCloser(bytes.NewReader(data))
		}
		return nil
	}
	calls := 0
	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(503)
			return
		}
		if r.URL.Path != "/api/v1/connector/usage" {
			t.Error("not Java cloud usage")
		}
		proxy.ServeHTTP(w, r)
	}))
	defer bridge.Close()
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	epoch, _ := strconv.ParseInt(os.Getenv("NEWNOA_PRIVATE_USAGE_LIVE_EPOCH"), 10, 64)
	c := newUsageCapture(true)
	c.feed([]byte("data: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3,\"prompt_tokens_details\":{\"cached_tokens\":4}}}\n\ndata: [DONE]\n\n"))
	v := c.counts()
	v.RequestID = os.Getenv("NEWNOA_PRIVATE_USAGE_LIVE_REQUEST")
	v.NodeID = os.Getenv("NEWNOA_PRIVATE_USAGE_LIVE_NODE")
	v.GatewayID = os.Getenv("NEWNOA_PRIVATE_USAGE_LIVE_GATEWAY")
	v.Epoch = epoch
	if err = h.queueUsage(v); err != nil {
		t.Fatal(err)
	}
	h.db.Close()
	h, err = openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	n := &connector{nodeID: v.NodeID, credential: os.Getenv("NEWNOA_PRIVATE_USAGE_LIVE_CREDENTIAL"), control: bridge.URL, client: bridge.Client(), usageStore: h}
	n.flushUsage(context.Background())
	var pending int
	h.db.QueryRow("SELECT count(*) FROM private_usage_outbox").Scan(&pending)
	if pending != 1 {
		t.Fatal("failed upload lost")
	}
	h.db.Exec("UPDATE private_usage_outbox SET next_attempt=0")
	n.flushUsage(context.Background())
	h.db.QueryRow("SELECT count(*) FROM private_usage_outbox").Scan(&pending)
	if pending != 0 {
		var last string
		h.db.QueryRow("SELECT last_error FROM private_usage_outbox LIMIT 1").Scan(&last)
		t.Fatal("Java cloud did not acknowledge", last)
	}
	if err = n.sendUsage(context.Background(), v); err != nil {
		t.Fatal("duplicate report not idempotent", err)
	}
}
