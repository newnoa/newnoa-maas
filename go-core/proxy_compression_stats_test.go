package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCompressionDailyStatsPersistAndIsolateAccounts(t *testing.T) {
	c := newCore(t.TempDir(), "https://example.invalid")
	defer c.shutdown()
	now := time.Date(2026, 11, 18, 16, 0, 0, 0, time.UTC)
	if err := c.recordCompressionStats(context.Background(), "account-a", 1000, 700, now); err != nil {
		t.Fatal(err)
	}
	if err := c.recordCompressionStats(context.Background(), "account-a", 500, 500, now); err != nil {
		t.Fatal(err)
	}
	if err := c.recordCompressionStats(context.Background(), "account-b", 9000, 1000, now); err != nil {
		t.Fatal(err)
	}
	if err := c.recordCompressionStats(context.Background(), "account-a", 100, 50, now.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	stats, err := c.readCompressionStats("account-a", now)
	if err != nil || stats.BeforeTokens != 1500 || stats.AfterTokens != 1200 || stats.Requests != 2 || stats.Day != "2026-11-19" {
		t.Fatalf("%+v %v", stats, err)
	}
	h, _ := c.historyStore()
	h.db.Close()
	c.history = nil
	stats, err = c.readCompressionStats("account-a", now)
	if err != nil || stats.BeforeTokens != 1500 {
		t.Fatalf("restart: %+v %v", stats, err)
	}
	empty, err := c.readCompressionStats("unknown", now)
	if err != nil || empty.Requests != 0 {
		t.Fatalf("isolation: %+v %v", empty, err)
	}
	if err := c.recordCompressionStats(context.Background(), "account-a", 1, 2, now); err == nil {
		t.Fatal("invalid counters accepted")
	}
}

func TestCompressionCountersMatchForwardedBodyAndDisabledRequests(t *testing.T) {
	c := newCore(t.TempDir(), "https://example.invalid")
	defer c.shutdown()
	raw := compressionRequest("/v1/chat/completions", compressionFixture())
	var forwarded []byte
	handler := c.compressInference(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded, _ = io.ReadAll(r.Body); w.WriteHeader(200) }))
	send := func() {
		r := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(raw))
		r.Header.Set("Content-Type", "application/json")
		r = r.WithContext(context.WithValue(r.Context(), compressionAccountContext{}, "account-a"))
		handler.ServeHTTP(httptest.NewRecorder(), r)
	}
	send()
	if bytes.Equal(raw, forwarded) {
		t.Fatal("body was not compressed")
	}
	before, after := countCompressionTokens(raw), countCompressionTokens(forwarded)
	if _, err := c.configureCompression(json.RawMessage(`{"enabled":false}`)); err != nil {
		t.Fatal(err)
	}
	send()
	if !bytes.Equal(raw, forwarded) {
		t.Fatal("disabled request changed")
	}
	stats, err := c.readCompressionStats("account-a", time.Now())
	if err != nil || stats.BeforeTokens != int64(before*2) || stats.AfterTokens != int64(after+before) || stats.Requests != 2 {
		t.Fatalf("%+v %v", stats, err)
	}
}

func TestCompressionStatsRPCUsesVerifiedIdentity(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"id": "00000000-0000-0000-0000-000000000001"})
	}))
	defer api.Close()
	c := newCore(t.TempDir(), api.URL)
	c.control = api.URL
	defer c.shutdown()
	if _, err := c.handle("proxy.compression.stats", json.RawMessage(`{}`)); err == nil {
		t.Fatal("anonymous stats accepted")
	}
	if _, err := c.handle("proxy.compression.stats", json.RawMessage(`{"session":"test","account":"other"}`)); err == nil {
		t.Fatal("caller-selected account accepted")
	}
	if _, err := c.handle("proxy.compression.stats", json.RawMessage(`{"session":"test"}`)); err != nil {
		t.Fatal(err)
	}
}

func TestCompressionDefaultsDistinguishOnlineAndLocalCore(t *testing.T) {
	t.Setenv("NEWNOA_ENV", "dev")
	t.Setenv("NEWNOA_PUBLIC_PROXY_ENABLED", "false")
	t.Setenv("NEWNOA_PROXY_COMPRESSION_DEFAULT", "")
	c := &core{}
	if !c.compressionStatus().Enabled {
		t.Fatal("local default must be on")
	}
	t.Setenv("NEWNOA_PROXY_COMPRESSION_DEFAULT", "false")
	if c.compressionStatus().Enabled {
		t.Fatal("online deployment override ignored")
	}
	t.Setenv("NEWNOA_PROXY_COMPRESSION_DEFAULT", "")
	t.Setenv("NEWNOA_ENV", "prd")
	if c.compressionStatus().Enabled {
		t.Fatal("production Core default must be off")
	}
	t.Setenv("NEWNOA_ENV", "dev")
	c.publicStateless = true
	if c.compressionStatus().Enabled {
		t.Fatal("public Core default must be off")
	}
}

func TestCompressionStatisticsCannotWaitForBusyHistoryConnection(t *testing.T) {
	c := newCore(t.TempDir(), "https://example.invalid")
	defer c.shutdown()
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	connection, err := h.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	started := time.Now()
	err = c.recordCompressionStats(context.Background(), "account", 100, 80, time.Now())
	if err == nil || time.Since(started) > 250*time.Millisecond {
		t.Fatalf("busy database delayed inference: %v %v", err, time.Since(started))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started = time.Now()
	err = c.recordCompressionStats(ctx, "account", 100, 80, time.Now())
	if err == nil || time.Since(started) > 100*time.Millisecond {
		t.Fatal("cancellation ignored", err)
	}
}
