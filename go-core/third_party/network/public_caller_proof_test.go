package network

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPublicCallerProofBindsRequestAndDestination(t *testing.T) {
	const key = "fixture-internal-service-token-with-32-characters"
	now := time.Unix(2000000000, 0)
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader("{}"))
	r.Header.Set("Authorization", "Bearer nn_consumer_123456")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "unique-call")
	digest := strings.Repeat("a", 64)
	proof, err := SignPublicCaller(key, "node-a", r, digest, "203.0.113.9", now)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set(PublicCallerProofHeader, proof)
	r.Header.Set(PublicCallerIPHeader, "203.0.113.9")
	if got, ip, err := VerifyPublicCaller(key, "node-a", r, now); err != nil || got != digest || ip != "203.0.113.9" {
		t.Fatalf("valid request rejected %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func()
	}{
		{"other-node", func() {}}, {"other-key", func() { r.Header.Set("Authorization", "Bearer nn_other_123456") }},
		{"other-path", func() { r.URL.Path = "/v1/responses" }}, {"other-ip", func() { r.Header.Set(PublicCallerIPHeader, "203.0.113.8") }},
		{"other-idempotency", func() { r.Header.Set("Idempotency-Key", "another") }}, {"duplicated-proof", func() { r.Header.Add(PublicCallerProofHeader, proof) }},
		{"duplicated-authorization", func() { r.Header.Add("Authorization", "Bearer nn_other_123456") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := r.Clone(r.Context())
			tc.change()
			node := "node-a"
			if tc.name == "other-node" {
				node = "node-b"
			}
			if _, _, err := VerifyPublicCaller(key, node, r, now); err == nil {
				t.Fatal("changed binding accepted")
			}
			r = copy
		})
	}
	for _, at := range []time.Time{now.Add(31 * time.Second), now.Add(-61 * time.Second)} {
		if _, _, err := VerifyPublicCaller(key, "node-a", r, at); err == nil {
			t.Fatal("expired/future proof accepted")
		}
	}
	if strings.Contains(proof, key) {
		t.Fatal("service credential sent in proof")
	}
}

func TestPublicCallerProofRejectsMissingAndMalformedBindings(t *testing.T) {
	const key = "fixture-internal-service-token-with-32-characters"
	now := time.Unix(2000000000, 0)
	request := func() *http.Request {
		r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
		r.Header.Set("Authorization", "Bearer nn_consumer_123456")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "unique")
		return r
	}
	for _, change := range []func(*http.Request){func(r *http.Request) { r.Header.Del("Idempotency-Key") }, func(r *http.Request) { r.Header.Set("Authorization", "Bearer ses_desktop") }, func(r *http.Request) { r.Header.Set("Authorization", "Bearer nn_test_credential") }, func(r *http.Request) { r.URL.ForceQuery = true }, func(r *http.Request) { r.Method = "GET" }} {
		r := request()
		change(r)
		if _, err := SignPublicCaller(key, "node", r, strings.Repeat("a", 64), "203.0.113.9", now); err == nil {
			t.Fatal("incomplete request signed")
		}
	}
	r := request()
	if _, err := SignPublicCaller("short", "node", r, strings.Repeat("a", 64), "203.0.113.9", now); err == nil {
		t.Fatal("weak service secret used")
	}
	if _, err := SignPublicCaller(key, "node", r, "not-a-digest", "203.0.113.9", now); err == nil {
		t.Fatal("invalid body digest signed")
	}
	if _, err := SignPublicCaller(key, "node", r, strings.Repeat("a", 64), "not-an-ip", now); err == nil {
		t.Fatal("invalid caller signed")
	}
	for _, proof := range []string{"", "v1.bad", "v1.not-time." + strings.Repeat("a", 64) + ".abc", "v1.2000000030.invalid.abc", "v1.2000000030." + strings.Repeat("a", 64) + ".***", strings.Repeat("x", 257)} {
		r := request()
		r.Header.Set(PublicCallerProofHeader, proof)
		r.Header.Set(PublicCallerIPHeader, "203.0.113.9")
		if _, _, err := VerifyPublicCaller(key, "node", r, now); err == nil {
			t.Fatal("malformed proof accepted")
		}
	}
	if _, _, err := VerifyPublicCaller(key, "node", request(), now); err == nil {
		t.Fatal("missing proof accepted")
	}
}

func TestPublicCallerProofAcceptsEveryAuthorizedAPIKeyFormat(t *testing.T) {
	const internal = "fixture-internal-service-token-with-32-characters"
	for _, key := range []string{"sk-" + strings.Repeat("A", 43), "nn_legacy_1234567890", "custom_key_1234567890"} {
		r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
		r.Header.Set("Authorization", "Bearer "+key)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "format-case")
		proof, err := SignPublicCaller(internal, "node", r, strings.Repeat("a", 64), "203.0.113.9", time.Now())
		if err != nil {
			t.Fatalf("API-authorized key format rejected: %v", err)
		}
		r.Header.Set(PublicCallerProofHeader, proof)
		r.Header.Set(PublicCallerIPHeader, "203.0.113.9")
		if _, _, err := VerifyPublicCaller(internal, "node", r, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
}
