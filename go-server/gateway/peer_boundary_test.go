package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGatewayRejectsLANPeerEvenWhenItsPinAndEpochAreValid(t *testing.T) {
	for _, mode := range []string{"direct", "wrong-pin", "blocked-egress", "disconnect"} {
		t.Run(mode, func(t *testing.T) { testGatewayPeerBoundary(t, mode) })
	}
}
func testGatewayPeerBoundary(t *testing.T, mode string) {
	const node = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const model = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
	var executions, reservations, finalized, readiness int
	peer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/peer/ready" {
			readiness++
			io.WriteString(w, `{"nodeId":"`+node+`","epoch":2}`)
			return
		}
		executions++
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Api-Key") != "" {
			t.Error("platform credentials reached provider Core")
		}
		if r.Header.Get("X-Newnoa-Gateway") != "gateway-fixture" || r.Header.Get("X-Newnoa-Permit") != "permit_fixture" || r.Header.Get("X-Newnoa-Epoch") != "2" {
			t.Error("missing gateway-bound execution fence")
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["model"] != "upstream-fixture" {
			t.Error("model not rewritten once")
		}
		if mode == "disconnect" {
			connection, _, _ := w.(http.Hijacker).Hijack()
			connection.Close()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}`)
	}))
	defer peer.Close()
	digest := sha256.Sum256(peer.Certificate().Raw)
	if mode == "wrong-pin" {
		digest = [32]byte{}
	}
	control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer internal-fixture" {
			t.Error("control auth missing")
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			json.NewEncoder(w).Encode(map[string]any{"model": "upstream-fixture", "peer": map[string]any{"available": true, "private": true, "url": peer.URL, "certificateSha256": hex.EncodeToString(digest[:]), "nodeId": node, "epoch": 2, "model": "upstream-fixture"}})
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			reservations++
			if body["nodeId"] != node || body["epoch"] != float64(2) || body["transportPath"] != "direct" {
				t.Error("reservation was not fenced to connected node/epoch")
			}
			json.NewEncoder(w).Encode(map[string]any{"requestId": "cccccccc-bbbb-cccc-dddd-eeeeeeeeeeee", "permit": "permit_fixture", "nodeId": node, "epoch": 2, "model": "upstream-fixture"})
		case strings.HasSuffix(r.URL.Path, "/transport"):
			io.WriteString(w, `{}`)
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			finalized++
			expected := "succeeded"
			if mode == "disconnect" {
				expected = "unknown"
			}
			if body["status"] != expected || (mode == "disconnect" && body["transportPath"] != nil) || (mode != "disconnect" && body["transportPath"] != "direct") || (mode != "disconnect" && body["inputTokens"] != float64(2)) {
				t.Errorf("incomplete shared evidence: %v", body)
			}
			io.WriteString(w, `{}`)
		default:
			t.Errorf("unexpected API call %s", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer control.Close()
	allow := []string{"127.0.0.1"}
	if mode == "blocked-egress" {
		allow = nil
	}
	gateway := newGateway(control.URL, "internal-fixture", "gateway-fixture", allow)
	defer gateway.peers.Reset()
	request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"`+model+`","messages":[{"role":"user","content":"fixture"}],"stream":false,"max_tokens":16}`))
	request.Header.Set("X-Api-Key", "nn_"+strings.Repeat("k", 40))
	request.Header.Set("Authorization", "Bearer ses_client_login")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, request)
	expectedStatus, expectedCalls := 503, 0
	if response.Code != expectedStatus || executions != expectedCalls || reservations != expectedCalls || finalized != expectedCalls {
		t.Fatalf("status=%d executions=%d reservations=%d finalized=%d body=%s", response.Code, executions, reservations, finalized, response.Body.String())
	}
	if expectedCalls > 0 && mode != "disconnect" && response.Header().Get("X-Newnoa-Path") != "direct" {
		t.Fatal("Core cannot record actual Server path")
	}
	if mode == "disconnect" && response.Header().Get("X-Newnoa-Path") != "" {
		t.Fatal("failed execution claimed a confirmed path")
	}
	if readiness != 0 {
		t.Fatal("Go Server probed forbidden LAN provider")
	}
}
