package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAllCoreChatInferenceUsesGatewayWithoutClientAdmissionOrPeerSecrets(t *testing.T) {
	calls, controlCalls := 0, 0
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { controlCalls++; w.WriteHeader(500) }))
	defer control.Close()
	raw := `{"model":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","messages":[{"role":"user","content":"preserved"}],"max_tokens":16}`
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer custom_consumer_key_123" {
			t.Error("caller identity was changed")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != raw {
			t.Errorf("payload changed: %s", body)
		}
		w.Header().Set("X-Newnoa-Path", "peer-tunnel")
		w.Header().Set("X-Newnoa-Request", "gateway-request")
		io.WriteString(w, "gateway response")
	}))
	defer gateway.Close()
	handler, err := privateInference(control.URL, gateway.URL)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(raw))
	request.Header.Set("Authorization", "Bearer custom_consumer_key_123")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 200 || calls != 1 || controlCalls != 0 {
		t.Fatalf("status=%d gateway=%d control=%d", response.Code, calls, controlCalls)
	}
	if response.Header().Get("X-Newnoa-Path") != "peer-tunnel" || response.Header().Get("X-Newnoa-Request") != "gateway-request" {
		t.Fatal("Server history/diagnostic headers were lost")
	}
}
