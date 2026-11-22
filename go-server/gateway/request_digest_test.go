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

func TestDispatchDigestsBindInferencePathAndActualBody(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		for _, limits := range []string{`,"max_tokens":16,"max_output_tokens":16`, "", `,"max_tokens":null,"max_output_tokens":null`} {
			t.Run(path+limits, func(t *testing.T) {
				var executionDigest string
				var forwarded bool
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, _ := io.ReadAll(r.Body)
					sum := sha256.Sum256(append([]byte("POST "+path+"\n"), raw...))
					var body map[string]any
					json.Unmarshal(raw, &body)
					if r.URL.Path != path || body["model"] != "upstream-model" || executionDigest != hex.EncodeToString(sum[:]) {
						t.Error("execution digest did not bind path and rewritten body")
					}
					for _, field := range []string{"max_tokens", "max_output_tokens"} {
						value, present := body[field]
						wantValue := any(nil)
						if strings.Contains(limits, ":16") {
							wantValue = float64(16)
						}
						if present != strings.Contains(limits, field) || value != wantValue {
							t.Errorf("output limit %s changed: present=%v value=%v", field, present, value)
						}
					}
					forwarded = true
					w.Write([]byte(`{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],"status":"completed","output":[]}`))
				}))
				defer upstream.Close()
				original := `{"model":"deployment","messages":[{}],"input":"test"` + limits + `}`
				api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case strings.HasSuffix(r.URL.Path, "/authorize"):
						w.Write([]byte(`{"model":"upstream-model"}`))
					case strings.HasSuffix(r.URL.Path, "/dispatches"):
						var body map[string]any
						json.NewDecoder(r.Body).Decode(&body)
						sum := sha256.Sum256([]byte("POST " + path + "\n" + original))
						if body["bodyDigest"] != hex.EncodeToString(sum[:]) {
							t.Error("original request digest did not bind path")
						}
						executionDigest, _ = body["executionDigest"].(string)
						json.NewEncoder(w).Encode(map[string]any{"requestId": "fixture", "permit": "fixture", "endpoint": upstream.URL})
					default:
						w.Write([]byte(`{}`))
					}
				}))
				defer api.Close()
				g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
				req := httptest.NewRequest("POST", path, strings.NewReader(original))
				req.Header.Set("X-Api-Key", "nn_fixture")
				req.Header.Set("Authorization", "Bearer ses_client_login")
				res := httptest.NewRecorder()
				g.ServeHTTP(res, req)
				if res.Code != 200 || !forwarded {
					t.Fatalf("request failed: status=%d forwarded=%v", res.Code, forwarded)
				}
			})
		}
	}
}
