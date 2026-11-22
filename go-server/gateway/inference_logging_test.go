package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInferenceCompletionLogsUsageAndFailureWithoutSecrets(t *testing.T) {
	for _, tc := range []struct {
		name            string
		status          int
		finalizeFailure bool
	}{{"success", 200, false}, {"upstream rejected", 429, false}, {"settlement pending", 200, true}} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(previous)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Request-Id", "req_test123")
				w.WriteHeader(tc.status)
				io.WriteString(w, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"private-output"}]}],"usage":{"input_tokens":12,"output_tokens":2,"input_tokens_details":{"cached_tokens":0}}}`)
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"fixture","deploymentId":"deployment"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": "request-test", "permit": "secret-permit", "endpoint": upstream.URL, "apiKey": "secret-upstream", "pooledCredential": true})
				case strings.HasSuffix(r.URL.Path, "/finalize") && tc.finalizeFailure:
					w.WriteHeader(503)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "secret-internal", "fixture", []string{"127.0.0.1"})
			req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"fixture","input":"private-prompt"}`))
			req.Header.Set("Authorization", "Bearer ses_secret-caller")
			req.Header.Set("X-Api-Key", "nn_secret-caller")
			response := httptest.NewRecorder()
			g.ServeHTTP(response, req)
			scanner := bufio.NewScanner(bytes.NewReader(logs.Bytes()))
			var row map[string]any
			for scanner.Scan() {
				var candidate map[string]any
				if json.Unmarshal(scanner.Bytes(), &candidate) == nil && candidate["msg"] == "inference.completed" {
					row = candidate
				}
			}
			if row == nil {
				t.Fatalf("missing completion log: status=%d body=%s logs=%s", response.Code, response.Body.String(), logs.String())
			}
			if row["request_id"] != "request-test" || row["paid_pool"] != true || row["upstream_status"] != float64(tc.status) || row["upstream_request_id"] != "req_test123" || row["finalized"] != !tc.finalizeFailure {
				t.Fatalf("missing diagnostic fields: %v", row)
			}
			if tc.status == 200 {
				if row["input_tokens"] != float64(12) || row["output_tokens"] != float64(2) || row["cached_input_tokens"] != float64(0) || row["outcome"] != "succeeded" {
					t.Fatalf("usage/outcome: %v", row)
				}
			} else {
				if _, ok := row["input_tokens"]; ok {
					t.Fatal("unknown usage became zero")
				}
				if row["outcome"] != "failed" {
					t.Fatal("upstream error lost")
				}
			}
			// Scanner consumes its source; inspect the captured fields as serialized metadata.
			encoded, _ := json.Marshal(row)
			for _, secret := range []string{"secret-", "private-", upstream.URL} {
				if strings.Contains(string(encoded), secret) {
					t.Fatalf("log leaked %s", secret)
				}
			}
		})
	}
}
