package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGatewayLargeAgentRequests(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				raw, err := io.ReadAll(r.Body)
				if err != nil || len(raw) <= maxBody || r.URL.Path != path {
					t.Error("large agent request changed")
				}
				w.Header().Set("Content-Type", "application/json")
				if path == "/v1/messages" {
					io.WriteString(w, `{"id":"msg_fixture","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":1}}`)
				} else {
					io.WriteString(w, `{"status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":1}}`)
				}
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": "agent-request", "permit": "fixture", "endpoint": upstream.URL})
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			image := strings.Repeat("A", 2<<20)
			body := `{"model":"fixture","max_output_tokens":16,"input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,` + image + `"}]}]}`
			if path == "/v1/messages" {
				body = `{"model":"fixture","max_tokens":16,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + image + `"}}]}]}`
			}
			for _, tc := range []struct {
				body string
				code int
			}{{body, 200}, {body + strings.Repeat(" ", 32<<20), 413}} {
				req := httptest.NewRequest("POST", path, strings.NewReader(tc.body))
				req.Header.Set("Authorization", "Bearer ses_fixture")
				req.Header.Set("X-Api-Key", "nn_fixture")
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				g.ServeHTTP(w, req)
				if w.Code != tc.code {
					t.Fatalf("body=%d code=%d want=%d response=%s", len(tc.body), w.Code, tc.code, w.Body.String())
				}
			}
			if calls != 1 {
				t.Fatalf("upstream calls=%d want=1", calls)
			}
		})
	}
}
