package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestImageCheckpointSurvivesFinalizeFailure(t *testing.T) {
	for _, mode := range []string{"unavailable", "lost-ack", "checkpoint-unavailable", "failed-upstream"} {
		t.Run(mode, func(t *testing.T) {
			response := fmt.Sprintf(`{"data":[{"b64_json":%q}],"usage":{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2},"output_tokens":4}}`, imagePNG)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "failed-upstream" {
					w.WriteHeader(500)
				}
				fmt.Fprint(w, response)
			}))
			defer upstream.Close()
			var mu sync.Mutex
			var durable map[string]any
			var persistedSequence int64
			finalCalls := 0
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					fmt.Fprint(w, `{"model":"image","imageResultHosting":false}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": "image-recovery", "permit": "bound-permit", "endpoint": upstream.URL, "apiKey": "upstream-only", "modelType": "image", "modelCategory": "image"})
				case strings.HasSuffix(r.URL.Path, "/meter"):
					var body struct {
						Permit   string         `json:"permit"`
						Sequence int64          `json:"sequence"`
						Snapshot map[string]any `json:"snapshot"`
					}
					if json.NewDecoder(r.Body).Decode(&body) != nil || body.Permit != "bound-permit" {
						t.Error("checkpoint permit lost")
						w.WriteHeader(400)
						return
					}
					if mode == "checkpoint-unavailable" && body.Sequence == 2 {
						w.WriteHeader(503)
						return
					}
					durable = body.Snapshot
					persistedSequence = body.Sequence
					json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": "image-recovery", "sequence": body.Sequence})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					finalCalls++
					if mode == "checkpoint-unavailable" {
						fmt.Fprint(w, `{}`)
						return
					}
					if durable == nil || durable["imageUsage"] == nil || persistedSequence != 2 {
						t.Error("closure attempted before durable image usage")
					}
					if mode == "lost-ack" {
						conn, _, e := w.(http.Hijacker).Hijack()
						if e != nil {
							t.Error(e)
							return
						}
						conn.Close()
						return
					}
					w.WriteHeader(503)
				default:
					fmt.Fprint(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "gateway", []string{"127.0.0.1"})
			req := httptest.NewRequest("POST", "/v1/images/generations", strings.NewReader(`{"model":"image","prompt":"[TEST] image","quality":"high"}`))
			req.Header.Set("Authorization", "Bearer ses_fixture")
			req.Header.Set("X-Api-Key", "nn_fixture")
			out := httptest.NewRecorder()
			g.ServeHTTP(out, req)
			mu.Lock()
			defer mu.Unlock()
			if mode == "checkpoint-unavailable" {
				if out.Code != 502 || !strings.Contains(out.Body.String(), "settlement pending") || out.Header().Get("X-Newnoa-Meter") != "pending" {
					t.Fatalf("undurable usage presented as final: %d %s", out.Code, out.Body.String())
				}
				return
			}
			if finalCalls != 3 {
				t.Fatalf("final attempts=%d", finalCalls)
			}
			usage, ok := durable["imageUsage"].(map[string]any)
			if !ok || usage["textInputTokens"] != float64(10) || usage["imageOutputTokens"] != float64(4) || durable["terminalUsage"] != true || durable["estimatedInputTokens"] != nil {
				t.Fatalf("not recoverable: %v", durable)
			}
			if mode == "failed-upstream" {
				if out.Code != 502 {
					t.Fatalf("failure=%d", out.Code)
				}
			} else if out.Code != 200 {
				t.Fatalf("response=%d %s", out.Code, out.Body.String())
			}
		})
	}
}
