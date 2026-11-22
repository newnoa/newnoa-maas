package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGatewayFencesLocalNodeToItsOwnServer(t *testing.T) {
	const node = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	for _, stage := range []string{"authorized", "dispatch", "same-owner", "missing-owner", "epoch-changed", "node-changed"} {
		t.Run(stage, func(t *testing.T) {
			t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-local")
			if stage == "missing-owner" {
				t.Setenv("NEWNOA_FRP_OWNER_ID", "")
			}
			executions, reservations := 0, 0
			owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				executions++
				io.WriteString(w, `{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
			}))
			defer owner.Close()
			t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", owner.URL)
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					if body["gatewayId"] != "server-local" {
						t.Error("authorize must identify the executing Server")
					}
					binding := "owner-local"
					if stage == "authorized" {
						binding = "owner-other"
					}
					json.NewEncoder(w).Encode(map[string]any{"model": "fixture", "nodeId": node, "epoch": 2, "ownerId": binding})
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					reservations++
					binding := "owner-local"
					if stage == "dispatch" {
						binding = "owner-other"
					}
					nodeID, epoch := node, 2
					if stage == "epoch-changed" {
						epoch = 3
					}
					if stage == "node-changed" {
						nodeID = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
					}
					json.NewEncoder(w).Encode(map[string]any{"requestId": "request-fixture", "permit": "permit-fixture", "model": "fixture", "nodeId": nodeID, "epoch": epoch, "ownerId": binding, "route": owner.URL})
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal-fixture", "server-local", []string{"127.0.0.1"})
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"fixture"}],"max_tokens":16}`))
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("X-Api-Key", "nn_fixture")
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			if stage == "same-owner" {
				if w.Code != 200 || executions != 1 || reservations != 1 {
					t.Fatalf("status=%d executions=%d reservations=%d", w.Code, executions, reservations)
				}
			} else if stage == "epoch-changed" || stage == "node-changed" {
				if w.Code != 502 || executions != 0 || reservations != 1 {
					t.Fatalf("status=%d executions=%d reservations=%d", w.Code, executions, reservations)
				}
			} else if w.Code != 503 || executions != 0 || (stage != "dispatch" && reservations != 0) {
				t.Fatalf("status=%d executions=%d reservations=%d", w.Code, executions, reservations)
			}
		})
	}
}
