package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPersonalAdmissionRejectionRemainsRetryable(t *testing.T) {
	for _, scenario := range []struct {
		node    bool
		outcome string
		want    int
	}{
		{true, "not-started", 429}, {true, "", 502}, {false, "not-started", 502}, {true, "unknown", 502},
	} {
		t.Run(fmt.Sprintf("node=%t/outcome=%s", scenario.node, scenario.outcome), func(t *testing.T) {
			t.Setenv("NEWNOA_FRP_OWNER_ID", "owner-fixture")
			owner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Newnoa-Outcome", scenario.outcome)
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(429)
			}))
			defer owner.Close()
			t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", owner.URL)
			finalized := make(chan string, 1)
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					fmt.Fprint(w, `{"model":"fixture","modelType":"llm"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					body := map[string]any{"requestId": "test", "permit": "permit_test", "endpoint": owner.URL, "route": owner.URL}
					if scenario.node {
						body["nodeId"] = "node"
						body["ownerId"] = "owner-fixture"
					}
					json.NewEncoder(w).Encode(body)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					var body struct{ Status string }
					json.NewDecoder(r.Body).Decode(&body)
					finalized <- body.Status
					fmt.Fprint(w, `{}`)
				default:
					fmt.Fprint(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "test", []string{"127.0.0.1"})
			r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`))
			r.Header.Set("X-Api-Key", "nn_fixture")
			r.Header.Set("Authorization", "Bearer ses_client_login")
			r.Header.Set("Anthropic-Version", "2023-06-01")
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			if w.Code != scenario.want {
				t.Fatalf("status %d, want %d: %s", w.Code, scenario.want, w.Body.String())
			}
			if scenario.want == 429 && (w.Header().Get("Retry-After") != "1" || !strings.Contains(w.Body.String(), "rate_limit_error")) {
				t.Fatalf("retry metadata lost: %v %s", w.Header(), w.Body.String())
			}
			wantState := "failed"
			if scenario.outcome == "unknown" {
				wantState = "unknown"
			}
			if state := <-finalized; state != wantState {
				t.Fatalf("finalized %s, want %s", state, wantState)
			}
		})
	}
}

func TestControlRetryAfter(t *testing.T) {
	for _, status := range []int{401, 429, 503, 500} {
		for _, hint := range []string{"", "0", "12", "Wed, 21 Oct 2026 07:28:00 GMT", "-1", "+1", "1.5", "tomorrow", strings.Repeat("1", 129)} {
			for _, native := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/%s/messages=%t", status, hint, native), func(t *testing.T) {
					api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Retry-After", hint)
						w.WriteHeader(status)
						fmt.Fprint(w, `{"error":"private-control-detail"}`)
					}))
					defer api.Close()
					g := newGateway(api.URL, "internal", "test", nil)
					err := g.control(context.Background(), "/dispatches", map[string]string{}, nil)
					if err == nil {
						t.Fatal("expected rejection")
					}
					recorder := httptest.NewRecorder()
					var writer http.ResponseWriter = recorder
					if native {
						writer = &messagesWriter{writer}
					}
					controlFailure(writer, err)
					wantStatus := status
					if status >= 500 {
						wantStatus = 503
					}
					wantHint := ""
					if status != 401 && (hint == "0" || hint == "12" || hint == "Wed, 21 Oct 2026 07:28:00 GMT") {
						wantHint = hint
					}
					if recorder.Code != wantStatus || recorder.Header().Get("Retry-After") != wantHint {
						t.Fatalf("status=%d retry=%q, want %d %q", recorder.Code, recorder.Header().Get("Retry-After"), wantStatus, wantHint)
					}
					body := recorder.Body.String()
					if strings.Contains(body, "private-control-detail") {
						t.Fatal("internal error leaked")
					}
					if native && status == 429 && !strings.Contains(body, "rate_limit_error") {
						t.Fatalf("lost native rate limit: %s", body)
					}
				})
			}
		}
	}
}
