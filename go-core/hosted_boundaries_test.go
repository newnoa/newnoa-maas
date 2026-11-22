package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestHostedAdmissionAndExecutionFailuresNeverReplay(t *testing.T) {
	for _, scenario := range []string{"wrong-account", "wrong-key", "wrong-token", "short-token", "illegal-token", "long-token", "bad-revision", "duplicate-admission", "admission-unavailable", "private-endpoint", "redirect", "upstream-disconnect", "upstream-rejected", "bad-stream-options", "bad-beta", "bad-account"} {
		t.Run(scenario, func(t *testing.T) {
			var executions atomic.Int32
			admissions, finishes, fallbacks := 0, 0, 0
			var observed usageReport
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				executions.Add(1)
				switch scenario {
				case "redirect":
					http.Redirect(w, r, "http://127.0.0.1/secret", 302)
				case "upstream-disconnect":
					conn, _, _ := w.(http.Hijacker).Hijack()
					conn.Close()
				case "upstream-rejected":
					http.Error(w, "rejected", 429)
				default:
					w.Header().Set("Content-Type", "application/json")
					io.WriteString(w, `{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
				}
			}))
			defer upstream.Close()
			endpoint := "http://upstream.test/custom/v1"
			if scenario == "private-endpoint" {
				endpoint = "http://127.0.0.1:9999"
			}
			d := hostedDispatch{RequestID: hostedFixtureID, AccountID: hostedFixtureID, KeyID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual", ModelType: "llm", Endpoint: endpoint, APIKey: "secret", Revision: 1}
			switch scenario {
			case "wrong-account":
				d.AccountID = "00000000-0000-0000-0000-000000000001"
			case "wrong-key":
				d.KeyID = "bad"
			case "wrong-token":
				d.ReportToken = "ses_bad"
			case "short-token":
				d.ReportToken = "hreport_"
			case "illegal-token":
				d.ReportToken = "hreport_" + strings.Repeat("!", 43)
			case "long-token":
				d.ReportToken = "hreport_" + strings.Repeat("a", 91)
			case "bad-revision":
				d.Revision = 0
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hostedMeterFixtureAck(t, w, r) {
					return
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/models"):
					io.WriteString(w, `[{"id":"`+hostedFixtureID+`"}]`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					admissions++
					if scenario == "admission-unavailable" {
						http.Error(w, "no", 503)
						return
					}
					json.NewEncoder(w).Encode(d)
				case strings.HasSuffix(r.URL.Path, "/finish"):
					finishes++
					decodeHostedFixtureClosure(t, r, &observed)
					io.WriteString(w, `{"finalized":true}`)
				case r.URL.Path == "/api/v1/me":
					if scenario == "bad-account" {
						http.Error(w, "expired", 401)
						return
					}
					io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control = api.URL
			c.onlineSupply = fixturePublicPolicy(upstream)
			defer c.shutdown()
			if scenario == "duplicate-admission" {
				h, _ := c.historyStore()
				if err := h.queueHosted(hostedFixtureID, d, usageReport{Status: "unknown"}); err != nil {
					t.Fatal(err)
				}
			}
			raw := `{"model":"` + hostedFixtureID + `","messages":[]}`
			path := "/v1/chat/completions"
			if scenario == "bad-stream-options" {
				raw = `{"model":"` + hostedFixtureID + `","stream":true,"stream_options":[]}`
			}
			if scenario == "bad-beta" {
				path = "/v1/messages"
			}
			r := httptest.NewRequest("POST", path, strings.NewReader(raw))
			r.Header.Set("Authorization", "Bearer ses_fixture")
			r.Header.Set("X-Api-Key", "nn_fixture")
			r.Header.Set("Content-Type", "application/json")
			if scenario == "bad-beta" {
				r.Header.Set("Anthropic-Beta", "invalid-beta")
			}
			w := httptest.NewRecorder()
			c.hostedInference(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { fallbacks++ })).ServeHTTP(w, r)
			if fallbacks != 0 || w.Code < 400 {
				t.Fatalf("unsafe fallback: status=%d fallbacks=%d", w.Code, fallbacks)
			}
			expectedExecutions := 0
			if strings.HasPrefix(scenario, "upstream-") || scenario == "redirect" {
				expectedExecutions = 1
			}
			if executions.Load() != int32(expectedExecutions) {
				t.Fatalf("executions=%d want=%d", executions.Load(), expectedExecutions)
			}
			if finishes > 0 {
				expected := "failed"
				if scenario == "upstream-disconnect" {
					expected = "unknown"
				}
				if observed.Status != expected {
					t.Fatalf("reported status=%s want=%s", observed.Status, expected)
				}
			}
			if scenario == "bad-beta" || scenario == "bad-account" {
				if admissions != 0 {
					t.Fatal("admitted invalid identity or protocol")
				}
			}
		})
	}
}

func TestHostedModelsMergeWithoutDuplicatesAndRetainLocalRoutes(t *testing.T) {
	const second = "00000000-0000-0000-0000-000000000001"
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		io.WriteString(w, `[{"id":"`+hostedFixtureID+`"},{"id":"`+second+`"}]`)
	}))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control = api.URL
	fallback := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"data":[{"id":"`+hostedFixtureID+`"},{"id":"local-model"}]}`)
	})
	r := httptest.NewRequest("GET", "/v1/models", nil)
	r.Header.Set("Authorization", "Bearer ses_fixture")
	r.Header.Set("X-Api-Key", "nn_fixture")
	w := httptest.NewRecorder()
	c.hostedInference(fallback).ServeHTTP(w, r)
	var result struct {
		Data []map[string]any `json:"data"`
	}
	json.Unmarshal(w.Body.Bytes(), &result)
	if w.Code != 200 || len(result.Data) != 3 {
		t.Fatalf("merged=%s", w.Body.String())
	}
}

func TestHostedControlRejectsMalformedAndUnsafeResponses(t *testing.T) {
	for _, test := range []struct {
		name, path, control, reply string
		body                       any
	}{
		{name: "invalid control", control: "file:///tmp/private"},
		{name: "invalid request path", path: "/bad\npath"},
		{name: "unsupported payload", body: make(chan int)},
		{name: "invalid response", reply: `{"invalid"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, test.reply) }))
			defer api.Close()
			control := api.URL
			if test.control != "" {
				control = test.control
			}
			var result map[string]any
			if err := hostedControl(context.Background(), control, test.path, "Bearer ses_fixture", "nn_fixture", test.body, &result); err == nil {
				t.Fatal("accepted malformed hosted exchange")
			}
		})
	}
}

func TestHostedNonOnlineRequestsRetainExistingHandler(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `[]`) }))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control = api.URL
	for _, test := range []struct{ method, path, body string }{{"POST", "/v1/chat/completions", `{"model":"named-model"}`}, {"POST", "/v1/chat/completions", `{"model":"` + hostedFixtureID + `"}`}, {"GET", "/other", ``}, {"GET", "/v1/models", ``}} {
		called := 0
		r := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
		r.Header.Set("Authorization", "Bearer ses_fixture")
		r.Header.Set("X-Api-Key", "nn_fixture")
		c.hostedInference(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ })).ServeHTTP(httptest.NewRecorder(), r)
		if called != 1 {
			t.Fatalf("existing route lost: %s %s", test.method, test.path)
		}
	}
	c.setProxySession("ses_current")
	r := httptest.NewRequest("GET", "/v1/models", nil)
	r.Header.Set("Authorization", "Bearer ses_other")
	r.Header.Set("X-Api-Key", "nn_fixture")
	w := httptest.NewRecorder()
	c.hostedInference(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("wrong account reached existing route") })).ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("cross account request accepted")
	}
}
