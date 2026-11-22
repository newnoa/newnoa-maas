package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Exercise actual executeHosted, not a recovery helper in isolation: API may
// commit admission before its response is dropped, malformed or rejected locally.
func TestHostedAdmissionCompensatesLostResponseBeforeProvider(t *testing.T) {
	var attemptIDs sync.Map
	for _, public := range []bool{false, true} {
		for _, failure := range []string{"commit-drop", "short-body", "malformed", "invalid-identity", "cancel-after-commit", "abort-outage"} {
			t.Run(map[bool]string{false: "native", true: "public"}[public]+"/"+failure, func(t *testing.T) {
				var providers, admissions, aborts atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					providers.Add(1)
					t.Error("provider invoked after ambiguous admission")
				}))
				defer upstream.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx = context.WithValue(ctx, publicCallerSessionContext{}, "ses_original_public_session")
				ctx = context.WithValue(ctx, hostedAbortContextKey{}, "original-context")
				var admitted map[string]any
				var abortBody map[string]any
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.URL.Path == "/api/v1/me":
						io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
					case strings.HasSuffix(r.URL.Path, "/dispatch-attempts/abort"):
						aborts.Add(1)
						if r.Context().Err() != nil {
							t.Error("abort reused cancelled request context")
						}
						if public {
							if r.URL.Path != "/api/v1/internal/public/hosted/dispatch-attempts/abort" || r.Header.Get("Authorization") != "Bearer internal-original" || r.Header.Get("X-Api-Key") != "" {
								t.Error("public abort credential/path changed")
							}
						} else if r.URL.Path != "/api/v1/client/hosted/dispatch-attempts/abort" || r.Header.Get("Authorization") != "Bearer ses_original" || r.Header.Get("X-Api-Key") != "nn_original" {
							t.Error("native abort credential/path changed")
						}
						if err := json.NewDecoder(r.Body).Decode(&abortBody); err != nil {
							t.Error(err)
						}
						want := map[string]any{}
						for _, key := range []string{"attemptId", "model", "inferencePath", "bodyDigest"} {
							want[key] = admitted[key]
						}
						if public {
							want["secret"] = admitted["secret"]
							want["session"] = admitted["session"]
						}
						if !reflect.DeepEqual(want, abortBody) {
							t.Errorf("abort tuple differs/leaks metadata: got=%v want=%v", abortBody, want)
						}
						if failure == "abort-outage" {
							http.Error(w, "[TEST] cleanup-secret", 503)
							return
						}
						io.WriteString(w, `{"accepted":true}`)
					case strings.HasSuffix(r.URL.Path, "/dispatches"):
						admissions.Add(1)
						if err := json.NewDecoder(r.Body).Decode(&admitted); err != nil {
							t.Error(err)
						}
						attempt, err := uuid.Parse(historyText(admitted["attemptId"]))
						if err != nil || attempt.Version() != 4 || attempt.Variant() != uuid.RFC4122 {
							t.Error("single admission missing cryptographic UUIDv4 attempt")
						}
						if _, loaded := attemptIDs.LoadOrStore(attempt, struct{}{}); loaded {
							t.Error("new hosted admission reused an earlier attempt")
						}
						if admitted["model"] != hostedFixtureID || admitted["inferencePath"] != "/v1/messages" || admitted["bodyDigest"] != hash([]byte("POST /v1/messages\n"+messagesBinding("interleaved-thinking-2025-05-14")+`{"model":"`+hostedFixtureID+`","messages":[]}`)) {
							t.Error("admission lost original model/path/beta/body digest")
						}
						if public {
							if r.Header.Get("Authorization") != "Bearer internal-original" || admitted["secret"] != "ses_original" || admitted["session"] != "ses_original_public_session" {
								t.Error("public admission identity changed")
							}
						}
						switch failure {
						case "commit-drop", "abort-outage":
							conn, _, err := w.(http.Hijacker).Hijack()
							if err != nil {
								t.Error(err)
								return
							}
							conn.Close()
						case "malformed":
							io.WriteString(w, `{"requestId":"[TEST] provider-secret",`)
						case "short-body":
							w.Header().Set("Content-Length", "500")
							io.WriteString(w, `{"requestId":"[TEST] provider-secret"}`)
						case "cancel-after-commit":
							cancel()
							<-r.Context().Done()
						case "invalid-identity":
							json.NewEncoder(w).Encode(hostedDispatch{RequestID: hostedFixtureID, AccountID: "wrong", KeyID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual", Endpoint: upstream.URL, APIKey: "[TEST] provider-secret", Revision: 1})
						}
					default:
						t.Errorf("unexpected route after ambiguous admission: %s", r.URL.Path)
						w.WriteHeader(404)
					}
				}))
				defer api.Close()
				c := newCore(t.TempDir(), "http://127.0.0.1:1")
				c.control, c.publicStateless, c.publicInternalToken = api.URL, public, "internal-original"
				c.onlineSupply = fixturePublicPolicy(upstream)
				defer c.shutdown()
				raw := `{"model":"` + hostedFixtureID + `","messages":[]}`
				r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(raw)).WithContext(ctx)
				r.Header.Set("Authorization", "Bearer ses_original")
				r.Header.Set("X-Api-Key", "nn_original")
				r.Header.Set("Content-Type", "application/json")
				r.Header.Set("Anthropic-Beta", "interleaved-thinking-2025-05-14")
				r.Header.Set("Idempotency-Key", "[TEST] original-idem")
				w := httptest.NewRecorder()
				c.executeHosted(w, r, hostedFixtureID, []byte(raw))
				if providers.Load() != 0 || admissions.Load() != 1 || aborts.Load() != 1 || w.Code != 502 || !strings.Contains(w.Body.String(), "request was not replayed") || strings.Contains(w.Body.String(), "secret") || strings.Contains(w.Body.String(), "hreport_") {
					t.Fatalf("unsafe compensation calls provider=%d admission=%d abort=%d status=%d body=%q", providers.Load(), admissions.Load(), aborts.Load(), w.Code, w.Body.String())
				}
			})
		}
	}
}

type hostedAbortContextKey struct{}

func TestHostedAdmissionExplicitRejectNeverInvokesProviderOrRetries(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "public"}[public], func(t *testing.T) {
			var admission, abort atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/v1/me" {
					io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/abort") {
					abort.Add(1)
				} else {
					admission.Add(1)
				}
				http.Error(w, "[TEST] private rejection", 403)
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control, c.publicStateless, c.publicInternalToken = api.URL, public, "internal-original"
			defer c.shutdown()
			raw := `{"model":"` + hostedFixtureID + `"}`
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(raw))
			r.Header.Set("Authorization", "Bearer ses_original")
			w := httptest.NewRecorder()
			c.executeHosted(w, r, hostedFixtureID, []byte(raw))
			if admission.Load() != 1 || abort.Load() != 0 || w.Code != 502 || strings.Contains(w.Body.String(), "private rejection") {
				t.Fatalf("rejection retried/leaked: admission=%d abort=%d body=%s", admission.Load(), abort.Load(), w.Body.String())
			}
		})
	}
}

func TestHostedAbortDetachedContextHasEightSecondBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), hostedAbortContextKey{}, "original"))
	cancel()
	cleanup, stop := hostedAbortContext(ctx)
	defer stop()
	deadline, ok := cleanup.Deadline()
	if cleanup.Err() != nil || cleanup.Value(hostedAbortContextKey{}) != "original" || !ok || time.Until(deadline) > 8*time.Second || time.Until(deadline) < 7*time.Second {
		t.Fatal("abort lost original context or fixed 8-second bound")
	}
}
