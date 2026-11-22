package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestHostedAbortKeepsFrozenIdentityAfterCallerConfigurationChanges(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "public"}[public], func(t *testing.T) {
			calls := 0
			want := map[string]any{"attemptId": hostedFixtureID, "model": hostedFixtureID, "inferencePath": "/v1/chat/completions", "bodyDigest": hash([]byte("[TEST] original"))}
			if public {
				want["secret"] = "ses_original"
				want["session"] = "ses_original_public"
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var got map[string]any
				json.NewDecoder(r.Body).Decode(&got)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("abort followed changed tuple: %v", got)
				}
				if public {
					if r.Header.Get("Authorization") != "Bearer internal-original" || r.Header.Get("X-Api-Key") != "" || r.URL.Path != "/api/v1/internal/public/hosted/dispatch-attempts/abort" {
						t.Error("public abort followed changed authority")
					}
				} else if r.Header.Get("Authorization") != "Bearer ses_original" || r.Header.Get("X-Api-Key") != "nn_original" || r.URL.Path != "/api/v1/client/hosted/dispatch-attempts/abort" {
					t.Error("native abort followed changed authority")
				}
				io.WriteString(w, `{"accepted":true}`)
			}))
			defer api.Close()
			c := &core{control: api.URL, publicStateless: public, publicInternalToken: "internal-original"}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), publicCallerSessionContext{}, "ses_original_public"))
			defer cancel()
			params := map[string]any{}
			for k, v := range want {
				params[k] = v
			}
			caller := newHostedAdmissionCaller(c, ctx, "Bearer ses_original", "nn_original", params)
			params["bodyDigest"] = "changed"
			params["model"] = "changed"
			params["secret"] = "ses_replacement"
			params["session"] = "ses_replacement"
			c.control = "http://127.0.0.1:1"
			c.publicInternalToken = "internal-replacement"
			c.publicStateless = !public
			cancel()
			if err := caller.abort(ctx); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatal("cleanup replayed or skipped original API")
			}
		})
	}
}

func TestHostedAbortRequiresAcceptedAcknowledgment(t *testing.T) {
	for _, reply := range []string{`{}`, `{"accepted":false}`, `{"accepted":"true"}`, `{"accepted":true} {}`, `[TEST] invalid secret response`} {
		t.Run(reply, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, reply) }))
			defer api.Close()
			caller := newHostedAdmissionCaller(&core{control: api.URL}, context.Background(), "Bearer ses_original", "nn_original", map[string]any{})
			if err := caller.abort(context.Background()); err == nil || err.Error() != "hosted admission cleanup pending" {
				t.Fatal("invalid cleanup ack accepted or unsafe error returned")
			}
		})
	}
}
