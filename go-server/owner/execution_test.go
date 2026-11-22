package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOwnerExecutionFirstByteAndSpoof(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(30 * time.Millisecond):
		}
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	for _, execution := range []bool{false, true} {
		t.Run(fmt.Sprint(execution), func(t *testing.T) {
			client := &http.Client{Transport: ownerImageTimeout(&http.Transport{ResponseHeaderTimeout: 5 * time.Millisecond})}
			defer client.CloseIdleConnections()
			req, _ := http.NewRequest("POST", server.URL+"/v1/chat/completions", nil)
			req.Header.Set("X-Newnoa-Execution-Deadline", "2099-01-01T00:00:00Z")
			if execution {
				req = req.WithContext(context.WithValue(req.Context(), ownerExecutionContextKey{}, true))
			}
			res, err := client.Do(req)
			if res != nil {
				res.Body.Close()
			}
			if (err == nil) != execution {
				t.Fatalf("execution %v error %v", execution, err)
			}
		})
	}
}

func TestOwnerOnlyServingSelectsExecutionWait(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"port":21001}`) }))
	defer api.Close()
	o := newOwner("owner-a", api.URL, "internal-fixture")
	for _, prefix := range []string{"", "probe/", "publication-probe/", "evaluation/"} {
		t.Run(prefix, func(t *testing.T) {
			o.upstream.Transport = embeddingTransport(func(r *http.Request) (*http.Response, error) {
				execution := r.Context().Value(ownerExecutionContextKey{}) == true
				if execution != (prefix == "") {
					t.Errorf("wrong execution timeout selection for %q", prefix)
				}
				if execution {
					deadline, ok := r.Context().Deadline()
					if !ok || time.Until(deadline) > 31*time.Minute || time.Until(deadline) < 30*time.Minute {
						t.Error("unbounded owner bridge")
					}
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			})
			req := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/3/"+prefix+"v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hi"}]}`))
			req.Header.Set("Authorization", "Bearer internal-fixture")
			req.Header.Set("X-Newnoa-Execution-Deadline", "2099-01-01T00:00:00Z")
			w := httptest.NewRecorder()
			o.ServeHTTP(w, req)
			if w.Code != 200 {
				t.Fatalf("code %d body %s", w.Code, w.Body.String())
			}
		})
	}
}
