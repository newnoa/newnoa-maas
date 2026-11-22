package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestResponsesInputTypeBeforeAuthorization(t *testing.T) {
	var calls atomic.Int32
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/inference-protection/events") {
			w.Write([]byte(`{}`))
			return
		}
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
	for _, tc := range []struct {
		input string
		valid bool
	}{
		{`null`, false}, {`42`, false}, {`true`, false}, {`{}`, false},
		{`"hello"`, true}, {`""`, true}, {`[]`, true},
		{`[{"role":"user","content":"hello"}]`, true},
	} {
		t.Run(tc.input, func(t *testing.T) {
			calls.Store(0)
			req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"fixture","max_output_tokens":16,"input":`+tc.input+`}`))
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_client_login")
			res := httptest.NewRecorder()
			g.ServeHTTP(res, req)
			if tc.valid {
				if calls.Load() != 1 || res.Code != http.StatusUnauthorized {
					t.Fatalf("valid input did not reach authorization: calls=%d status=%d", calls.Load(), res.Code)
				}
			} else if calls.Load() != 0 || res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), "input") {
				t.Fatalf("invalid input reached control plane or wrong error: calls=%d status=%d body=%s", calls.Load(), res.Code, res.Body.String())
			}
		})
	}
}

func TestOutputLimitBeforeAuthorization(t *testing.T) {
	var calls atomic.Int32
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/inference-protection/events") {
			w.Write([]byte(`{}`))
			return
		}
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages"} {
		fields := []string{"max_tokens", "max_completion_tokens"}
		if path == "/v1/responses" {
			fields = []string{"max_output_tokens"}
		} else if path == "/v1/messages" {
			fields = []string{"max_tokens"}
		}
		for _, field := range fields {
			for _, value := range []string{"", "null", "0", "-1", "1.5", "2000001", "1e100", `"16"`, "true", "[]", "{}", "1", "16", "2000000"} {
				t.Run(path+"/"+field+"/"+value, func(t *testing.T) {
					calls.Store(0)
					extra := ""
					if value != "" {
						extra = `,"` + field + `":` + value
					}
					req := httptest.NewRequest("POST", path, strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"test"}],"input":"test"`+extra+`}`))
					req.Header.Set("X-Api-Key", "nn_fixture")
					req.Header.Set("Authorization", "Bearer ses_client_login")
					res := httptest.NewRecorder()
					g.ServeHTTP(res, req)
					valid := value == "1" || value == "16" || value == "2000000" || (path != "/v1/messages" && (value == "" || value == "null"))
					if valid {
						if res.Code != 401 || calls.Load() != 1 {
							t.Fatalf("valid limit blocked: status=%d calls=%d", res.Code, calls.Load())
						}
					} else if res.Code != 400 || calls.Load() != 0 {
						t.Fatalf("invalid limit reached authorization: status=%d calls=%d", res.Code, calls.Load())
					}
				})
			}
		}
	}
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{}],"max_tokens":16,"max_completion_tokens":32}`))
	req.Header.Set("X-Api-Key", "nn_fixture")
	req.Header.Set("Authorization", "Bearer ses_client_login")
	res := httptest.NewRecorder()
	calls.Store(0)
	g.ServeHTTP(res, req)
	if res.Code != 400 || calls.Load() != 0 {
		t.Fatalf("conflicting limits accepted: status=%d calls=%d", res.Code, calls.Load())
	}
}
