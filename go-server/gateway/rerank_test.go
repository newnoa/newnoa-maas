package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRerankValidation(t *testing.T) {
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{"query":"q","documents":["a","b"],"top_n":1}`, true},
		{`{"query":"q","documents":["a","b"]}`, true},
		{`{"query":" ","documents":["a"]}`, false},
		{`{"query":"q","documents":[]}`, false},
		{`{"query":"q","documents":[{}]}`, false},
		{`{"query":"q","documents":["a"],"top_n":2}`, false},
		{`{"query":"q","documents":["a"],"top_n":0.5}`, false},
		{`{"query":"q","documents":["a"],"return_documents":1}`, false},
	} {
		var body map[string]any
		json.Unmarshal([]byte(tc.body), &body)
		_, _, valid := rerankInput(body)
		if valid != tc.valid {
			t.Fatal(tc.body)
		}
	}
	good := `{"results":[{"index":1,"relevance_score":2.1},{"index":0,"relevance_score":-0.1}]}`
	if !validRerank([]byte(good), 2, 2) {
		t.Fatal("finite descending scores rejected")
	}
	for _, bad := range []string{strings.Replace(good, `"index":1`, `"index":0`, 1), strings.Replace(good, `"index":1`, `"index":2`, 1), strings.Replace(good, `"index":1,`, "", 1), strings.Replace(good, "2.1", "null", 1), strings.Replace(good, "2.1", "-1", 1), `{"results":[]}`} {
		if validRerank([]byte(bad), 2, 2) {
			t.Fatal("invalid result accepted", bad)
		}
	}
}

func TestRerankExecutionResultValidation(t *testing.T) {
	for _, tc := range []struct {
		name, usage  string
		valid, known bool
	}{
		{"success-without-usage", "", true, false},
		{"invalid-result", "", false, false},
		{"llama-cpp-prompt-total", `,"usage":{"prompt_tokens":150,"total_tokens":150}`, true, true},
		{"explicit-zero-output", `,"usage":{"prompt_tokens":150,"total_tokens":150,"completion_tokens":0}`, true, true},
		{"missing-total", `,"usage":{"prompt_tokens":150}`, true, false},
		{"inconsistent-total", `,"usage":{"prompt_tokens":150,"total_tokens":151}`, true, false},
		{"negative-tokens", `,"usage":{"prompt_tokens":-1,"total_tokens":-1}`, true, false},
		{"too-many-tokens", `,"usage":{"prompt_tokens":1000000001,"total_tokens":1000000001}`, true, false},
		{"null-total", `,"usage":{"prompt_tokens":150,"total_tokens":null}`, true, false},
		{"null-prompt", `,"usage":{"prompt_tokens":null,"total_tokens":150}`, true, false},
		{"fractional-tokens", `,"usage":{"prompt_tokens":1.5,"total_tokens":1.5}`, true, false},
		{"string-tokens", `,"usage":{"prompt_tokens":"150","total_tokens":"150"}`, true, false},
		{"nonzero-output", `,"usage":{"prompt_tokens":150,"total_tokens":150,"completion_tokens":1}`, true, false},
		{"search-units-are-not-tokens", `,"meta":{"billed_units":{"search_units":1}}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			state := ""
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				if r.URL.Path != "/v1/rerank" || body["model"] != "upstream-reranker" {
					t.Error("target changed")
				}
				if tc.valid {
					w.Write([]byte(`{"results":[{"index":1,"relevance_score":0.9}]` + tc.usage + `}`))
				} else {
					w.Write([]byte(`{"results":[{"index":3,"relevance_score":0.9}]}`))
				}
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					w.Write([]byte(`{"model":"upstream-reranker"}`))
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					if body["inferencePath"] != "/v1/rerank" {
						t.Error("type missing")
					}
					json.NewEncoder(w).Encode(map[string]any{"requestId": "rerank-test", "permit": "fixture", "endpoint": upstream.URL})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					state, _ = body["status"].(string)
					if tc.known {
						if body["inputTokens"] != float64(150) || body["outputTokens"] != float64(0) {
							t.Errorf("lost reported rerank usage: input=%v output=%v", body["inputTokens"], body["outputTokens"])
						}
					} else if body["inputTokens"] != nil || body["outputTokens"] != nil {
						t.Error("invented token usage")
					}
					w.Write([]byte(`{}`))
				default:
					w.Write([]byte(`{}`))
				}
			}))
			defer api.Close()
			gateway := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			req := httptest.NewRequest("POST", "/v1/rerank", strings.NewReader(`{"model":"deployment","query":"q","documents":["a","b"],"top_n":1}`))
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_client_login")
			res := httptest.NewRecorder()
			gateway.ServeHTTP(res, req)
			want, code := "unknown", 502
			if tc.valid {
				want, code = "succeeded", 200
			}
			if calls != 1 || state != want || res.Code != code {
				t.Fatalf("calls=%d state=%s code=%d", calls, state, res.Code)
			}
		})
	}
}
