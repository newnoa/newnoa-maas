package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddingEvaluationRequiresFullProtocolAndUsageEvidence(t *testing.T) {
	testProtocolEvaluation(t, "/v1/embeddings", `{"model":"fixture","input":"Hello","encoding_format":"float"}`, []protocolEvaluationCase{
		{`{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":3,"total_tokens":3}}`, 200, "succeeded", 3},
		{`{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":3}}`, 502, "unknown", 0},
		{`{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":3,"total_tokens":4}}`, 502, "unknown", 0},
		{`{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":"3","total_tokens":"3"}}`, 502, "unknown", 0},
		{`{"object":"list","data":[{"object":"embedding","index":2,"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":3,"total_tokens":3}}`, 502, "unknown", 0},
	})
}

type protocolEvaluationCase struct {
	result string
	code   int
	state  string
	tokens float64
}

func TestRerankingEvaluationRequiresFullProtocolAndUsageEvidence(t *testing.T) {
	testProtocolEvaluation(t, "/v1/rerank", `{"model":"fixture","query":"cat","documents":["cat","dog"],"top_n":2}`, []protocolEvaluationCase{
		{`{"results":[{"index":0,"relevance_score":0.9},{"index":1,"relevance_score":0.1}],"usage":{"prompt_tokens":3,"total_tokens":3}}`, 200, "succeeded", 3},
		{`{"results":[{"index":0,"relevance_score":0.9},{"index":1,"relevance_score":0.1}],"usage":{"prompt_tokens":3}}`, 502, "unknown", 0},
		{`{"results":[{"index":0,"relevance_score":0.9},{"index":1,"relevance_score":0.1}],"usage":{"prompt_tokens":3,"total_tokens":4}}`, 502, "unknown", 0},
		{`{"results":[{"index":0,"relevance_score":0.9},{"index":1,"relevance_score":0.1}],"usage":{"prompt_tokens":"3","total_tokens":"3"}}`, 502, "unknown", 0},
		{`{"results":[{"index":0,"relevance_score":0.9},{"index":1,"relevance_score":0.1}],"usage":{"prompt_tokens":3.5,"total_tokens":3.5}}`, 502, "unknown", 0},
		{`{"results":[{"index":0,"relevance_score":0.9},{"index":1,"relevance_score":0.1}],"usage":{"prompt_tokens":null,"total_tokens":null}}`, 502, "unknown", 0},
		{`{"results":[{"index":0,"relevance_score":0.9},{"index":0,"relevance_score":0.1}],"usage":{"prompt_tokens":3,"total_tokens":3}}`, 502, "unknown", 0},
		{`{"results":[{"index":2,"relevance_score":0.9},{"index":1,"relevance_score":0.1}],"usage":{"prompt_tokens":3,"total_tokens":3}}`, 502, "unknown", 0},
		{`{"results":[{"index":0,"relevance_score":0.1},{"index":1,"relevance_score":0.9}],"usage":{"prompt_tokens":3,"total_tokens":3}}`, 502, "unknown", 0},
	})
}

func TestEvaluationAcceptsVersionedModelBaseURL(t *testing.T) {
	testProtocolEvaluation(t, "/v1/embeddings", `{"model":"fixture","input":"Hello","encoding_format":"float"}`, []protocolEvaluationCase{{`{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":3,"total_tokens":3}}`, 200, "succeeded", 3}}, "/v1")
}

func testProtocolEvaluation(t *testing.T, path, fixed string, cases []protocolEvaluationCase, basePath ...string) {
	for _, tc := range cases {
		t.Run(tc.result, func(t *testing.T) {
			t.Setenv("NEWNOA_EVALUATOR_TOKEN", strings.Repeat("e", 32))
			calls, settles := 0, 0
			sum := sha256.Sum256([]byte("POST " + path + "\n" + fixed))
			digest := hex.EncodeToString(sum[:])
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				raw, _ := io.ReadAll(r.Body)
				if r.URL.Path != path || string(raw) != fixed {
					t.Error("fixed template changed")
				}
				io.WriteString(w, tc.result)
			}))
			defer upstream.Close()
			endpoint := upstream.URL
			if len(basePath) > 0 {
				endpoint += basePath[0]
			}
			control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/dispatch"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": evalRequest, "attemptId": evalAttempt, "purpose": "evaluation", "permit": "eval-fixture", "endpoint": endpoint, "body": fixed, "bodyDigest": digest, "reserveTokens": 64, "inferencePath": path})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					settles++
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					if body["status"] != tc.state || body["inputTokens"] != tc.tokens || body["outputTokens"] != float64(0) {
						t.Error("wrong resource evidence", body)
					}
					io.WriteString(w, `{"resultDigest":"stored"}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer control.Close()
			identity, _ := json.Marshal(map[string]any{"workerId": "fixture", "fencingToken": 1, "requestId": evalRequest, "attemptId": evalAttempt})
			req := httptest.NewRequest("POST", "/internal/evaluations/"+evalJob+"/greeting", bytes.NewReader(identity))
			req.Header.Set("Authorization", "Bearer "+strings.Repeat("e", 32))
			res := httptest.NewRecorder()
			newGateway(control.URL, strings.Repeat("i", 32), "fixture", []string{"127.0.0.1"}).ServeHTTP(res, req)
			if res.Code != tc.code || calls != 1 || settles != 1 {
				t.Fatalf("status=%d calls=%d settles=%d body=%s", res.Code, calls, settles, res.Body)
			}
		})
	}
}
