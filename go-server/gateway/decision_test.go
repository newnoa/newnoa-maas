package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const decisionFixture = `{"model":"deployment","state":{"text":"fixture"},"questions":{"q":{"type":"choice","criteria":{"yes":"yes","no":"no"}}},"allow_none":true,"think_mode":"never"}`

func TestDecisionExecution(t *testing.T) {
	for _, tc := range []struct {
		output string
		code   int
		state  string
	}{
		{`{"answers":{"q":{"type":"choice","choice":"yes","confidence":0.8,"probabilities":{"yes":0.9,"no":0.1}}},"usage":{"input_tokens":3,"output_tokens":0}}`, 200, "succeeded"},
		{`{"answers":{"q":{"type":"choice","choice":null,"none_probability":0.8,"confidence":0.2,"probabilities":{"yes":0.5,"no":0.5}}}}`, 200, "succeeded"},
		{`{"answers":{"q":{"type":"choice","choice":"other","confidence":0.8,"probabilities":{"yes":0.9,"no":0.1}}}}`, 502, "unknown"},
		{`{"answers":{}}`, 502, "unknown"},
	} {
		t.Run(tc.output, func(t *testing.T) {
			calls := 0
			state := ""
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				if r.URL.Path != "/v1/systemone" || body["model"] != "upstream-decision" || body["think_mode"] != "never" {
					t.Error("native request changed")
				}
				io.WriteString(w, tc.output)
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"upstream-decision"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					if body["inferencePath"] != "/v1/systemone" {
						t.Error("missing type binding")
					}
					json.NewEncoder(w).Encode(map[string]any{"requestId": "decision-test", "endpoint": upstream.URL, "permit": "fixture"})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					state, _ = body["status"].(string)
					if strings.Contains(tc.output, `"usage"`) {
						if body["inputTokens"] != float64(3) || body["outputTokens"] != float64(0) {
							t.Error("native token evidence lost")
						}
					} else if body["inputTokens"] != nil || body["outputTokens"] != nil {
						t.Error("invented token evidence")
					}
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			req := httptest.NewRequest("POST", "/v1/systemone", strings.NewReader(decisionFixture))
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_client_login")
			res := httptest.NewRecorder()
			newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"}).ServeHTTP(res, req)
			if res.Code != tc.code || state != tc.state || calls != 1 {
				t.Fatalf("status=%d state=%s calls=%d", res.Code, state, calls)
			}
			if tc.code == 200 && res.Body.String() != tc.output {
				t.Error("probabilities changed")
			}
		})
	}
}

func TestDecisionInputBoundaries(t *testing.T) {
	for _, raw := range []string{
		`{"model":"x","state":{},"questions":{}}`,
		`{"model":"x","questions":{"q":{"type":"noul"}}}`,
		`{"state":"x","questions":{"q":{"type":"box"}}}`,
		`{"state":"x","questions":{"q":{"type":"choice","criteria":[]}}}`,
		`{"state":"x","questions":{"q":{"type":"score","criteria":{}}}}`,
		`{"state":"x","questions":{"q":{"type":"noul","criteria":{"other":"x"}}}}`,
		`{"state":"x","questions":{"q":{"type":"noul"}},"allow_none":1}`,
		`{"state":"x","questions":{"q":{"type":"noul"}},"gate":1.1}`,
		`{"state":"x","questions":{"q":{"type":"noul"}},"think_budget":true}`,
		`{"state":"x","questions":{"q":{"type":"noul"}},"think_budget":1.5}`,
		`{"state":"x","questions":{"q":{"type":"noul"}},"think_mode":"other"}`,
		`{"state":"x","questions":{"q":{"type":"noul"}},"images":[]}`,
	} {
		var body map[string]any
		json.Unmarshal([]byte(raw), &body)
		if _, ok := decisionInput(body); ok {
			t.Fatal("invalid input accepted", raw)
		}
	}
}

func TestDecisionNoulAndScorePreserveNativeNumbers(t *testing.T) {
	var body map[string]any
	json.Unmarshal([]byte(`{"state":"fixture","questions":{"yes":{"type":"noul"},"rank":{"type":"score","criteria":["low","high"]}}}`), &body)
	questions, ok := decisionInput(body)
	if !ok {
		t.Fatal("valid questions rejected")
	}
	good := `{"answers":{"yes":{"type":"noul","noul":0.7},"rank":{"type":"score","score":0.25,"confidence":0.5,"probabilities":{"0":0.75,"1":0.25}}}}`
	if !validDecision([]byte(good), questions) {
		t.Fatal("valid native distribution rejected")
	}
	for _, bad := range []string{
		strings.Replace(good, `"noul":0.7`, `"noul":true`, 1),
		strings.Replace(good, `"noul":0.7`, `"noul":null`, 1),
		strings.Replace(good, `"noul":0.7`, `"noul":1.1`, 1),
		strings.Replace(good, `"score":0.25`, `"score":1`, 1),
		strings.Replace(good, `"0":0.75`, `"0":0.5`, 1),
		strings.Replace(good, `"1":0.25`, `"other":0.25`, 1),
		strings.Replace(good, `"confidence":0.5`, `"confidence":null`, 1),
		strings.Replace(good, `"type":"score"`, `"type":"choice"`, 1),
	} {
		if validDecision([]byte(bad), questions) {
			t.Fatal("invalid evidence accepted", bad)
		}
	}
}
