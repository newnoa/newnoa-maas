package main

import "testing"

func TestDecisionEvaluationRequiresNativeProtocolAndStrictUsage(t *testing.T) {
	testProtocolEvaluation(t, "/v1/systemone", `{"model":"fixture","state":"A cat is an animal.","questions":{"probe":{"type":"noul"}}}`, []protocolEvaluationCase{
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":3,"completion_tokens":0}}`, 200, "succeeded", 3},
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":3,"completion_tokens":0,"total_tokens":3}}`, 200, "succeeded", 3},
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":3,"completion_tokens":0,"total_tokens":null}}`, 502, "unknown", 0},
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":3}}`, 502, "unknown", 0},
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":"3","completion_tokens":0}}`, 502, "unknown", 0},
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":3,"completion_tokens":null}}`, 502, "unknown", 0},
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":3.5,"completion_tokens":0}}`, 502, "unknown", 0},
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":65,"completion_tokens":0}}`, 502, "unknown", 0},
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":3,"completion_tokens":-1}}`, 502, "unknown", 0},
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":3,"completion_tokens":62}}`, 502, "unknown", 0},
		{`{"answers":{"probe":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":3,"completion_tokens":0,"total_tokens":4}}`, 502, "unknown", 0},
		{`{"answers":{"other":{"type":"noul","noul":0.9}},"usage":{"prompt_tokens":3,"completion_tokens":0}}`, 502, "unknown", 0},
		{`{"answers":{"probe":{"type":"noul","noul":2}},"usage":{"prompt_tokens":3,"completion_tokens":0}}`, 502, "unknown", 0},
	})
	testProtocolEvaluation(t, "/v1/systemone", `{"model":"fixture","state":"fruit","questions":{"probe":{"type":"choice","criteria":{"apple":"fruit","truck":"vehicle"}}}}`, []protocolEvaluationCase{
		{`{"answers":{"probe":{"type":"choice","choice":"apple","confidence":0.8,"probabilities":{"apple":0.9,"truck":0.1}}},"usage":{"prompt_tokens":3,"completion_tokens":0}}`, 200, "succeeded", 3},
		{`{"answers":{"probe":{"type":"choice","choice":"apple","confidence":0.8,"probabilities":{"apple":0.9,"truck":0.9}}},"usage":{"prompt_tokens":3,"completion_tokens":0}}`, 502, "unknown", 0},
	})
	testProtocolEvaluation(t, "/v1/systemone", `{"model":"fixture","state":"fruit","questions":{"probe":{"type":"score","criteria":["low","high"]}}}`, []protocolEvaluationCase{
		{`{"answers":{"probe":{"type":"score","score":0.25,"confidence":0.8,"probabilities":{"0":0.75,"1":0.25}}},"usage":{"prompt_tokens":3,"completion_tokens":0}}`, 200, "succeeded", 3},
		{`{"answers":{"probe":{"type":"score","score":0.75,"confidence":0.8,"probabilities":{"0":0.75,"1":0.25}}},"usage":{"prompt_tokens":3,"completion_tokens":0}}`, 502, "unknown", 0},
	})
}
