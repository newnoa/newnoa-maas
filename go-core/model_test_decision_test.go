package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func decisionTestBody() string {
	return `{"keyId":"` + testModelID + `","model":"` + testModelID + `","conversationId":"` + testModelID + `","requestId":"` + testModelID + `","decision":{"state":"反馈登录失败","questions":{"team":{"type":"choice","criteria":{"technical":"登录故障","unknown":"信息不足"}}},"think_mode":"never","return_details":true}}`
}

func TestModelTestDecisionBoundaries(t *testing.T) {
	raw := decisionTestBody()
	if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(raw)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		strings.Replace(raw, `"decision":{`, `"messages":null,"decision":{`, 1),
		strings.Replace(raw, `"decision":{`, `"audio":null,"decision":{`, 1),
		strings.Replace(raw, `"decision":{`, `"maxTokens":null,"decision":{`, 1),
		strings.Replace(raw, `"decision":{`, `"language":"en","decision":{`, 1),
		strings.Replace(raw, `"decision":{`, `"decision":{"model":"other",`, 1),
		strings.Replace(raw, `"decision":{`, `"decision":{"endpoint":"https://evil.invalid",`, 1),
		strings.Replace(raw, `"state":"反馈登录失败"`, `"state":null`, 1),
		strings.Replace(raw, `"questions":{`, `"questions":null,"unused":{`, 1),
		strings.Replace(raw, `"反馈登录失败"`, `"`+strings.Repeat("x", 1024*1024)+`"`, 1),
	} {
		if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(bad)); err == nil {
			t.Fatal("accepted invalid decision", len(bad))
		}
	}
}

func TestModelTestDecisionUsesBoundCredentialAndNativeJSON(t *testing.T) {
	const secret = "nn_test_01234567890123456789012345678901"
	kind := "decision"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/me/model-tests/credential":
			json.NewEncoder(w).Encode(map[string]any{"secret": secret, "modelType": kind, "expiresAt": time.Now().Add(time.Minute).Format(time.RFC3339)})
		case "/api/v1/client/history/identity":
			io.WriteString(w, `{"accountId":"00000000-0000-0000-0000-000000000001","keyId":"test-key"}`)
		case "/api/v1/client/history/policy":
			io.WriteString(w, `{"mode":"off","id":"00000000-0000-0000-0000-000000000099"}`)
		case "/api/v1/client/targets/" + testModelID:
			io.WriteString(w, `{"available":false,"private":false}`)
		default:
			t.Error("decision touched history/control route", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer control.Close()
	calls := 0
	status := 200
	const result = `{"model":"upstream","answers":{"team":{"type":"choice","choice":"technical","confidence":0.8,"probabilities":{"technical":0.9,"unknown":0.1}}},"usage":{"input_tokens":10,"output_tokens":0}}`
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if r.URL.Path != "/v1/systemone" || r.Header.Get("X-Api-Key") != secret || body["model"] != testModelID || body["think_mode"] != "never" || body["stream"] != nil {
			t.Error("incorrect native request", r.URL.Path, body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status == 200 {
			io.WriteString(w, result)
		} else {
			io.WriteString(w, `{"error":"private upstream credential"}`)
		}
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	c.server = &http.Server{}
	c.control = control.URL
	defer c.shutdown()
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.db.Exec("INSERT INTO history_local_settings(viewer,enabled) VALUES(?,0)", testModelID); err != nil {
		t.Fatal(err)
	}
	handler, err := c.modelTestHandler()
	if err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, modelTestRequest(decisionTestBody()))
	if res.Code != 200 || res.Body.String() != result || !strings.HasPrefix(res.Header().Get("Content-Type"), "application/json") || calls != 1 {
		t.Fatal(res.Code, res.Body.String(), calls)
	}
	if c.history == nil {
		t.Fatal("missing history policy check")
	}
	if rows, err := c.history.list("00000000-0000-0000-0000-000000000001", "", 10); err != nil || len(rows) != 0 {
		t.Fatal("off policy captured model test", rows, err)
	}
	kind = "llm"
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, modelTestRequest(decisionTestBody()))
	if res.Code != 422 || calls != 1 {
		t.Fatal("type mismatch executed")
	}
	kind = "decision"
	status = 422
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, modelTestRequest(decisionTestBody()))
	if res.Code != 422 || strings.Contains(res.Body.String(), "private") || calls != 2 {
		t.Fatal("upstream error leaked or replayed")
	}
}
