package main

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSVGQuestionsAndProxyOff(t *testing.T) {
	if len(svgQuestions) != 10 || svgQuestions[2].Name != "小熊清扫落叶" || svgQuestions[6].Name != "小兔栽种小树苗" {
		t.Fatal("fixed questions changed")
	}
	c := newCore(t.TempDir(), "https://example.test")
	handler, err := c.modelTestHandler()
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{modelTestBody(), asrTestBody([]byte("RIFFfixture")), decisionTestBody()} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, modelTestRequest(body))
		if response.Code != 409 {
			t.Fatalf("proxy off admitted model test: %d %s", response.Code, response.Body.String())
		}
	}
	if _, err = c.handle("svg.tests.create", json.RawMessage(`{"questionId":7,"keyId":"`+testModelID+`","model":"`+testModelID+`","submissionId":"`+testModelID+`","session":"session-test-only"}`)); err == nil {
		t.Fatal("proxy off SVG admitted")
	}
}

func svgFixture(t *testing.T, hold <-chan struct{}) (*core, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/me":
			if r.Header.Get("Authorization") == "Bearer other-session-only" {
				io.WriteString(w, `{"id":"00000000-0000-0000-0000-000000000002"}`)
				return
			}
			io.WriteString(w, `{"id":"`+testModelID+`"}`)
		case "/api/v1/me/model-tests/keys":
			io.WriteString(w, `[{"id":"`+testModelID+`","models":[{"id":"`+testModelID+`","name":"[TEST]SVG","modelType":"llm","online":true}]}]`)
		case "/api/v1/me/model-tests/credential":
			io.WriteString(w, `{"secret":"nn_test_01234567890123456789012345678901","expiresAt":"`+time.Now().Add(time.Minute).Format(time.RFC3339)+`","modelType":"llm","supportsReasoningNone":true}`)
		case "/api/v1/client/history/identity":
			io.WriteString(w, `{"accountId":"`+testModelID+`","keyId":"`+testModelID+`"}`)
		case "/api/v1/client/history/policy":
			io.WriteString(w, `{"mode":"off","id":"`+testModelID+`"}`)
		case "/api/v1/client/targets/" + testModelID:
			io.WriteString(w, `{"available":false,"private":false}`)
		case "/api/v1/client/requests/" + testModelID + "/consumption":
			if auth := r.Header.Get("Authorization"); auth != "Bearer session-test-only" && auth != "Bearer nn_test_01234567890123456789012345678901" {
				t.Error("consumption lookup lost caller identity")
			}
			io.WriteString(w, `{"requestId":"`+testModelID+`","pointsStatus":"known","pointsConsumed":"0.000000"}`)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(control.Close)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body struct {
			ReasoningEffort string `json:"reasoning_effort"`
			Messages        []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil {
			t.Error("invalid model body")
		}
		expectedAgent := "NEWNOA Model Test"
		if len(body.Messages) > 0 && strings.HasPrefix(body.Messages[0].Content, "用内联SVG") {
			expectedAgent = "NEWNOA SVG Test"
			if body.ReasoningEffort != "none" {
				t.Error("SVG default did not request direct drawing")
			}
		}
		if r.Header.Get("X-Newnoa-Agent") != expectedAgent {
			t.Error("SVG and dialogue history identities mixed", r.Header.Get("X-Newnoa-Agent"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Newnoa-Request", testModelID)
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"<svg>\"}}]}\n\n")
		w.(http.Flusher).Flush()
		if hold != nil {
			select {
			case <-hold:
			case <-r.Context().Done():
				return
			}
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"<circle r='5'/></svg>\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":120,\"completion_tokens\":80}}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(gateway.Close)
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	c.control = control.URL
	c.server = &http.Server{}
	c.setProxySession("session-test-only")
	t.Cleanup(c.shutdown)
	return c, calls
}
func svgCreate(t *testing.T, c *core, submission string) map[string]any {
	t.Helper()
	raw := json.RawMessage(`{"questionId":7,"keyId":"` + testModelID + `","model":"` + testModelID + `","submissionId":"` + submission + `","session":"session-test-only"}`)
	value, err := c.handle("svg.tests.create", raw)
	if err != nil {
		t.Fatal(err)
	}
	return value.(map[string]any)
}
func svgGet(t *testing.T, c *core, id string) map[string]any {
	t.Helper()
	value, err := c.handle("svg.tests.get", json.RawMessage(`{"id":"`+id+`","session":"session-test-only"}`))
	if err != nil {
		t.Fatal(err)
	}
	return value.(map[string]any)
}
func waitSVG(t *testing.T, check func() bool) {
	t.Helper()
	end := time.Now().Add(5 * time.Second)
	for time.Now().Before(end) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("SVG task did not reach expected state")
}
func TestSVGLocalJobPersistsAfterReturningAndNeverReplays(t *testing.T) {
	release := make(chan struct{})
	c, calls := svgFixture(t, release)
	submission := uuid.NewString()
	job := svgCreate(t, c, submission)
	id := job["id"].(string)
	waitSVG(t, func() bool { return calls.Load() == 1 })
	if svgGet(t, c, id)["status"] != "running" {
		t.Fatal("must keep loading while in flight")
	}
	if svgCreate(t, c, submission)["id"] != id {
		t.Fatal("submission replayed")
	}
	close(release)
	waitSVG(t, func() bool { return svgGet(t, c, id)["status"] == "succeeded" })
	got := svgGet(t, c, id)
	if got["content"] != "<svg><circle r='5'/></svg>" || got["points"] != "0.000000" {
		t.Fatal(got)
	}
	if calls.Load() != 1 {
		t.Fatal("inference replayed")
	}
	c.svgMu.Lock()
	c.svgStores = nil
	c.svgMu.Unlock()
	if svgGet(t, c, id)["content"] != got["content"] {
		t.Fatal("persisted result lost")
	}
	var persisted bytes.Buffer
	filepath.WalkDir(filepath.Join(c.home, ".newnoa", "svg-tests"), func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			raw, _ := os.ReadFile(path)
			persisted.Write(raw)
			info, _ := os.Stat(path)
			if info.Mode().Perm() != 0600 {
				t.Error("private permissions", info.Mode())
			}
		}
		return err
	})
	if strings.Contains(persisted.String(), "session-test-only") || strings.Contains(persisted.String(), "nn_test_") {
		t.Fatal("credential persisted")
	}
	if _, err := c.handle("svg.tests.get", json.RawMessage(`{"id":"`+id+`","session":"other-session-only"}`)); err == nil {
		t.Fatal("another account read saved SVG result")
	}
}
func TestSVGProxyStopCancelsAndRestartDoesNotReplay(t *testing.T) {
	release := make(chan struct{})
	c, calls := svgFixture(t, release)
	job := svgCreate(t, c, uuid.NewString())
	id := job["id"].(string)
	waitSVG(t, func() bool { return calls.Load() == 1 })
	if _, err := c.handle("proxy.stop", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if svgGet(t, c, id)["status"] != "cancelled" {
		t.Fatal("proxy stop left running task")
	}
	close(release)
	c.svgMu.Lock()
	c.svgStores = nil
	c.svgMu.Unlock()
	if svgGet(t, c, id)["status"] != "cancelled" || calls.Load() != 1 {
		t.Fatal("stopped task replayed")
	}
}
func TestSVGStreamRejectsIncompleteAndCapsOutput(t *testing.T) {
	for _, content := range []string{"<svg>", "<svg>" + strings.Repeat("a", svgContentLimit) + "</svg>"} {
		raw, _ := json.Marshal(map[string]any{"choices": []map[string]any{{"delta": map[string]string{"content": content}, "finish_reason": "stop"}}})
		w := &svgResponse{header: http.Header{"Content-Type": []string{"text/event-stream"}}, status: 200}
		w.Write([]byte("data: " + string(raw) + "\n\ndata: [DONE]\n\n"))
		if _, err := parseSVGResponse(w); err == nil {
			t.Fatal("unclosed or oversized SVG accepted")
		}
	}
	for _, body := range []string{`data: {"choices":[{"delta":{"content":"<svg></svg>"}}]}` + "\n\ndata: [DONE]\n\n", `data: {"choices":[{"delta":{"content":"<svg></svg>"},"finish_reason":"stop"}]}` + "\n\n", `data: {"choices":[{"delta":{"content":"reasoning only"},"finish_reason":"stop"}]}` + "\n\ndata: [DONE]\n\n"} {
		w := &svgResponse{header: http.Header{"Content-Type": []string{"text/event-stream"}}, status: 200}
		w.Write([]byte(body))
		if _, err := parseSVGResponse(w); err == nil {
			t.Fatal("incomplete/nonSVG accepted")
		}
	}
}

func TestSVGLengthFinishPreservesPartialArtifactButRejectsSuccess(t *testing.T) {
	content := "<!doctype html><html><body><svg><circle r='5'/></svg><svg><path"
	raw, _ := json.Marshal(map[string]any{"choices": []map[string]any{{"delta": map[string]string{"content": content}, "finish_reason": "length"}}, "usage": map[string]int{"prompt_tokens": 120, "completion_tokens": 32768}})
	w := &svgResponse{header: http.Header{"Content-Type": []string{"text/event-stream"}}, status: 200}
	w.Write([]byte("data: " + string(raw) + "\n\ndata: [DONE]\n\n"))
	got, err := parseSVGResponse(w)
	if err == nil {
		t.Fatal("truncated output with an earlier complete SVG was accepted as successful")
	}
	if got.Content != content || got.FinishReason != "length" || got.OutputTokens == nil || *got.OutputTokens != 32768 {
		t.Fatal("partial output evidence was lost", got)
	}
}

func TestSVGCreateReacquiresStoreAfterProxyRestartDuringCatalogue(t *testing.T) {
	inferenceRelease := make(chan struct{})
	defer close(inferenceRelease)
	c, calls := svgFixture(t, inferenceRelease)
	upstream, err := url.Parse(c.control)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	catalogueStarted, catalogueRelease := make(chan struct{}), make(chan struct{})
	var catalogues atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/me/model-tests/keys" && catalogues.Add(1) == 1 {
			close(catalogueStarted)
			<-catalogueRelease
		}
		proxy.ServeHTTP(w, r)
	}))
	defer control.Close()
	c.control = control.URL
	submission := uuid.NewString()
	raw := json.RawMessage(`{"questionId":7,"keyId":"` + testModelID + `","model":"` + testModelID + `","submissionId":"` + submission + `","session":"session-test-only"}`)
	type creation struct {
		value any
		err   error
	}
	created := make(chan creation, 1)
	go func() {
		value, err := c.handle("svg.tests.create", raw)
		created <- creation{value, err}
	}()
	select {
	case <-catalogueStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("catalogue was not reached")
	}
	if _, err := c.handle("proxy.stop", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	restart, _ := json.Marshal(map[string]int{"port": availableProxyPort(t)})
	if _, err := c.handle("proxy.start", restart); err != nil {
		t.Fatal(err)
	}
	close(catalogueRelease)
	result := <-created
	if result.err != nil {
		t.Fatal(result.err)
	}
	id := result.value.(map[string]any)["id"].(string)
	waitSVG(t, func() bool { return calls.Load() == 1 })
	if got := svgGet(t, c, id); got["status"] != "running" {
		t.Fatal("new task was detached from the current store", got)
	}
	if duplicate := svgCreate(t, c, submission); duplicate["id"] != id || calls.Load() != 1 {
		t.Fatal("restarted store lost submission identity", duplicate)
	}
	listed, err := c.handle("svg.tests.list", json.RawMessage(`{"session":"session-test-only"}`))
	if err != nil || listed.(map[string]any)["total"] != 1 {
		t.Fatal("restarted store lost the active task", listed, err)
	}
	if rows := listed.(map[string]any)["items"].([]map[string]any); rows[0]["keyId"] != testModelID {
		t.Fatal("restored drawing missing its original key ID")
	}
}

func TestSVGActiveLimitAndSessionChangeCancel(t *testing.T) {
	release := make(chan struct{})
	c, _ := svgFixture(t, release)
	defer close(release)
	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, svgCreate(t, c, uuid.NewString())["id"].(string))
	}
	raw := json.RawMessage(`{"questionId":7,"keyId":"` + testModelID + `","model":"` + testModelID + `","submissionId":"` + uuid.NewString() + `","session":"session-test-only"}`)
	if _, err := c.handle("svg.tests.create", raw); err == nil {
		t.Fatal("active task limit missing")
	}
	c.setProxySession("other-session-only")
	c.svgMu.Lock()
	store, err := c.svgStore(testModelID)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if store.jobs[id].Status != "cancelled" {
			t.Error("old account task remains running")
		}
	}
	c.svgMu.Unlock()
}
func TestSVGResponseFramesDoNotExpandIncomingLimit(t *testing.T) {
	value := bytes.Repeat([]byte{'x'}, maxFrame+1)
	var encoded bytes.Buffer
	if writeFrame(&encoded, value) == nil {
		t.Fatal("ordinary frame limit weakened")
	}
	if err := writeFrameLimit(&encoded, value, 8*maxFrame); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrame(&encoded); err == nil {
		t.Fatal("incoming frame limit weakened")
	}
}

func TestProxyStopCancelsModelDialogue(t *testing.T) {
	release := make(chan struct{})
	c, calls := svgFixture(t, release)
	defer close(release)
	handler, err := c.modelTestHandler()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(httptest.NewRecorder(), modelTestRequest(modelTestBody()))
	}()
	waitSVG(t, func() bool { return calls.Load() == 1 })
	if _, err = c.handle("proxy.stop", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("proxy stop left model dialogue active")
	}
	if calls.Load() != 1 {
		t.Fatal("dialogue replayed")
	}
}
