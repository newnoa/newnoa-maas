package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPublicSVGTasksAreAccountScopedAndOutliveThePage(t *testing.T) {
	const one, two = "ses_website_user_one_credential", "ses_website_user_two_credential"
	accounts := map[string]string{one: "11111111-1111-4111-8111-111111111111", two: "22222222-2222-4222-8222-222222222222"}
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	api := privateControlTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		session := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		switch r.URL.Path {
		case "/api/v1/me":
			if accounts[session] == "" {
				w.WriteHeader(401)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"id": accounts[session]})
		case "/api/v1/me/model-tests/keys":
			json.NewEncoder(w).Encode([]any{map[string]any{"id": testModelID, "models": []any{map[string]any{"id": testModelID, "name": "[TEST] public SVG", "modelType": "llm", "online": true}}}})
		case "/api/v1/me/model-tests/credential":
			if accounts[session] == "" {
				w.WriteHeader(401)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"secret": "nn_test_" + session, "expiresAt": time.Now().Add(time.Minute).Format(time.RFC3339), "modelType": "llm", "supportsReasoningNone": true})
		case "/api/v1/internal/public/authorize":
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			if accounts[body["session"]] == "" || body["secret"] != "nn_test_"+body["session"] {
				t.Error("shared authorization lost original session and test key binding")
			}
			io.WriteString(w, `{"hosted":false}`)
		case "/api/v1/client/requests/" + testModelID + "/consumption":
			if session != two {
				t.Error("consumption lookup lost completed task account")
			}
			io.WriteString(w, `{"pointsStatus":"known","pointsConsumed":"0.123456"}`)
		case "/api/v1/client/targets/" + testModelID:
			io.WriteString(w, `{"available":false,"private":false}`)
		default:
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["reasoning_effort"] != "none" {
			t.Error("public SVG lost direct drawing option")
		}
		if r.Header.Get("X-Forwarded-For") != "8.8.8.8" {
			t.Errorf("trusted website caller IP lost: %q", r.Header.Get("X-Forwarded-For"))
		}
		if token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "); accounts[token] == "" || r.Header.Get("X-Api-Key") != "nn_test_"+token {
			t.Error("Go Server did not receive the caller session paired with its test key")
		}
		w.Header().Set("X-Newnoa-Request", testModelID)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"<svg>\"}}]}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"</svg>\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	c.control = api.URL
	c.proxySession = "ses_wrong_desktop_session"
	handler, err := c.publicProxyHandler(publicTestSecret, "internal-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.shutdown()
	rpc := func(token, method string, params map[string]any) (int, map[string]any) {
		raw, _ := json.Marshal(map[string]any{"method": method, "params": params})
		r := httptest.NewRequest("POST", "/internal/www/core", strings.NewReader(string(raw)))
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		r = r.WithContext(ctx)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Newnoa-Caller-IP", "8.8.8.8")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		var value map[string]any
		json.Unmarshal(w.Body.Bytes(), &value)
		return w.Code, value
	}
	if code, value := rpc(one, "proxy.status", map[string]any{}); code != 200 || value["running"] != true {
		t.Fatalf("public Core unavailable: %d %v", code, value)
	}
	for _, secret := range []string{"", "wrong"} {
		r := httptest.NewRequest("POST", "/internal/www/core", strings.NewReader(`{"method":"proxy.status","params":{}}`))
		r.Header.Set("Authorization", "Bearer "+one)
		r.Header.Set("X-Newnoa-Public-Proxy", secret)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 401 {
			t.Fatal("untrusted service called public Core")
		}
	}
	if code, _ := rpc("ses_expired_website_credential", "proxy.status", map[string]any{}); code != 401 {
		t.Fatal("invalid website session was accepted")
	}
	for _, method := range []string{"proxy.stop", "proxy.start", "history.list", "agent.apply"} {
		if code, _ := rpc(one, method, map[string]any{}); code == 200 {
			t.Fatalf("website escaped whitelist: %s", method)
		}
	}
	if code, _ := rpc(one, "svg.tests.questions", map[string]any{"session": two}); code == 200 {
		t.Fatal("caller injected another session")
	}
	jobs := map[string]string{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i, token := range []string{one, two} {
		wg.Add(1)
		go func(i int, token string) {
			defer wg.Done()
			code, job := rpc(token, "svg.tests.create", map[string]any{"questionId": 7, "keyId": testModelID, "model": testModelID, "thinking": false, "submissionId": []string{"33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444"}[i]})
			if code != 200 {
				t.Errorf("create %d %v", code, job)
				return
			}
			mu.Lock()
			jobs[token] = job["id"].(string)
			mu.Unlock()
		}(i, token)
	}
	wg.Wait()
	if len(jobs) != 2 {
		t.Fatal("two accounts could not create jobs")
	}
	if code, _ := rpc(two, "svg.tests.get", map[string]any{"id": jobs[one]}); code == 200 {
		t.Fatal("foreign account read first task")
	}
	// Each command's request context was cancelled by rpc after its response.
	for token, id := range jobs {
		if code, job := rpc(token, "svg.tests.get", map[string]any{"id": id}); code != 200 || job["status"] != "running" {
			t.Fatalf("page exit lost task: %d %v", code, job)
		}
	}
	if code, _ := rpc(one, "svg.tests.cancel", map[string]any{"id": jobs[one]}); code != 200 {
		t.Fatal("cancel failed")
	}
	unblock()
	deadline := time.Now().Add(3 * time.Second)
	for {
		code, job := rpc(two, "svg.tests.get", map[string]any{"id": jobs[two]})
		if code == 200 && job["status"] == "succeeded" {
			if job["points"] != "0.123456" {
				t.Fatalf("private API actual consumption missing: %v", job)
			}
			if job["content"] != "<svg></svg>" {
				t.Fatalf("wrong generated result: %v", job)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("detached task did not complete: %d %v", code, job)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, job := rpc(one, "svg.tests.get", map[string]any{"id": jobs[one]}); job["status"] != "cancelled" {
		t.Fatal("one account cancellation affected another account")
	}
	if c.proxySession != "ses_wrong_desktop_session" || c.history != nil {
		t.Fatal("website borrowed desktop identity/history")
	}
	filepath.Walk(c.home, func(file string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			raw, _ := os.ReadFile(file)
			if strings.Contains(string(raw), one) || strings.Contains(string(raw), two) {
				t.Error("session persisted in task file")
			}
		}
		return nil
	})
}

func TestPublicProxySessionPairIsRestrictedToTestCredentials(t *testing.T) {
	c := &core{control: "http://127.0.0.1:1", gateway: "http://127.0.0.1:1"}
	handler, err := c.publicProxyHandler(publicTestSecret, "internal-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.shutdown()
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture"}`))
	r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
	r.Header.Set("Authorization", "Bearer ordinary_api_key_credential")
	r.Header.Set("X-Newnoa-Caller-Session", "ses_website_fixture_credential")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("trusted test session header widened ordinary API key behavior")
	}
}

func TestPublicSVGSessionRevocationStopsDetachedWork(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/me" {
			t.Errorf("session check borrowed local history: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer api.Close()
	c := &core{control: api.URL}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go c.guardPublicSVGSession(ctx, "ses_revoked_website_credential", stop)
	select {
	case <-ctx.Done():
	case <-time.After(14 * time.Second):
		t.Fatal("revoked session continued detached work")
	}
}

func TestSharedPublicSVGShutdownCancelsDetachedTasks(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	shared := &core{publicSVGContext: ctx, publicSVGCancel: stop}
	c := &core{publicSVGCore: shared}
	c.close()
	if ctx.Err() == nil || c.publicSVGCore != nil {
		t.Fatal("shared SVG runtime outlived its proxy")
	}
}

func TestPublicSVGStoresEvictIdleAccountsWithoutRetainingResultContents(t *testing.T) {
	c := &core{home: t.TempDir(), control: "http://127.0.0.1:1", publicStateless: true}
	store, err := c.svgStore("one")
	if err != nil {
		t.Fatal(err)
	}
	job := &svgTest{ID: "11111111-1111-4111-8111-111111111111", QuestionID: 1, Status: "succeeded", Content: "<svg>" + strings.Repeat(" ", 1024) + "</svg>"}
	if err = store.save(job); err != nil {
		t.Fatal(err)
	}
	c.svgStores = nil
	store, err = c.svgStore("one")
	if err != nil {
		t.Fatal(err)
	}
	saved := store.jobs[job.ID]
	if saved.Content != "" || !saved.contentOnDisk {
		t.Fatal("completed SVG content retained in shared cache")
	}
	if row, err := store.detail(saved); err != nil || row["content"] != job.Content {
		t.Fatalf("evicted content not readable: %v %v", row, err)
	}
	active := &svgTest{ID: "22222222-2222-4222-8222-222222222222", Status: "running"}
	store.jobs[active.ID] = active
	if _, err = c.svgStore("two"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.svgStore("three"); err != nil {
		t.Fatal(err)
	}
	if len(c.svgStores) != 2 {
		t.Fatal("idle shared account caches retained")
	}
	if current, err := c.svgStore("one"); err != nil || current != store || current.jobs[active.ID] != active {
		t.Fatal("active task identity changed during cache eviction")
	}
	active.Status = "cancelled"
	store.workers = 1
	if current, err := c.svgStore("one"); err != nil || current != store {
		t.Fatal("cancelled task store evicted before worker exit")
	}
}

func TestPublicSVGSaveFailureKeepsRecoverableResultInMemory(t *testing.T) {
	c := &core{home: t.TempDir(), control: "http://127.0.0.1:1", publicStateless: true}
	store, err := c.svgStore("one")
	if err != nil {
		t.Fatal(err)
	}
	job := &svgTest{ID: "11111111-1111-4111-8111-111111111111", QuestionID: 1, Status: "running"}
	store.jobs[job.ID] = job
	if err = store.save(job); err != nil {
		t.Fatal(err)
	}
	job.Status, job.Content = "succeeded", "<svg>recover me</svg>"
	store.directory = filepath.Join(t.TempDir(), "blocked")
	if err = os.WriteFile(store.directory, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = store.save(job); err == nil || !job.unsaved {
		t.Fatal("save failure was not tracked")
	}
	if _, err = c.svgStore("two"); err != nil {
		t.Fatal(err)
	}
	current, err := c.svgStore("one")
	if err != nil || current != store {
		t.Fatal("unsaved result store was evicted")
	}
	row, err := current.detail(job)
	if err != nil || row["content"] != "<svg>recover me</svg>" {
		t.Fatal("unsaved SVG result became unavailable")
	}
}
