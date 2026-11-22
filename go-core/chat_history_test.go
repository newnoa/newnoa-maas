package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func historyPolicyFixture(t *testing.T, h *chatHistory, initial string) func(string) {
	t.Helper()
	var mu sync.Mutex
	mode := initial
	policies := map[string]string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/client/history/identity" {
			io.WriteString(w, `{"accountId":"00000000-0000-0000-0000-000000000001","keyId":"test-key"}`)
			return
		}
		if r.URL.Path == "/api/v1/client/history/policy" {
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			key := body["conversationKey"]
			frozen, ok := policies[key]
			if !ok {
				frozen = mode
				policies[key] = frozen
			}
			json.NewEncoder(w).Encode(map[string]any{"mode": frozen, "id": "00000000-0000-0000-0000-000000000099"})
			return
		}
		w.WriteHeader(503)
	}))
	h.control = server.URL
	t.Cleanup(server.Close)
	return func(next string) { mu.Lock(); defer mu.Unlock(); mode = next }
}

func TestChatHistorySQLiteIsolationAndSSE(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	setMode := historyPolicyFixture(t, h, "local")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Newnoa-Path", "direct")
		w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\",\"tool_calls\":[{\"id\":\"call_1\"}]}}]}\n\ndata: [DONE]\n\n"))
		http.NewResponseController(w).Flush()
	})
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"model-id","messages":[{"role":"user","content":"question"}],"tools":[{"name":"tool"}]}`))
	w := httptest.NewRecorder()
	h.capture(next, "owner", "key-id").ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), "[DONE]") {
		t.Fatal("stream changed")
	}
	rows, err := h.list("owner", "", 20)
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	row, err := h.get("owner", rows[0]["id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(row["request"].(string), "question") || !strings.Contains(row["response"].(string), "tool_calls") || row["transportPath"] != "direct" {
		t.Fatal(row)
	}
	if other, _ := h.list("other", "", 20); len(other) != 0 {
		t.Fatal("cross-account history")
	}
	if _, err = h.get("other", rows[0]["id"].(string)); err == nil {
		t.Fatal("cross-account detail")
	}
	setMode("off")
	h.capture(next, "owner", "key-id").ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"model-id"}`)))
	if rows, _ = h.list("owner", "", 20); len(rows) != 1 {
		t.Fatal("off still captured")
	}
}

func TestHistorySharesConversationAndRequestIdentityWithServer(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	historyPolicyFixture(t, h, "local")
	const requestID = "00000000-0000-0000-0000-000000000123"
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Newnoa-Conversation-Id") == "" {
			t.Error("Server cannot share the frozen conversation policy")
		}
		w.Header().Set("X-Newnoa-Request", requestID)
		w.Write([]byte(`{"choices":[]}`))
	})
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"private","messages":[{"role":"user","content":"question"}]}`))
	h.capture(next, "owner", "key-id").ServeHTTP(httptest.NewRecorder(), r)
	rows, err := h.list("owner", "", 20)
	if err != nil || len(rows) != 1 || rows[0]["id"] != requestID {
		t.Fatalf("local/cloud request identity differs: %v %v", rows, err)
	}
}

func TestHistoryRestartCloudQueueAndAccountRPC(t *testing.T) {
	account := "00000000-0000-0000-0000-000000000001"
	other := "00000000-0000-0000-0000-000000000002"
	home := t.TempDir()
	cloudFails := true
	mode := "off"
	uploads := 0
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/client/requests/00000000-0000-0000-0000-000000000010/consumption":
			io.WriteString(w, `{"requestId":"00000000-0000-0000-0000-000000000010","pointsStatus":"unavailable","pointsConsumed":null}`)
		case "/api/v1/me":
			if r.Header.Get("Authorization") == "Bearer session" {
				io.WriteString(w, `{"id":"`+account+`"}`)
			} else {
				io.WriteString(w, `{"id":"`+other+`"}`)
			}
		case "/api/v1/me/history/settings":
			if r.Method == "POST" {
				var values map[string]any
				json.NewDecoder(r.Body).Decode(&values)
				mode = values["mode"].(string)
				if !values["enabled"].(bool) {
					mode = "off"
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"enabled": mode != "off", "mode": mode, "version": 1, "canSyncOwn": mode == "cloud", "cloudEnabled": mode == "cloud"})
		case "/api/v1/me/history/sync/start":
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "snapshot": "00000000-0000-0000-0000-000000000099", "after": "0", "upper": "0", "totalRecords": 0, "totalCandidates": 0})
		case "/api/v1/me/history/sync/batch":
			json.NewEncoder(w).Encode(map[string]any{"schemaVersion": 1, "items": []any{}, "nextCursor": "0", "done": true})
		case "/api/v1/client/history/policy":
			json.NewEncoder(w).Encode(map[string]any{"mode": mode, "id": "00000000-0000-0000-0000-000000000099"})
		case "/api/v1/client/history/identity":
			io.WriteString(w, `{"accountId":"`+account+`","keyId":"key"}`)
		case "/api/v1/client/history", "/api/v1/me/history":
			uploads++
			if cloudFails {
				w.WriteHeader(503)
				return
			}
			io.WriteString(w, `{"saved":true}`)
		default:
			t.Error("unexpected path", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer control.Close()
	configureTestHistorySQLite(t, home, account)
	c := newCore(home, control.URL)
	c.control = control.URL
	rpc := func(method, params string) (any, error) { return c.historyCommand(method, json.RawMessage(params)) }
	if _, err := rpc("history.configure", `{"session":"session","mode":"cloud","enabled":true}`); err != nil {
		t.Fatal(err)
	}
	c.history.db.Close()
	// No settings RPC after restart: the middleware must load persisted consent itself.
	c = newCore(home, control.URL)
	c.control = control.URL
	text := strings.Repeat("隐私\"\n", 300000)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Newnoa-Request", "00000000-0000-0000-0000-000000000010")
		io.WriteString(w, text)
	})
	r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"test","input":"hello"}`))
	r.Header.Set("Authorization", "Bearer nn_test")
	c.recordInference(next).ServeHTTP(httptest.NewRecorder(), r)
	defer c.history.db.Close()
	rows, err := rpc("history.list", `{"session":"session"}`)
	if err != nil {
		t.Fatal(err)
	}
	records := rows.([]map[string]any)
	if len(records) != 1 || records[0]["pending"] != true || uploads != 1 {
		t.Fatal(records, uploads)
	}
	id := records[0]["id"].(string)
	metadata, err := rpc("history.get", `{"session":"session","id":"`+id+`"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := metadata.(map[string]any)["response"]; exists {
		t.Fatal("large body sent in metadata frame")
	}
	var full strings.Builder
	offset := 0
	for {
		value, err := rpc("history.content", fmt.Sprintf(`{"session":"session","id":%q,"field":"response","offset":%d}`, id, offset))
		if err != nil {
			t.Fatal(err)
		}
		chunk := value.(map[string]any)
		raw, _ := json.Marshal(chunk)
		if len(raw) > maxFrame {
			t.Fatal("chunk exceeds IPC frame")
		}
		full.WriteString(chunk["text"].(string))
		offset = chunk["nextOffset"].(int)
		if chunk["done"] == true {
			break
		}
	}
	if full.String() != text {
		t.Fatal("content was corrupted or truncated")
	}
	if _, err = rpc("history.content", `{"session":"other","id":"`+id+`","field":"response"}`); err == nil {
		t.Fatal("cross-account read")
	}
	cloudFails = false
	waitSync := func(result any, err error, wantFailure bool) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if result.(map[string]any)["jobId"] == "" {
			t.Fatal(result)
		}
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
			value, e := rpc("history.sync.status", `{"session":"session"}`)
			if e != nil {
				t.Fatal(e)
			}
			job := value.(historyJob)
			if job.State != "running" {
				if (job.State == "failed") != wantFailure {
					t.Fatal(job)
				}
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("sync did not finish")
	}
	result, err := rpc("history.sync", `{"session":"session"}`)
	waitSync(result, err, false)
	var pending int
	if err = c.history.db.QueryRow("SELECT count(*) FROM exchanges WHERE pending=1").Scan(&pending); err != nil || pending != 0 {
		t.Fatal("pending upload not committed", pending, err)
	}
	result, err = rpc("history.sync", `{"session":"session"}`)
	waitSync(result, err, false)
	if uploads != 2 {
		t.Fatal(result, err, uploads)
	}
	if _, err = rpc("history.configure", `{"session":"session","mode":"local","enabled":false}`); err != nil {
		t.Fatal(err)
	}
	result, err = rpc("history.sync", `{"session":"session"}`)
	waitSync(result, err, true)
	if uploads != 2 {
		t.Fatal("local record uploaded", result, err)
	}
}

func TestHistoryPreservesAbortedStreamAndBoundedOutput(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	historyPolicyFixture(t, h, "local")
	handler := h.capture(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, strings.Repeat("a", historyBodyLimit+1))
		panic(http.ErrAbortHandler)
	}), "owner", "key")
	func() {
		defer func() {
			if recover() != http.ErrAbortHandler {
				t.Fatal("abort changed")
			}
		}()
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test"}`)))
	}()
	rows, err := h.list("owner", "", 10)
	if err != nil || len(rows) != 1 {
		t.Fatal(rows, err)
	}
	value, err := h.get("owner", rows[0]["id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if value["interrupted"] != true || value["responseTruncated"] != true || len(value["response"].(string)) != historyBodyLimit {
		t.Fatal("partial capture lost")
	}
}

func TestHistoryConsentChangesCannotUpgradeInflightLocalData(t *testing.T) {
	for _, current := range []string{"cloud", "off"} {
		t.Run(current, func(t *testing.T) {
			h, err := openChatHistory(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer h.db.Close()
			setMode := historyPolicyFixture(t, h, "local")
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				setMode(current)
				io.WriteString(w, "local-only")
			})
			h.capture(next, "owner", "key").ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"test"}`)))
			rows, err := h.list("owner", "", 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 || rows[0]["pending"] != false {
				t.Fatal("local request upgraded to cloud", rows)
			}
		})
	}
}
func TestHistoryMultibyteTruncationStaysWithinCloudLimit(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	historyPolicyFixture(t, h, "local")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, strings.Repeat("隐", historyBodyLimit/3+1))
	})
	h.capture(next, "owner", "key").ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"test"}`)))
	rows, _ := h.list("owner", "", 10)
	value, err := h.get("owner", rows[0]["id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	response := value["response"].(string)
	if len(response) > historyBodyLimit || !utf8.ValidString(response) || value["responseTruncated"] != true {
		t.Fatal("cloud-incompatible truncation")
	}
}

func TestConversationPolicyIsPinnedAcrossTurnsAndDevices(t *testing.T) {
	for _, initial := range []string{"off", "local", "cloud"} {
		t.Run(initial, func(t *testing.T) {
			h, err := openChatHistory(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer h.db.Close()
			setMode := historyPolicyFixture(t, h, initial)
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "result") })
			request := func(store *chatHistory, id string) {
				r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"test","metadata":{"conversation_id":"`+id+`"}}`))
				store.capture(next, "owner", "key").ServeHTTP(httptest.NewRecorder(), r)
			}
			request(h, "existing")
			setMode("cloud")
			request(h, "existing")
			rows, _ := h.list("owner", "", 10)
			if initial == "off" {
				if len(rows) != 0 {
					t.Fatal("off conversation started recording")
				}
			} else {
				if len(rows) != 2 {
					t.Fatal(rows)
				}
				for _, row := range rows {
					value, _ := h.get("owner", row["id"].(string))
					if value["storageMode"] != initial {
						t.Fatal("conversation policy changed")
					}
				}
			}
			// A second device shares the policy service, but none of the first device's SQLite content.
			second, err := openChatHistory(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer second.db.Close()
			second.control = h.control
			if rows, _ := second.list("owner", "", 10); len(rows) != 0 {
				t.Fatal("local content appeared on second device")
			}
			request(second, "existing")
			request(second, "new")
			newRows, _ := second.list("owner", "", 10)
			expected := 2
			if initial == "off" {
				expected = 1
			}
			if len(newRows) != expected {
				t.Fatal(newRows)
			}
		})
	}
}

func TestHistoryCapturesNativeAudioMetadataWithoutContent(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	historyPolicyFixture(t, h, "local")
	raw := `{"model":"asr-model","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"UklGRmZpeHR1cmU=","format":"wav"}}]}]}`
	forwarded := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if string(data) != raw {
			t.Error("audio modified")
		}
		forwarded = true
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"transcript"}}]}`)
	})
	response := httptest.NewRecorder()
	h.capture(next, "owner", "key-id").ServeHTTP(response, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(raw)))
	rows, err := h.list("owner", "", 20)
	if err != nil || len(rows) != 1 || !forwarded || response.Code != 200 {
		t.Fatal("audio history lost", len(rows), err, response.Code)
	}
	value, _ := h.get("owner", rows[0]["id"].(string))
	if strings.Contains(value["request"].(string), "UklGRmZpeHR1cmU=") || !strings.Contains(value["response"].(string), "transcript") || value["attachments"] == nil {
		t.Fatal("audio content saved or metadata lost", value)
	}
}

func TestEasyTierCloudHistoryUploadDoesNotBlockModelResponse(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	started, release, uploaded := make(chan struct{}), make(chan struct{}), make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/policy") {
			io.WriteString(w, `{"mode":"cloud","id":"00000000-0000-0000-0000-000000000099"}`)
			return
		}
		close(started)
		<-release
		io.WriteString(w, `{"saved":true}`)
		close(uploaded)
	}))
	defer api.Close()
	h.control = api.URL
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Newnoa-Path", "easytier-direct")
		w.Header().Set("X-Newnoa-Request", "00000000-0000-0000-0000-000000000010")
		io.WriteString(w, `{"choices":[{"message":{"content":"OK"}}]}`)
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.capture(next, "owner", "key").ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[]}`)))
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("cloud upload not started")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("model response waited for cloud upload")
	}
	close(release)
	<-uploaded
	deadline := time.Now().Add(time.Second)
	for {
		var pending int
		h.db.QueryRow("SELECT count(*) FROM exchanges WHERE pending=1").Scan(&pending)
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cloud upload never acknowledged")
		}
		time.Sleep(time.Millisecond)
	}
}
