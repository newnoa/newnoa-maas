package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const hostedFixtureID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
const hostedFixtureReportToken = "hreport_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func fixturePublicPolicy(server *httptest.Server) *supplyPolicy {
	return &supplyPolicy{edition: "online", lookup: func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
	}, connect: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(server.URL, "http://"))
	}}
}
func TestHostedDirectOutboxRetriesOnlyFinishWithoutServer(t *testing.T) {
	executions, fallbacks, finishes := 0, 0, 0
	failFinish := true
	var firstReport map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executions++
		if r.URL.Path != "/custom/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer upstream-secret" || r.Header.Get("X-Api-Key") != "" {
			t.Error("upstream path or credentials")
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["model"] != "real-model" {
			t.Error("wrong model")
		}
		w.Header().Set("Content-Type", "application/json")
		time.Sleep(time.Millisecond)
		io.WriteString(w, `{"choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_write_tokens":2,"cached_tokens":4}}}`)
	}))
	defer upstream.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/finish") {
			finishes++
			if r.Header.Get("Authorization") != "Bearer "+hostedFixtureReportToken || r.Header.Get("X-Api-Key") != "" {
				t.Error("finish identity leaked")
			}
			var v map[string]any
			decodeHostedFixtureClosure(t, r, &v)
			if v["status"] != "succeeded" || v["cacheWriteTokens"] != float64(2) || v["inputTokens"] != float64(12) || v["transportConfirmed"] != true {
				t.Errorf("finish=%v", v)
			}
			for _, key := range []string{"firstTokenMs", "durationMs", "outputTps"} {
				if v[key] == nil || firstReport != nil && v[key] != firstReport[key] {
					t.Errorf("timing lost or changed on retry: %s=%v", key, v[key])
				}
			}
			firstReport = v
			if failFinish {
				w.WriteHeader(503)
				return
			}
			io.WriteString(w, `{"finalized":true}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer ses_fixture" || r.URL.Path != "/api/v1/me" && r.Header.Get("X-Api-Key") != "nn_fixture" {
			t.Error("missing dual credentials")
		}
		switch r.URL.Path {
		case "/api/v1/client/hosted/models":
			io.WriteString(w, `[{"id":"`+hostedFixtureID+`","name":"fixture","model":"real-model","modelType":"llm"}]`)
		case "/api/v1/client/hosted/dispatches":
			var params map[string]any
			json.NewDecoder(r.Body).Decode(&params)
			if params["callerSource"] != "codex" || params["reasoningEffort"] != "high" || params["clientIp"] == nil {
				t.Error("hosted identity metadata missing", params)
			}
			json.NewEncoder(w).Encode(map[string]any{"accountId": hostedFixtureID, "keyId": hostedFixtureID, "requestId": hostedFixtureID, "reportToken": hostedFixtureReportToken, "model": "real-model", "modelType": "llm", "endpoint": "http://upstream.test/custom", "apiKey": "upstream-secret", "revision": 2})
		case "/api/v1/me":
			io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
		default:
			t.Errorf("unexpected API %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control = api.URL
	c.onlineSupply = fixturePublicPolicy(upstream)
	defer c.shutdown()
	handler := c.hostedInference(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fallbacks++; http.Error(w, "server offline", 502) }))
	request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"`+hostedFixtureID+`","messages":[],"reasoning_effort":"high"}`))
	request.Header.Set("User-Agent", "codex_cli_rs/1.0")
	request.Header.Set("Authorization", "Bearer ses_fixture")
	request.Header.Set("X-Api-Key", "nn_fixture")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 200 || executions != 1 || fallbacks != 0 || finishes != 1 || response.Header().Get("X-Newnoa-Path") != "core-hosted-direct" {
		t.Fatalf("status=%d exec=%d fallback=%d finish=%d body=%s", response.Code, executions, fallbacks, finishes, response.Body.String())
	}
	h, _ := c.historyStore()
	var pending int
	h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox").Scan(&pending)
	if pending != 1 {
		t.Fatal("lost unacknowledged completion")
	}
	failFinish = false
	h.db.Exec("UPDATE hosted_usage_outbox SET next_attempt=0")
	c.flushHosted(context.Background(), h)
	h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox").Scan(&pending)
	if pending != 0 || executions != 1 || finishes != 2 {
		t.Fatal("retry replayed execution or lost report")
	}
	request = httptest.NewRequest("GET", "/v1/models", nil)
	request.Header.Set("Authorization", "Bearer ses_fixture")
	request.Header.Set("X-Api-Key", "nn_fixture")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 200 || !strings.Contains(response.Body.String(), hostedFixtureID) {
		t.Fatalf("models unavailable without server: %s", response.Body.String())
	}
	var payload string
	h.db.QueryRow("SELECT COALESCE((SELECT payload FROM exchanges LIMIT 1),'')").Scan(&payload)
	if strings.Contains(payload, "upstream-secret") {
		t.Fatal("upstream key in history")
	}
}

func TestHostedOutboxCrashRecoveryAndAccountIsolation(t *testing.T) {
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	d := hostedDispatch{RequestID: hostedFixtureID, ReportToken: "hreport_old_account"}
	if err = h.queueHosted("account-old", d, usageReport{Status: "unknown"}); err != nil {
		t.Fatal(err)
	}
	h.db.Close()
	h, err = openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	// Advance the crash lease expiry without waiting for the upstream deadline.
	h.db.Exec("UPDATE hosted_usage_outbox SET next_attempt=0")
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		calls++
		if r.URL.Path != "/api/v1/client/hosted/dispatches/"+hostedFixtureID+"/finish" || r.Header.Get("Authorization") != "Bearer hreport_old_account" || r.Header.Get("X-Api-Key") != "" {
			t.Error("rebound old report to current account")
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["status"] != "unknown" || body["inputTokens"] != nil || body["cacheWriteTokens"] != nil {
			t.Errorf("fabricated crashed outcome %v", body)
		}
		io.WriteString(w, `{"finalized":true}`)
	}))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control = api.URL
	c.setProxySession("ses_new_account")
	c.flushHosted(context.Background(), h)
	var count int
	h.db.QueryRow("SELECT count(*) FROM hosted_usage_outbox").Scan(&count)
	if calls != 1 || count != 0 {
		t.Fatal("crash report was not recovered")
	}
}
func TestHostedAuthorizationFailureNeverFallsBack(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control = api.URL
	fallback := 0
	handler := c.hostedInference(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fallback++ }))
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"`+hostedFixtureID+`"}`))
	r.Header.Set("Authorization", "Bearer ses_revoked")
	r.Header.Set("X-Api-Key", "nn_revoked")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if fallback != 0 || w.Code < 400 {
		t.Fatalf("revoked request reached Server: code=%d fallback=%d", w.Code, fallback)
	}
}

func TestHostedDirectSupportsProtocolsAndUnknownUsage(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/v1/responses", "/v1/embeddings", "/v1/rerank", "/v1/systemone", "/v1/audio/transcriptions", "/v1/chat/completions"} {
		t.Run(path, func(t *testing.T) {
			raw := []byte(`{"model":"` + hostedFixtureID + `","input":"text","stream":true}`)
			contentType := "application/json"
			if path == "/v1/embeddings" || path == "/v1/rerank" || path == "/v1/systemone" {
				raw = []byte(`{"model":"` + hostedFixtureID + `","input":"text"}`)
			}
			if path == "/v1/audio/transcriptions" {
				var buffer bytes.Buffer
				form := multipart.NewWriter(&buffer)
				form.WriteField("model", hostedFixtureID)
				form.WriteField("stream", "true")
				file, _ := form.CreateFormFile("file", "fixture.wav")
				file.Write([]byte("audio"))
				form.Close()
				raw = buffer.Bytes()
				contentType = form.FormDataContentType()
			}
			status := "succeeded"
			var finish map[string]any
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != path {
					t.Errorf("path=%s", r.URL.Path)
				}
				received, _ := io.ReadAll(r.Body)
				if hostedRequestedModel(r, received) != "real-model" {
					t.Error("model not rewritten")
				}
				if path == "/v1/messages" {
					if r.Header.Get("X-Api-Key") != "secret" || r.Header.Get("Authorization") != "" || r.Header.Get("Anthropic-Version") != "2023-06-01" {
						t.Error("native credentials")
					}
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":0,\"cache_creation_input_tokens\":4,\"cache_read_input_tokens\":5}}}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":3}}\n\ndata: {\"type\":\"message_stop\"}\n\n")
					return
				}
				if path == "/v1/audio/transcriptions" {
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "data: {\"type\":\"transcript.text.done\",\"text\":\"transcript\",\"usage\":{\"input_tokens\":12,\"output_tokens\":3}}\n\n")
					return
				}
				if path == "/v1/responses" {
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":12,\"output_tokens\":3,\"input_tokens_details\":{\"cache_write_tokens\":4,\"cached_tokens\":5}}}}\n\n")
					return
				}
				if path == "/v1/chat/completions" {
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "data: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\n")
					status = "unknown"
					return
				}
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, `{"data":[],"results":[]}`)
			}))
			defer upstream.Close()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hostedMeterFixtureAck(t, w, r) {
					return
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/models"):
					io.WriteString(w, `[{"id":"`+hostedFixtureID+`"}]`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "real-model", ModelType: "llm", Endpoint: "http://upstream.test/v1", APIKey: "secret", Revision: 1})
				case strings.HasSuffix(r.URL.Path, "/finish"):
					decodeHostedFixtureClosure(t, r, &finish)
					io.WriteString(w, `{"finalized":true}`)
				case r.URL.Path == "/api/v1/me":
					io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
				default:
					t.Errorf("unexpected API %s", r.URL.Path)
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), "http://127.0.0.1:1")
			c.control = api.URL
			c.onlineSupply = fixturePublicPolicy(upstream)
			defer c.shutdown()
			request := httptest.NewRequest("POST", path, bytes.NewReader(raw))
			request.Header.Set("Authorization", "Bearer ses_fixture")
			request.Header.Set("X-Api-Key", "nn_fixture")
			request.Header.Set("Content-Type", contentType)
			w := httptest.NewRecorder()
			c.hostedInference(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("used Go Server") })).ServeHTTP(w, request)
			if calls != 1 || finish["status"] != status {
				t.Fatalf("calls=%d finish=%v", calls, finish)
			}
			if path == "/v1/messages" && (finish["inputTokens"] != float64(11) || finish["cacheWriteTokens"] != float64(4)) {
				t.Fatalf("native usage=%v", finish)
			}
			if path == "/v1/rerank" || path == "/v1/systemone" || path == "/v1/embeddings" {
				if finish["inputTokens"] != nil || finish["outputTokens"] != nil || finish["cacheWriteTokens"] != nil {
					t.Fatalf("unknown usage fabricated %v", finish)
				}
			}
		})
	}
}

func TestHostedCloudHistoryWaitsForCompletionReport(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	uploads := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/policy") {
			io.WriteString(w, `{"mode":"cloud","id":"`+hostedFixtureID+`"}`)
			return
		}
		uploads++
		io.WriteString(w, `{"deleted":false}`)
	}))
	defer api.Close()
	h.control = api.URL
	d := hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken}
	if err = h.queueHosted("owner", d, usageReport{Status: "unknown"}); err != nil {
		t.Fatal(err)
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Newnoa-Path", "core-hosted-direct")
		w.Header().Set("X-Newnoa-Request", hostedFixtureID)
		io.WriteString(w, `{"choices":[]}`)
	})
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"`+hostedFixtureID+`"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.capture(next, "owner", "key").ServeHTTP(w, r)
	var pending int
	h.db.QueryRow("SELECT pending FROM exchanges WHERE account='owner' AND id=?", hostedFixtureID).Scan(&pending)
	saved, _ := h.get("owner", hostedFixtureID)
	if saved["usagePending"] != true {
		t.Fatal("pending hosted completion hidden from local history")
	}
	if uploads != 0 || pending != 1 {
		t.Fatalf("cloud falsely saved before finish: uploads=%d pending=%d", uploads, pending)
	}
}

func TestHostedSwitchCancelsOldAdmission(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/models") {
			close(started)
			select {
			case <-r.Context().Done():
				return
			case <-time.After(time.Second):
				io.WriteString(w, `[]`)
			}
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control = api.URL
	c.setProxySession("ses_old")
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"`+hostedFixtureID+`"}`))
	r.Header.Set("Authorization", "Bearer ses_old")
	r.Header.Set("X-Api-Key", "nn_fixture")
	w := httptest.NewRecorder()
	go func() {
		defer close(finished)
		c.hostedInference(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("old request reached Server") })).ServeHTTP(w, r)
	}()
	<-started
	c.setProxySession("ses_new")
	select {
	case <-finished:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("account switch did not cancel old admission")
	}
}

func TestHostedModelRewritePreservesIntegerPayload(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `9007199254740993`) {
			t.Errorf("structured request lost integer precision: %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"result":[]}`)
	}))
	defer upstream.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/models"):
			io.WriteString(w, `[{"id":"`+hostedFixtureID+`"}]`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(hostedDispatch{AccountID: hostedFixtureID, KeyID: hostedFixtureID, RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken, Model: "actual-model", Endpoint: "http://fixture.test", APIKey: "secret", Revision: 1})
		case strings.HasSuffix(r.URL.Path, "/finish"):
			io.WriteString(w, `{"finalized":true}`)
		default:
			io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control = api.URL
	c.onlineSupply = fixturePublicPolicy(upstream)
	defer c.shutdown()
	r := httptest.NewRequest("POST", "/v1/systemone", strings.NewReader(`{"model":"`+hostedFixtureID+`","seed":9007199254740993}`))
	r.Header.Set("Authorization", "Bearer ses_fixture")
	r.Header.Set("X-Api-Key", "nn_fixture")
	r.Header.Set("Content-Type", "application/json")
	c.hostedInference(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("used Go Server") })).ServeHTTP(httptest.NewRecorder(), r)
}

func TestHostedSameAccountRenewalKeepsActiveRequest(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.setProxySession("ses_old")
	c.proxySessionMu.RLock()
	ctx := c.proxySessionContext
	c.proxySessionMu.RUnlock()
	c.renewProxySession("ses_refreshed")
	if ctx.Err() != nil {
		t.Fatal("ordinary same-account refresh cancelled active inference")
	}
	c.setProxySession("")
	if ctx.Err() == nil {
		t.Fatal("logout retained old inference")
	}
}

func TestHostedPendingReportsResumeWhenProxyStarts(t *testing.T) {
	home := t.TempDir()
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	d := hostedDispatch{RequestID: hostedFixtureID, ReportToken: hostedFixtureReportToken}
	if err = h.queueHosted("old-account", d, usageReport{Status: "unknown"}); err != nil {
		t.Fatal(err)
	}
	h.db.Exec("UPDATE hosted_usage_outbox SET next_attempt=0")
	h.db.Close()
	received := make(chan struct{}, 1)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		if strings.HasSuffix(r.URL.Path, "/finish") {
			received <- struct{}{}
			io.WriteString(w, `{"finalized":true}`)
			return
		}
		io.WriteString(w, `[]`)
	}))
	defer api.Close()
	c := newCore(home, "http://127.0.0.1:1")
	c.control = api.URL
	defer c.shutdown()
	if err = c.startProxyOn("", availableProxyPort(t), false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("startup did not resume persisted reports")
	}
}

func TestHostedDirectDoesNotExposeUpstreamErrorBody(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"error":"invalid credential upstream-secret"}`)
	}))
	defer upstream.Close()
	var status string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hostedMeterFixtureAck(t, w, r) {
			return
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/finish"):
			var report map[string]any
			decodeHostedFixtureClosure(t, r, &report)
			status, _ = report["status"].(string)
			io.WriteString(w, `{"finalized":true}`)
		case r.URL.Path == "/api/v1/client/hosted/models":
			io.WriteString(w, `[{"id":"`+hostedFixtureID+`","name":"fixture","model":"real-model","modelType":"llm"}]`)
		case r.URL.Path == "/api/v1/client/hosted/dispatches":
			json.NewEncoder(w).Encode(map[string]any{"accountId": hostedFixtureID, "keyId": hostedFixtureID, "requestId": hostedFixtureID, "reportToken": hostedFixtureReportToken, "model": "real-model", "modelType": "llm", "endpoint": "http://upstream.test/v1", "apiKey": "upstream-secret", "revision": 1})
		case r.URL.Path == "/api/v1/me":
			io.WriteString(w, `{"id":"`+hostedFixtureID+`"}`)
		default:
			t.Errorf("unexpected API %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	c := newCore(t.TempDir(), "http://127.0.0.1:1")
	c.control = api.URL
	c.onlineSupply = fixturePublicPolicy(upstream)
	defer c.shutdown()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"`+hostedFixtureID+`","messages":[]}`))
	req.Header.Set("Authorization", "Bearer ses_fixture")
	req.Header.Set("X-Api-Key", "nn_fixture")
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	c.hostedInference(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected fallback") })).ServeHTTP(res, req)
	if strings.Contains(res.Body.String(), "upstream-secret") || res.Code != http.StatusBadGateway || status != "failed" {
		t.Fatalf("status=%d report=%q body=%s", res.Code, status, res.Body.String())
	}
}
