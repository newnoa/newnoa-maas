package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHistoryConsumptionWithoutCloudAndRestartAccountFence(t *testing.T) {
	account := "00000000-0000-0000-0000-000000000001"
	other := "00000000-0000-0000-0000-000000000002"
	request := "00000000-0000-0000-0000-000000000123"
	var settled atomic.Bool
	var lookups atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/me" {
			id := account
			if r.Header.Get("Authorization") == "Bearer other-session" {
				id = other
			}
			json.NewEncoder(w).Encode(map[string]any{"id": id})
			return
		}
		if r.URL.Path == "/api/v1/client/requests/"+request+"/consumption" {
			lookups.Add(1)
			if r.Method != "GET" || r.Header.Get("Authorization") != "Bearer original-key" && r.Header.Get("Authorization") != "Bearer original-session" {
				t.Error("wrong snapshot caller", r.Method, r.Header.Get("Authorization"))
				w.WriteHeader(403)
				return
			}
			status := "pending"
			var points any
			if settled.Load() {
				status = "known"
				points = "0.000000"
			}
			json.NewEncoder(w).Encode(map[string]any{"requestId": request, "inputTokens": 0, "outputTokens": 0, "cacheWriteTokens": 0, "cachedInputTokens": 0, "pointsStatus": status, "pointsConsumed": points})
			return
		}
		t.Error("metadata hydration used cloud history or replayed inference", r.URL.Path)
		w.WriteHeader(403)
	}))
	defer server.Close()
	home := t.TempDir()
	configureTestHistorySQLite(t, home, account)
	c := newCore(home, server.URL)
	c.control = server.URL
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"id": request, "requestId": request, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "[TEST] private", "agent": "[TEST] agent", "request": "[TEST] immutable", "pointsStatus": "pending"}
	if err = h.save(account, value, false); err != nil {
		t.Fatal(err)
	}
	if err = h.hydrateConsumption(context.Background(), account, request, "Bearer original-key"); err != nil {
		t.Fatal(err)
	}
	if lookups.Load() != 1 {
		t.Fatal("local capture did not query API consumption independently of cloud history")
	}
	record, err := m.store.Get(context.Background(), account, "own", request)
	if err != nil || record["pointsStatus"] != "pending" {
		t.Fatal(record, err)
	}
	c.shutdown()
	m.store.Close()
	h.db.Close()
	settled.Store(true)
	c = newCore(home, server.URL)
	c.control = server.URL
	h, err = c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	// Drive consumption explicitly without the independent cloud-sync worker racing this fixture.
	c.hostedCancel()
	<-c.hostedDone
	m, err = h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.shutdown(); m.store.Close(); h.db.Close() }()
	// Wrong active account cannot retry another account's pending request.
	c.setProxySession("other-session")
	before := lookups.Load()
	c.flushConsumption(context.Background(), h)
	if lookups.Load() != before {
		t.Fatal("another account used pending request authorization")
	}
	if _, err = h.db.Exec("UPDATE history_consumption_pending SET next_attempt=0"); err != nil {
		t.Fatal(err)
	}
	c.setProxySession("original-session")
	c.flushConsumption(context.Background(), h)
	record, err = m.store.Get(context.Background(), account, "own", request)
	if err != nil || record["pointsConsumed"] != "0.000000" || record["pointsStatus"] != "known" || record["request"] != "[TEST] immutable" {
		t.Fatal("free/local snapshot not persisted after restart", record, err)
	}
	var count int
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&count); err != nil || count != 0 {
		t.Fatal("terminal snapshot still queued", count, err)
	}
}

func TestHistoryKnownSnapshotCannotRegressOrEraseObservedUsage(t *testing.T) {
	value := map[string]any{"inputTokens": int64(9), "pointsConsumed": "0.000000", "pointsStatus": "known"}
	mergeHistoryTrustedMetadata(value, map[string]any{"inputTokens": nil, "pointsConsumed": nil, "pointsStatus": "pending"})
	if value["inputTokens"] != int64(9) || value["pointsConsumed"] != "0.000000" || value["pointsStatus"] != "known" {
		t.Fatal("late pending snapshot erased retained evidence", value)
	}
}

func TestHistoryCaptureHydratesConsumptionAsynchronouslyWithoutCloud(t *testing.T) {
	account := "00000000-0000-0000-0000-000000000001"
	request := "00000000-0000-0000-0000-000000000123"
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/client/history/policy":
			json.NewEncoder(w).Encode(map[string]any{"mode": "local", "id": account})
		case "/api/v1/client/requests/" + request + "/consumption":
			if r.Header.Get("Authorization") != "Bearer original-key" {
				t.Error("lost original caller credential")
			}
			calls.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"requestId": request, "pointsStatus": "known", "pointsConsumed": "0.000000", "inputTokens": 0, "outputTokens": 0, "cacheWriteTokens": 0, "cachedInputTokens": 0, "modelName": "[TEST] trusted model name", "keyName": "[TEST] trusted key name", "keyPrefix": "nn_test", "deploymentId": "[TEST] deployment", "keyId": "[TEST] key"})
		default:
			t.Error("free/local capture requested cloud membership or replayed inference", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	configureTestHistorySQLite(t, c.home, account)
	c.control = server.URL
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.shutdown(); m.store.Close(); h.db.Close() }()
	var inference atomic.Int64
	handler := h.capture(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inference.Add(1)
		w.Header().Set("X-Newnoa-Request", request)
		w.Write([]byte(`{"choices":[]}`))
	}), account, "[TEST] key")
	r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"[TEST] free model"}`))
	r.Header.Set("Authorization", "Bearer original-key")
	handler.ServeHTTP(httptest.NewRecorder(), r)
	deadline := time.After(3 * time.Second)
	for {
		record, err := m.store.Get(context.Background(), account, "own", request)
		if err == nil && record["pointsStatus"] == "known" {
			if record["pointsConsumed"] != "0.000000" || record["inputTokens"] != float64(0) || record["modelName"] != "[TEST] trusted model name" || record["keyName"] != "[TEST] trusted key name" || record["keyPrefix"] != "nn_test" {
				t.Fatal(record)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("asynchronous snapshot missing", record, err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	if calls.Load() != 1 || inference.Load() != 1 {
		t.Fatal("hydration replayed inference or duplicate lookup", calls.Load(), inference.Load())
	}
	filters, err := m.store.Filters(context.Background(), account, historyFilterQuery{Scope: "own", Kind: "keys", Limit: 10})
	if err != nil || len(filters.Items) != 1 || filters.Items[0].Name != "[TEST] trusted key name" {
		t.Fatal("hydrated name not indexed", filters, err)
	}
}

func TestHistoryDefinitiveLocalAuthorizationRejectionStoresZero(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	historyPolicyFixture(t, h, "local")
	for _, status := range []int{401, 403, 502} {
		handler := h.capture(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			historyRejectBeforeAuthorization(w, "[TEST] rejected before admission", status)
		}), "[TEST] account", "[TEST] key")
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"[TEST] denied"}`)))
	}
	rows, err := h.list("[TEST] account", "", 10)
	if err != nil || len(rows) != 3 {
		t.Fatal(rows, err)
	}
	for _, row := range rows {
		record, err := h.get("[TEST] account", historyText(row["id"]))
		if err != nil || record["pointsConsumed"] != "0.000000" || record["pointsStatus"] != "known" {
			t.Fatal(record, err)
		}
	}
}

func TestConsumptionRestartRecoversMissingNamesOnlyOnce(t *testing.T) {
	home := t.TempDir()
	account := "00000000-0000-0000-0000-000000000001"
	request := "00000000-0000-0000-0000-000000000123"
	h, err := openChatHistory(home)
	if err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"id": request, "requestId": request, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "[TEST] model", "agent": "[TEST] agent", "pointsStatus": "known", "pointsConsumed": "0.000000"}
	if err = h.save(account, value, false); err != nil {
		t.Fatal(err)
	}
	h.db.Close()
	configureTestHistorySQLite(t, home, account)
	c := newCore(home, "")
	h, err = c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&count); err != nil || count != 1 {
		t.Fatal("pre-upgrade settled local record names never retried", count, err)
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"requestId": request, "pointsStatus": "unavailable", "pointsConsumed": nil})
	}))
	defer api.Close()
	h.control = api.URL
	// Deleted naming evidence can be unavailable; it must not erase a prior settled cost or retry every restart.
	if err = h.hydrateConsumption(context.Background(), account, request, "Bearer original-key"); err != nil {
		t.Fatal(err)
	}
	c.shutdown()
	m.store.Close()
	h.db.Close()
	h, err = openChatHistoryAt(defaultHistoryDatabase(home).SQLiteDirectory)
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&count); err != nil || count != 0 {
		t.Fatal("unavailable deleted names repeatedly requeued", count, err)
	}
	var raw string
	if err = h.db.QueryRow("SELECT payload FROM exchanges WHERE account=? AND id=?", account, request).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	json.Unmarshal([]byte(raw), &value)
	if value["pointsStatus"] != "known" || value["pointsConsumed"] != "0.000000" {
		t.Fatal("name recovery overwrote settled usage", value)
	}
}

func TestHistoryQueryAfterRestartHydratesVerifiedAccountWithoutProxySession(t *testing.T) {
	home := t.TempDir()
	account := "00000000-0000-0000-0000-000000000001"
	other := "00000000-0000-0000-0000-000000000002"
	request := "00000000-0000-0000-0000-000000000123"
	otherRequest := "00000000-0000-0000-0000-000000000124"
	old, err := openChatHistoryAt(defaultHistoryDatabase(home).SQLiteDirectory)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ account, id string }{{account, request}, {other, otherRequest}} {
		if err = old.save(row.account, map[string]any{"id": row.id, "requestId": row.id, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "[TEST] model", "agent": "[TEST] agent", "pointsStatus": "known", "pointsConsumed": "0.000000"}, false); err != nil {
			t.Fatal(err)
		}
	}
	old.db.Close()
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls atomic.Int64
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/me":
			if r.Header.Get("Authorization") != "Bearer viewing-session" {
				t.Error("unverified history session")
				w.WriteHeader(401)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"id": account})
		case "/api/v1/client/requests/" + request + "/consumption":
			calls.Add(1)
			if r.Header.Get("Authorization") != "Bearer viewing-session" {
				t.Error("lost verified history credential")
			}
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"requestId": request, "pointsStatus": "known", "pointsConsumed": "0.000000", "modelName": "[TEST] recovered model", "keyName": "[TEST] recovered key"})
		default:
			t.Error("history view hydrated another account or used cloud membership", r.URL.Path)
			w.WriteHeader(403)
		}
	}))
	defer api.Close()
	configureTestHistorySQLite(t, home, account)
	c := newCore(home, api.URL)
	c.control = api.URL
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		c.shutdown()
		m.store.Close()
		if child := h.manager.readyAccount(other); child != nil {
			child.store.Close()
		}
		h.db.Close()
	}()
	if c.proxySession != "" {
		t.Fatal("test requires fresh Core without proxy session")
	}
	returned := make(chan error, 1)
	go func() {
		_, err := c.historyCommand("history.query", json.RawMessage(`{"session":"viewing-session","scope":"own"}`))
		returned <- err
	}()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("history query blocked on consumption network")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("verified history RPC never triggered pending hydration without proxy")
	}
	release <- struct{}{}
	deadline := time.After(3 * time.Second)
	for {
		record, err := m.store.Get(context.Background(), account, "own", request)
		if err == nil && record["modelName"] == "[TEST] recovered model" && record["keyName"] == "[TEST] recovered key" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("viewing existing local record did not recover names", record, err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	var count int
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending WHERE account=?", other).Scan(&count); err != nil || count != 1 {
		t.Fatal("viewing session crossed account fence", count, err)
	}
	if calls.Load() != 1 {
		t.Fatal("duplicate consumption lookup", calls.Load())
	}
}
