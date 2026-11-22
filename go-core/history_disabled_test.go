package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHistoryDatabaseOffStopsRecordingAndSurvivesRestart(t *testing.T) {
	account := "00000000-0000-0000-0000-000000000001"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(map[string]any{"id": account}) }))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	configureTestHistorySQLite(t, c.home, account)
	c.control = server.URL
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.shutdown(); h.db.Close() }()
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"id": "[TEST] before", "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "[TEST] model", "agent": "[TEST]", "request": "[TEST] body"}
	if err = h.save(account, value, false); err != nil {
		t.Fatal(err)
	}
	result, err := c.historyCommand("history.database.configure", json.RawMessage(`{"session":"verified","engine":"off"}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.(map[string]any)["state"] != "complete" {
		t.Fatal("off must save immediately", result)
	}
	queued := map[string]any{"id": "[TEST] queued", "createdAt": value["createdAt"], "request": "[TEST] recovery body"}
	raw, _ := json.Marshal(queued)
	if _, err = h.db.Exec("INSERT INTO history_pending(viewer,scope,kind,id,payload) VALUES(?,'own','record','[TEST] queued',?),(?,'own','delete','[TEST] before','{}')", account, string(raw), account); err != nil {
		t.Fatal(err)
	}
	m.flushPending(context.Background())
	var pending int
	if err = h.db.QueryRow("SELECT count(*) FROM history_pending WHERE viewer=?", account).Scan(&pending); err != nil || pending != 2 {
		t.Fatal("disabled flush acknowledged backlog", pending, err)
	}
	if err = m.store.Save(context.Background(), account, "own", queued); err == nil {
		t.Fatal("disabled repository silently acknowledged write")
	}
	value["id"] = "[TEST] disabled"
	if err = h.save(account, value, true); err != nil {
		t.Fatal(err)
	}
	if err = m.commit(context.Background(), account, "others", "record", "[TEST] other", value); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"exchanges", "history_pending"} {
		var count int
		if err = h.db.QueryRow("SELECT count(*) FROM " + table + " WHERE id='[TEST] disabled'").Scan(&count); err != nil || count != 0 {
			t.Fatal("disabled body written", table, count, err)
		}
	}
	if _, err = c.historyCommand("history.sync", json.RawMessage(`{"session":"verified"}`)); err == nil {
		t.Fatal("disabled sync accepted")
	}
	for _, method := range []string{"history.export", "history.delete"} {
		if _, err = c.historyCommand(method, json.RawMessage(`{"session":"verified","format":"json","id":"[TEST] before"}`)); err == nil {
			t.Fatal("disabled operation accepted", method)
		}
	}
	m.store.Close()
	restored, err := newAccountHistoryManager(c.home, h, account, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { restored.store.Close() }()
	if restored.config.Engine != "off" {
		t.Fatal("off lost on restart", restored.config)
	}
	rows, err := restored.store.Query(context.Background(), account, historyQuery{})
	if err != nil || rows.Total != 0 || rows.Items == nil {
		t.Fatal("off query", rows, err)
	}
	target := defaultHistoryDatabase(c.home)
	if err = restored.migrate(context.Background(), target); err != nil {
		t.Fatal("reenable", err)
	}
	rows, err = restored.store.Query(context.Background(), account, historyQuery{})
	if err != nil || rows.Total != 1 {
		t.Fatal("original records lost", rows, err)
	}
	if _, err = restored.store.Get(context.Background(), account, "own", "[TEST] queued"); err != nil {
		t.Fatal("queued record not recovered", err)
	}
	if _, err = restored.store.Get(context.Background(), account, "own", "[TEST] before"); err == nil {
		t.Fatal("queued deletion lost")
	}
}

func TestHistoryDisabledRetainsOnlyMatchingConnectionCredentials(t *testing.T) {
	config := historyDatabaseConfig{Engine: "off", ResumeEngine: "postgres", Host: "localhost", Port: 15432, Database: "history", Username: "test", Password: "[TEST] password"}
	target := config
	target.Engine, target.Password = "postgres", ""
	if !config.sameConnection(target) {
		t.Fatal("saved connection not recognized after disabling")
	}
	for _, field := range []string{"engine", "host", "port", "database", "username"} {
		changed := target
		switch field {
		case "engine":
			changed.Engine = "mysql"
		case "host":
			changed.Host = "other"
		case "port":
			changed.Port++
		case "database":
			changed.Database = "other"
		case "username":
			changed.Username = "other"
		}
		if config.sameConnection(changed) {
			t.Fatal("credentials reused for different", field)
		}
	}
}

func TestHistoryDisabledPreservesConsumptionLookup(t *testing.T) {
	account, request := "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/client/requests/"+request+"/consumption" {
			json.NewEncoder(w).Encode(map[string]any{"requestId": request, "pointsStatus": "known", "pointsConsumed": "0"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"id": account})
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	configureTestHistorySQLite(t, c.home, account)
	c.control = server.URL
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.shutdown(); h.db.Close() }()
	m, err := h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	value := map[string]any{"id": "[TEST] lookup", "model": "[TEST] model", "agent": "[TEST] agent", "requestId": request, "pointsStatus": "pending", "createdAt": time.Now().UTC().Format(time.RFC3339Nano)}
	if err = h.save(account, value, false); err != nil {
		t.Fatal(err)
	}
	if _, err = m.disableRecording(); err != nil {
		t.Fatal(err)
	}
	if err = h.hydrateConsumption(context.Background(), account, "[TEST] lookup", "Bearer verified"); err == nil {
		t.Fatal("off acknowledged consumption metadata")
	}
	var count int
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending WHERE account=?", account).Scan(&count); err != nil || count != 1 {
		t.Fatal("lookup queue lost", count, err)
	}
	if err = m.migrate(context.Background(), defaultHistoryDatabase(c.home)); err != nil {
		t.Fatal(err)
	}
	if err = h.hydrateConsumption(context.Background(), account, "[TEST] lookup", "Bearer verified"); err != nil {
		t.Fatal(err)
	}
	record, err := m.store.Get(context.Background(), account, "own", "[TEST] lookup")
	if err != nil || record["pointsStatus"] != "known" {
		t.Fatal("lookup not recovered", record, err)
	}
	if err = h.db.QueryRow("SELECT count(*) FROM history_consumption_pending WHERE account=?", account).Scan(&count); err != nil || count != 0 {
		t.Fatal("completed lookup not cleared", count, err)
	}
}

func configureTestHistorySQLite(t *testing.T, home, account string) {
	t.Helper()
	m := &historyManager{home: home, owner: account}
	raw, err := json.Marshal(defaultHistoryDatabase(home))
	if err != nil {
		t.Fatal(err)
	}
	if err = atomicFile(m.configPath(), raw); err != nil {
		t.Fatal(err)
	}
}
