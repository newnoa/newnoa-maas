package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHistoryMeterEvidenceHydratesProvisionalThenFinalAndPersists(t *testing.T) {
	const account = "00000000-0000-0000-0000-000000000001"
	const request = "00000000-0000-0000-0000-000000000123"
	var final atomic.Bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/client/requests/"+request+"/consumption" || r.Header.Get("Authorization") != "Bearer original-session" {
			t.Error("history evidence crossed account/request boundary")
			w.WriteHeader(403)
			return
		}
		source, phase, points := "estimated", "provisional", "0.300000"
		if final.Load() {
			source, phase, points = "upstream", "final", "0.100000"
		}
		json.NewEncoder(w).Encode(map[string]any{"requestId": request, "usageSource": source, "billingPhase": phase, "pointsStatus": "known", "pointsConsumed": points, "inputTokens": 2, "outputTokens": 1})
	}))
	defer api.Close()
	home := t.TempDir()
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
	defer func() { c.shutdown(); m.store.Close(); h.db.Close() }()
	value := map[string]any{"id": request, "requestId": request, "createdAt": time.Now().UTC().Format(time.RFC3339Nano), "model": "[TEST] fixture", "agent": "[TEST] fixture", "request": "[TEST] immutable request", "pointsStatus": "pending"}
	if err = h.save(account, value, false); err != nil {
		t.Fatal(err)
	}
	if err = h.hydrateConsumption(context.Background(), account, request, "Bearer original-session"); err != nil {
		t.Fatal(err)
	}
	var pending int
	h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&pending)
	row, err := m.store.Get(context.Background(), account, "own", request)
	if err != nil || row["usageSource"] != "estimated" || row["billingPhase"] != "provisional" || pending != 1 {
		t.Fatalf("provisional evidence lost or terminalized: row=%v pending=%d err=%v", row, pending, err)
	}
	// Simulate an older client having removed a known-points provisional row
	// from its queue: restart must rebuild the pending evidence lookup.
	c.hostedCancel()
	<-c.hostedDone
	if _, err = h.db.Exec("DELETE FROM history_consumption_pending; DELETE FROM history_meta WHERE key='consumption-backfill-v1'"); err != nil {
		t.Fatal(err)
	}
	c.shutdown()
	m.store.Close()
	h.db.Close()
	c = newCore(home, api.URL)
	c.control = api.URL
	h, err = c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	waitHistoryBackfill(t, h)
	m, err = h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&pending)
	if pending != 1 {
		t.Fatal("restart lost provisional reconciliation lookup")
	}
	final.Store(true)
	if err = h.hydrateConsumption(context.Background(), account, request, "Bearer original-session"); err != nil {
		t.Fatal(err)
	}
	row, err = m.store.Get(context.Background(), account, "own", request)
	if err != nil || row["usageSource"] != "upstream" || row["billingPhase"] != "final" || row["pointsConsumed"] != "0.100000" || row["request"] != "[TEST] immutable request" {
		t.Fatalf("final correction/immutable body lost: %v %v", row, err)
	}
	h.db.QueryRow("SELECT count(*) FROM history_consumption_pending").Scan(&pending)
	if pending != 0 {
		t.Fatal("final evidence kept polling")
	}
	for _, format := range []string{"json", "csv"} {
		out, err := writeHistoryExport(context.Background(), m.store, account, historyQuery{Scope: "own"}, format, t.TempDir(), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(out.Path)
		if err != nil || !strings.Contains(string(raw), "0.100000") || strings.Contains(string(raw), "usageSource") || strings.Contains(string(raw), "billingPhase") {
			t.Fatalf("%s lost exact amount or exposed internal billing provenance: %s %v", format, raw, err)
		}
	}
	c.shutdown()
	m.store.Close()
	h.db.Close()
	c = newCore(home, api.URL)
	c.control = api.URL
	h, err = c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	m, err = h.manager.forAccount(account)
	if err != nil {
		t.Fatal(err)
	}
	row, err = m.store.Get(context.Background(), account, "own", request)
	if err != nil || row["usageSource"] != "upstream" || row["billingPhase"] != "final" || row["pointsConsumed"] != "0.100000" {
		t.Fatalf("SQLite restart stripped billing evidence: %v %v", row, err)
	}
}

func TestHistoryFinalMeterEvidenceCannotRegressWithLateProvisional(t *testing.T) {
	value := map[string]any{"pointsStatus": "known", "pointsConsumed": "0.100000", "inputTokens": int64(2), "usageSource": "upstream", "billingPhase": "final"}
	mergeHistoryTrustedMetadata(value, map[string]any{"pointsStatus": "known", "pointsConsumed": "0.300000", "inputTokens": int64(12), "usageSource": "estimated", "billingPhase": "provisional"})
	if value["usageSource"] != "upstream" || value["billingPhase"] != "final" || value["pointsConsumed"] != "0.100000" || value["inputTokens"] != int64(2) {
		t.Fatalf("late provisional regressed final evidence: %v", value)
	}
}

func TestHistoryLegacyNullMetadataDoesNotStripProvisionalEvidence(t *testing.T) {
	value := map[string]any{"pointsStatus": "known", "usageSource": "estimated", "billingPhase": "provisional"}
	mergeHistoryTrustedMetadata(value, map[string]any{"pointsStatus": "known", "usageSource": nil, "billingPhase": nil})
	if value["usageSource"] != "estimated" || value["billingPhase"] != "provisional" || !historyConsumptionPending(value) {
		t.Fatalf("legacy metadata stripped retryable provenance: %v", value)
	}
}
