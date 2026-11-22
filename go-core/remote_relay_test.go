package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestRemoteRelayBatchesAndAcknowledgesTerminalReceipts(t *testing.T) {
	m, _, _ := remoteFixture(t)
	m.relayOperations = map[string]bool{}
	for i := 0; i < 50; i++ {
		id := randomToken()
		m.operations[id] = &remoteReceipt{OperationID: id, Status: "providerAccepted", created: time.Now().Add(time.Duration(i) * time.Millisecond)}
		m.relayOperations[id] = true
	}
	counts := []int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Receipts []remoteReceipt `json:"receipts"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if len(body.Receipts) > 32 {
			t.Error("API publish receipt cap exceeded")
			w.WriteHeader(413)
			return
		}
		counts = append(counts, len(body.Receipts))
		json.NewEncoder(w).Encode(map[string]bool{"published": true})
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	c.control = server.URL
	for i := 0; i < 2; i++ {
		if err := c.publishChangedRemoteState(context.Background(), m, "fixture-session", nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(counts) != 2 || counts[0] != 32 || counts[1] != 18 || len(m.relayOperations) != 0 {
		t.Fatalf("receipt batch/ack %+v remaining%d", counts, len(m.relayOperations))
	}
}
func TestRemoteRelayPrunesExpiredReceiptInsteadOfPoisoningSnapshots(t *testing.T) {
	m, _, _ := remoteFixture(t)
	m.relayOperations = map[string]bool{"expired": true}
	m.operations["expired"] = &remoteReceipt{OperationID: "expired", Status: "providerAccepted", created: time.Now()}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	c.control = server.URL
	if c.publishChangedRemoteState(context.Background(), m, "fixture", nil) == nil {
		t.Fatal("publish error hidden")
	}
	if m.relayOperations["expired"] {
		t.Fatal("expired receipt poisons all later publishes")
	}
}

func TestRemoteRelayFailureChangesSnapshotRevision(t *testing.T) {
	m, _, _ := remoteFixture(t)
	initial := m.snapshot()["revision"].(float64)
	fail := true
	var published map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var body struct {
			Snapshot map[string]any `json:"snapshot"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		published = body.Snapshot
		json.NewEncoder(w).Encode(map[string]bool{"published": true})
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	c.control = server.URL
	if c.publishChangedRemoteState(context.Background(), m, "fixture", nil) == nil {
		t.Fatal("publish failure hidden")
	}
	failed := m.snapshot()
	if failed["error"] == "" || failed["revision"].(float64) <= initial {
		t.Fatal("failure changed snapshot without advancing revision")
	}
	fail = false
	if err := c.publishChangedRemoteState(context.Background(), m, "fixture", nil); err != nil {
		t.Fatal(err)
	}
	recovered := m.snapshot()
	if recovered["error"] != "" || recovered["revision"].(float64) <= published["revision"].(float64) {
		t.Fatal("recovery changed snapshot without advancing revision")
	}
}

func TestRemoteRelayAdmissionRejectionPreservesLocalOperationAndPublishesRejection(t *testing.T) {
	for _, mode := range []string{"invalid", "conflict", "fullReads"} {
		m, a, _ := remoteReadFixture(t)
		cwd := m.home
		command := remoteEnvelope("relay-rejected", "session.create", "", map[string]any{"agent": "codex", "cwd": filepath.Join(cwd, "missing")})
		command.ExpectedHostEpoch = m.epoch
		if mode == "conflict" {
			m.operations[command.OperationID] = &remoteReceipt{OperationID: command.OperationID, Status: "providerAccepted", Digest: "original", created: time.Now()}
		}
		if mode == "fullReads" {
			command = remoteEnvelope("relay-rejected", "attachment.read", a.SessionID, map[string]any{"attachmentId": a.ID, "offset": 0})
			command.ExpectedHostEpoch = m.epoch
			for i := 0; i < 8; i++ {
				id := randomToken()
				m.operations[id] = &remoteReceipt{OperationID: id, Status: "accepted", Transient: true, created: time.Now()}
			}
		}
		initial := m.snapshot()["revision"].(float64)
		publishedRevision := make(chan float64, 1)
		publishedReceipts := make(chan []remoteReceipt, 1)
		ctx, cancel := context.WithCancel(context.Background())
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if serveRelayFixtureWS(w, r, m.hostID, true) {
				return
			}
			switch filepath.Base(r.URL.Path) {
			case "poll":
				json.NewEncoder(w).Encode(map[string]any{"commands": []any{map[string]any{"command": command, "grant": "fixture", "commandDigest": remoteDigest(command)}}})
			case "verify-grant":
				json.NewEncoder(w).Encode(map[string]any{"valid": true, "command": command, "scope": command.Type, "hostEpoch": m.epoch})
			case "publish":
				var body struct {
					Snapshot map[string]any  `json:"snapshot"`
					Receipts []remoteReceipt `json:"receipts"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				select {
				case publishedRevision <- body.Snapshot["revision"].(float64):
				default:
				}
				select {
				case publishedReceipts <- body.Receipts:
				default:
				}
				json.NewEncoder(w).Encode(map[string]bool{"published": true})
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		c := newCore(t.TempDir(), server.URL)
		c.control = server.URL
		m.session = "fixture"
		done := make(chan struct{})
		go func() { defer close(done); c.remoteRelay(ctx, m) }()
		var revision float64
		select {
		case revision = <-publishedRevision:
			cancel()
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("relay did not publish")
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("relay did not settle")
		}
		cancel()
		server.Close()
		m.mu.Lock()
		r := m.operations[command.OperationID]
		m.mu.Unlock()
		receipts := <-publishedReceipts
		if len(receipts) != 1 || receipts[0].Status != "rejected" || receipts[0].Error == "" || receipts[0].Result != nil {
			t.Fatalf("cloud command received wrong receipt: %+v", receipts)
		}
		if mode == "conflict" {
			if r.Digest != "original" || r.Status != "providerAccepted" {
				t.Fatal("rejection overwrote original operation")
			}
		} else if r != nil || revision != initial {
			t.Fatal("rejection mutated local operation state")
		}
		if mode == "fullReads" {
			m.mu.Lock()
			count := 0
			for _, r := range m.operations {
				if r.Transient {
					count++
				}
			}
			m.mu.Unlock()
			if count != 8 {
				t.Fatal("relay rejection bypassed read cache bound")
			}
		}
	}
}

func TestRemoteRelayPublishesLateExactReceiptAfterUnknown(t *testing.T) {
	m, _, _ := remoteFixture(t)
	m.relayOperations = map[string]bool{"send": true}
	m.operations["send"] = &remoteReceipt{OperationID: "send", Status: "unknown", created: time.Now()}
	var got []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Receipts []remoteReceipt }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		for _, receipt := range body.Receipts {
			if receipt.OperationID != "send" {
				t.Error("unrelated receipt published")
			}
			got = append(got, receipt.Status)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	c := newCore(t.TempDir(), server.URL)
	c.control = server.URL
	for _, status := range []string{"unknown", "unknown", "providerAccepted", "providerAccepted"} {
		m.operations["send"].Status = status
		if err := c.publishChangedRemoteState(context.Background(), m, "fixture", nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 || got[0] != "unknown" || got[1] != "providerAccepted" {
		t.Fatalf("late receipt was lost or duplicated: %v", got)
	}
	if m.relayOperations["send"] || m.relayPublished["send"] != "" {
		t.Fatal("confirmed receipt was not retired")
	}
}
