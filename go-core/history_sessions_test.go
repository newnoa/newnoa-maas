package main

import (
	"fmt"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHistoryConversationPaginationIncludesAllTurnsAndScopesCursor(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	storage := uuid.NewString()
	ids := []string{}
	for i := 0; i < 63; i++ {
		id := uuid.NewString()
		ids = append(ids, id)
		value := map[string]any{"id": id, "storageId": storage, "conversationId": "shared-session", "createdAt": time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second).Format("2006-01-02T15:04:05.000000000Z"), "model": "model", "agent": "agent"}
		if err := h.save("owner", value, false); err != nil {
			t.Fatal(err)
		}
	}
	foreign := uuid.NewString()
	if err := h.save("other", map[string]any{"id": foreign, "storageId": storage, "createdAt": "2026-11-08T00:00:00Z", "model": "secret", "agent": "agent"}, false); err != nil {
		t.Fatal(err)
	}
	first, err := h.listSession("owner", storage, "", 50)
	if err != nil || len(first) != 50 {
		t.Fatal(len(first), err)
	}
	if first[0]["id"] != ids[62] || first[49]["id"] != ids[13] || first[0]["storageId"] != storage {
		t.Fatal(first[0], first[49])
	}
	older, err := h.listSession("owner", storage, first[49]["id"].(string), 50)
	if err != nil || len(older) != 13 {
		t.Fatal(len(older), err)
	}
	seen := map[string]bool{}
	for _, row := range append(first, older...) {
		id := row["id"].(string)
		if seen[id] || id == foreign {
			t.Fatal("duplicate/cross-account turn", id)
		}
		seen[id] = true
	}
	if len(seen) != 63 {
		t.Fatal(len(seen))
	}
	if _, err := h.listSession("owner", storage, foreign, 50); err == nil {
		t.Fatal("accepted another account cursor")
	}
}

func TestLegacyHistoryWithoutStorageIDStaysIndependent(t *testing.T) {
	h, err := openChatHistory(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.db.Close()
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("legacy-%d", i)
		if err := h.save("owner", map[string]any{"id": id, "createdAt": "2026-11-07T00:00:00Z", "model": "model", "agent": "agent"}, false); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := h.list("owner", "", 50)
	if err != nil || len(rows) != 2 {
		t.Fatal(rows, err)
	}
	for _, row := range rows {
		if row["storageId"] != row["id"] {
			t.Fatal("legacy requests were merged", row)
		}
	}
}

func TestConversationIDsRejectAmbiguousSourcesBeforeUpstream(t *testing.T) {
	calls := 0
	policy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(503) }))
	defer policy.Close()
	for _, body := range []string{`{"model":"model","metadata":{"conversation_id":"other"}}`, `{"model":"model","metadata":{"conversation_id":123}}`} {
		upstream := false
		h := &chatHistory{control: policy.URL}
		r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
		r.Header.Set("X-Newnoa-Conversation-Id", "session")
		response := httptest.NewRecorder()
		h.capture(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { upstream = true; w.WriteHeader(200) }), "owner", "key").ServeHTTP(response, r)
		if response.Code != 400 || upstream {
			t.Fatalf("ambiguous session reached upstream: %d", response.Code)
		}
	}
	if calls != 0 {
		t.Fatal("invalid IDs reached policy lookup", calls)
	}
}
