package main

import (
	"encoding/base64"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func historyTestRecord(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	wire, ok := body["archive"].(map[string]any)
	if !ok {
		return body
	}
	if wire["codec"] != "zstd-json-v1" {
		t.Fatal("unexpected history codec")
	}
	compressed, err := base64.StdEncoding.DecodeString(wire["bodyBase64"].(string))
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(8<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	raw, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != int(wire["rawBytes"].(float64)) {
		t.Fatal("raw history length mismatch")
	}
	var value map[string]any
	if err = json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func TestCloudHistoryCompressesBeforeUploadAndRetriesTransientFailures(t *testing.T) {
	attempts := 0
	requestID, storage := uuid.NewString(), uuid.NewString()
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/policy") {
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["requestId"] != requestID {
				t.Error("policy missing authorized request identity")
			}
			json.NewEncoder(w).Encode(map[string]any{"mode": "cloud", "id": storage})
			return
		}
		attempts++
		if attempts < 3 {
			w.WriteHeader(503)
			return
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["archive"] == nil {
			t.Error("history uploaded plaintext")
		}
		value := historyTestRecord(t, body)
		if value["response"] != "answer" {
			t.Error("response not lossless")
		}
		io.WriteString(w, `{"saved":true}`)
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", nil)
	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	writer, finish := g.captureCloudHistory(httptest.NewRecorder(), r, "caller", uuid.NewString(), requestID, []byte(`{"model":"fixture"}`))
	writer.Write([]byte("answer"))
	finish()
	if attempts != 3 {
		t.Fatalf("upload attempts %d, want3", attempts)
	}
}

func TestForcedCloudHistoryCannotBeSkippedByCallerConversationMetadata(t *testing.T) {
	for _, tc := range []struct {
		name     string
		headers  []string
		metadata any
	}{
		{"duplicate header", []string{"one", "two"}, nil},
		{"oversized header", []string{strings.Repeat("a", 201)}, nil},
		{"conflicting metadata", []string{"one"}, "two"},
		{"non-string metadata", nil, 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestID, storage := uuid.NewString(), uuid.NewString()
			policies, uploads := 0, 0
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				if strings.HasSuffix(r.URL.Path, "/policy") {
					policies++
					if body["requestId"] != requestID {
						t.Error("missing request policy binding")
					}
					json.NewEncoder(w).Encode(map[string]any{"id": storage, "mode": "cloud", "capturePublisher": true})
					return
				}
				uploads++
				value := historyTestRecord(t, body)
				if value["response"] != "answer" {
					t.Error("lost response")
				}
				if value["conversationId"] != "" && value["conversationId"] != nil && value["conversationId"] != requestID {
					t.Error("untrusted conversation survived")
				}
				io.WriteString(w, `{"saved":true}`)
			}))
			defer api.Close()
			body := map[string]any{"model": "fixture"}
			if tc.metadata != nil {
				body["metadata"] = map[string]any{"conversation_id": tc.metadata}
			}
			raw, _ := json.Marshal(body)
			r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
			for _, h := range tc.headers {
				r.Header.Add("X-Newnoa-Conversation-Id", h)
			}
			response := httptest.NewRecorder()
			g := newGateway(api.URL, "internal", "fixture", nil)
			writer, finish := g.captureCloudHistory(response, r, "caller", uuid.NewString(), requestID, raw)
			writer.Write([]byte("answer"))
			finish()
			if policies != 1 || uploads != 1 {
				t.Fatalf("forced capture bypass: policies=%d uploads=%d", policies, uploads)
			}
			if response.Body.String() != "answer" {
				t.Fatal("inference changed")
			}
		})
	}
}

func TestCloudHistoryReceiptNeverClaimsSavedForPausedOrUnknownResponse(t *testing.T) {
	for _, tc := range []struct {
		name, receipt, want string
		attempts            int
	}{{"paused", `{"saved":false,"paused":true}`, "paused", 1}, {"unknown", `{}`, "pending", 3}, {"saved", `{"saved":true}`, "saved", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			uploads := 0
			storage := uuid.NewString()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/policy") {
					json.NewEncoder(w).Encode(map[string]any{"id": storage, "mode": "cloud"})
					return
				}
				uploads++
				io.WriteString(w, tc.receipt)
			}))
			defer api.Close()
			response := httptest.NewRecorder()
			g := newGateway(api.URL, "internal", "fixture", nil)
			writer, finish := g.captureCloudHistory(response, httptest.NewRequest("POST", "/v1/chat/completions", nil), "caller", uuid.NewString(), uuid.NewString(), []byte(`{"model":"fixture"}`))
			writer.Write([]byte("answer"))
			finish()
			if got := response.Result().Trailer.Get("X-Newnoa-History"); got != tc.want {
				t.Fatalf("receipt trailer=%q want%q", got, tc.want)
			}
			if uploads != tc.attempts {
				t.Fatalf("uploads=%d want%d", uploads, tc.attempts)
			}
		})
	}
}
