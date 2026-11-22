package main

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGatewayCapturesTypedHistoryAfterRealForwarding(t *testing.T) {
	for _, tc := range []struct{ path, kind, input, output string }{
		{"/v1/embeddings", "embedding", `{"model":"deployment","input":"text","dimensions":2}`, `{"object":"list","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":2}}`},
		{"/v1/rerank", "reranker", `{"model":"deployment","query":"q","documents":["a","b"],"top_n":1}`, `{"results":[{"index":1,"relevance_score":0.8}]}`},
		{"/v1/systemone", "decision", decisionFixture, `{"answers":{"q":{"type":"choice","choice":"yes","confidence":0.8,"probabilities":{"yes":0.9,"no":0.1}}}}`},
		{"/v1/audio/transcriptions", "asr", "", `{"text":"transcript"}`},
		{"/v1/chat/completions", "world", `{"model":"deployment","messages":[{"role":"user","content":"hello"}],"max_tokens":16}`, `{"choices":[{"message":{"role":"assistant","content":"world"},"finish_reason":"stop"}]}`},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			var saved, finalized, confirmed map[string]any
			calls := 0
			id, key, storage := uuid.NewString(), uuid.NewString(), uuid.NewString()
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				raw, _ := io.ReadAll(r.Body)
				if tc.kind == "asr" {
					if !bytes.Contains(raw, []byte("PRIVATE-BINARY")) {
						t.Error("lost audio")
					}
				} else {
					var body map[string]any
					json.Unmarshal(raw, &body)
					if body["model"] != "authorized-model" {
						t.Error("model rewrite failed")
					}
				}
				w.Header().Set("X-Newnoa-Model-Type", "forged")
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, tc.output)
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					json.NewEncoder(w).Encode(map[string]any{"model": "authorized-model", "modelType": "llm", "history": map[string]any{"enabled": true, "keyId": key}})
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": id, "permit": "fixture", "endpoint": upstream.URL, "modelType": tc.kind})
				case strings.HasSuffix(r.URL.Path, "/transport"):
					confirmed = body
					io.WriteString(w, `{}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					finalized = body
					io.WriteString(w, `{}`)
				case r.URL.Path == "/api/v1/client/history/policy":
					json.NewEncoder(w).Encode(map[string]any{"mode": "cloud", "id": storage})
				case r.URL.Path == "/api/v1/client/history":
					body = historyTestRecord(t, body)
					saved = body
					io.WriteString(w, `{"saved":true}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			raw := []byte(tc.input)
			contentType := "application/json"
			if tc.kind == "asr" {
				var buf bytes.Buffer
				form := multipart.NewWriter(&buf)
				form.WriteField("model", "deployment")
				form.WriteField("language", "zh")
				file, _ := form.CreateFormFile("file", "sample.wav")
				file.Write([]byte("PRIVATE-BINARY"))
				form.Close()
				raw = buf.Bytes()
				contentType = form.FormDataContentType()
			}
			req := httptest.NewRequest("POST", tc.path, bytes.NewReader(raw))
			req.Header.Set("Content-Type", contentType)
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_client_login")
			req.Header.Set("X-Newnoa-Model-Type", "spoofed")
			req.Header.Set("X-Newnoa-Path", "frp")
			res := httptest.NewRecorder()
			g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			defer g.peers.Reset()
			g.ServeHTTP(res, req)
			if res.Code != 200 || calls != 1 {
				t.Fatalf("forwarding status=%d calls=%d body=%s", res.Code, calls, res.Body)
			}
			if saved == nil || saved["modelType"] != tc.kind || res.Header().Get("X-Newnoa-Model-Type") != tc.kind {
				t.Fatalf("missing trusted capture/header: %v %v", saved, res.Header())
			}
			if saved["transportPath"] != "direct" || finalized["transportPath"] != "direct" || confirmed["transportPath"] != "direct" || confirmed["gatewayId"] != "fixture" {
				t.Fatal("missing actual external connection evidence", saved, finalized)
			}
			if tc.kind == "asr" {
				if strings.Contains(saved["request"].(string), "PRIVATE-BINARY") || !strings.Contains(saved["request"].(string), `"language":"zh"`) {
					t.Fatal("unsafe multipart capture", saved)
				}
				files, ok := saved["attachments"].([]any)
				if !ok || len(files) != 1 {
					t.Fatal("missing file metadata", saved)
				}
			}
		})
	}
}

func TestGatewayDoesNotInventActualPathForUnknownOrLegacyOwner(t *testing.T) {
	for _, mode := range []string{"disconnected", "legacy-owner"} {
		t.Run(mode, func(t *testing.T) {
			var finalized map[string]any
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, `{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}`)
			}))
			endpoint := upstream.URL
			if mode == "disconnected" {
				upstream.Close()
			} else {
				defer upstream.Close()
			}
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"authorized","modelType":"llm"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					dispatch := map[string]any{"requestId": uuid.NewString(), "endpoint": endpoint, "permit": "fixture"}
					if mode == "legacy-owner" {
						dispatch["nodeId"] = uuid.NewString()
						dispatch["route"] = endpoint
						dispatch["epoch"] = 1
					}
					json.NewEncoder(w).Encode(dispatch)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					finalized = body
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			defer g.peers.Reset()
			req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"deployment","messages":[{"role":"user","content":"hello"}],"max_tokens":16}`))
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_fixture")
			req.Header.Set("X-Newnoa-Path", "frp")
			res := httptest.NewRecorder()
			g.ServeHTTP(res, req)
			if finalized == nil || finalized["transportPath"] != nil || res.Header().Get("X-Newnoa-Path") != "" || res.Header().Get("X-Newnoa-Model-Type") != "" {
				t.Fatal("selected path invented actual connectivity", mode, res.Code, finalized, res.Header())
			}
		})
	}
}
