package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHistoryCapturesSupportedProtocolsAndMetadataOnlyAudio(t *testing.T) {
	for _, path := range []string{"/v1/embeddings", "/v1/rerank", "/v1/systemone", "/v1/audio/transcriptions"} {
		t.Run(path, func(t *testing.T) {
			h, err := openChatHistory(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer h.db.Close()
			historyPolicyFixture(t, h, "local")
			raw := []byte(`{"model":"fixture","input":"text"}`)
			contentType := "application/json"
			if path == "/v1/audio/transcriptions" {
				var buf bytes.Buffer
				form := multipart.NewWriter(&buf)
				form.WriteField("model", "fixture")
				form.WriteField("language", "zh")
				file, _ := form.CreateFormFile("file", "sample.wav")
				file.Write([]byte("PRIVATE-BINARY"))
				form.Close()
				raw = buf.Bytes()
				contentType = form.FormDataContentType()
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if !bytes.Equal(body, raw) {
					t.Error("capture mutated inference body")
				}
				w.Header().Set("X-Newnoa-Model-Type", "asr")
				io.WriteString(w, `{"text":"transcript"}`)
			}))
			defer upstream.Close()
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				req, _ := http.NewRequest("POST", upstream.URL+r.URL.Path, r.Body)
				res, e := http.DefaultClient.Do(req)
				if e != nil {
					t.Fatal(e)
				}
				defer res.Body.Close()
				w.Header().Set("X-Newnoa-Model-Type", res.Header.Get("X-Newnoa-Model-Type"))
				io.Copy(w, res.Body)
			})
			req := httptest.NewRequest("POST", path, bytes.NewReader(raw))
			req.Header.Set("Content-Type", contentType)
			c := newCore(t.TempDir(), upstream.URL)
			c.control = h.control
			c.history = h
			c.recordInference(next).ServeHTTP(httptest.NewRecorder(), req)
			rows, _ := h.list("00000000-0000-0000-0000-000000000001", "", 10)
			if len(rows) != 1 {
				t.Fatalf("missing capture %v", rows)
			}
			value, _ := h.get("00000000-0000-0000-0000-000000000001", rows[0]["id"].(string))
			if rows[0]["protocol"] != path || rows[0]["httpStatus"] != int64(200) {
				t.Fatal("missing list protocol/status", rows)
			}
			if rows[0]["keyId"] != "test-key" {
				t.Fatal("local history list cannot resolve its authorized API key", rows)
			}
			if path == "/v1/audio/transcriptions" && rows[0]["attachmentCount"] != int64(1) {
				t.Fatal("missing attachment count", rows)
			}
			if value["modelType"] != "asr" || rows[0]["modelType"] != "asr" {
				t.Fatal("missing type", value, rows)
			}
			if path == "/v1/audio/transcriptions" {
				var body map[string]any
				if json.Unmarshal([]byte(value["request"].(string)), &body) != nil || body["language"] != "zh" || strings.Contains(value["request"].(string), "PRIVATE-BINARY") {
					t.Fatal("multipart history unsafe", value)
				}
				files, ok := value["attachments"].([]any)
				if !ok || len(files) != 1 {
					t.Fatal("missing attachment", value)
				}
			}
		})
	}
}

func TestHistoryPayloadKeepsNumbersAndBoundsAttachmentMetadata(t *testing.T) {
	raw := []byte(`{"model":"fixture","state":{"id":9007199254740993},"messages":[{"content":[{"type":"input_audio","input_audio":{"data":"PRIVATE-BASE64","format":"wav"}},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"PRIVATE-IMAGE"}},{"type":"input_file","filename":"report.txt","file_data":"PRIVATE-DOC"}]}]}`)
	_, captured, files, err := historyPayload(raw, "application/json")
	if err != nil || !bytes.Contains(captured, []byte("9007199254740993")) || len(files) != 3 {
		t.Fatalf("metadata/numeric fidelity lost: %s %v %v", captured, files, err)
	}
	if bytes.Contains(captured, []byte("PRIVATE-")) {
		t.Fatalf("inline binary entered history: %s", captured)
	}
	many := []byte(`{"messages":[{"type":"input_image","name":"` + strings.Repeat("x", 1000) + `","image_url":"data:image/png;base64,PRIVATE-IMAGE"}]}`)
	_, _, files, err = historyPayload(many, "application/json")
	if err != nil || len(files[0]["name"].(string)) > 200 {
		t.Fatal("unbounded attachment name", files, err)
	}
}

func TestHistoryPayloadPreservesStructuredSourceData(t *testing.T) {
	_, captured, files, err := historyPayload([]byte(`{"state":{"source":{"data":"business value"}},"messages":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"PRIVATE"}}]}`), "application/json")
	if err != nil || !bytes.Contains(captured, []byte("business value")) || bytes.Contains(captured, []byte("PRIVATE")) || len(files) != 1 || files[0]["mime"] != "image/png" {
		t.Fatal("source fidelity lost", string(captured), files, err)
	}
}
