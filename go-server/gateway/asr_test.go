package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTranscriptionExecution(t *testing.T) {
	for _, output := range []string{`{"text":"hello"}`, `{"text":""}`, `{"text":null}`, `{"error":"bad"}`} {
		t.Run(output, func(t *testing.T) {
			var execution, state string
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				raw, _ := io.ReadAll(r.Body)
				sum := sha256.Sum256(append([]byte("POST /v1/audio/transcriptions\n"+r.Header.Get("Content-Type")+"\n"), raw...))
				if hex.EncodeToString(sum[:]) != execution {
					t.Error("multipart content type not bound")
				}
				r.Body = io.NopCloser(bytes.NewReader(raw))
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Fatal(err)
				}
				defer r.MultipartForm.RemoveAll()
				file, _, err := r.FormFile("file")
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				audio, _ := io.ReadAll(file)
				if r.FormValue("model") != "upstream-whisper" || string(audio) != "RIFFfixture" {
					t.Error("model or audio changed")
				}
				io.WriteString(w, output)
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"upstream-whisper"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					if body["inferencePath"] != "/v1/audio/transcriptions" {
						t.Error("ASR type missing")
					}
					execution, _ = body["executionDigest"].(string)
					json.NewEncoder(w).Encode(map[string]any{"requestId": "asr-test", "permit": "fixture", "endpoint": upstream.URL})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					state, _ = body["status"].(string)
					if body["inputTokens"] != nil || body["outputTokens"] != nil {
						t.Error("invented token billing")
					}
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			var buf bytes.Buffer
			form := multipart.NewWriter(&buf)
			form.WriteField("model", "deployment")
			form.WriteField("response_format", "json")
			file, _ := form.CreateFormFile("file", "sample.wav")
			file.Write([]byte("RIFFfixture"))
			form.Close()
			req := httptest.NewRequest("POST", "/v1/audio/transcriptions", &buf)
			req.Header.Set("Content-Type", form.FormDataContentType())
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_client_login")
			res := httptest.NewRecorder()
			newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"}).ServeHTTP(res, req)
			code, want := 502, "unknown"
			if output == `{"text":"hello"}` || output == `{"text":""}` {
				code, want = 200, "succeeded"
			}
			if res.Code != code || state != want || calls != 1 {
				t.Fatalf("code=%d state=%s calls=%d body=%s", res.Code, state, calls, res.Body)
			}
		})
	}
}

func TestTranscriptionRejectsAmbiguousOrUnsupportedFields(t *testing.T) {
	for _, fields := range [][][2]string{
		{{"model", "one"}, {"model", "two"}},
		{{"response_format", "text"}}, {{"stream", "1"}}, {{"stream", "TRUE"}}, {{"stream", "true"}, {"stream", "false"}},
		{{"temperature", "NaN"}}, {{"temperature", "1.1"}},
		{{"language", ""}}, {{"unknown", "x"}},
	} {
		var buf bytes.Buffer
		form := multipart.NewWriter(&buf)
		form.WriteField("model", "fixture")
		for _, field := range fields {
			form.WriteField(field[0], field[1])
		}
		file, _ := form.CreateFormFile("file", "audio.wav")
		file.Write([]byte("RIFFtest"))
		form.Close()
		if _, err := parseTranscription(buf.Bytes(), form.FormDataContentType()); err == nil {
			t.Fatalf("accepted %v", fields)
		}
	}
	for _, contentType := range []string{"application/json", "multipart/form-data", "multipart/form-data; boundary=wrong"} {
		if _, err := parseTranscription([]byte(`{}`), contentType); err == nil {
			t.Fatal("accepted malformed multipart")
		}
	}
	raw := "--b\r\nContent-Disposition: form-data; name=\"model\"\r\nContent-Disposition: form-data; name=\"other\"\r\n\r\nmodel\r\n--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"audio.wav\"\r\n\r\nRIFFtest\r\n--b--\r\n"
	if _, err := parseTranscription([]byte(raw), "multipart/form-data; boundary=b"); err == nil {
		t.Fatal("duplicate disposition accepted")
	}
}
