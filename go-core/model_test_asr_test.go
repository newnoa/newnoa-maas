package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func asrTestBody(data []byte) string {
	raw, _ := json.Marshal(map[string]any{"keyId": testModelID, "model": testModelID, "conversationId": testModelID, "requestId": testModelID, "audio": map[string]string{"data": base64.StdEncoding.EncodeToString(data), "filename": "sample.wav", "mimeType": "audio/wav"}, "language": "Chinese", "prompt": "识别语音"})
	return string(raw)
}
func TestModelTestASRValidation(t *testing.T) {
	raw := asrTestBody([]byte("RIFFfixture"))
	if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(raw)); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		strings.Replace(raw, `"audio":{`, `"maxTokens":0,"audio":{`, 1),
		strings.Replace(raw, `"audio":{`, `"maxTokens":null,"audio":{`, 1),
		strings.Replace(raw, `"audio":{`, `"messages":null,"audio":{`, 1),
		strings.Replace(raw, `"Chinese"`, `null`, 1),
		strings.Replace(raw, `"audio":{`, `"messages":[],"audio":{`, 1),
		strings.Replace(raw, `"audio":{`, `"unknown":true,"audio":{`, 1),
		strings.Replace(raw, `"sample.wav"`, `"../sample.wav"`, 1),
		strings.Replace(raw, `"audio/wav"`, `"text/plain"`, 1),
		strings.Replace(raw, `"Chinese"`, `"`+strings.Repeat("x", 65)+`"`, 1),
		strings.Replace(raw, `"识别语音"`, `"`+strings.Repeat("x", 4097)+`"`, 1),
		strings.Replace(raw, base64.StdEncoding.EncodeToString([]byte("RIFFfixture")), `AA==\n`, 1),
		strings.Replace(raw, base64.StdEncoding.EncodeToString([]byte("RIFFfixture")), `AB==`, 1),
		asrTestBody(nil), asrTestBody(make([]byte, 20*1024*1024+1)),
	} {
		if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(invalid)); err == nil {
			t.Fatalf("accepted invalid audio request (length %d)", len(invalid))
		}
	}
	if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(asrTestBody(make([]byte, 20*1024*1024)))); err != nil {
		t.Fatal("20 MiB rejected", err)
	}
}

func TestModelTestASRAcceptsUppercaseExtensionsWithMatchingMIME(t *testing.T) {
	for _, format := range []struct{ filename, mime string }{{"SPEECH.WAV", "audio/wav"}, {"SPEECH.MP3", "audio/mpeg"}, {"SPEECH.FLAC", "audio/flac"}} {
		t.Run(format.filename, func(t *testing.T) {
			body := strings.Replace(asrTestBody([]byte("fixture")), `"sample.wav"`, `"`+format.filename+`"`, 1)
			body = strings.Replace(body, `"audio/wav"`, `"`+format.mime+`"`, 1)
			input, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(body))
			if err != nil || input.Audio.Filename != format.filename {
				t.Errorf("matching uppercase filename rejected or changed: %v", err)
			}
			for _, otherMIME := range []string{"audio/wav", "audio/mpeg", "audio/flac", "Audio/WAV"} {
				if otherMIME == format.mime {
					continue
				}
				invalid := strings.Replace(body, `"`+format.mime+`"`, `"`+otherMIME+`"`, 1)
				if _, err := decodeModelTest(httptest.NewRecorder(), modelTestRequest(invalid)); err == nil {
					t.Errorf("accepted mismatched/noncanonical MIME %s", otherMIME)
				}
			}
		})
	}
}
func TestModelTestASRUsesCredentialTypeMultipartAndNativeSSE(t *testing.T) {
	const secret = "nn_test_01234567890123456789012345678901"
	credentialType := "asr"
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/me/model-tests/credential":
			json.NewEncoder(w).Encode(map[string]any{"secret": secret, "modelType": credentialType, "expiresAt": time.Now().Add(time.Minute).Format(time.RFC3339)})
		case "/api/v1/client/history/identity":
			io.WriteString(w, `{"accountId":"00000000-0000-0000-0000-000000000001","keyId":"test-key"}`)
		case "/api/v1/client/history/policy":
			io.WriteString(w, `{"mode":"off","id":"00000000-0000-0000-0000-000000000099"}`)
		case "/api/v1/client/targets/" + testModelID:
			io.WriteString(w, `{"available":false,"private":false}`)
		default:
			t.Error("audio touched history/control route", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer control.Close()
	calls := 0
	stream := "event: transcription.text.delta\ndata: {\"delta\":\"你好\"}\n\nevent: transcription.text.done\ndata: {\"text\":\"你好\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}\n\n"
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/v1/audio/transcriptions" || r.Header.Get("X-Api-Key") != secret || r.Header.Get("Idempotency-Key") != testModelID {
			t.Error("incorrect inference request")
		}
		if err := r.ParseMultipartForm(modelTestRequestLimit); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		defer r.MultipartForm.RemoveAll()
		if r.FormValue("model") != testModelID || r.FormValue("stream") != "true" || r.FormValue("language") != "Chinese" || r.FormValue("prompt") != "识别语音" {
			t.Error("wrong fields", r.MultipartForm.Value)
		}
		file, h, err := r.FormFile("file")
		if err != nil {
			t.Error(err)
			return
		}
		defer file.Close()
		data, _ := io.ReadAll(file)
		if string(data) != "RIFFfixture" || h.Filename != "sample.wav" || h.Header.Get("Content-Type") != "audio/wav" {
			t.Error("audio changed")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, stream)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	c.server = &http.Server{}
	c.control = control.URL
	defer c.shutdown()
	h, err := c.historyStore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.db.Exec("INSERT INTO history_local_settings(viewer,enabled) VALUES(?,0)", testModelID); err != nil {
		t.Fatal(err)
	}
	handler, err := c.modelTestHandler()
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, modelTestRequest(asrTestBody([]byte("RIFFfixture"))))
	if response.Code != 200 || response.Body.String() != stream || calls != 1 {
		t.Fatal(response.Code, response.Body.String(), calls)
	}
	if c.history == nil {
		t.Fatal("missing history policy check")
	}
	if rows, err := c.history.list("00000000-0000-0000-0000-000000000001", "", 10); err != nil || len(rows) != 0 {
		t.Fatal("off policy captured model test", rows, err)
	}
	for _, body := range []string{modelTestBody(), asrTestBody([]byte("RIFFfixture"))} {
		if strings.Contains(body, `"audio"`) {
			credentialType = "llm"
		} else {
			credentialType = "asr"
		}
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, modelTestRequest(body))
		if response.Code != 422 || calls != 1 {
			t.Fatal("credential type mismatch executed", response.Code, calls)
		}
	}
}
func TestAudioMultipartAliasAndLargeProxyBody(t *testing.T) {
	const secret = "nn_abcdefghijklmnopqrstuvwxyz0123456789"
	var raw bytes.Buffer
	form := multipart.NewWriter(&raw)
	form.WriteField("model", "newnoa/"+testModelID)
	file, _ := form.CreateFormFile("file", "sample.wav")
	file.Write(bytes.Repeat([]byte("a"), maxFrame+1))
	form.WriteField("stream", "true")
	form.Close()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(modelTestRequestLimit); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		defer r.MultipartForm.RemoveAll()
		if r.FormValue("model") != testModelID || r.FormValue("stream") != "true" {
			t.Error("alias not resolved")
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			t.Error(err)
			return
		}
		defer file.Close()
		data, _ := io.ReadAll(file)
		if len(data) != maxFrame+1 {
			t.Error("large audio lost")
		}
		io.WriteString(w, `{"text":"ok"}`)
	}))
	defer gateway.Close()
	c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
	if err := c.startProxyAt(secret, availableProxyPort(t)); err != nil {
		t.Fatal(err)
	}
	defer c.close()
	req, _ := http.NewRequest("POST", c.proxyBaseURL()+"/v1/audio/transcriptions", bytes.NewReader(raw.Bytes()))
	req.Header.Set("Authorization", "Bearer "+c.localKey)
	req.Header.Set("Content-Type", form.FormDataContentType())
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatal("large audio rejected", res.StatusCode)
	}
}

func TestInferenceBodyBoundsKeepControlProtocolsSmall(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/audio/transcriptions", "/v1/embeddings", "/v1/responses", "/v1/messages", "/v1/systemone"} {
		t.Run(path, func(t *testing.T) {
			calls := 0
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				data, _ := io.ReadAll(r.Body)
				if len(data) != maxFrame+1 {
					t.Error("body changed")
				}
				io.WriteString(w, `{}`)
			}))
			defer gateway.Close()
			c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
			defer c.shutdown()
			handler, err := privateInference("http://127.0.0.1:1", gateway.URL)
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest("POST", path, strings.NewReader(strings.Repeat("a", maxFrame+1))))
			allowed := path == "/v1/chat/completions" || path == "/v1/audio/transcriptions" || path == "/v1/responses" || path == "/v1/messages"
			if allowed && (response.Code != 200 || calls != 1) || !allowed && (response.Code != 413 || calls != 0) {
				t.Fatal("wrong inference body bound", response.Code, calls)
			}
			if allowed {
				response = httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest("POST", path, strings.NewReader(strings.Repeat("a", modelTestRequestLimit+1))))
				if response.Code != 413 || calls != 1 {
					t.Fatal("oversized inference forwarded", response.Code, calls)
				}
			}
		})
	}
}
