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

func audioRequest(t *testing.T, stream string, size int) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	form := multipart.NewWriter(&buf)
	form.WriteField("model", "deployment")
	if stream != "" {
		form.WriteField("stream", stream)
	}
	file, _ := form.CreateFormFile("file", "audio.wav")
	file.Write(bytes.Repeat([]byte("a"), size))
	form.Close()
	req := httptest.NewRequest("POST", "/v1/audio/transcriptions", &buf)
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.Header.Set("X-Api-Key", "nn_fixture")
	req.Header.Set("Authorization", "Bearer ses_client_login")
	return req
}

func TestTranscriptionNativeStreamAndUsage(t *testing.T) {
	delta := "data: {\"type\":\"transcript.text.delta\",\"delta\":\"hello\"}\n\n"
	done := "data: {\"type\":\"transcript.text.done\",\"text\":\"language English<asr_text>hello\",\"usage\":{\"type\":\"tokens\",\"input_tokens\":171,\"output_tokens\":30,\"input_tokens_details\":{\"cached_tokens\":9}}}\n\n"
	for _, tc := range []struct {
		name, stream, output, state string
		usage                       bool
	}{
		{"json", "", `{"type":"transcript.text.done","text":"language English<asr_text>hello","usage":{"input_tokens":171,"output_tokens":30,"input_tokens_details":{"cached_tokens":9}}}`, "succeeded", true},
		{"json false", "false", `{"text":"hello"}`, "succeeded", false},
		{"stream", "true", delta + done + "data: [DONE]\n\n", "succeeded", true},
		{"done without sentinel", "true", done, "succeeded", true},
		{"missing usage", "true", delta + "data: {\"type\":\"transcript.text.done\",\"text\":\"hello\"}\n\n", "succeeded", false},
		{"sentinel alone", "true", "data: [DONE]\n\n", "unknown", false},
		{"delta truncated", "true", delta, "unknown", false},
		{"unterminated done", "true", strings.TrimSuffix(done, "\n"), "unknown", false},
		{"null done", "true", "data: {\"type\":\"transcript.text.done\",\"text\":null}\n\ndata: [DONE]\n\n", "unknown", false},
		{"upstream error", "true", delta + "data: {\"type\":\"error\",\"error\":\"failed\"}\n\n", "unknown", false},
		{"error after done", "true", done + "data: {\"error\":\"failed\"}\n\n", "unknown", true},
		{"invalid delta", "true", "data: {\"type\":\"transcript.text.delta\",\"delta\":null}\n\n" + done, "unknown", false},
		{"error with delta", "true", "data: {\"type\":\"transcript.text.delta\",\"delta\":\"partial\",\"error\":\"failed\"}\n\n" + done, "unknown", false},
		{"duplicate done", "true", done + done, "unknown", true},
		{"delta usage is not final", "true", "data: {\"type\":\"transcript.text.delta\",\"delta\":\"hello\",\"usage\":{\"input_tokens\":171,\"output_tokens\":30}}\n\ndata: {\"type\":\"transcript.text.done\",\"text\":\"hello\"}\n\n", "succeeded", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var final map[string]any
			calls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					t.Fatal(err)
				}
				defer r.MultipartForm.RemoveAll()
				if r.FormValue("model") != "qwen3-asr-0.6b" || r.FormValue("stream") != tc.stream {
					t.Error("multipart field changed")
				}
				if tc.stream == "true" {
					w.Header().Set("Content-Type", "text/event-stream")
				}
				io.WriteString(w, tc.output)
			}))
			defer upstream.Close()
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"qwen3-asr-0.6b"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": "asr", "permit": "fixture", "endpoint": upstream.URL})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					json.NewDecoder(r.Body).Decode(&final)
					final = meterFixtureSelectedEvidence(r, final)
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			res := httptest.NewRecorder()
			aborted := serveGatewayCapturingAbort(newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"}), res, audioRequest(t, tc.stream, 10))
			if aborted != (tc.stream == "true" && tc.state == "unknown") {
				t.Fatalf("incorrect stream termination: aborted=%v", aborted)
			}
			if calls != 1 || final["status"] != tc.state {
				t.Fatalf("code=%d calls=%d final=%v body=%s", res.Code, calls, final, res.Body)
			}
			if tc.state == "succeeded" && res.Body.String() != tc.output {
				t.Fatalf("response changed: %q", res.Body.String())
			}
			if tc.usage {
				if final["inputTokens"] != float64(171) || final["outputTokens"] != float64(30) || final["cachedInputTokens"] != float64(9) {
					t.Fatalf("usage changed: %v", final)
				}
			} else if final["inputTokens"] != nil || final["outputTokens"] != nil || final["cachedInputTokens"] != nil {
				t.Fatalf("invented usage: %v", final)
			}
		})
	}
}

func TestAudioFileSizeLimit(t *testing.T) {
	for _, size := range []int{20 << 20, (20 << 20) + 1} {
		req := audioRequest(t, "true", size)
		raw, _ := io.ReadAll(req.Body)
		_, err := parseTranscription(raw, req.Header.Get("Content-Type"))
		if (err == nil) != (size == 20<<20) {
			t.Fatalf("size=%d err=%v", size, err)
		}
	}
}

func TestTranscriptionRejectsErrorWithText(t *testing.T) {
	if validTranscription([]byte(`{"text":"partial","error":"failed"}`)) {
		t.Fatal("accepted upstream error")
	}
}

func TestGatewayAudioBodyLimit(t *testing.T) {
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }))
	defer api.Close()
	for _, tc := range []struct {
		path       string
		size, code int
	}{
		{"audio/transcriptions", 2 << 20, 403}, {"audio/transcriptions", 32 << 20, 413},
		{"chat/completions", 32 << 20, 413}, {"responses", 2 << 20, 400},
		{"messages", 2 << 20, 400}, {"responses", 32 << 20, 413},
		{"messages", 32 << 20, 413}, {"embeddings", 2 << 20, 413},
	} {
		req := audioRequest(t, "", tc.size)
		req.URL.Path = "/v1/" + tc.path
		res := httptest.NewRecorder()
		newGateway(api.URL, "internal", "fixture", nil).ServeHTTP(res, req)
		if res.Code != tc.code {
			t.Fatalf("path=%s size=%d got=%d want=%d", tc.path, tc.size, res.Code, tc.code)
		}
	}
}

func TestNativeInputAudioChatPreservesContent(t *testing.T) {
	encoded := strings.Repeat("AAAA", (2<<20)/4)
	content := `[{"role":"user","content":[{"type":"text","text":"Transcribe"},{"type":"input_audio","input_audio":{"data":"` + encoded + `","format":"wav"}}]}]`
	var expected any
	json.Unmarshal([]byte(content), &expected)
	canonical, _ := json.Marshal(expected)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if json.NewDecoder(r.Body).Decode(&body) != nil || string(body["messages"]) != string(canonical) || string(body["model"]) != `"qwen3-asr-0.6b"` {
			t.Error("native audio chat changed")
		}
		io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":171,"completion_tokens":30}}`)
	}))
	defer upstream.Close()
	var final map[string]any
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			io.WriteString(w, `{"model":"qwen3-asr-0.6b"}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(map[string]any{"requestId": "audio-chat", "permit": "fixture", "endpoint": upstream.URL})
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			json.NewDecoder(r.Body).Decode(&final)
			final = meterFixtureSelectedEvidence(r, final)
			io.WriteString(w, `{}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	defer api.Close()
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"deployment","messages":`+content+`,"max_tokens":300}`))
	req.Header.Set("X-Api-Key", "nn_fixture")
	req.Header.Set("Authorization", "Bearer ses_client_login")
	res := httptest.NewRecorder()
	newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"}).ServeHTTP(res, req)
	if res.Code != 200 || final["status"] != "succeeded" || final["inputTokens"] != float64(171) || final["outputTokens"] != float64(30) {
		t.Fatalf("code=%d final=%v", res.Code, final)
	}
}

func TestTranscriptionEvidenceFragmentationAndUnknownUsage(t *testing.T) {
	for _, usage := range []string{"", `,"usage":{"input_tokens":171,"output_tokens":30}`, `,"usage":{"input_tokens":-1,"output_tokens":30}`, `,"usage":{"input_tokens":171}`} {
		e := evidence{asr: true}
		stream := "event: transcript.text.done\r\ndata: {\"type\":\"transcript.text.done\",\"text\":\"hello\"" + usage + "}\r\n\r\ndata: [DONE]\r\n\r\n"
		for _, b := range []byte(stream) {
			e.feed([]byte{b})
		}
		if !e.complete() || e.cached != nil {
			t.Fatalf("fragmented stream invalid or cache invented: %+v", e)
		}
		if usage == `,"usage":{"input_tokens":171,"output_tokens":30}` {
			if e.input == nil || *e.input != 171 || e.output == nil || *e.output != 30 {
				t.Fatalf("valid usage lost: %+v", e)
			}
		} else if e.input != nil || e.output != nil {
			t.Fatalf("invented usage: %+v", e)
		}
	}
}

func TestTranscriptionDoneBeforeTransportTruncationNeverSucceeds(t *testing.T) {
	done := "data: {\"type\":\"transcript.text.done\",\"text\":\"hello\",\"usage\":{\"input_tokens\":171,\"output_tokens\":30}}\n\n"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, done)
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}))
	defer upstream.Close()
	var final map[string]any
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			io.WriteString(w, `{"model":"qwen3-asr-0.6b"}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			json.NewEncoder(w).Encode(map[string]any{"requestId": "truncated-asr", "permit": "fixture", "endpoint": upstream.URL})
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			json.NewDecoder(r.Body).Decode(&final)
			io.WriteString(w, `{}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	defer api.Close()
	res := httptest.NewRecorder()
	if !serveGatewayCapturingAbort(newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"}), res, audioRequest(t, "true", 10)) {
		t.Fatal("truncation did not abort downstream")
	}
	if res.Body.String() != done || final["status"] != "unknown" || final["inputTokens"] != float64(171) || final["outputTokens"] != float64(30) {
		t.Fatalf("truncation after done incorrectly finalized: final=%v body=%q", final, res.Body.String())
	}
}
