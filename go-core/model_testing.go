package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/newnoa/newnoa-maas/go-server/network/metering"
)

//go:embed recommendation_formats.json
var recommendationFormatData []byte

var recommendationFormats = func() map[string]json.RawMessage {
	var formats map[string]json.RawMessage
	if err := json.Unmarshal(recommendationFormatData, &formats); err != nil {
		panic(err)
	}
	return formats
}()

const modelTestResponseLimit = 4 * 1024 * 1024

var pictureSizePattern = regexp.MustCompile(`^[1-9][0-9]*x[1-9][0-9]*$`)

var modelTestCredentialPattern = regexp.MustCompile(`^nn_[A-Za-z0-9_-]{32,96}$`)

type modelTestInput struct {
	KeyID    string `json:"keyId"`
	Model    string `json:"model"`
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	RecommendationFormat string `json:"recommendationFormat,omitempty"`
	MaxTokens            *int   `json:"maxTokens,omitempty"`
	Thinking             *bool  `json:"thinking,omitempty"`
	ConversationID       string `json:"conversationId"`
	RequestID            string `json:"requestId,omitempty"`
	Audio                *struct {
		Data     string `json:"data"`
		Filename string `json:"filename"`
		MIMEType string `json:"mimeType"`
	} `json:"audio,omitempty"`
	Language *string `json:"language,omitempty"`
	Prompt   *string `json:"prompt,omitempty"`
	Picture  *struct {
		Prompt    string  `json:"prompt"`
		Size      string  `json:"size"`
		Reference *string `json:"reference,omitempty"`
	} `json:"picture,omitempty"`
	Decision map[string]json.RawMessage `json:"decision,omitempty"`
}

func decodeModelTest(w http.ResponseWriter, r *http.Request) (modelTestInput, error) {
	var input modelTestInput
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, modelTestRequestLimit))
	var raw json.RawMessage
	if decoder.Decode(&raw) != nil || decoder.Decode(new(any)) != io.EOF {
		return input, errors.New("invalid model test")
	}
	decoder = json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || !nodePattern.MatchString(input.KeyID) || !nodePattern.MatchString(input.Model) || !nodePattern.MatchString(input.ConversationID) || (input.RequestID != "" && !nodePattern.MatchString(input.RequestID)) {
		return input, errors.New("invalid model test")
	}
	if input.RequestID == "" {
		input.RequestID = uuid.NewString()
	}
	var fields map[string]json.RawMessage
	json.Unmarshal(raw, &fields)
	if fields["recommendationFormat"] != nil && (recommendationFormats[input.RecommendationFormat] == nil || len(input.Messages) == 0 || input.Audio != nil || input.Decision != nil || input.Picture != nil) {
		return input, errors.New("invalid recommendation format")
	}
	if fields["picture"] != nil {
		pic := input.Picture
		if pic == nil || strings.TrimSpace(pic.Prompt) == "" || strings.ContainsRune(pic.Prompt, 0) || len(pic.Prompt) > 32768 ||
			pic.Reference != nil && !metering.ValidEditReference(*pic.Reference) {
			return input, errors.New("invalid image test")
		}
		for field := range fields {
			switch field {
			case "picture", "keyId", "model", "conversationId", "requestId":
			default:
				return input, errors.New("unexpected image test field")
			}
		}
		if pic.Size != "" && !pictureSizePattern.MatchString(pic.Size) {
			return input, errors.New("invalid image size")
		}
		return input, nil
	}
	if fields["decision"] != nil {
		if input.Decision == nil || len(raw) > 1024*1024 || bytes.Equal(bytes.TrimSpace(input.Decision["state"]), []byte("null")) || len(input.Decision["state"]) == 0 {
			return input, errors.New("invalid decision evidence")
		}
		for _, field := range []string{"audio", "messages", "maxTokens", "language", "prompt", "thinking"} {
			if fields[field] != nil {
				return input, errors.New("decision cannot include chat or audio fields")
			}
		}
		for field := range input.Decision {
			switch field {
			case "state", "questions", "think_mode", "think_budget", "gate", "mix", "allow_none", "none_check", "return_details", "return_thought", "thought_generation":
			default:
				return input, errors.New("unsupported decision field")
			}
		}
		var questions map[string]json.RawMessage
		if json.Unmarshal(input.Decision["questions"], &questions) != nil || len(questions) < 1 || len(questions) > 128 {
			return input, errors.New("invalid decision questions")
		}
		return input, nil
	}
	if input.Audio != nil {
		var fields map[string]json.RawMessage
		json.Unmarshal(raw, &fields)
		if fields["thinking"] != nil || fields["maxTokens"] != nil || fields["messages"] != nil || (fields["language"] != nil && input.Language == nil) || (fields["prompt"] != nil && input.Prompt == nil) {
			return input, errors.New("audio cannot include chat fields")
		}
		audio := input.Audio
		ext := map[string]string{"audio/wav": ".wav", "audio/mpeg": ".mp3", "audio/flac": ".flac"}[audio.MIMEType]
		if ext == "" || len(audio.Filename) > 255 || !utf8.ValidString(audio.Filename) || !strings.HasSuffix(strings.ToLower(audio.Filename), ext) || len(audio.Filename) <= len(ext) || strings.ContainsAny(audio.Filename, "/\\\r\n\x00\"<>:|?*") || strings.ContainsFunc(audio.Filename, func(r rune) bool { return r < 32 || r == 127 }) {
			return input, errors.New("invalid audio filename or MIME type")
		}
		if len(audio.Data) > base64.StdEncoding.EncodedLen(20*1024*1024) {
			return input, errors.New("audio too large")
		}
		decoded, err := base64.StdEncoding.Strict().DecodeString(audio.Data)
		if err != nil || len(decoded) == 0 || len(decoded) > 20*1024*1024 || base64.StdEncoding.EncodeToString(decoded) != audio.Data {
			return input, errors.New("invalid audio base64")
		}
		for _, field := range []struct {
			value *string
			limit int
		}{{input.Language, 64}, {input.Prompt, 4096}} {
			if field.value != nil && (len(*field.value) > field.limit || !utf8.ValidString(*field.value) || strings.ContainsFunc(*field.value, func(r rune) bool { return r < 32 && r != '\n' && r != '\t' || r == 127 })) {
				return input, errors.New("invalid audio text")
			}
		}
		return input, nil
	}
	if (fields["thinking"] != nil && input.Thinking == nil) || input.Language != nil || input.Prompt != nil || input.MaxTokens == nil || *input.MaxTokens < 1 || *input.MaxTokens > 65535 || len(input.Messages) < 1 || len(input.Messages) > 40 {
		return input, errors.New("invalid model test")
	}
	textSize := 0
	for _, message := range input.Messages {
		if message.Role != "user" && message.Role != "assistant" {
			return input, errors.New("invalid role")
		}
		size, err := validateTestContent(message.Content, message.Role)
		if err != nil {
			return input, err
		}
		textSize += size
	}
	if textSize > 4*1024*1024 || input.Messages[len(input.Messages)-1].Role != "user" {
		return input, errors.New("invalid model test messages")
	}
	return input, nil
}

const modelTestRequestLimit = 32 * 1024 * 1024

func validateTestContent(raw json.RawMessage, role string) (int, error) {
	invalid := errors.New("invalid model test content")
	var text string
	validText := func(value string) bool {
		return strings.TrimSpace(value) != "" && utf8.ValidString(value) && !strings.ContainsRune(value, 0) && len(value) <= 32768
	}
	if json.Unmarshal(raw, &text) == nil {
		if role == "assistant" && len(text) <= 2*1024*1024 && strings.TrimSpace(text) != "" && utf8.ValidString(text) && !strings.ContainsRune(text, 0) {
			return len(text), nil
		}
		if !validText(text) {
			return 0, invalid
		}
		return len(text), nil
	}
	if role != "user" {
		return 0, invalid
	}
	var parts []struct {
		Type  string  `json:"type"`
		Text  *string `json:"text,omitempty"`
		Image *struct {
			URL string `json:"url"`
		} `json:"image_url,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&parts) != nil || len(parts) < 1 || len(parts) > 2 {
		return 0, invalid
	}
	size, images := 0, 0
	for _, part := range parts {
		switch part.Type {
		case "text":
			if part.Text == nil || part.Image != nil || !validText(*part.Text) {
				return 0, invalid
			}
			size += len(*part.Text)
		case "image_url":
			if part.Image == nil || part.Text != nil {
				return 0, invalid
			}
			prefix, data, ok := strings.Cut(part.Image.URL, ",")
			if !ok || (prefix != "data:image/png;base64" && prefix != "data:image/jpeg;base64" && prefix != "data:image/webp;base64" && prefix != "data:image/gif;base64") || len(data) > base64.StdEncoding.EncodedLen(10*1024*1024) {
				return 0, invalid
			}
			decoded, err := base64.StdEncoding.Strict().DecodeString(data)
			if err != nil || len(decoded) == 0 || len(decoded) > 10*1024*1024 || "data:"+http.DetectContentType(decoded)+";base64" != prefix {
				return 0, invalid
			}
			images++
		default:
			return 0, invalid
		}
	}
	if images != 1 || size > 32768 {
		return 0, invalid
	}
	return size, nil
}

// Keep upstream error bodies and authentication headers inside Core.
type modelTestWriter struct {
	language string
	http.ResponseWriter
	header       http.Header
	cancel       context.CancelFunc
	status, size int
	limit        int
	jsonResponse bool
}

func (w *modelTestWriter) Header() http.Header         { return w.header }
func (w *modelTestWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *modelTestWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	jsonContent := w.jsonResponse && strings.HasPrefix(w.header.Get("Content-Type"), "application/json")
	if status != 200 || (!jsonContent && (w.jsonResponse || !strings.HasPrefix(w.header.Get("Content-Type"), "text/event-stream"))) {
		if status < 400 || status > 599 {
			status = 502
		}
		w.status = status
		writeLocalError(w.ResponseWriter, status, w.language, browserLocalMessage("model.test", nil, errors.New("Model test unavailable; refresh authorization before retrying")))
		return
	}
	contentType := "text/event-stream; charset=utf-8"
	if jsonContent {
		contentType = "application/json; charset=utf-8"
	}
	w.ResponseWriter.Header().Set("Content-Type", contentType)
	if requestID := w.header.Get("X-Newnoa-Request"); nodePattern.MatchString(requestID) {
		w.ResponseWriter.Header().Set("X-Newnoa-Request", requestID)
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *modelTestWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	if w.status != 200 {
		return len(data), nil
	}
	limit := w.limit
	if limit == 0 {
		limit = modelTestResponseLimit
	}
	if w.size+len(data) > limit {
		if svg, ok := w.ResponseWriter.(*svgResponse); ok {
			return 0, svg.fail("SVG response exceeds limit")
		}
		w.cancel()
		return 0, errors.New("model test response too large")
	}
	w.size += len(data)
	return w.ResponseWriter.Write(data)
}
func (w *modelTestWriter) Flush() {
	if w.status == 200 {
		http.NewResponseController(w.ResponseWriter).Flush()
	}
}

func (c *core) modelTestHandler() (http.Handler, error) {
	inference, err := privateInferenceWithSelector(c.control, c.gateway, c.gatewaySelector)
	if err != nil {
		return nil, err
	}
	forward := inference
	inference = c.recordInference(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		out := r.Clone(r.Context())
		c.attachProxySession(out)
		c.hostedInference(forward).ServeHTTP(w, out)
	}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		reject := func(status int, message string) {
			writeLocalError(w, status, browserLanguage(r), browserLocalMessage("model.test", nil, errors.New(message)))
		}
		if r.Method != "POST" || r.URL.Path != "/api/local/model-test" || r.URL.RawQuery != "" || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			reject(404, "Not found")
			return
		}
		c.mu.Lock()
		running := c.server != nil
		if running && c.modelTestContext == nil {
			c.modelTestContext, c.modelTestCancel = context.WithCancel(context.Background())
		}
		proxyContext := c.modelTestContext
		c.mu.Unlock()
		if !running {
			reject(409, "Enable the proxy before model testing")
			return
		}
		authorization := r.Header.Get("Authorization")
		if !strings.HasPrefix(authorization, "Bearer ") || len(authorization) < 16 || len(authorization) > 4096 || strings.ContainsAny(authorization, "\r\n") {
			reject(401, "Login required")
			return
		}
		input, err := decodeModelTest(w, r)
		if err != nil {
			reject(400, "Invalid model test")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), inferenceWrapperTimeout)
		defer cancel()
		stopProxy := context.AfterFunc(proxyContext, cancel)
		defer stopProxy()
		var credential struct {
			Secret                string `json:"secret"`
			ExpiresAt             string `json:"expiresAt"`
			SupportsImages        bool   `json:"supportsImages"`
			ModelType             string `json:"modelType"`
			SupportsReasoningNone bool   `json:"supportsReasoningNone"`
		}
		if err = historyRemote(ctx, c.control, "/me/model-tests/credential", authorization, map[string]string{"keyId": input.KeyID, "model": input.Model}, &credential); err != nil {
			reject(403, "Model test authorization unavailable; refresh your key and model selection")
			return
		}
		if (input.RecommendationFormat != "" && credential.ModelType != "recommendation") || (input.Picture != nil && credential.ModelType != "image") || (input.Decision != nil && credential.ModelType != "decision") || (input.Audio != nil && credential.ModelType != "asr") || (input.Picture == nil && input.Decision == nil && input.Audio == nil && credential.ModelType != "" && credential.ModelType != "llm" && credential.ModelType != "recommendation") || (input.Thinking != nil && credential.ModelType != "recommendation" && r.Context().Value(svgModelTestContext{}) != true) {
			reject(422, "Selected model does not support this input type")
			return
		}
		for _, message := range input.Messages {
			if bytes.HasPrefix(bytes.TrimSpace(message.Content), []byte("[")) && !credential.SupportsImages {
				reject(422, "Selected model does not support image input")
				return
			}
		}
		expires, expiryError := time.Parse(time.RFC3339Nano, credential.ExpiresAt)
		if !modelTestCredentialPattern.MatchString(credential.Secret) || expiryError != nil || !expires.After(time.Now()) {
			reject(502, "Invalid model test authorization")
			return
		}
		payload := map[string]any{"model": input.Model, "messages": input.Messages, "stream": true, "stream_options": map[string]bool{"include_usage": true}, "max_tokens": input.MaxTokens}
		if credential.ModelType == "recommendation" {
			if format := recommendationFormats[input.RecommendationFormat]; format != nil {
				payload["response_format"] = format
				if input.Thinking == nil || *input.Thinking {
					payload["reasoning_budget_tokens"] = 512
				}
			}
			payload["temperature"], payload["top_p"], payload["top_k"] = 0.75, 0.95, 20
			payload["chat_template_kwargs"] = map[string]bool{"enable_thinking": input.Thinking == nil || *input.Thinking}
		}
		if r.Context().Value(svgModelTestContext{}) == true && credential.SupportsReasoningNone && (input.Thinking == nil || !*input.Thinking) {
			payload["reasoning_effort"] = "none"
		}
		raw, _ := json.Marshal(payload)
		request, _ := http.NewRequestWithContext(ctx, "POST", "http://localhost/v1/chat/completions", bytes.NewReader(raw))
		if pic := input.Picture; pic != nil {
			path := imageGenerationPath
			body := map[string]any{"model": input.Model, "prompt": pic.Prompt, "n": 1}
			if pic.Size != "" {
				body["size"] = pic.Size
			}
			if pic.Reference != nil {
				path = imageEditPath
				body["images"] = []map[string]string{{"image_url": *pic.Reference}}
			}
			raw, _ = json.Marshal(body)
			request, _ = http.NewRequestWithContext(ctx, "POST", "http://localhost"+path, bytes.NewReader(raw))
			request.Header.Set("Content-Type", "application/json")
		} else if input.Decision != nil {
			input.Decision["model"], _ = json.Marshal(input.Model)
			raw, _ = json.Marshal(input.Decision)
			request, _ = http.NewRequestWithContext(ctx, "POST", "http://localhost/v1/systemone", bytes.NewReader(raw))
			request.Header.Set("Content-Type", "application/json")
		} else if input.Audio != nil {
			var body bytes.Buffer
			form := multipart.NewWriter(&body)
			form.WriteField("model", input.Model)
			form.WriteField("stream", "true")
			if input.Language != nil {
				form.WriteField("language", *input.Language)
			}
			if input.Prompt != nil {
				form.WriteField("prompt", *input.Prompt)
			}
			header := make(textproto.MIMEHeader)
			header.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": input.Audio.Filename}))
			header.Set("Content-Type", input.Audio.MIMEType)
			part, _ := form.CreatePart(header)
			decoded, _ := base64.StdEncoding.DecodeString(input.Audio.Data)
			part.Write(decoded)
			form.Close()
			request, _ = http.NewRequestWithContext(ctx, "POST", "http://localhost/v1/audio/transcriptions", bytes.NewReader(body.Bytes()))
			request.Header.Set("Content-Type", form.FormDataContentType())
		} else {
			request.Header.Set("Content-Type", "application/json")
		}
		request.RemoteAddr = r.RemoteAddr
		request.GetBody = nil
		request.Header.Set("Authorization", "Bearer "+credential.Secret)
		request.Header.Set("X-Newnoa-Conversation-Id", input.ConversationID)
		request.Header.Set("Idempotency-Key", input.RequestID)
		request.Header.Set("X-Newnoa-Agent", "NEWNOA Model Test")
		if r.Context().Value(svgModelTestContext{}) == true {
			request.Header.Set("X-Newnoa-Agent", "NEWNOA SVG Test")
		}
		limit := modelTestResponseLimit
		if input.Picture != nil {
			limit = imageResponseLimit
		}
		if r.Context().Value(svgModelTestContext{}) == true {
			limit = svgStreamLimit
		}
		inference.ServeHTTP(&modelTestWriter{language: browserLanguage(r), limit: limit, ResponseWriter: w, header: make(http.Header), cancel: cancel, jsonResponse: input.Decision != nil || input.Picture != nil}, request)
	}), nil
}

// Called under c.mu; independent of the user-configured local proxy.
func (c *core) modelTestEndpoint() (any, error) {
	if c.modelTestServer != nil {
		return map[string]string{"url": c.modelTestURL, "token": c.modelTestToken}, nil
	}
	handler, err := c.modelTestHandler()
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.New("model test transport unavailable")
	}
	token := randomToken()
	server := &http.Server{ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384, Handler: browserBoundary(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") != "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Newnoa-Model-Test")), []byte(token)) != 1 {
			http.Error(w, "Forbidden", 403)
			return
		}
		handler.ServeHTTP(w, r)
	}))}
	c.modelTestServer = server
	c.modelTestURL = "http://" + listener.Addr().String() + "/api/local/model-test"
	c.modelTestToken = token
	go server.Serve(listener)
	return map[string]string{"url": c.modelTestURL, "token": token}, nil
}
