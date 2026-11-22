package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/newnoa/newnoa-maas/go-server/network/metering"
	"io"
	"net/http"
	"strings"
	"time"
)

const imageGenerationPath = "/v1/images/generations"
const imageEditPath = metering.ImageEditPath

func isImagePath(path string) bool { return path == imageGenerationPath || path == imageEditPath }

const imageResponseLimit = 32 * 1024 * 1024

type imageResponseTransport struct{ regular, images, execution http.RoundTripper }

func (t imageResponseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	if index := strings.LastIndex(path, "/v1/"); index >= 0 {
		path = path[index:]
	}
	if request.Method == http.MethodPost && historyInferencePath(path) && request.Context().Value(authorizedExecutionContext{}) == true && t.execution != nil {
		return t.execution.RoundTrip(request)
	}
	if strings.HasSuffix(request.URL.Path, "/images/generations") || strings.HasSuffix(request.URL.Path, "/images/edits") {
		return t.images.RoundTrip(request)
	}
	return t.regular.RoundTrip(request)
}

func (t imageResponseTransport) CloseIdleConnections() {
	for _, transport := range []http.RoundTripper{t.regular, t.images, t.execution} {
		if closer, ok := transport.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
	}
}

func withImageResponseTimeout(transport *http.Transport) http.RoundTripper {
	images := transport.Clone()
	images.ResponseHeaderTimeout = 120 * time.Second
	execution := transport.Clone()
	execution.ResponseHeaderTimeout = 0 // API execution context bounds inference, not image downloads or probes
	return imageResponseTransport{regular: transport, images: images, execution: execution}
}

func validateImageRequest(path string, raw []byte) error {
	var body map[string]any
	if json.Unmarshal(raw, &body) != nil || body == nil {
		return errors.New("invalid image generation request")
	}
	if path == imageEditPath && !metering.ValidEditImages(body) {
		return errors.New("invalid edit reference")
	}
	model, modelOK := body["model"].(string)
	prompt, promptOK := body["prompt"].(string)
	if !modelOK || strings.TrimSpace(model) == "" || len(model) > 200 || !promptOK || strings.TrimSpace(prompt) == "" {
		return errors.New("image generation requires model and prompt")
	}
	if stream, exists := body["stream"]; exists && stream != false {
		return errors.New("streaming image generation is not supported")
	}
	if count, exists := body["n"]; exists {
		n, ok := count.(float64)
		if !ok || n < 1 || n > 10 || n != float64(int(n)) {
			return errors.New("image count must be an integer from 1 to 10")
		}
	}
	return nil
}

func modelServiceBase(path string) string {
	base := strings.TrimRight(path, "/")
	for _, suffix := range []string{"/chat/completions", "/responses", "/messages", "/embeddings", "/rerank", "/audio/transcriptions", "/systemone", "/images/generations", "/images/edits"} {
		if strings.HasSuffix(base, "/v1"+suffix) {
			return strings.TrimSuffix(base, suffix)
		}
	}
	return base
}

func boundImageResponse(response *http.Response) error {
	if strings.HasPrefix(strings.ToLower(response.Header.Get("Content-Type")), "text/event-stream") {
		return errors.New("streaming image responses are not supported")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, imageResponseLimit+1))
	if err != nil || len(raw) > imageResponseLimit {
		return errors.New("image response exceeds limit or is incomplete")
	}
	response.Body.Close()
	response.Body = io.NopCloser(bytes.NewReader(raw))
	return nil
}

func imageResponseMetadata(raw []byte) []byte {
	var body map[string]any
	if json.Unmarshal(raw, &body) != nil || body == nil {
		return []byte(`{"imageResult":"content not saved","incomplete":true}`)
	}
	images, ok := body["data"].([]any)
	if !ok {
		return raw
	}
	for _, entry := range images {
		image, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		for _, key := range []string{"b64_json", "url"} {
			if value, ok := image[key].(string); ok && value != "[content not saved]" {
				image[key] = "[content not saved]"
				if key == "b64_json" {
					image["encodedLength"] = len(value)
				}
			}
		}
		image["contentSaved"] = false
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return bytes.Clone(raw)
	}
	return encoded
}
