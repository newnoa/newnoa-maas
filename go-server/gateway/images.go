package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
)

func imagePath(path string) bool {
	return path == "/v1/images/generations" || path == "/v1/images/edits"
}

const maxImageResponse = 32 << 20

type imageResponseTransport struct{ regular, images, execution http.RoundTripper }

func (t imageResponseTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Context().Value(executionContextKey{}) == true {
		return t.execution.RoundTrip(r)
	}
	if strings.HasSuffix(r.URL.Path, "/images/generations") || strings.HasSuffix(r.URL.Path, "/images/edits") {
		return t.images.RoundTrip(r)
	}
	return t.regular.RoundTrip(r)
}

func withImageResponseTimeout(transport *http.Transport) http.RoundTripper {
	images := transport.Clone()
	images.ResponseHeaderTimeout = 120 * time.Second
	execution := transport.Clone()
	execution.ResponseHeaderTimeout = maxExecutionTimeout + time.Minute
	return imageResponseTransport{regular: transport, images: images, execution: execution}
}

func (t imageResponseTransport) CloseIdleConnections() {
	for _, transport := range []http.RoundTripper{t.regular, t.images, t.execution} {
		if closer, ok := transport.(interface{ CloseIdleConnections() }); ok {
			closer.CloseIdleConnections()
		}
	}
}

func validImageInput(body map[string]any) bool {
	prompt, ok := body["prompt"].(string)
	if !ok || strings.TrimSpace(prompt) == "" {
		return false
	}
	if value, exists := body["n"]; exists {
		n, ok := value.(float64)
		if !ok || n < 1 || n > 10 || math.Trunc(n) != n {
			return false
		}
	}
	if value, exists := body["stream"]; exists && value != false {
		return false
	}
	return true
}

func validImages(data []byte) bool {
	var result struct {
		Data []struct {
			Base64 string `json:"b64_json"`
			URL    string `json:"url"`
		} `json:"data"`
		Error json.RawMessage `json:"error"`
	}
	if len(data) > maxImageResponse || json.Unmarshal(data, &result) != nil || len(result.Error) > 0 && string(result.Error) != "null" || len(result.Data) == 0 || len(result.Data) > 10 {
		return false
	}
	for _, item := range result.Data {
		if item.Base64 != "" {
			raw, err := base64.StdEncoding.Strict().DecodeString(item.Base64)
			if err != nil || !validImageBytes(raw) {
				return false
			}
		} else {
			u, err := url.Parse(item.URL)
			if err != nil || len(item.URL) > 8192 || strings.IndexFunc(item.URL, unicode.IsControl) >= 0 || u.Hostname() == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http") {
				return false
			}
		}
	}
	return true
}

func validImageBytes(raw []byte) bool {
	// shortcut: WebP uses a bounded RIFF check; add a decoder if pixel validation is required.
	if http.DetectContentType(raw) == "image/webp" {
		return len(raw) >= 30 && int64(binary.LittleEndian.Uint32(raw[4:8]))+8 == int64(len(raw))
	}
	config, _, err := image.DecodeConfig(bytes.NewReader(raw))
	return err == nil && config.Width > 0 && config.Height > 0 && config.Width <= 32768 && config.Height <= 32768 && int64(config.Width)*int64(config.Height) <= 100_000_000
}

func imageHistoryResponse(raw string) string {
	var result map[string]any
	if json.Unmarshal([]byte(raw), &result) != nil {
		return "[image response not saved]"
	}
	items, _ := result["data"].([]any)
	for _, value := range items {
		if item, ok := value.(map[string]any); ok {
			delete(item, "b64_json")
			delete(item, "url")
			item["contentSaved"] = false
		}
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return "[image response not saved]"
	}
	return string(encoded)
}
