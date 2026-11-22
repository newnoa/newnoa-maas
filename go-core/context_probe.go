package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

var contextKeys = map[string]bool{"n_ctx": true, "context_length": true, "max_model_len": true, "num_ctx": true, "max_context_length": true}

func probeContext(endpoint, method, apiKey, model string) (map[string]any, error) {
	return probeContextWithPolicy(endpoint, method, apiKey, model, supplyPolicy{edition: "domestic"})
}
func probeContextWithPolicy(endpoint, method, apiKey, model string, policy supplyPolicy) (map[string]any, error) {
	u, err := policy.endpoint(strings.TrimRight(endpoint, "/"))
	if err != nil {
		return nil, err
	}
	// Accept either a service root or its OpenAI-compatible API base. Keep
	// reverse-proxy prefixes while avoiding /v1/v1/models and /v1/props.
	u.Path = strings.TrimSuffix(modelServiceBase(u.Path), "/v1")
	u.RawPath = ""
	client := policy.client(5 * time.Second)
	defer client.CloseIdleConnections()
	// Model-capability endpoints cannot establish a loaded runtime context.
	paths := []string{"/props", "/v1/models"}
	if strings.EqualFold(method, "ollama") {
		// /api/show reports model capability, not the loaded runner's context.
		paths = []string{"/api/ps"}
	}
	for _, path := range paths {
		target := *u
		target.Path = strings.TrimRight(target.Path, "/") + path
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, target.String(), nil)
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		res, e := client.Do(req)
		if e != nil {
			continue
		}
		const metadataLimit = 256 * 1024
		content, readErr := io.ReadAll(io.LimitReader(res.Body, metadataLimit+1))
		res.Body.Close()
		if res.StatusCode < 200 || res.StatusCode >= 300 || readErr != nil || len(content) > metadataLimit {
			continue
		}
		var body any
		if json.Unmarshal(content, &body) != nil {
			continue
		}
		finder := findContext
		if path == "/api/ps" {
			finder = func(v any) (int64, string) { return findOllamaRunningContext(v, model) }
		}
		if path == "/v1/models" {
			finder = func(v any) (int64, string) { return findContextForModel(v, model) }
		}
		if value, key := finder(body); value > 0 {
			return map[string]any{"detected": true, "contextLength": value, "source": method + path, "field": key, "model": model}, nil
		}
	}
	return map[string]any{"detected": false, "source": "none", "model": model, "reason": "service metadata did not expose a context length"}, nil
}

func findOllamaRunningContext(value any, model string) (int64, string) {
	root, ok := value.(map[string]any)
	if !ok || model == "" {
		return 0, ""
	}
	items, _ := root["models"].([]any)
	// A registry port before the last slash is not a model tag.
	hasTag := strings.LastIndex(model, ":") > strings.LastIndex(model, "/")
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for _, field := range []string{"name", "model"} {
			name, _ := m[field].(string)
			if name == model || (!hasTag && name == model+":latest") {
				return findContext(map[string]any{"context_length": m["context_length"]})
			}
		}
	}
	return 0, ""
}

func isLoopbackHost(host string) bool {
	ip := net.ParseIP(host)
	return host == "localhost" || (ip != nil && ip.IsLoopback())
}

func findContext(value any) (int64, string) {
	switch v := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.SliceStable(keys, func(i, j int) bool {
			if contextKeyRank(keys[i]) == contextKeyRank(keys[j]) {
				return keys[i] < keys[j]
			}
			return contextKeyRank(keys[i]) < contextKeyRank(keys[j])
		})
		for _, key := range keys {
			child := v[key]
			if contextKeyRank(key) < 100 {
				switch n := child.(type) {
				case float64:
					if n > 0 && n <= 2_000_000 && n == float64(int64(n)) {
						return int64(n), key
					}
				case json.Number:
					if i, e := strconv.ParseInt(string(n), 10, 64); e == nil && i > 0 && i <= 2_000_000 {
						return i, key
					}
				case string:
					if i, e := strconv.ParseInt(n, 10, 64); e == nil && i > 0 && i <= 2_000_000 {
						return i, key
					}
				}
			}
		}
		// Prefer valid fields on this object before searching nested metadata.
		for _, key := range keys {
			if n, key := findContext(v[key]); n > 0 {
				return n, key
			}
		}
	case []any:
		for _, child := range v {
			if n, key := findContext(child); n > 0 {
				return n, key
			}
		}
	}
	return 0, ""
}

func findContextForModel(value any, model string) (int64, string) {
	if model == "" {
		return 0, ""
	}
	root, ok := value.(map[string]any)
	if !ok {
		return 0, ""
	}
	if items, ok := root["data"].([]any); ok {
		for _, item := range items {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			for _, field := range []string{"id", "name", "model"} {
				if name, ok := m[field].(string); ok && name != "" {
					if name == model {
						return findContext(m)
					}
					break
				}
			}
		}
		return 0, ""
	}
	return 0, ""
}

func contextKeyRank(key string) int {
	normalized := strings.ToLower(strings.TrimSpace(key))
	if contextKeys[normalized] {
		return map[string]int{"n_ctx": 0, "context_length": 1, "max_model_len": 2, "num_ctx": 3, "max_context_length": 4}[normalized]
	}
	for suffix, rank := range map[string]int{".n_ctx": 0, ".context_length": 1, ".max_model_len": 2, ".num_ctx": 3, ".max_context_length": 4} {
		if strings.HasSuffix(normalized, suffix) {
			return rank
		}
	}
	return 100
}
