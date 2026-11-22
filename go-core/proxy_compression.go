package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/tiktoken-go/tokenizer"
	"newnoa/desktop/core/internal/jsonsplice"
	"newnoa/desktop/core/internal/toolcompact"
)

const compressionBodyLimit = 1 << 20
const compressionTextLimit = 256 << 10

var compressionSurrogate = regexp.MustCompile(`(?i)\\ud[89a-f][0-9a-f]{2}`)

var compressionSlots = make(chan struct{}, 2)
var compressionTokenOnce sync.Once
var compressionCodec tokenizer.Codec

func countCompressionTokens(data []byte) int {
	compressionTokenOnce.Do(func() { compressionCodec, _ = tokenizer.Get(tokenizer.O200kBase) })
	if compressionCodec == nil {
		return -1
	}
	count, err := compressionCodec.Count(string(data))
	if err != nil {
		return -1
	}
	return count
}

type compressionPolicyContext struct{}

type persistedCompression struct {
	Enabled  *bool  `json:"enabled"`
	Revision uint64 `json:"revision"`
}

type proxyCompressionStatus struct {
	Enabled   bool   `json:"enabled"`
	Available bool   `json:"available"`
	Revision  uint64 `json:"revision"`
	Error     string `json:"error,omitempty"`
}

func (c *core) compressionPath() string {
	return filepath.Join(c.home, ".newnoa", "proxy-compression.json")
}
func (c *core) compressionStatus() proxyCompressionStatus {
	c.compressionMu.RLock()
	defer c.compressionMu.RUnlock()
	if c.compression == nil {
		return proxyCompressionStatus{Enabled: c.defaultCompressionEnabled(), Available: true, Revision: 1}
	}
	return *c.compression
}
func (c *core) defaultCompressionEnabled() bool {
	return !c.publicStateless && os.Getenv("NEWNOA_PROXY_COMPRESSION_DEFAULT") != "false" && !(os.Getenv("NEWNOA_ENV") == "prd" && !dockerRuntime()) && os.Getenv("NEWNOA_PUBLIC_PROXY_ENABLED") != "true"
}
func (c *core) loadCompression() {
	path := c.compressionPath()
	err := c.safeConfigFile(path)
	var raw []byte
	if err == nil {
		raw, err = os.ReadFile(path)
		if os.IsNotExist(err) {
			return
		}
	}
	var saved persistedCompression
	var fields map[string]json.RawMessage
	exact := json.Unmarshal(raw, &fields) == nil && len(fields) == 2 && fields["enabled"] != nil && fields["revision"] != nil
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err != nil || !exact || len(raw) > 512 || !strictCompressionJSON(raw, 8) || decoder.Decode(&saved) != nil || saved.Enabled == nil || saved.Revision == 0 {
		c.compression = &proxyCompressionStatus{Enabled: true, Available: false, Revision: 1, Error: "Compression settings are unavailable; save the setting again"}
		return
	}
	c.compression = &proxyCompressionStatus{Enabled: *saved.Enabled, Available: true, Revision: saved.Revision}
}
func (c *core) configureCompression(raw json.RawMessage) (proxyCompressionStatus, error) {
	var values map[string]json.RawMessage
	if len(raw) > 128 || !strictCompressionJSON(raw, 4) || json.Unmarshal(raw, &values) != nil || len(values) != 1 {
		return c.compressionStatus(), errors.New("invalid compression settings")
	}
	var enabled bool
	if string(values["enabled"]) == "null" || json.Unmarshal(values["enabled"], &enabled) != nil {
		return c.compressionStatus(), errors.New("invalid compression settings")
	}
	c.compressionMu.Lock()
	defer c.compressionMu.Unlock()
	old := proxyCompressionStatus{Enabled: true, Available: true, Revision: 1}
	if c.compression != nil {
		old = *c.compression
	}
	next := proxyCompressionStatus{Enabled: enabled, Available: true, Revision: old.Revision + 1}
	if c.safeConfigFile(c.compressionPath()) != nil {
		return old, errors.New("Could not save compression settings")
	}
	data, _ := json.Marshal(persistedCompression{Enabled: &next.Enabled, Revision: next.Revision})
	if atomicFile(c.compressionPath(), data) != nil {
		return old, errors.New("Could not save compression settings")
	}
	c.compression = &next
	c.historyEvents.publishService()
	return next, nil
}

// Reject duplicate keys and excessive nesting before selecting any string span.
func strictCompressionJSON(raw []byte, maxDepth int) bool {
	// Preserve surrogate escape text; encoding/json normalizes lone surrogates.
	if !utf8.Valid(raw) || compressionSurrogate.Match(raw) {
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value func(int) bool
	value = func(depth int) bool {
		if depth > maxDepth {
			return false
		}
		token, err := dec.Token()
		if err != nil {
			return false
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return true
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for dec.More() {
				key, err := dec.Token()
				name, ok := key.(string)
				if err != nil || !ok || keys[name] {
					return false
				}
				keys[name] = true
				if !value(depth + 1) {
					return false
				}
			}
			end, err := dec.Token()
			return err == nil && end == json.Delim('}')
		case '[':
			for dec.More() {
				if !value(depth + 1) {
					return false
				}
			}
			end, err := dec.Token()
			return err == nil && end == json.Delim(']')
		}
		return false
	}
	if !value(0) {
		return false
	}
	_, err := dec.Token()
	return err == io.EOF
}

// A matching value also skips safely; decoded tokens cover escaped field names.
func hasCompressionCache(raw []byte) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	for {
		token, err := dec.Token()
		if err != nil {
			return false
		}
		if token == "cache_control" {
			return true
		}
	}
}

func compressProxyPayload(path string, raw []byte) []byte {
	if len(raw) > compressionBodyLimit || !strictCompressionJSON(raw, 64) {
		return raw
	}
	root, ok := jsonsplice.Root(raw)
	if !ok {
		return raw
	}
	if path != "/v1/chat/completions" && path != "/v1/responses" && path != "/v1/messages" {
		return raw
	}
	// Explicit cache boundaries are immutable, including tool results behind them.
	if path == "/v1/messages" && hasCompressionCache(raw) {
		return raw
	}
	var candidates []jsonsplice.Candidate
	var spans []jsonsplice.Span
	examined := 0
	add := func(span jsonsplice.Span) {
		examined++
		if examined > 32 {
			return
		}
		text, ok := jsonsplice.String(raw, span)
		if !ok || len(text) < 512 || len(text) > compressionTextLimit || !strictCompressionJSON([]byte(text), 64) {
			return
		}
		spans = append(spans, span)
	}
	field := func(obj jsonsplice.Span, name string) {
		if span, ok := jsonsplice.Field(raw, obj, name); ok {
			add(span)
		}
	}
	switch path {
	case "/v1/chat/completions", "/v1/messages":
		list, ok := jsonsplice.Field(raw, root, "messages")
		if !ok {
			return raw
		}
		messages, ok := jsonsplice.Elements(raw, list)
		if !ok {
			return raw
		}
		for _, message := range messages {
			role, _ := jsonsplice.StringField(raw, message, "role")
			if path == "/v1/chat/completions" {
				if role == "tool" {
					field(message, "content")
				}
				continue
			}
			if role != "user" {
				continue
			}
			content, ok := jsonsplice.Field(raw, message, "content")
			if !ok {
				continue
			}
			parts, _ := jsonsplice.Elements(raw, content)
			for _, part := range parts {
				kind, _ := jsonsplice.StringField(raw, part, "type")
				if kind != "tool_result" {
					continue
				}
				result, ok := jsonsplice.Field(raw, part, "content")
				if !ok {
					continue
				}
				if raw[result.Start] == '"' {
					add(result)
					continue
				}
				texts, _ := jsonsplice.Elements(raw, result)
				for _, text := range texts {
					kind, _ := jsonsplice.StringField(raw, text, "type")
					if kind == "text" {
						field(text, "text")
					}
				}
			}
		}
	case "/v1/responses":
		input, ok := jsonsplice.Field(raw, root, "input")
		if !ok {
			return raw
		}
		items, _ := jsonsplice.Elements(raw, input)
		for _, item := range items {
			kind, _ := jsonsplice.StringField(raw, item, "type")
			if kind == "function_call_output" {
				field(item, "output")
			}
		}
	}
	if examined > 32 {
		return raw
	}
	var replacements [][]byte
	for _, span := range spans {
		text, _ := jsonsplice.String(raw, span)
		compact := toolcompact.Compact([]byte(text), countCompressionTokens)
		if bytes.Equal(compact, []byte(text)) {
			continue
		}
		candidates = append(candidates, jsonsplice.Candidate{Span: span, Original: []byte(text)})
		replacements = append(replacements, compact)
	}
	out, err := jsonsplice.Replace(raw, candidates, replacements)
	if err != nil {
		return raw
	}
	return out
}

func (c *core) compressInference(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status, pinned := r.Context().Value(compressionPolicyContext{}).(proxyCompressionStatus)
		if !pinned {
			status = c.compressionStatus()
		}
		if r.Method != http.MethodPost || r.Body == nil || r.ContentLength > compressionBodyLimit || r.Header.Get("Content-Encoding") != "" || r.Header.Get("Digest") != "" || r.Header.Get("Content-Digest") != "" || r.Header.Get("Content-MD5") != "" || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") || (r.URL.Path != "/v1/chat/completions" && r.URL.Path != "/v1/responses" && r.URL.Path != "/v1/messages") {
			next.ServeHTTP(w, r)
			return
		}
		select {
		case compressionSlots <- struct{}{}:
		default:
			next.ServeHTTP(w, r)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(r.Body, compressionBodyLimit+1))
		if err != nil {
			<-compressionSlots
			http.Error(w, "Could not read proxy request", http.StatusBadRequest)
			return
		}
		if len(raw) > compressionBodyLimit {
			r.Body = io.NopCloser(io.MultiReader(bytes.NewReader(raw), r.Body))
			<-compressionSlots
			next.ServeHTTP(w, r)
			return
		}
		out := raw
		if status.Enabled && status.Available {
			out = compressProxyPayload(r.URL.Path, raw)
		}
		before, after := -1, -1
		account, _ := r.Context().Value(compressionAccountContext{}).(string)
		if account != "" {
			before = countCompressionTokens(raw)
			after = before
			if !bytes.Equal(raw, out) {
				after = countCompressionTokens(out)
			}
		}
		// Whole-body tokenization may favor the original even when a tool fragment shrinks.
		if before >= 0 && after > before {
			out = raw
			after = before
		}
		<-compressionSlots
		setInferenceBody(r, out)
		r.Header.Del("Content-Length")
		if account != "" && before > 0 && after >= 0 {
			// Statistics are best effort; a local database failure must not block inference.
			_ = c.recordCompressionStats(r.Context(), account, before, after, time.Now())
		}
		next.ServeHTTP(w, r)
	})
}

// Pin before history capture, then compress before hosted authorization/body proof.
func (c *core) localInference(next http.Handler) http.Handler {
	handler := c.recordInference(c.compressInference(next))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), compressionPolicyContext{}, c.compressionStatus()))
		handler.ServeHTTP(w, r)
	})
}
