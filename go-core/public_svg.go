package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

func (c *core) publicSVGAccount(session string) (string, error) {
	return c.publicSVGAccountContext(context.Background(), session)
}

func (c *core) publicSVGAccountContext(parent context.Context, session string) (string, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	var account struct {
		ID string `json:"id"`
	}
	if err := c.controlRemote(ctx, "/me", "Bearer "+session, nil, &account); err != nil {
		return "", err
	}
	if !nodePattern.MatchString(account.ID) {
		return "", errors.New("Login required")
	}
	return account.ID, nil
}

// A detached website task must still stop when its original session is revoked.
func (c *core) guardPublicSVGSession(ctx context.Context, session string, stop context.CancelFunc) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := c.publicSVGAccountContext(ctx, session); err != nil {
				stop()
				return
			}
		}
	}
}

func (c *core) servePublicSVGCommand(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	method := "svg.tests.list"
	reject := func(status int, message string) {
		writeLocalError(w, status, browserLanguage(r), browserLocalMessage(method, nil, errors.New(message)))
	}
	if r.Method != "POST" || r.URL.RawQuery != "" || r.URL.ForceQuery || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		reject(400, "Invalid SVG test command")
		return
	}
	var command struct {
		Method string                     `json:"method"`
		Params map[string]json.RawMessage `json:"params"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&command) != nil || decoder.Decode(new(any)) != io.EOF || command.Params == nil {
		reject(400, "Invalid SVG test command")
		return
	}
	fields := map[string]map[string]bool{"proxy.status": {}, "svg.tests.questions": {}, "svg.tests.create": {"questionId": true, "keyId": true, "model": true, "submissionId": true, "thinking": true}, "svg.tests.list": {"page": true, "status": true}, "svg.tests.get": {"id": true}, "svg.tests.cancel": {"id": true}, "svg.tests.delete": {"id": true}}
	allowed, ok := fields[command.Method]
	if !ok {
		reject(403, "Local operation unavailable in H5")
		return
	}
	method = command.Method
	for field := range command.Params {
		if !allowed[field] {
			reject(400, "Invalid SVG test command")
			return
		}
	}
	session := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if _, err := c.publicSVGAccount(session); err != nil {
		reject(401, "Login required")
		return
	}
	if command.Method == "proxy.status" {
		json.NewEncoder(w).Encode(map[string]any{"running": true, "runtimeMode": "www"})
		return
	}
	callerIP := ""
	if values := r.Header.Values("X-Newnoa-Caller-IP"); len(values) > 1 || (len(values) == 1 && net.ParseIP(values[0]) == nil) {
		reject(400, "Invalid caller address")
		return
	} else if len(values) == 1 {
		callerIP = net.ParseIP(values[0]).String()
	}
	command.Params["session"], _ = json.Marshal(session)
	raw, _ := json.Marshal(command.Params)
	value, err := c.svgCommandWithCaller(command.Method, raw, callerIP)
	if err != nil {
		reject(400, err.Error())
		return
	}
	json.NewEncoder(w).Encode(value)
}

func (c *core) publicSVGModelTestHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fail := func(status int) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			io.WriteString(w, `{"error":{"message":"SVG model request failed"}}`)
		}
		input, err := decodeModelTest(w, r)
		if err != nil || input.Decision != nil || input.Audio != nil || c.publicSVGInference == nil {
			fail(400)
			return
		}
		var credential struct {
			Secret                string    `json:"secret"`
			ModelType             string    `json:"modelType"`
			ExpiresAt             time.Time `json:"expiresAt"`
			SupportsReasoningNone bool      `json:"supportsReasoningNone"`
		}
		if c.controlRemote(r.Context(), "/me/model-tests/credential", r.Header.Get("Authorization"), map[string]string{"keyId": input.KeyID, "model": input.Model}, &credential) != nil || credential.ModelType != "llm" || !credential.ExpiresAt.After(time.Now()) || !modelTestCredentialPattern.MatchString(credential.Secret) {
			fail(403)
			return
		}
		payload := map[string]any{"model": input.Model, "messages": input.Messages, "max_tokens": input.MaxTokens, "stream": true, "stream_options": map[string]bool{"include_usage": true}}
		if credential.SupportsReasoningNone && (input.Thinking == nil || !*input.Thinking) {
			payload["reasoning_effort"] = "none"
		}
		raw, _ := json.Marshal(payload)
		request, _ := http.NewRequestWithContext(r.Context(), "POST", "http://localhost/v1/chat/completions", bytes.NewReader(raw))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+credential.Secret)
		request.Header.Set("X-Newnoa-Caller-Session", strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		request.Header.Set("Idempotency-Key", input.RequestID)
		request.Header.Set("X-Newnoa-Agent", "NEWNOA SVG Test")
		if caller := r.Header.Get("X-Newnoa-Caller-IP"); caller != "" {
			request.Header.Set("X-Newnoa-Caller-IP", caller)
		}
		c.publicSVGInference.ServeHTTP(w, request)
	})
}
