package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func evaluationUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return err == nil
}

func evaluationScore(sample, content string) bool {
	switch sample {
	case "greeting":
		return strings.TrimSpace(content) == "OK"
	case "arithmetic":
		return strings.TrimSpace(content) == "2"
	case "structured-output":
		decoder := json.NewDecoder(strings.NewReader(content))
		start, e1 := decoder.Token()
		key, e2 := decoder.Token()
		value, e3 := decoder.Token()
		end, e4 := decoder.Token()
		_, tail := decoder.Token()
		return e1 == nil && e2 == nil && e3 == nil && e4 == nil && start == json.Delim('{') && key == "ok" && value == true && end == json.Delim('}') && tail == io.EOF
	}
	return false
}

func (g *gateway) evaluation(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if len(g.evalToken) < 32 || g.evalToken == g.token {
		fail(w, 503, "Evaluation execution disabled")
		return
	}
	header := r.Header.Get("Authorization")
	if len(header) > 512 || !strings.HasPrefix(header, "Bearer ") || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(header, "Bearer ")), []byte(g.evalToken)) != 1 {
		fail(w, 401, "Evaluation worker credential required")
		return
	}
	if r.Method != "POST" {
		fail(w, 405, "POST required")
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 4 || !evaluationUUID(parts[2]) || (parts[3] != "greeting" && parts[3] != "arithmetic" && parts[3] != "structured-output") {
		fail(w, 404, "Unknown evaluation route")
		return
	}
	if r.URL.RawQuery != "" {
		fail(w, 400, "Evaluation query parameters are not accepted")
		return
	}
	var input struct {
		Worker  string `json:"workerId"`
		Fence   int64  `json:"fencingToken"`
		Request string `json:"requestId"`
		Attempt string `json:"attemptId"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || strings.TrimSpace(input.Worker) == "" || len(input.Worker) > 80 || input.Fence < 1 || !evaluationUUID(input.Request) || !evaluationUUID(input.Attempt) {
		fail(w, 400, "Invalid evaluation identity")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	var dispatch struct {
		Request     string  `json:"requestId"`
		Attempt     string  `json:"attemptId"`
		Purpose     string  `json:"purpose"`
		Permit      string  `json:"permit"`
		Endpoint    string  `json:"endpoint"`
		Body        string  `json:"body"`
		Digest      string  `json:"bodyDigest"`
		Reserve     int64   `json:"reserveTokens"`
		NodeID      *string `json:"nodeId"`
		OwnerID     string  `json:"ownerId"`
		Epoch       int64   `json:"epoch"`
		Route       string  `json:"route"`
		Path        string  `json:"inferencePath"`
		Encoding    string  `json:"bodyEncoding"`
		ContentType string  `json:"contentType"`
	}
	identity := map[string]any{"workerId": input.Worker, "fencingToken": input.Fence, "requestId": input.Request, "attemptId": input.Attempt, "gatewayId": g.id}
	if err := g.control(ctx, "/evaluations/"+parts[2]+"/"+parts[3]+"/dispatch", identity, &dispatch); err != nil {
		controlFailure(w, err)
		return
	}
	if dispatch.Path == "" {
		dispatch.Path = "/v1/chat/completions"
	}
	if (dispatch.Path != "/v1/chat/completions" && dispatch.Path != "/v1/embeddings" && dispatch.Path != "/v1/rerank" && dispatch.Path != "/v1/systemone" && dispatch.Path != "/v1/audio/transcriptions") || dispatch.Request != input.Request || dispatch.Attempt != input.Attempt || dispatch.Purpose != "evaluation" || dispatch.Reserve < 64 || dispatch.Reserve > 65536 || len(dispatch.Body) > 8192 {
		fail(w, 502, "Invalid evaluation permit")
		return
	}
	auth := map[string]any{"permit": dispatch.Permit, "gatewayId": g.id, "bodyDigest": dispatch.Digest}
	state := "failed"
	var usedInput, usedOutput int64
	var response []byte
	settled := false
	finish := func() (string, error) {
		hash := sha256.Sum256(response)
		body := map[string]any{"permit": dispatch.Permit, "gatewayId": g.id, "bodyDigest": dispatch.Digest, "status": state, "inputTokens": usedInput, "outputTokens": usedOutput, "artifactDigest": hex.EncodeToString(hash[:])}
		finishCtx, finishCancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer finishCancel()
		var result struct {
			Digest string `json:"resultDigest"`
		}
		err := g.control(finishCtx, "/evaluations/"+dispatch.Request+"/finalize", body, &result)
		return result.Digest, err
	}
	defer func() {
		if !settled {
			if _, err := finish(); err != nil {
				log.Printf("evaluation_finalize_pending request=%s", dispatch.Request)
			}
		}
	}()
	requestBody := []byte(dispatch.Body)
	contentType := "application/json"
	if dispatch.Path == "/v1/audio/transcriptions" {
		var decodeErr error
		requestBody, decodeErr = base64.StdEncoding.Strict().DecodeString(dispatch.Body)
		if dispatch.Encoding != "base64" || dispatch.ContentType != "multipart/form-data; boundary=newnoa-asr-smoke-v1" || decodeErr != nil {
			fail(w, 502, "Invalid ASR evaluation encoding")
			return
		}
		contentType = dispatch.ContentType
		if _, err := parseTranscription(requestBody, contentType); err != nil {
			fail(w, 502, "Invalid ASR evaluation body")
			return
		}
	} else if dispatch.Encoding != "" || dispatch.ContentType != "" {
		fail(w, 502, "Unexpected evaluation encoding")
		return
	}
	bound := requestBody
	if dispatch.Path != "/v1/chat/completions" {
		prefix := "POST " + dispatch.Path + "\n"
		if dispatch.Path == "/v1/audio/transcriptions" {
			prefix += contentType + "\n"
		}
		bound = append([]byte(prefix), requestBody...)
	}
	sum := sha256.Sum256(bound)
	if hex.EncodeToString(sum[:]) != dispatch.Digest {
		fail(w, 502, "Evaluation body changed")
		return
	}
	endpoint := dispatch.Endpoint
	if dispatch.NodeID != nil {
		if !g.ownsNode(dispatch.OwnerID) {
			fail(w, 503, "Evaluation node is bound to another Go Server or its binding is unavailable")
			return
		}
		if !evaluationUUID(*dispatch.NodeID) || dispatch.Epoch < 1 || dispatch.Route == "" {
			fail(w, 502, "Invalid evaluation node scope")
			return
		}
		endpoint = dispatch.Route
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		fail(w, 502, "Invalid evaluation endpoint")
		return
	}
	port := u.Port()
	if port == "" {
		port = "443"
		if u.Scheme == "http" {
			port = "80"
		}
	}
	if dispatch.NodeID == nil {
		if _, err = safeAddress(ctx, net.JoinHostPort(u.Hostname(), port), g.allow); err != nil {
			fail(w, 502, "Evaluation egress rejected")
			return
		}
	}
	requestURL, err := upstreamURL(endpoint, dispatch.Path)
	if err != nil {
		fail(w, 502, "Invalid evaluation request")
		return
	}
	up, err := http.NewRequestWithContext(ctx, "POST", requestURL, bytes.NewReader(requestBody))
	if err != nil {
		fail(w, 502, "Invalid evaluation request")
		return
	}
	up.Header.Set("Content-Type", contentType)
	up.GetBody = nil // Never replay a consumed sample after an ambiguous transport failure.
	transport := g.egress
	if dispatch.NodeID != nil {
		transport = g.owner
		if err := g.authorizeOwnerRequest(up, dispatch.OwnerID); err != nil {
			fail(w, 502, "Untrusted evaluation owner destination")
			return
		}
		up.Header.Set("X-Newnoa-Request", dispatch.Request)
		up.Header.Set("X-Newnoa-Permit", dispatch.Permit)
		up.Header.Set("X-Newnoa-Gateway", g.id)
		up.Header.Set("X-Newnoa-Epoch", strconv.FormatInt(dispatch.Epoch, 10))
	} else if err = g.control(ctx, "/evaluations/"+dispatch.Request+"/consume", auth, nil); err != nil {
		controlFailure(w, err)
		return
	}
	state = "unknown"
	res, err := transport.Do(up)
	if err != nil {
		fail(w, 502, "Evaluation outcome unknown")
		return
	}
	defer res.Body.Close()
	response, err = io.ReadAll(io.LimitReader(res.Body, 65537))
	if err != nil || len(response) > 65536 || res.StatusCode < 200 || res.StatusCode >= 300 {
		fail(w, 502, "Evaluation response incomplete")
		return
	}
	outputText := ""
	if dispatch.Path == "/v1/audio/transcriptions" {
		var ok bool
		usedInput, usedOutput, ok = evaluationTranscriptionUsage(response, dispatch.Reserve)
		if !ok {
			fail(w, 502, "ASR evaluation protocol or usage evidence incomplete")
			return
		}
		state = "succeeded"
	} else if dispatch.Path == "/v1/systemone" {
		var request map[string]any
		var result struct {
			Usage *struct {
				Input  *int64          `json:"prompt_tokens"`
				Output *int64          `json:"completion_tokens"`
				Total  json.RawMessage `json:"total_tokens"`
			} `json:"usage"`
		}
		if !utf8.Valid(response) || json.Unmarshal([]byte(dispatch.Body), &request) != nil || json.Unmarshal(response, &result) != nil || result.Usage == nil {
			fail(w, 502, "Evaluation protocol evidence incomplete")
			return
		}
		questions, ok := decisionInput(request)
		if !ok || !validDecision(response, questions) || result.Usage.Input == nil || result.Usage.Output == nil {
			fail(w, 502, "Evaluation protocol or usage evidence incomplete")
			return
		}
		in, out := *result.Usage.Input, *result.Usage.Output
		if in < 0 || out < 0 || in > dispatch.Reserve || out > dispatch.Reserve-in {
			fail(w, 502, "Evaluation usage evidence invalid")
			return
		}
		if len(result.Usage.Total) > 0 {
			var total *int64
			if json.Unmarshal(result.Usage.Total, &total) != nil || total == nil || *total != in+out {
				fail(w, 502, "Evaluation usage evidence invalid")
				return
			}
		}
		usedInput, usedOutput, state = in, out, "succeeded"
	} else if dispatch.Path == "/v1/embeddings" || dispatch.Path == "/v1/rerank" {
		var request map[string]any
		var result struct {
			Usage *struct {
				Prompt *int64 `json:"prompt_tokens"`
				Total  *int64 `json:"total_tokens"`
			} `json:"usage"`
		}
		if !utf8.Valid(response) || json.Unmarshal([]byte(dispatch.Body), &request) != nil || json.Unmarshal(response, &result) != nil || result.Usage == nil {
			fail(w, 502, "Evaluation protocol evidence incomplete")
			return
		}
		valid := validEmbeddings(response, embeddingInput(request["input"]), 0)
		if dispatch.Path == "/v1/rerank" {
			documents, count, ok := rerankInput(request)
			valid = ok && validRerank(response, documents, count)
		}
		if !valid {
			fail(w, 502, "Evaluation protocol evidence incomplete")
			return
		}
		if result.Usage.Prompt == nil || result.Usage.Total == nil {
			fail(w, 502, "Evaluation usage evidence invalid")
			return
		}
		in, total := *result.Usage.Prompt, *result.Usage.Total
		if in < 0 || in > dispatch.Reserve || total != in {
			fail(w, 502, "Evaluation usage evidence invalid")
			return
		}
		usedInput, usedOutput = in, 0
		state = "succeeded"
	} else {
		var result struct {
			Choices []struct {
				FinishReason string `json:"finish_reason"`
				Message      struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"choices"`
			Usage *struct {
				Input  json.Number `json:"prompt_tokens"`
				Output json.Number `json:"completion_tokens"`
			} `json:"usage"`
		}
		if !utf8.Valid(response) || json.Unmarshal(response, &result) != nil || len(result.Choices) != 1 || result.Usage == nil {
			fail(w, 502, "Evaluation protocol evidence incomplete")
			return
		}
		in, errIn := result.Usage.Input.Int64()
		out, errOut := result.Usage.Output.Int64()
		if errIn != nil || errOut != nil || in < 0 || out < 0 || out > 16 || in > dispatch.Reserve-out {
			fail(w, 502, "Evaluation usage evidence invalid")
			return
		}
		usedInput, usedOutput = in, out
		if result.Choices[0].FinishReason == "" {
			fail(w, 502, "Evaluation termination evidence incomplete")
			return
		}
		state = "failed"
		if result.Choices[0].FinishReason == "stop" && evaluationScore(parts[3], result.Choices[0].Message.Content) {
			state = "succeeded"
		}
		outputText = result.Choices[0].Message.Content
	}
	resultDigest, err := finish()
	settled = true
	if err != nil {
		log.Printf("evaluation_finalize_pending request=%s", dispatch.Request)
		fail(w, 503, "Evaluation settlement pending")
		return
	}
	hash := sha256.Sum256(response)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Request-Id", dispatch.Request)
	json.NewEncoder(w).Encode(map[string]any{"requestId": dispatch.Request, "attemptId": dispatch.Attempt, "status": state, "output": outputText, "responseBody": string(response), "inputTokens": usedInput, "outputTokens": usedOutput, "artifactDigest": hex.EncodeToString(hash[:]), "resultDigest": resultDigest})
}
