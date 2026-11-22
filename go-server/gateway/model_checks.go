package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	peernet "github.com/newnoa/newnoa-maas/go-server/network"
)

type modelCheck struct {
	NodeID     string `json:"nodeId"`
	OwnerID    string `json:"ownerId"`
	Epoch      int64  `json:"epoch"`
	ProbeRoute string `json:"probeRoute"`
	Path       string `json:"inferencePath"`
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Endpoint   string `json:"endpoint"`
	Model      string `json:"model"`
	Credential string `json:"credential"`
}
type modelMeasurement struct {
	EndpointPingMs            float64 `json:"endpointPingMs,omitempty"`
	EndpointPingStatus        string  `json:"endpointPingStatus,omitempty"`
	EndpointPingError         string  `json:"endpointPingError,omitempty"`
	PrefillSource             string  `json:"prefillSource,omitempty"`
	GenerationSource          string  `json:"generationSource,omitempty"`
	Error                     string  `json:"error,omitempty"`
	Status                    string  `json:"status"`
	NetworkLatencyMs          float64 `json:"networkLatencyMs,omitempty"`
	TtftMs                    float64 `json:"ttftMs,omitempty"`
	PrefillTokensPerSecond    float64 `json:"prefillTokensPerSecond,omitempty"`
	GenerationTokensPerSecond float64 `json:"generationTokensPerSecond,omitempty"`
	InputTokens               int64   `json:"inputTokens,omitempty"`
	OutputTokens              int64   `json:"outputTokens,omitempty"`
}

func modelCheckURL(endpoint, path string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http") {
		return "", fmt.Errorf("invalid model endpoint")
	}
	base := strings.TrimRight(u.Path, "/")
	for _, suffix := range []string{"/chat/completions", "/responses", "/messages", "/embeddings", "/rerank", "/audio/transcriptions", "/systemone", "/images/generations"} {
		base = strings.TrimSuffix(base, suffix)
	}
	if !strings.HasSuffix(base, "/v1") {
		base += "/v1"
	}
	u.Path = base + path
	u.RawPath = ""
	return u.String(), nil
}
func modelProbeURL(job modelCheck, path string) (string, error) {
	u, err := url.Parse(job.ProbeRoute)
	probe := "probe"
	if job.Kind == "publication" {
		probe = "publication-probe"
	}
	if err != nil || !evaluationUUID(job.ID) || !evaluationUUID(job.NodeID) || job.Epoch < 1 || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") || !strings.HasSuffix(u.Path, fmt.Sprintf("/tunnels/%s/%d/%s", job.NodeID, job.Epoch, probe)) {
		return "", fmt.Errorf("invalid provider probe route")
	}
	return strings.TrimRight(job.ProbeRoute, "/") + path, nil
}
func probeBodyDigest(method, path, contentType string, raw []byte) string {
	prefix := method + " " + path + "\n"
	if path == "/v1/audio/transcriptions" {
		prefix += contentType + "\n"
	}
	sum := sha256.Sum256(append([]byte(prefix), raw...))
	return hex.EncodeToString(sum[:])
}
func (g *gateway) pingModelEndpoint(parent context.Context, endpoint string, ping func(context.Context, net.IPAddr) peernet.EndpointPing) peernet.EndpointPing {
	failed := peernet.EndpointPing{Status: "unavailable", Error: "Endpoint ICMP probe blocked by egress policy"}
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return failed
	}
	port := u.Port()
	if port == "" {
		port = "443"
		if u.Scheme == "http" {
			port = "80"
		}
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	pinned, err := safeAddress(ctx, net.JoinHostPort(u.Hostname(), port), g.allow)
	if err != nil {
		return failed
	}
	host, _, err := net.SplitHostPort(pinned)
	if err != nil {
		return failed
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return failed
	}
	return ping(ctx, net.IPAddr{IP: ip})
}
func (g *gateway) checkModel(parent context.Context, job modelCheck) modelMeasurement {
	result := modelMeasurement{Status: "failed", Error: "Model probe could not be completed"}
	if job.Path == "/v1/images/generations" && job.Kind == "benchmark" {
		result.Error = "Image benchmark is not supported"
		return result
	}
	timeout := 10 * time.Second
	if job.Kind == "publication" {
		timeout = 120 * time.Second
	}
	if job.Kind == "benchmark" {
		timeout = 90 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	path := "/models"
	method := http.MethodGet
	var payload []byte
	contentType := "application/json"
	if job.Kind == "publication" {
		if job.Path == "" {
			job.Path = "/v1/chat/completions"
		}
		path = strings.TrimPrefix(job.Path, "/v1")
		method = http.MethodPost
		contentType, payload = modelHealthBody(job)
	}
	if job.Kind == "benchmark" {
		path = "/chat/completions"
		method = http.MethodPost
		payload, _ = json.Marshal(map[string]any{"model": job.Model, "messages": []map[string]string{{"role": "user", "content": "Benchmark " + job.ID + ": read the following context and produce a short paragraph describing it. " + strings.Repeat("the ", 4096)}}, "max_tokens": 128, "stream": true, "stream_options": map[string]bool{"include_usage": true}, "temperature": 0})
	}
	node := job.NodeID != "" || job.ProbeRoute != ""
	var endpoint string
	var err error
	client := g.egress
	if node {
		if !g.ownsNode(job.OwnerID) {
			result.Error = "Provider node is bound to another Go Server or its binding is unavailable"
			return result
		}
		endpoint, err = modelProbeURL(job, "/v1"+path)
		client = g.owner
	} else {
		endpoint, err = modelCheckURL(job.Endpoint, path)
	}
	if err != nil {
		return result
	}
	if job.Kind == "latency" {
		result.EndpointPingStatus = "unavailable"
		result.EndpointPingError = "Endpoint ICMP probe unavailable"
		if !node {
			ping := g.pingModelEndpoint(ctx, job.Endpoint, peernet.PingIP)
			result.EndpointPingMs, result.EndpointPingStatus, result.EndpointPingError = ping.Milliseconds, ping.Status, ping.Error
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if err != nil {
		return result
	}
	if !node && job.Credential != "" && (job.Kind == "health" || job.Path != "/v1/messages") {
		request.Header.Set("Authorization", "Bearer "+job.Credential)
	}
	request.Header.Set("Content-Type", contentType)
	if job.Path == "/v1/messages" && job.Kind != "health" {
		if !node {
			request.Header.Set("x-api-key", job.Credential)
		}
		request.Header.Set("anthropic-version", "2023-06-01")
	}

	if node {
		if err := g.authorizeOwnerRequest(request, job.OwnerID); err != nil {
			result.Error = "Untrusted provider owner destination"
			return result
		}
		var approval struct {
			Permit string `json:"permit"`
		}
		binding := probeBodyDigest(method, "/v1"+path, contentType, payload)
		prepare, prefix := "/model-checks/", "probe_"
		if job.Kind == "publication" {
			prepare, prefix = "/publication-checks/", "pubprobe_"
		}
		if g.control(ctx, prepare+job.ID+"/prepare", map[string]any{"gatewayId": g.id, "bodyDigest": binding, "httpMethod": method, "inferencePath": "/v1" + path}, &approval) != nil || !strings.HasPrefix(approval.Permit, prefix) || len(approval.Permit) > 100 {
			result.Error = "Provider probe permit unavailable"
			return result
		}
		request.Header.Set("X-Newnoa-Request", job.ID)
		request.Header.Set("X-Newnoa-Permit", approval.Permit)
		request.Header.Set("X-Newnoa-Gateway", g.id)
		request.Header.Set("X-Newnoa-Epoch", strconv.FormatInt(job.Epoch, 10))
	}
	// A one-use provider permit must not turn an empty GET into a replayable request.
	if node && method == http.MethodGet {
		request.Body = io.NopCloser(bytes.NewReader(payload))
		request.ContentLength = -1
	}
	request.GetBody = nil
	start := time.Now()
	response, err := client.Do(request)
	if err != nil {
		var networkError net.Error
		if job.Kind == "publication" && (errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.As(err, &networkError) && networkError.Timeout()) {
			result.Error = "Protocol probe timed out"
		}
		return result
	}
	defer response.Body.Close()
	if node && job.Kind == "publication" && response.Header.Get("X-Newnoa-Probe-Upstream") != "1" {
		result.Error = "Protocol probe returned invalid response"
		return result
	}
	networkLatencyMs := float64(time.Since(start)) / float64(time.Millisecond)
	if job.Kind == "latency" && node {
		pingElapsed, _ := strconv.ParseFloat(response.Header.Get("X-Newnoa-Probe-Ping-Elapsed-Ms"), 64)
		if pingElapsed > 0 && pingElapsed <= 3000 && pingElapsed < networkLatencyMs {
			networkLatencyMs -= pingElapsed
		}
		pingStatus := response.Header.Get("X-Newnoa-Probe-Ping-Status")
		pingMs, _ := strconv.ParseFloat(response.Header.Get("X-Newnoa-Probe-Ping-Ms"), 64)
		switch pingStatus {
		case "succeeded":
			if pingMs > 0 && pingMs <= 2000 {
				result.EndpointPingMs, result.EndpointPingStatus, result.EndpointPingError = pingMs, pingStatus, ""
			}
		case "unreachable", "permission-denied", "unavailable":
			result.EndpointPingStatus = pingStatus
			result.EndpointPingError = "Endpoint ICMP probe " + pingStatus
		}
	}
	if job.Kind == "latency" && node && response.Header.Get("X-Newnoa-Probe-Upstream") != "1" {
		result.Error = "Provider probe did not reach the model endpoint"
		return result
	}
	if job.Kind == "latency" && response.StatusCode < 500 {
		result.Status = "succeeded"
		result.Error = ""
		result.NetworkLatencyMs = networkLatencyMs
		return result
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		result.Error = fmt.Sprintf("Upstream HTTP %d", response.StatusCode)
		if job.Kind == "publication" {
			switch response.StatusCode {
			case 401, 403:
				result.Error = "Protocol probe authentication failed"
			case 429:
				result.Error = "Protocol probe rate limited"
			case 404, 405, 501:
				result.Error = "Protocol probe unsupported"
			}
		}
		return result
	}
	if job.Kind != "benchmark" {
		if job.Kind == "publication" {
			result.Error = "Protocol probe returned invalid response"
		}
		limit := maxBody
		if job.Path == "/v1/images/generations" && job.Kind != "health" {
			limit = maxImageResponse
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, int64(limit)+1))
		if err != nil || len(data) > limit {
			if job.Kind == "publication" && errors.Is(ctx.Err(), context.DeadlineExceeded) {
				result.Error = "Protocol probe timed out"
			}
			return result
		}
		if validModelHealth(job, data) {
			result.Status = "succeeded"
			result.Error = ""
			result.NetworkLatencyMs = float64(time.Since(start)) / float64(time.Millisecond)
		}
		return result
	}
	if !strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
		result.Error = "Upstream did not return a token stream"
		return result
	}
	scanner := bufio.NewScanner(io.LimitReader(response.Body, 4*maxBody))
	scanner.Buffer(make([]byte, 4096), maxBody)
	var proof evidence
	var first, last time.Time
	var nativePrefill, nativeGeneration float64
	for scanner.Scan() {
		line := scanner.Text()
		proof.feed([]byte(line + "\n"))
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var payload struct {
			Choices []struct {
				Delta struct {
					Content   string `json:"content"`
					Reasoning string `json:"reasoning_content"`
				} `json:"delta"`
			} `json:"choices"`
			Timings struct {
				Prefill    float64 `json:"prompt_per_second"`
				Generation float64 `json:"predicted_per_second"`
			} `json:"timings"`
		}
		if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &payload) != nil {
			continue
		}
		for _, choice := range payload.Choices {
			if choice.Delta.Content != "" || choice.Delta.Reasoning != "" {
				last = time.Now()
				if first.IsZero() {
					first = last
				}
			}
		}
		if payload.Timings.Prefill > 0 {
			nativePrefill = payload.Timings.Prefill
		}
		if payload.Timings.Generation > 0 {
			nativeGeneration = payload.Timings.Generation
		}
	}
	if scanner.Err() != nil || !proof.complete() || first.IsZero() || proof.input == nil || proof.output == nil || *proof.input <= 0 || *proof.output <= 0 {
		result.Error = fmt.Sprintf("Incomplete token stream or usage (complete=%t, tokenSeen=%t, usage=%t)", proof.complete(), !first.IsZero(), proof.input != nil && proof.output != nil)
		return result
	}
	result.InputTokens = *proof.input
	result.OutputTokens = *proof.output
	result.TtftMs = float64(first.Sub(start)) / float64(time.Millisecond)
	// End-to-end fallback includes transport time; native server timings take precedence.
	result.PrefillSource = "upstream"
	result.GenerationSource = "upstream"
	result.PrefillTokensPerSecond = nativePrefill
	if nativePrefill == 0 {
		result.PrefillSource = "end-to-end-estimate"
		result.PrefillTokensPerSecond = float64(result.InputTokens) / first.Sub(start).Seconds()
	}
	result.GenerationTokensPerSecond = nativeGeneration
	if nativeGeneration == 0 && last.After(first) && result.OutputTokens > 1 {
		result.GenerationSource = "end-to-end-estimate"
		result.GenerationTokensPerSecond = float64(result.OutputTokens-1) / last.Sub(first).Seconds()
	}
	if result.GenerationTokensPerSecond <= 0 {
		return modelMeasurement{Status: "failed", Error: "Insufficient token timing data to measure generation speed"}
	}
	result.Status = "succeeded"
	result.Error = ""
	return result
}
func (g *gateway) runModelChecks(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	slots := make(chan struct{}, 8)
	for {
		for _, checkPath := range []string{"/publication-checks/", "/model-checks/"} {
			available := cap(slots) - len(slots)
			if available == 0 {
				break
			}
			var jobs []modelCheck
			if g.control(ctx, checkPath+"claim", map[string]any{"gatewayId": g.id, "limit": available}, &jobs) == nil {
				for _, job := range jobs {
					if len(slots) == cap(slots) {
						break
					}
					slots <- struct{}{}
					go func(job modelCheck, path string) {
						defer func() { <-slots }()
						measurement := g.checkModel(ctx, job)
						raw, _ := json.Marshal(measurement)
						var report map[string]any
						_ = json.Unmarshal(raw, &report)
						report["gatewayId"] = g.id
						if path == "/publication-checks/" {
							g.reportPublicationCheck(ctx, path+job.ID+"/result", report)
							return
						}
						finishCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
						defer cancel()
						_ = g.control(finishCtx, path+job.ID+"/result", report, nil)
					}(job, checkPath)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (g *gateway) reportPublicationCheck(parent context.Context, path string, report map[string]any) {
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	for attempt := 0; attempt < 4; attempt++ {
		err := g.control(ctx, path, report, nil)
		if err == nil || ctx.Err() != nil {
			return
		}
		var failure *controlError
		if errors.As(err, &failure) && failure.status < 500 && failure.status != 408 && failure.status != 429 {
			return
		}
		if attempt == 3 {
			return
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * 250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
