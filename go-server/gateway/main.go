package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	peernet "github.com/newnoa/newnoa-maas/go-server/network"
	"github.com/newnoa/newnoa-maas/go-server/network/admission"
	metering "github.com/newnoa/newnoa-maas/go-server/network/metering"
	modelrpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"io"
	"log"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const maxBody = 1 << 20

type evidence struct {
	asr                          bool
	anthropic                    bool
	messageStarted, messageEnded bool
	nativeUsage                  messagesUsage
	nativeBlocks                 map[int]bool
	nativeBlockCount             int
	responses                    bool
	allowOutputLimit             bool
	pending                      []byte
	pendingScanned               int
	event                        []byte
	done                         bool
	input, output, cached        *int64
	write                        *int64
	invalid                      bool
}

func (e *evidence) complete() bool {
	return e.done && !e.invalid && len(e.pending) == 0 && len(e.event) == 0
}

func (e *evidence) feed(p []byte) {
	if e.invalid {
		return
	}
	limit := 65536
	if e.responses {
		// Responses repeats tool schemas and full output in its terminal event.
		limit = 8 * maxBody
	}
	e.pending = append(e.pending, p...)
	for {
		i := bytes.IndexByte(e.pending[e.pendingScanned:], '\n')
		if i < 0 {
			e.pendingScanned = len(e.pending)
			break
		}
		i += e.pendingScanned
		line := strings.TrimSuffix(string(e.pending[:i]), "\r")
		e.pending = e.pending[i+1:]
		e.pendingScanned = 0
		if line == "" {
			if len(e.event) > 0 {
				e.completeEvent(strings.TrimSuffix(string(e.event), "\n"))
				e.event = nil
			}
		} else if strings.HasPrefix(line, "data:") {
			data := strings.TrimPrefix(line, "data:")
			data = strings.TrimPrefix(data, " ")
			e.event = append(e.event, []byte(data)...)
			e.event = append(e.event, '\n')
		}
		if e.invalid || len(e.event) > limit {
			e.invalid = true
			e.done = false
			e.event = nil
			e.pending = nil
			e.pendingScanned = 0
			return
		}
	}
	if len(e.pending)+len(e.event) > limit {
		e.invalid = true
		e.done = false
		e.pending = nil
		e.pendingScanned = 0
		e.event = nil
	}
}
func (e *evidence) completeEvent(data string) {
	if e.asr {
		e.transcriptionEvent(data)
		return
	}
	if e.anthropic {
		e.messagesEvent(data)
		return
	}
	if e.done {
		e.invalid = true
		e.done = false
		return
	}
	if data == "[DONE]" {
		if !e.responses {
			e.done = true
		}
		return
	}
	var envelope struct {
		Type     string          `json:"type"`
		Response json.RawMessage `json:"response"`
		Error    json.RawMessage `json:"error"`
	}
	parsed := e.responses && json.Unmarshal([]byte(data), &envelope) == nil
	if parsed && envelope.Type == metering.ResponsesProviderMetadata && (len(envelope.Error) == 0 || string(envelope.Error) == "null") {
		return
	}
	// Failed/incomplete terminal events can still report real work already performed.
	e.usage([]byte(data))
	if parsed && len(envelope.Response) > 0 {
		e.usage(envelope.Response)
	}
	if e.responses && e.allowOutputLimit {
		var terminal struct {
			Type     string `json:"type"`
			Response struct {
				Status     string `json:"status"`
				Incomplete struct {
					Reason string `json:"reason"`
				} `json:"incomplete_details"`
			} `json:"response"`
		}
		if json.Unmarshal([]byte(data), &terminal) == nil && terminal.Type == "response.incomplete" && terminal.Response.Status == "incomplete" && terminal.Response.Incomplete.Reason == "max_output_tokens" {
			e.done = true
			return
		}
	}
	var payload map[string]any
	if json.Unmarshal([]byte(data), &payload) != nil || payload["error"] != nil || payload["type"] == "error" || payload["type"] == "response.failed" || payload["type"] == "response.incomplete" {
		e.invalid = true
		e.done = false
		return
	}
	var event struct {
		Type     string `json:"type"`
		Response struct {
			Status string          `json:"status"`
			Usage  json.RawMessage `json:"usage"`
		} `json:"response"`
	}
	if e.responses && json.Unmarshal([]byte(data), &event) == nil && event.Type == "response.completed" && event.Response.Status == "completed" {
		e.done = true
	}
}
func (e *evidence) usage(data []byte) {
	var b struct {
		Usage *struct {
			Prompt        *int64          `json:"prompt_tokens"`
			Completion    *int64          `json:"completion_tokens"`
			Input         *int64          `json:"input_tokens"`
			Output        *int64          `json:"output_tokens"`
			Creation      *int64          `json:"cache_creation_input_tokens"`
			PromptDetails json.RawMessage `json:"prompt_tokens_details"`
			InputDetails  json.RawMessage `json:"input_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(data, &b) != nil || b.Usage == nil {
		return
	}
	input, output := b.Usage.Prompt, b.Usage.Completion
	if input == nil && output == nil {
		input, output = b.Usage.Input, b.Usage.Output
	}
	if input != nil && output != nil && *input >= 0 && *input <= 1000000000 && *output >= 0 && *output <= 1000000000 {
		e.input = input
		e.output = output
		details := b.Usage.PromptDetails
		if b.Usage.Prompt == nil && b.Usage.Completion == nil {
			details = b.Usage.InputDetails
		}
		e.cached = nil
		e.write = nil
		var cache struct {
			Cached   *int64 `json:"cached_tokens"`
			Write    *int64 `json:"cache_write_tokens"`
			Creation *int64 `json:"cache_creation_tokens"`
		}
		if len(details) > 0 && json.Unmarshal(details, &cache) != nil {
			return
		}
		if cache.Write == nil {
			cache.Write = cache.Creation
		}
		if cache.Write == nil {
			cache.Write = b.Usage.Creation
		}
		if cache.Write != nil && *cache.Write >= 0 && *cache.Write <= *input {
			e.write = cache.Write
		}
		writeCount := int64(0)
		if e.write != nil {
			writeCount = *e.write
		}
		if cache.Cached != nil && *cache.Cached >= 0 && *cache.Cached+writeCount <= *input {
			e.cached = cache.Cached
		}
	}
}

func validCompletion(data []byte, responses bool) bool {
	var result map[string]any
	if json.Unmarshal(data, &result) != nil || result["error"] != nil {
		return false
	}
	if responses {
		_, ok := result["output"].([]any)
		return ok && result["status"] == "completed"
	}
	choices, ok := result["choices"].([]any)
	if !ok || len(choices) == 0 {
		return false
	}
	for _, choice := range choices {
		item, ok := choice.(map[string]any)
		if !ok {
			return false
		}
		if _, ok := item["message"].(map[string]any); !ok {
			return false
		}
		switch item["finish_reason"] {
		case "stop", "length", "tool_calls", "function_call", "content_filter":
		default:
			return false
		}
	}
	return true
}

type gateway struct {
	balanceRules   balanceRules
	uploads        admission.Budget
	bootID         string
	build          runtimeBuild
	evalToken      string
	allow          []string
	owner          *http.Client
	ownerOrigin    string
	ownerBridge    http.Handler
	api, token, id string
	client         *http.Client
	egress         *http.Client
	imageDownload  *http.Client
	active         atomic.Int64
	modelActive    atomic.Int64
	connectionsMu  sync.Mutex
	connections    map[net.Conn]http.ConnState
	peers          *peernet.Manager
	frp            http.Handler
	frpError       error
}

func validAdvertiseAddress(address string) bool {
	if strings.HasPrefix(address, "https://") {
		u, err := url.Parse(address)
		return err == nil && u.Hostname() != "" && u.User == nil && u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && (u.Path == "" || u.Path == "/") && u.Port() != "0"
	}
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	n, portErr := strconv.Atoi(port)
	return err == nil && portErr == nil && n > 0 && n <= 65535 && ip != nil && (ip.IsPrivate() || ip.IsLoopback())
}

func (g *gateway) reportMembership(ctx context.Context, address string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	resources := &resourceSampler{}
	bootID := g.bootID
	if bootID == "" {
		bootID = uuid.NewString()
	}
	for {
		count := g.modelActive.Load()
		if count < 0 {
			count = 0
		}
		if count > 1_000_000 {
			count = 1_000_000
		}
		membership := map[string]any{"address": address, "activeRequests": count, "healthy": g.build.matches(), "bootId": bootID}
		hostname, measurements := resources.sample()
		activeTCP, idleTCP := g.connectionCounts()
		measurements["tcpActiveConnections"], measurements["tcpIdleConnections"] = activeTCP, idleTCP
		membership["hostname"], membership["resources"] = hostname, measurements
		if owner := os.Getenv("NEWNOA_FRP_OWNER_ID"); owner != "" {
			membership["ownerId"] = owner
		}
		if err := g.control(ctx, "/gateways/"+g.id+"/heartbeat", membership, nil); err != nil && ctx.Err() == nil {
			log.Printf("gateway membership heartbeat failed for %s: %v", g.id, err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

var blockedEgressPrefixes = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

func safeAddress(ctx context.Context, address string, allow []string) (string, error) {
	addresses, err := safeAddresses(ctx, address, allow, net.DefaultResolver.LookupIPAddr)
	if err != nil {
		return "", err
	}
	return addresses[0], nil
}

func safeAddresses(ctx context.Context, address string, allow []string, lookup func(context.Context, string) ([]net.IPAddr, error)) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := lookup(ctx, host)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil || len(ips) == 0 {
		return nil, errors.New("DNS resolution failed")
	}
	allowed := false
	for _, h := range allow {
		if strings.EqualFold(h, host) || strings.EqualFold(h, address) {
			allowed = true
		}
	}
	for _, addr := range ips {
		ip := addr.IP
		parsed, ok := netip.AddrFromSlice(ip)
		if !ok {
			return nil, errors.New("egress address not allowed")
		}
		blocked := false
		for _, prefix := range blockedEgressPrefixes {
			blocked = blocked || prefix.Contains(parsed.Unmap())
		}
		if !allowed && (blocked || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() || !ip.IsGlobalUnicast()) {
			return nil, errors.New("egress address not allowed")
		}
	}
	addresses := make([]string, 0, len(ips))
	for _, ip := range ips {
		addresses = append(addresses, net.JoinHostPort(ip.IP.String(), port))
	}
	return addresses, nil
}
func newGateway(api, token, id string, allow []string) *gateway {
	ownerOrigin := configuredOwnerOrigin()
	frp, frpError := newFRPWebsocketProxy(os.Getenv("NEWNOA_FRP_INTERNAL_URL"))
	ownerBridge := newOwnerTunnelProxy(token, &url.URL{Scheme: "http", Host: "127.0.0.1:18101"})
	transport := &http.Transport{Proxy: nil, MaxIdleConns: 100, MaxIdleConnsPerHost: 20, IdleConnTimeout: 60 * time.Second, ResponseHeaderTimeout: 30 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
		DialContext: safeDialer{allow: allow, timeout: 10 * time.Second}.DialContext}
	return &gateway{frp: frp, frpError: frpError, ownerBridge: ownerBridge, ownerOrigin: ownerOrigin, peers: peernet.NewManager(), evalToken: os.Getenv("NEWNOA_EVALUATOR_TOKEN"), allow: append([]string(nil), allow...), api: strings.TrimRight(api, "/"), token: token, id: id, client: &http.Client{Timeout: 8 * time.Second, Transport: instrumentClient(http.DefaultTransport, true), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, owner: &http.Client{Transport: instrumentClient(withImageResponseTimeout(&http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if strings.EqualFold(host, "localhost") {
			address = net.JoinHostPort("127.0.0.1", port)
		}
		return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, address)
	}, ResponseHeaderTimeout: 30 * time.Second, TLSHandshakeTimeout: 10 * time.Second}), true), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, egress: &http.Client{Transport: instrumentClient(withImageResponseTimeout(transport), false), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func fail(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, native := w.(*messagesWriter); native {
		kind := "api_error"
		switch status {
		case 400:
			kind = "invalid_request_error"
		case 401:
			kind = "authentication_error"
		case 403:
			kind = "permission_error"
		case 404:
			kind = "not_found_error"
		case 413:
			kind = "request_too_large"
		case 429:
			kind = "rate_limit_error"
		}
		json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": map[string]any{"type": kind, "message": message}})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": message, "type": "newnoa_error"}})
}

func writeDownstream(ctx context.Context, w http.ResponseWriter, data []byte, timeout time.Duration) error {
	controller := http.NewResponseController(w)
	deadline := time.Now().Add(timeout)
	if end, ok := ctx.Deadline(); ok && end.Before(deadline) {
		deadline = end
	}
	if err := controller.SetWriteDeadline(deadline); err != nil {
		if !errors.Is(err, http.ErrNotSupported) {
			return err
		}
	} else {
		// Bound downstream writes without timing out an idle upstream read.
		defer controller.SetWriteDeadline(time.Time{})
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return controller.Flush()
}

func downstreamCancelled(ctx context.Context, err error) bool {
	return ctx.Err() == context.Canceled && (errors.Is(err, context.Canceled) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) || grpcstatus.Code(err) == codes.Canceled)
}

type controlError struct {
	status     int
	body       []byte
	retryAfter string
}

func (e *controlError) Error() string { return fmt.Sprintf("control status %d", e.status) }

type proxySessionContext struct{}

func (g *gateway) control(ctx context.Context, path string, body any, out any) error {
	if session, paired := ctx.Value(proxySessionContext{}).(string); paired {
		if fields, ok := body.(map[string]any); ok && fields["secret"] != nil {
			credentials := make(map[string]any, len(fields)+1)
			for key, value := range fields {
				credentials[key] = value
			}
			credentials["session"] = session
			body = credentials
		}
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", g.api+"/api/v1/internal"+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+g.token)
	req.Header.Set("Content-Type", "application/json")
	res, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	b, err := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err != nil || len(b) > maxBody {
		return errors.New("invalid control response")
	}
	if res.StatusCode != 200 {
		return &controlError{status: res.StatusCode, body: b, retryAfter: res.Header.Get("Retry-After")}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(b, out)
}
func (g *gateway) finalize(ctx context.Context, path string, outcome map[string]any) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(time.Duration(attempt) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		attemptTimeout := 2 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			attemptTimeout = min(attemptTimeout, time.Until(deadline)/time.Duration(3-attempt))
		}
		attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
		err = g.control(attemptCtx, path, outcome, nil)
		cancel()
		if err == nil {
			return nil
		}
		var response *controlError
		if errors.As(err, &response) && response.status < 500 && response.status != http.StatusTooManyRequests {
			return err
		}
	}
	return err
}
func controlFailure(w http.ResponseWriter, err error) {
	var e *controlError
	if errors.As(err, &e) {
		status := e.status
		if status >= 500 {
			status = 503
		}
		if (status == 429 || status == 503) && validRetryAfter(e.retryAfter) {
			w.Header().Set("Retry-After", e.retryAfter)
		}
		message := http.StatusText(status)
		if status == http.StatusTooManyRequests {
			message = admissionErrorMessage(e.body)
		}
		if status == http.StatusRequestTimeout {
			message = "Request timed out; unable to connect"
		}
		fail(w, status, message)
		return
	}
	fail(w, 503, "Control plane unavailable")
}

func validRetryAfter(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	if _, err := time.Parse(http.TimeFormat, value); err == nil {
		return true
	}
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func upstreamURL(endpoint, requestPath string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", errors.New("invalid upstream endpoint")
	}
	base := strings.TrimRight(u.Path, "/")
	for _, suffix := range []string{"/chat/completions", "/responses", "/messages", "/embeddings", "/rerank", "/audio/transcriptions", "/systemone", "/images/generations", "/images/edits"} {
		if strings.HasSuffix(base, "/v1"+suffix) {
			base = strings.TrimSuffix(base, suffix)
			break
		}
	}
	if strings.HasSuffix(base, "/v1") && strings.HasPrefix(requestPath, "/v1/") {
		requestPath = strings.TrimPrefix(requestPath, "/v1")
	}
	u.Path = base + requestPath
	u.RawPath = ""
	return u.String(), nil
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	timing := peernet.CallTiming{Started: started}
	g.active.Add(1)
	defer g.active.Add(-1)
	if r.URL.Path == "/v1/messages" {
		w = &messagesWriter{w}
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.URL.Path == "/internal/runtime" && r.Method == "GET" {
		g.runtimeIdentity(w, r)
		return
	}
	if r.URL.Path == "/ready" && r.Method == "GET" {
		ready := g.ready(r)
		w.Header().Set("Content-Type", "application/json")
		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(map[string]bool{"ready": ready})
		return
	}
	if r.URL.Path == "/health" && r.Method == "GET" {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"up"}`))
		return
	}
	if !g.build.matches() {
		fail(w, 503, "Gateway build mismatch")
		return
	}
	if r.URL.Path == ownerTunnelPrefix || strings.HasPrefix(r.URL.Path, ownerTunnelPrefix+"/") {
		g.ownerBridge.ServeHTTP(w, r)
		return
	}
	if r.URL.Path == "/~!frp" {
		if g.frp == nil {
			fail(w, 503, "FRP service unavailable")
			return
		}
		g.frp.ServeHTTP(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/internal/evaluations/") {
		if r.Method == http.MethodPost {
			if err := setBodyReadDeadline(w, r.ContentLength, 4096); err != nil && !errors.Is(err, http.ErrNotSupported) {
				fail(w, http.StatusServiceUnavailable, "Gateway could not set request body deadline")
				return
			}
		}
		g.evaluation(w, r)
		return
	}
	if r.URL.Path == "/private/target" || r.URL.Path == "/private/dispatches" {
		fail(w, http.StatusGone, "Private local inference must execute through its bound Go Server")
		return
	}
	if r.URL.Path != "/v1/models" && r.URL.Path != "/v1/chat/completions" && r.URL.Path != "/v1/responses" && r.URL.Path != "/v1/messages" && r.URL.Path != "/v1/embeddings" && r.URL.Path != "/v1/rerank" && r.URL.Path != "/v1/audio/transcriptions" && r.URL.Path != "/v1/systemone" && !imagePath(r.URL.Path) {
		fail(w, 404, "Not found")
		return
	}
	if len(r.Header.Values("Authorization")) != 1 || len(r.Header.Values("X-Api-Key")) > 1 {
		fail(w, 401, "One caller identity required")
		return
	}
	h := r.Header.Get("Authorization")
	publicBodyDigest := ""
	if len(r.Header.Values(peernet.PublicCallerProofHeader)) > 0 {
		peerHost, _, _ := net.SplitHostPort(r.RemoteAddr)
		devLoopback := os.Getenv("NEWNOA_ENV") == "dev" && net.ParseIP(peerHost) != nil && net.ParseIP(peerHost).IsLoopback()
		if (!modelrpc.IsModelTransport(r) && !devLoopback) || len(r.Header.Values("X-Api-Key")) != 0 {
			fail(w, 401, "Invalid public caller proof")
			return
		}
		expected, caller, err := peernet.VerifyPublicCaller(g.token, g.id, r, time.Now())
		if err != nil {
			fail(w, 401, "Invalid public caller proof")
			return
		}
		publicBodyDigest = expected
		r = r.Clone(r.Context())
		r.Header = r.Header.Clone()
		r.Header.Del(peernet.PublicCallerProofHeader)
		r.Header.Del(peernet.PublicCallerIPHeader)
		r.RemoteAddr = net.JoinHostPort(caller, "0")
	} else {
		if len(r.Header.Values(peernet.PublicCallerIPHeader)) > 0 {
			fail(w, 401, "Invalid public caller proof")
			return
		}
		key := r.Header.Get("X-Api-Key")
		if key == "" || !strings.HasPrefix(h, "Bearer ses_") || len(h) <= len("Bearer ses_") || len(h) > 263 {
			fail(w, 401, "Login session required")
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), proxySessionContext{}, strings.TrimPrefix(h, "Bearer ")))
		r.Header = r.Header.Clone()
		r.Header.Del("X-Api-Key")
		h = "Bearer " + key
	}
	anthropic := r.URL.Path == "/v1/messages"
	beta := ""
	if anthropic {
		var betaErr error
		beta, betaErr = messagesBeta(r.Header)
		if betaErr != nil || r.Header.Get("Anthropic-Version") != "" && r.Header.Get("Anthropic-Version") != "2023-06-01" {
			fail(w, 400, "Unsupported Anthropic version or beta")
			return
		}
		if key := r.Header.Get("X-Api-Key"); key != "" {
			if h != "" {
				fail(w, 401, "Provide one authentication method")
				return
			}
			h = "Bearer " + key
		}
	}
	if !strings.HasPrefix(h, "Bearer ") || len(h) > 263 {
		fail(w, 401, "API key required")
		return
	}
	secret := strings.TrimPrefix(h, "Bearer ")
	if secret == "" || strings.ContainsAny(secret, " \t\r\n") {
		fail(w, 401, "Invalid API key")
		return
	}
	requestLimit := int64(maxBody)
	if r.URL.Path == metering.ImageEditPath || r.URL.Path == "/v1/chat/completions" || r.URL.Path == "/v1/audio/transcriptions" || r.URL.Path == "/v1/responses" || r.URL.Path == "/v1/messages" {
		requestLimit = 32 << 20
	}
	charged := admission.BodyLimit(r.ContentLength, requestLimit)
	var reservation *admission.Reservation
	if r.Method == http.MethodPost {
		reservation = g.uploads.ReserveFor(r.RemoteAddr, secret, charged)
		if reservation == nil {
			w.Header().Set("Retry-After", "1")
			fail(w, http.StatusTooManyRequests, "Gateway is busy")
			return
		}
		defer reservation.Release()
	}
	for _, name := range []string{"X-Newnoa-Permit", "X-Newnoa-Gateway", "X-Newnoa-Epoch"} {
		if len(r.Header.Values(name)) > 0 {
			g.callerViolation(w, r, secret, "forged_execution_metadata", "Reserved execution metadata is not allowed", 400)
			return
		}
	}
	if r.URL.Path == "/v1/models" {
		if r.Method != "GET" {
			fail(w, 405, "GET required")
			return
		}
		var rows []struct {
			ID        string `json:"id"`
			Name      string `json:"name"`
			Model     string `json:"model"`
			ModelType string `json:"modelType"`
		}
		if err := g.control(r.Context(), "/models", map[string]any{"secret": secret}, &rows); err != nil {
			controlFailure(w, err)
			return
		}
		data := []any{}
		counts := map[string]int{}
		for _, row := range rows {
			counts[row.Model]++
		}
		for _, row := range rows {
			id := row.ID
			if row.Model != "" && counts[row.Model] == 1 && uuid.Validate(row.Model) != nil {
				id = row.Model
			}
			data = append(data, map[string]any{"id": id, "deployment_id": row.ID, "object": "model", "owned_by": "newnoa", "model_type": row.ModelType, "name": row.Name})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
		return
	}
	if r.Method != "POST" {
		fail(w, 405, "POST required")
		return
	}
	if r.ContentLength > requestLimit {
		g.callerViolation(w, r, secret, "request_too_large", "Request too large", 413)
		return
	}
	if deadlineErr := setBodyReadDeadline(w, r.ContentLength, requestLimit); deadlineErr != nil && !errors.Is(deadlineErr, http.ErrNotSupported) {
		fail(w, 503, "Gateway could not set request body deadline")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, charged))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			g.callerViolation(w, r, secret, "request_too_large", "Request too large", 413)
		} else {
			fail(w, 400, "Incomplete request body")
		}
		return
	}
	if err := http.NewResponseController(w).SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		fail(w, 503, "Gateway could not clear request body deadline")
		return
	}
	if publicBodyDigest != "" {
		actual := sha256.Sum256(raw)
		if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(actual[:])), []byte(publicBodyDigest)) != 1 {
			fail(w, 401, "Public caller body changed")
			return
		}
	}
	if reservation != nil && !reservation.Promote() {
		w.Header().Set("Retry-After", "1")
		fail(w, http.StatusTooManyRequests, "Gateway is busy")
		return
	}
	var body map[string]any
	asr := r.URL.Path == "/v1/audio/transcriptions"
	var audio *transcription
	if asr {
		audio, err = parseTranscription(raw, r.Header.Get("Content-Type"))
		if err != nil {
			g.callerViolation(w, r, secret, "invalid_request", err.Error(), 400)
			return
		}
		body = map[string]any{"model": audio.model, "stream": audio.stream}
	} else if json.Unmarshal(raw, &body) != nil {
		g.callerViolation(w, r, secret, "invalid_json", "Invalid JSON", 400)
		return
	}
	model, ok := body["model"].(string)
	if !ok || model == "" {
		g.callerViolation(w, r, secret, "invalid_request", "model required", 400)
		return
	}
	callerModel := model
	responses := r.URL.Path == "/v1/responses"
	embeddings := r.URL.Path == "/v1/embeddings"
	rerank := r.URL.Path == "/v1/rerank"
	decision := r.URL.Path == "/v1/systemone"
	images := imagePath(r.URL.Path)
	if images && (!validImageInput(body) || r.URL.Path == metering.ImageEditPath && !metering.ValidEditImages(body)) {
		g.callerViolation(w, r, secret, "invalid_request", "Image generation requires a nonempty prompt, n between 1 and 10, and non-streaming output", 400)
		return
	}
	var questions map[string]decisionQuestion
	if decision {
		var valid bool
		questions, valid = decisionInput(body)
		if !valid {
			g.callerViolation(w, r, secret, "invalid_request", "Invalid decision state, questions or controls", 400)
			return
		}
	}
	documents, rankCount := 0, 0
	if rerank {
		var valid bool
		documents, rankCount, valid = rerankInput(body)
		if !valid {
			g.callerViolation(w, r, secret, "invalid_request", "Invalid rerank query, documents or top_n", 400)
			return
		}
	}
	inputCount, dimensions := 0, 0
	if embeddings {
		inputCount = embeddingInput(body["input"])
		if inputCount == 0 {
			g.callerViolation(w, r, secret, "invalid_request", "input must contain nonempty text or token arrays", 400)
			return
		}
		if format, exists := body["encoding_format"]; exists && format != "float" {
			g.callerViolation(w, r, secret, "invalid_request", "Only float embedding encoding is supported", 400)
			return
		}
		if value, exists := body["dimensions"]; exists {
			n, ok := value.(float64)
			if !ok || n < 1 || n > 65536 || math.Trunc(n) != n {
				g.callerViolation(w, r, secret, "invalid_request", "Invalid embedding dimensions", 400)
				return
			}
			dimensions = int(n)
		}
	}
	messages, ok := body["messages"].([]any)
	if !responses && !embeddings && !rerank && !asr && !decision && !images && (!ok || len(messages) == 0 || len(messages) > 1000) {
		g.callerViolation(w, r, secret, "invalid_messages", "messages required", 400)
		return
	}
	if anthropic {
		for _, value := range messages {
			message, ok := value.(map[string]any)
			if !ok || message["role"] != "user" && message["role"] != "assistant" && message["role"] != "system" {
				g.callerViolation(w, r, secret, "invalid_request", "Native messages require user, assistant, or system roles", 400)
				return
			}
			switch message["content"].(type) {
			case string, []any:
			default:
				g.callerViolation(w, r, secret, "invalid_request", "Native message content must be text or blocks", 400)
				return
			}
		}
		if _, exists := body["max_completion_tokens"]; exists {
			g.callerViolation(w, r, secret, "invalid_request", "Native messages require max_tokens", 400)
			return
		}
	}
	if responses {
		switch body["input"].(type) {
		case string, []any:
		default:
			g.callerViolation(w, r, secret, "invalid_input", "input must be a string or array", 400)
			return
		}
	}
	stream := false
	if v, exists := body["stream"]; exists {
		var valid bool
		stream, valid = v.(bool)
		if !valid {
			g.callerViolation(w, r, secret, "invalid_request", "stream must be boolean", 400)
			return
		}
	}
	if (embeddings || rerank || decision || images) && stream {
		g.callerViolation(w, r, secret, "invalid_request", "This inference endpoint does not support streaming", 400)
		return
	}
	if r.URL.Path == "/v1/chat/completions" && stream {
		options := map[string]any{}
		if value := body["stream_options"]; value != nil {
			var valid bool
			options, valid = value.(map[string]any)
			if !valid {
				g.callerViolation(w, r, secret, "invalid_request", "stream_options must be an object", 400)
				return
			}
			if usage, exists := options["include_usage"]; exists {
				if _, valid := usage.(bool); !valid {
					g.callerViolation(w, r, secret, "invalid_request", "include_usage must be boolean", 400)
					return
				}
			}
		}
		// Metering is required even when a modified caller disables upstream usage.
		options["include_usage"] = true
		body["stream_options"] = options
	}
	limitField := "max_tokens"
	if responses {
		limitField = "max_output_tokens"
	} else if _, present := body["max_completion_tokens"]; present && !anthropic {
		if _, legacy := body["max_tokens"]; legacy {
			g.callerViolation(w, r, secret, "invalid_output_limit", "Provide only one of max_tokens and max_completion_tokens", 400)
			return
		}
		limitField = "max_completion_tokens"
	}
	limit, valid := body[limitField].(float64)
	// OpenAI output limits are optional and nullable; native messages require one.
	if !embeddings && !rerank && !asr && !decision && !images && (anthropic || body[limitField] != nil) && (!valid || limit < 1 || limit > 2_000_000 || math.Trunc(limit) != limit) {
		g.callerViolation(w, r, secret, "invalid_output_limit", limitField+" must be an integer between 1 and 2000000", 400)
		return
	}
	prefix := "POST " + r.URL.Path + "\n"
	if asr {
		prefix += r.Header.Get("Content-Type") + "\n"
	}
	if anthropic {
		prefix += messagesBinding(beta)
	}
	digest := sha256.Sum256(append([]byte(prefix), raw...))
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	var authorized struct {
		InferencePath      string        `json:"inferencePath"`
		DeploymentID       string        `json:"deploymentId"`
		DeploymentMethod   string        `json:"deploymentMethod"`
		Revision           int64         `json:"revision"`
		Model              string        `json:"model"`
		ImageResultHosting *bool         `json:"imageResultHosting"`
		Peer               peernet.Route `json:"peer"`
		NodeID             *string       `json:"nodeId"`
		Epoch              int64         `json:"epoch"`
		OwnerID            string        `json:"ownerId"`
		History            struct {
			Enabled bool   `json:"enabled"`
			KeyID   string `json:"keyId"`
		} `json:"history"`
	}
	if err := g.control(ctx, "/authorize", map[string]any{"secret": secret, "model": model, "gatewayId": g.id}, &authorized); err != nil {
		controlFailure(w, err)
		return
	}
	if authorized.Model == "" {
		fail(w, 502, "Invalid authorized model")
		return
	}
	if authorized.DeploymentID != "" {
		model = authorized.DeploymentID
	}
	if authorized.NodeID != nil && !g.ownsNode(authorized.OwnerID) {
		fail(w, 503, "Node is bound to another Go Server or its binding is unavailable")
		return
	}
	callMetadata := peernet.CallMetadata(r, body)
	actualInferencePath := r.URL.Path
	adaptedMessages := anthropic && authorized.InferencePath != "" && authorized.InferencePath != "/v1/messages"
	if adaptedMessages {
		actualInferencePath = authorized.InferencePath
		var conversionErr error
		body, conversionErr = adaptMessagesRequest(body, actualInferencePath)
		if conversionErr != nil {
			fail(w, 400, conversionErr.Error())
			return
		}
	}
	var responseTools map[string]responsesToolName
	llamaResponses := responses && authorized.DeploymentMethod == "llama.cpp"
	if llamaResponses {
		if authorized.DeploymentID == "" || authorized.Revision <= 0 {
			fail(w, 502, "Missing authorized Responses compatibility identity")
			return
		}
		body, responseTools, err = normalizeResponsesFunctions(body)
		if err != nil {
			fail(w, 422, errResponsesTools.Error())
			return
		}
	}
	body["model"] = authorized.Model
	encoded, err := json.Marshal(body)
	contentType := "application/json"
	if asr {
		encoded, contentType, err = audio.encode(authorized.Model)
	}
	if int64(len(encoded)) > requestLimit {
		fail(w, 413, "Request too large after model rewrite")
		return
	}
	if err != nil {
		fail(w, 400, "Invalid request")
		return
	}
	prefix = "POST " + actualInferencePath + "\n"
	if asr {
		prefix += contentType + "\n"
	}
	if anthropic && !adaptedMessages {
		prefix += messagesBinding(beta)
	}
	execution := sha256.Sum256(append([]byte(prefix), encoded...))
	var peerConnection *peernet.Connection
	closePeerConnection := func() {}
	detachPeer := func() {}
	recordPeer := func(value *peernet.Fallback) error {
		return g.control(ctx, "/peer-fallbacks", map[string]any{"secret": secret, "gatewayId": g.id, "evidence": value}, nil)
	}
	if authorized.Peer.Available {
		route := authorized.Peer
		if authorized.NodeID == nil || route.NodeID != *authorized.NodeID || route.Epoch != authorized.Epoch {
			fail(w, 503, "Node binding changed")
			return
		}
		if route.Tunnel != nil {
			route.Tunnel.ConsumerSession, _ = ctx.Value(proxySessionContext{}).(string)
		}
		route.DirectDial = safeDialer{allow: g.allow, timeout: 3 * time.Second}.DialContext
		peerCtx, detach, closePeer := peerLifecycle(ctx)
		defer closePeer()
		detachPeer = detach
		peerConnection, err = peernet.ConnectFRP(peerCtx, route, model, secret, recordPeer)
		if err != nil {
			fail(w, 503, "Encrypted peer connection unavailable")
			return
		}
		var peerCloseOnce sync.Once
		closePeerConnection = func() { peerCloseOnce.Do(peerConnection.Close) }
		defer closePeerConnection()
		g.peers.Selected(peerConnection.Path())
	} else if authorized.Peer.Private {
		fail(w, 503, "Encrypted peer connection unavailable")
		return
	}
	params := map[string]any{"secret": secret, "model": model, "bodyDigest": hex.EncodeToString(digest[:]), "executionDigest": hex.EncodeToString(execution[:]), "gatewayId": g.id, "inferencePath": actualInferencePath}
	for key, value := range reservationHints(raw, r.URL.Path) {
		params[key] = value
	}
	for key, value := range callMetadata {
		params[key] = value
	}
	if peerConnection != nil {
		params["nodeId"] = authorized.Peer.NodeID
		params["epoch"] = authorized.Peer.Epoch
		params["transportPath"] = peerConnection.Path()
	}
	if idem := r.Header.Get("Idempotency-Key"); idem != "" {
		if len(idem) > 128 {
			g.callerViolation(w, r, secret, "invalid_request", "Idempotency-Key too long", 400)
			return
		}
		params["idempotencyKey"] = idem
	}
	callerWriter, admissionContext := w, ctx
	var originalHeaders http.Header
	if len(g.balanceRules) > 0 {
		originalHeaders = w.Header().Clone()
	}
	var rejectedRequest, rejectedPermit string
	var refused, retrying bool
	runAttempt := func() {
		params["attemptId"] = uuid.NewString()
		var dispatch dispatchResult
		var rejectionCode string
		var omitHistory bool
		err = g.control(ctx, "/dispatches", params, &dispatch)
		if err == nil && (dispatch.ID == "" || dispatch.Permit == "") {
			err = errors.New("invalid dispatch response")
		}
		if err != nil {
			g.compensateDispatchDelivery(ctx, params, err)
			controlFailure(w, err)
			return
		}
		if modelType := historyModelType(dispatch.ModelType); modelType != "" {
			w.Header().Set("X-Newnoa-Model-Type", modelType)
		} else {
			w.Header().Del("X-Newnoa-Model-Type")
		}
		if dispatch.ModelCategory == "text" || dispatch.ModelCategory == "image" {
			w.Header().Set("X-Newnoa-Model-Category", dispatch.ModelCategory)
		}
		w.Header().Set("X-Request-Id", dispatch.ID)
		w.Header().Set("X-Newnoa-Request", dispatch.ID)
		if authorized.History.Enabled && (peerConnection == nil || !coreCapturesHistory(r, peerConnection.Path())) {
			var finishHistory func()
			w, finishHistory = g.captureCloudHistory(w, r, secret, authorized.History.KeyID, dispatch.ID, raw)
			defer func() {
				if !omitHistory {
					finishHistory()
				}
			}()
		}
		actualPath := ""
		state := "failed"
		var abortCauseMu sync.Mutex
		firstAbortCause := "unknown"
		recordAbort := func(cause string) {
			abortCauseMu.Lock()
			defer abortCauseMu.Unlock()
			if firstAbortCause == "unknown" {
				firstAbortCause = cause
			}
		}
		upstreamStatus, upstreamRequestID := 0, ""
		var imageUsage *metering.ImageUsage
		var imageMetadata map[string]any
		var contentMeter *metering.Meter
		var meterReporter *meterCheckpoint
		var meterFailure error
		ev := evidence{allowOutputLimit: adaptedMessages, responses: responses || actualInferencePath == "/v1/responses", anthropic: anthropic && !adaptedMessages, asr: asr}
		checkpointImage := func(ctx context.Context) error {
			snapshot := metering.Snapshot{Estimator: "reported-only-v1", Encoding: "unavailable", ResponseStarted: true, TerminalUsage: true, ImageUsage: imageUsage, ImageMetadata: imageMetadata}
			if ev.input != nil || ev.output != nil || ev.cached != nil || ev.write != nil {
				snapshot.Reported = &metering.Usage{InputTokens: ev.input, OutputTokens: ev.output, CachedInputTokens: ev.cached, CacheWriteTokens: ev.write}
			}
			return meterReporter.persist(ctx, snapshot)
		}
		var bridge *messagesBridge
		var responsesBridge *responsesFunctionStream
		if len(responseTools) > 0 {
			responsesBridge = &responsesFunctionStream{aliases: responseTools}
		}
		if adaptedMessages {
			bridge = &messagesBridge{path: actualInferencePath, model: callerModel}
		}
		slog.InfoContext(ctx, "inference.dispatched", "request_id", dispatch.ID, "deployment_id", authorized.DeploymentID, "gateway_id", g.id, "protocol", r.URL.Path, "stream", stream, "paid_pool", dispatch.PooledCredential)
		defer func() {
			closePeerConnection()
			finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
			defer finishCancel()
			// Later defers close the response and cancel execution before this finalizer runs.
			if contentMeter != nil && meterReporter != nil {
				snapshot := contentMeter.Snapshot()
				if ev.invalid {
					snapshot.TerminalUsage = false
				}
				rawSnapshot, snapshotErr := json.Marshal(snapshot)
				if snapshotErr == nil && !bytes.Equal(rawSnapshot, meterReporter.lastSnapshot) {
					checkpointCtx, checkpointCancel := context.WithTimeout(finishCtx, 2*time.Second)
					checkpointErr := meterReporter.persist(checkpointCtx, snapshot)
					checkpointCancel()
					if checkpointErr != nil {
						log.Printf("meter_pending request=%s", dispatch.ID)
					}
				}
			}
			outcome := map[string]any{"gatewayId": g.id, "status": state, "executionClosed": true, "inputTokens": ev.input, "outputTokens": ev.output, "cachedInputTokens": ev.cached, "cacheWriteTokens": ev.write}
			if images {
				outcome["imageUsage"] = imageUsage
				outcome["imageMetadata"] = imageMetadata
			}
			if meterReporter != nil && meterReporter.sequence > 0 {
				outcome["meterSequence"] = meterReporter.sequence
				// A newer checkpoint may have committed despite a lost ACK. API selects
				// persisted evidence from this positive closure hint; never override it
				// with raw counters (including null keys) from an older observation.
				for _, key := range []string{"inputTokens", "outputTokens", "cachedInputTokens", "cacheWriteTokens"} {
					delete(outcome, key)
				}
			}
			timing.Finish(outcome, ev.output, time.Now())
			if actualPath != "" {
				outcome["transportPath"] = actualPath
			}
			finalizeErr := g.finalize(finishCtx, "/requests/"+dispatch.ID+"/finalize", outcome)
			if finalizeErr == nil && rejectionCode != "" {
				var report struct {
					Recorded bool `json:"recorded"`
				}
				balanceErr := g.control(finishCtx, "/requests/"+dispatch.ID+"/balance-rejection", map[string]any{
					"gatewayId": g.id, "permit": dispatch.Permit, "status": upstreamStatus, "code": rejectionCode,
				}, &report)
				if balanceErr == nil && report.Recorded && !retrying {
					rejectedRequest, rejectedPermit = dispatch.ID, dispatch.Permit
				}
			}
			if finalizeErr != nil {
				log.Printf("finalize_pending request=%s", dispatch.ID)
			}
			abortCauseMu.Lock()
			abortCause := firstAbortCause
			abortCauseMu.Unlock()
			if state == "succeeded" {
				abortCause = "none"
			}
			fields := []any{"abortCause", abortCause, "request_id", dispatch.ID, "deployment_id", authorized.DeploymentID, "gateway_id", g.id, "protocol", r.URL.Path, "stream", stream, "paid_pool", dispatch.PooledCredential, "outcome", state, "transport_path", actualPath, "upstream_status", upstreamStatus, "duration_ms", time.Since(started).Milliseconds(), "finalized", finalizeErr == nil}
			if abortCause == "meter-invalid" && meterReporter != nil && contentMeter != nil {
				stage := meterReporter.failureStage
				if stage != "feed" && stage != "finish" {
					stage = "unknown"
				}
				rejection := contentMeter.Rejection()
				fields = append(fields, "meterStage", stage, "meterErrorClass", meterErrorClass(meterFailure), "meterRejectSite", rejection.Site, "meterEvent", rejection.Event)
			}
			if upstreamRequestID != "" {
				fields = append(fields, "upstream_request_id", upstreamRequestID)
			}
			if ev.input != nil {
				fields = append(fields, "input_tokens", *ev.input)
			}
			if ev.output != nil {
				fields = append(fields, "output_tokens", *ev.output)
			}
			if ev.cached != nil {
				fields = append(fields, "cached_input_tokens", *ev.cached)
			}
			if ev.write != nil {
				fields = append(fields, "cache_write_tokens", *ev.write)
			}
			if id := r.Header.Get("X-Newnoa-Local-Request"); uuid.Validate(id) == nil {
				fields = append(fields, "local_request_id", id)
			}
			slog.InfoContext(ctx, "inference.completed", fields...)
		}()
		if err = g.waitForAdmission(ctx, &dispatch); err != nil {
			state = "cancelled"
			controlFailure(w, err)
			return
		}
		if dispatch.NodeID != nil && !g.ownsNode(dispatch.OwnerID) {
			fail(w, 503, "Node is bound to another Go Server or its binding is unavailable")
			return
		}
		if authorized.NodeID != nil && (dispatch.NodeID == nil || *dispatch.NodeID != *authorized.NodeID || dispatch.Epoch != authorized.Epoch || dispatch.OwnerID != authorized.OwnerID) {
			fail(w, 502, "Node admission identity changed")
			return
		}
		if peerConnection != nil {
			if dispatch.NodeID == nil || *dispatch.NodeID != authorized.Peer.NodeID || dispatch.Epoch != authorized.Peer.Epoch {
				fail(w, 502, "Peer admission identity changed")
				return
			}
			w.Header().Set("X-Newnoa-Request", dispatch.ID)
			if diagnostic := peerConnection.Fallback(); diagnostic != nil {
				diagnostic.RequestID = dispatch.ID
				diagnostic.Sequence = 3
				if err := recordPeer(diagnostic); err != nil {
					controlFailure(w, err)
					return
				}
			}
		}
		if llamaResponses && (dispatch.DeploymentID != authorized.DeploymentID || dispatch.Revision != authorized.Revision || dispatch.DeploymentMethod != authorized.DeploymentMethod || dispatch.Model != authorized.Model) {
			fail(w, 502, "Authorized Responses compatibility route changed")
			return
		}
		executionBudget, err := executionTimeout(dispatch.ExecutionTimeoutSeconds)
		if err != nil {
			fail(w, 502, "Invalid execution timeout")
			return
		}
		var consumed struct {
			ExecutionDeadlineAt json.RawMessage `json:"executionDeadlineAt"`
		}
		endpoint := dispatch.Endpoint
		if dispatch.NodeID != nil {
			endpoint = dispatch.Route
		}
		if peerConnection != nil {
			endpoint = peerConnection.URL()
		}
		upstream, err := upstreamURL(endpoint, actualInferencePath)
		if err != nil {
			fail(w, 502, "Invalid upstream endpoint")
			return
		}
		up, err := http.NewRequestWithContext(ctx, "POST", upstream, bytes.NewReader(encoded))
		if err != nil {
			fail(w, 502, "Invalid upstream")
			return
		}
		up.Header.Set("Content-Type", contentType)
		if anthropic && !adaptedMessages {
			up.Header.Set("Anthropic-Version", "2023-06-01")
			if beta != "" {
				up.Header.Set("Anthropic-Beta", beta)
			}
		}
		if dispatch.NodeID != nil {
			if peerConnection == nil {
				if err := g.authorizeOwnerRequest(up, dispatch.OwnerID); err != nil {
					fail(w, 502, "Untrusted owner destination")
					return
				}
			}
			up.Header.Set("X-Newnoa-Request", dispatch.ID)
			up.Header.Set("X-Newnoa-Permit", dispatch.Permit)
			up.Header.Set("X-Newnoa-Gateway", g.id)
			up.Header.Set("X-Newnoa-Epoch", strconv.FormatInt(dispatch.Epoch, 10))
		} else {
			if dispatch.Key != "" {
				if anthropic && !adaptedMessages {
					up.Header.Set("X-Api-Key", dispatch.Key)
				} else {
					up.Header.Set("Authorization", "Bearer "+dispatch.Key)
				}
			}
			if err = g.control(ctx, "/dispatches/"+dispatch.ID+"/consume", map[string]any{"gatewayId": g.id, "permit": dispatch.Permit}, &consumed); err != nil {
				controlFailure(w, err)
				return
			}
		}
		if ctx.Err() != nil {
			state = "cancelled"
			return
		}
		// Queue time is not execution time; API consume is the authority for direct calls.
		now := time.Now()
		deadline := now.Add(executionBudget)
		if dispatch.NodeID != nil {
			// Core consumes downstream and enforces its own API absolute deadline.
			deadline = deadline.Add(deviceConsumeAllowance)
		}
		if dispatch.NodeID == nil {
			deadline, err = executionDeadline(consumed.ExecutionDeadlineAt, executionBudget, now)
			if err != nil {
				fail(w, 502, "Invalid execution deadline")
				return
			}
		}
		detachPeer()
		executionCtx, executionCancel := context.WithDeadline(r.Context(), deadline)
		defer executionCancel()
		ctx = context.WithValue(executionCtx, executionContextKey{}, true)
		up = up.WithContext(ctx)
		if dispatch.Membership.Applies {
			up.Body = newBandwidthReader(up.Body, ctx, g, dispatch.ID)
		}
		up.GetBody = nil
		if actualInferencePath == "/v1/chat/completions" || actualInferencePath == "/v1/responses" || actualInferencePath == "/v1/messages" {
			meterRequest := encoded
			if reservationHints(raw, r.URL.Path)["reserveInputUnits"] == nil {
				// Unknown/media input remains unavailable; meter observable text output only.
				meterRequest = nil
			}
			contentMeter, err = metering.New(authorized.Model, actualInferencePath, meterRequest)
			if err != nil {
				fail(w, 502, "Meter initialization failed")
				return
			}
			meterReporter = &meterCheckpoint{gateway: g, request: dispatch.ID, permit: dispatch.Permit}
			if err = meterReporter.persist(ctx, contentMeter.Snapshot()); err != nil {
				fail(w, 502, "Meter storage unavailable")
				return
			}
		}
		if images {
			meterReporter = &meterCheckpoint{gateway: g, request: dispatch.ID, permit: dispatch.Permit}
			if err = meterReporter.persist(ctx, metering.Snapshot{Estimator: "reported-only-v1", Encoding: "unavailable"}); err != nil {
				fail(w, 502, "Meter storage unavailable")
				return
			}
		}
		state = "unknown"
		transport := g.egress
		if dispatch.NodeID != nil {
			transport = g.owner
		}
		if peerConnection != nil {
			transport = inferenceHTTPClient(peerConnection.Client())
			defer transport.CloseIdleConnections()
		}
		modelRequests.Add(1)
		g.modelActive.Add(1)
		defer g.modelActive.Add(-1)
		res, err := transport.Do(up)
		if err != nil {
			fail(w, 502, "Upstream execution outcome unknown")
			return
		}
		defer func() { _ = res.Body.Close() }()
		upstreamStatus = res.StatusCode
		if id := res.Header.Get("X-Request-Id"); len(id) > 0 && len(id) <= 128 && strings.IndexFunc(id, func(c rune) bool {
			return !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-')
		}) == -1 {
			upstreamRequestID = id
		}
		if dispatch.PooledCredential {
			res.Body = &credentialReader{ReadCloser: res.Body, ctx: ctx, interval: 5 * time.Second, report: func(activityCtx context.Context) error {
				activityErr := g.control(activityCtx, "/dispatches/"+dispatch.ID+"/activity", map[string]any{"gatewayId": g.id, "permit": dispatch.Permit}, nil)
				if activityErr != nil {
					recordAbort("activity-failed")
				}
				return activityErr
			}}
		}
		if peerConnection != nil {
			actualPath = peerConnection.Path()
		} else if dispatch.NodeID == nil {
			actualPath = "direct"
		}
		if actualPath != "" {
			w.Header().Set("X-Newnoa-Path", actualPath)
			// Only a response from the real transport confirms execution connectivity.
			evidenceCtx, evidenceCancel := context.WithTimeout(ctx, time.Second)
			evidenceErr := g.control(evidenceCtx, "/requests/"+dispatch.ID+"/transport", map[string]any{"gatewayId": g.id, "transportPath": actualPath}, nil)
			evidenceCancel()
			if evidenceErr != nil {
				log.Printf("transport_evidence_pending request=%s", dispatch.ID)
			}
		}
		if dispatch.Membership.Applies {
			res.Body = newBandwidthReader(res.Body, ctx, g, dispatch.ID)
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			if dispatch.PooledCredential && dispatch.NodeID == nil && g.balanceRules.mayMatch(endpoint, res.StatusCode) {
				failure, readErr := io.ReadAll(io.LimitReader(res.Body, balanceErrorLimit+1))
				if readErr == nil {
					rejectionCode = g.balanceRules.match(endpoint, res.StatusCode, res.Header, failure)
				}
				if rejectionCode != "" {
					state = "failed"
					recordAbort("upstream-balance-rejected")
					if !retrying {
						// The API keeps the failed attempt. Only the caller-visible
						// response belongs in conversation history.
						omitHistory = true
						refused = true
						return
					}
					fail(w, 502, "Upstream rejected request")
					return
				}
				res.Body = &prefixedResponseBody{Reader: io.MultiReader(bytes.NewReader(failure), res.Body), Closer: res.Body}
			}
			if images {
				if failure, readErr := io.ReadAll(io.LimitReader(res.Body, 8193)); readErr == nil && len(failure) <= 8192 {
					ev.usage(failure)
					imageUsage = metering.ParseImageUsage(failure)
					imageMetadata = metering.ImageMetadata(r.URL.Path, raw, failure)
					if checkpointImage(ctx) != nil {
						w.Header().Set("X-Newnoa-Meter", "pending")
						fail(w, 502, "Image usage storage unavailable; settlement pending")
						return
					}
				}
			}
			state = "failed"
			if res.Header.Get("X-Newnoa-Outcome") == "unknown" {
				state = "unknown"
				fail(w, 502, "Upstream execution outcome unknown")
				return
			}
			if dispatch.NodeID != nil && res.StatusCode == 429 && res.Header.Get("X-Newnoa-Outcome") == "not-started" {
				controlFailure(w, &controlError{status: 429, retryAfter: res.Header.Get("Retry-After")})
				return
			}
			if adaptedMessages && res.StatusCode == 429 {
				if retry := res.Header.Get("Retry-After"); len(retry) <= 64 {
					w.Header().Set("Retry-After", retry)
				}
				fail(w, 429, "Upstream rate limit exceeded")
				return
			}
			fail(w, 502, "Upstream rejected request")
			return
		}
		if stream {
			if !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
				fail(w, 502, "Upstream did not return SSE")
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Accel-Buffering", "no")
			w.WriteHeader(200)
			abortStream := func() {
				if bridge != nil {
					_ = writeDownstream(ctx, w, messagesSSE("error", map[string]any{"error": map[string]any{"type": "api_error", "message": "Upstream stream incomplete or invalid"}}), 30*time.Second)
				}
				panic(http.ErrAbortHandler)
			}
			buf := make([]byte, 32*1024)
			disconnected := false
			for {
				n, readErr := res.Body.Read(buf)
				output := buf[:n]
				if n > 0 {
					timing.Observe(buf[:n], true, time.Now())
					ev.feed(buf[:n])
					if ev.invalid {
						recordAbort("evidence-invalid")
						if contentMeter != nil {
							_, _ = contentMeter.Feed(buf[:n])
						}
						if r.Context().Err() == nil {
							abortStream()
						}
						return
					}
				}
				if contentMeter != nil {
					output, err = checkpointMeterBatch(ctx, contentMeter, meterReporter, output, readErr == io.EOF)
					if err != nil {
						if ctx.Err() != nil {
							recordAbort("context-cancelled")
						} else if errors.Is(err, errMeterCheckpoint) {
							recordAbort("meter-checkpoint-failed")
						} else {
							meterFailure = err
							recordAbort("meter-invalid")
						}
						executionCancel()
						if r.Context().Err() != nil {
							state = "cancelled"
							return
						}
						abortStream()
						return
					}
				}
				if responsesBridge != nil {
					output, err = responsesBridge.feed(output, readErr == io.EOF)
					if err != nil {
						recordAbort("bridge-invalid")
						if r.Context().Err() == nil {
							abortStream()
						}
						return
					}
				}
				if len(output) > 0 {
					if bridge != nil {
						output, err = bridge.feed(output)
						if err != nil {
							recordAbort("bridge-invalid")
							if r.Context().Err() == nil {
								abortStream()
							}
							return
						}
					}
					if len(output) > 0 {
						if r.Context().Err() != nil {
							recordAbort("context-cancelled")
							state = "cancelled"
							executionCancel()
							return
						}
						if err = writeDownstream(ctx, w, output, 30*time.Second); err != nil {
							recordAbort("downstream-write-failed")
							if downstreamCancelled(r.Context(), err) {
								state = "cancelled"
							}
							executionCancel()
							return
						}
					}
				}

				if readErr != nil {
					if ctx.Err() != nil {
						recordAbort("context-cancelled")
					} else if readErr == io.EOF && !ev.complete() {
						recordAbort("read-eof-before-terminal")
					} else if readErr != io.EOF {
						recordAbort("read-error")
					}
					if r.Context().Err() != nil {
						if !disconnected {
							state = "cancelled"
						}
						disconnected = true
					}
					if readErr == io.EOF && ev.complete() {
						if bridge != nil {
							output, conversionErr := bridge.finish(&ev)
							if conversionErr != nil {
								recordAbort("bridge-invalid")
								if !disconnected {
									abortStream()
								}
								return
							}
							if !disconnected {
								err = writeDownstream(ctx, w, output, 30*time.Second)
								if err != nil {
									recordAbort("downstream-write-failed")
									disconnected = true
									if downstreamCancelled(r.Context(), err) {
										state = "cancelled"
									}
								}
							}
						}
						if !disconnected {
							state = "succeeded"
						}
					} else if !disconnected {
						abortStream()
					}
					return
				}
			}
		}

		responseLimit := 8 * maxBody
		if images {
			responseLimit = maxImageResponse
		}
		data, err := io.ReadAll(io.LimitReader(res.Body, int64(responseLimit)+1))
		if err != nil || len(data) > responseLimit {
			if errors.Is(err, context.Canceled) && r.Context().Err() == context.Canceled {
				state = "cancelled"
			}
			fail(w, 502, "Incomplete upstream response")
			return
		}
		ev.usage(data)
		if images {
			imageUsage = metering.ParseImageUsage(data)
			imageMetadata = metering.ObserveImagePayload(r.URL.Path, raw, data)
			if checkpointImage(ctx) != nil {
				state = "unknown"
				w.Header().Set("X-Newnoa-Meter", "pending")
				fail(w, 502, "Image usage storage unavailable; settlement pending")
				return
			}
		}
		if rerank || embeddings || decision {
			ev.cached = nil
		}
		if rerank {
			ev.input, ev.output = inputOnlyTokenUsage(data)
		}
		if embeddings {
			var result struct {
				Usage struct {
					Prompt *int64 `json:"prompt_tokens"`
				} `json:"usage"`
			}
			if json.Unmarshal(data, &result) == nil && result.Usage.Prompt != nil && *result.Usage.Prompt >= 0 && *result.Usage.Prompt <= 1000000000 {
				ev.input = result.Usage.Prompt
			}
			_, ev.output = inputOnlyTokenUsage(data)
		}
		if contentMeter != nil {
			if _, err = checkpointMeterBatch(ctx, contentMeter, meterReporter, data, true); err != nil {
				executionCancel()
				fail(w, 502, "Meter storage or response unavailable")
				return
			}
		}
		if adaptedMessages {
			data, err = adaptMessagesResponse(data, actualInferencePath, callerModel, &ev)
			if err != nil {
				fail(w, 502, "Invalid upstream completion")
				return
			}
		}
		if len(responseTools) > 0 {
			var result map[string]any
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			if err = decoder.Decode(&result); err == nil {
				var changed bool
				changed, err = restoreResponsesFunctions(result, responseTools)
				if err == nil && changed {
					data, err = json.Marshal(result)
				}
			}
			if err != nil {
				fail(w, 502, "Invalid upstream tool call")
				return
			}
		}
		if (anthropic && !adaptedMessages && !ev.messagesResponse(data)) || (decision && !validDecision(data, questions)) || (asr && !validTranscription(data)) || (rerank && !validRerank(data, documents, rankCount)) || (embeddings && !validEmbeddings(data, inputCount, dimensions)) || (images && !validImages(data)) || (!anthropic && !embeddings && !rerank && !asr && !decision && !images && !validCompletion(data, responses)) {
			fail(w, 502, "Invalid upstream completion")
			return
		}
		if r.Context().Err() != nil {
			state = "cancelled"
			return
		}
		if images {
			// Only an explicit API decision may exempt a private model from URL hosting.
			if authorized.ImageResultHosting == nil {
				var result struct {
					Data []struct {
						URL string `json:"url"`
					} `json:"data"`
				}
				_ = json.Unmarshal(data, &result)
				for _, item := range result.Data {
					if item.URL != "" {
						fail(w, 502, "Image result policy unavailable")
						return
					}
				}
			} else if *authorized.ImageResultHosting {
				wasMeasured := imageMetadata["outputImages"] != nil
				data, err = g.measureImageURLs(ctx, data, imageMetadata)
				if err != nil {
					fail(w, 502, "Generated image measurement unavailable")
					return
				}
				data, err = g.hostImageURLs(ctx, dispatch.ID, dispatch.Permit, data)
				if err != nil {
					fail(w, 502, "Generated image storage unavailable")
					return
				}
				if !wasMeasured && imageMetadata["outputImages"] != nil && checkpointImage(ctx) != nil {
					fail(w, 502, "Image usage storage unavailable; settlement pending")
					return
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		timing.Observe(data, false, time.Now())
		if err = writeDownstream(ctx, w, data, 30*time.Second); err == nil {
			state = "succeeded"
		} else if downstreamCancelled(r.Context(), err) {
			state = "cancelled"
		}
	}
	runAttempt()
	if !refused {
		return
	}
	w = callerWriter
	if rejectedRequest == "" || r.Context().Err() != nil {
		fail(w, 502, "Upstream rejected request")
		return
	}
	clear(w.Header())
	for name, values := range originalHeaders {
		w.Header()[name] = values
	}
	ctx = admissionContext
	retrying = true
	params["balanceRetry"] = map[string]string{"requestId": rejectedRequest, "permit": rejectedPermit}
	timing = peernet.CallTiming{Started: time.Now()}
	runAttempt()
}
func main() {
	token := os.Getenv("NEWNOA_INTERNAL_TOKEN")
	if len(token) < 32 {
		log.Fatal("NEWNOA_INTERNAL_TOKEN is required")
	}
	api := os.Getenv("NEWNOA_CONTROL_URL")
	if api == "" {
		api = "http://127.0.0.1:8090"
	}
	id := os.Getenv("NEWNOA_GATEWAY_ID")
	if id == "" {
		log.Fatal("NEWNOA_GATEWAY_ID must be unique")
	}
	bind := os.Getenv("NEWNOA_GATEWAY_BIND")
	if bind == "" {
		bind = "127.0.0.1:6666"
	}
	advertise := os.Getenv("NEWNOA_GATEWAY_ADVERTISE_ADDR")
	if advertise != "" {
		if !validAdvertiseAddress(advertise) {
			log.Fatal("NEWNOA_GATEWAY_ADVERTISE_ADDR must be a private numeric IP:port or HTTPS origin")
		}
	}
	tlsConfig, err := listenerTLS(os.Getenv("NEWNOA_GATEWAY_TLS_CERT_FILE"), os.Getenv("NEWNOA_GATEWAY_TLS_KEY_FILE"))
	if err != nil {
		log.Fatal(err)
	}
	allow := strings.Split(os.Getenv("NEWNOA_EGRESS_ALLOW_HOSTS"), ",")
	build, err := loadRuntimeBuild(os.Getenv("NEWNOA_EXPECTED_GATEWAY_SHA256"))
	if err != nil {
		log.Fatal(err)
	}
	g := newGateway(api, token, id, allow)
	g.balanceRules, err = parseBalanceRules(os.Getenv("NEWNOA_UPSTREAM_BALANCE_REJECTIONS"))
	if err != nil {
		log.Fatal(err)
	}
	if g.frpError != nil {
		log.Fatal(g.frpError)
	}
	g.build = build
	defer g.peers.Reset()
	shutdownTelemetry, err := initTelemetry("newnoa-gateway")
	if err != nil {
		log.Fatal("OpenTelemetry initialization failed")
	}
	defer shutdownTelemetry()
	httpHandler := instrumentHTTP(g, "newnoa-gateway", gatewayRoute)
	rpcServer := modelrpc.NewServer(httpHandler)
	defer rpcServer.Stop()
	server := &http.Server{ConnState: g.connectionState, Addr: bind, Handler: modelrpc.Multiplex(rpcServer, httpHandler), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	server.TLSConfig = tlsConfig
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if advertise != "" {
		go g.reportMembership(ctx, advertise)
		go g.runModelChecks(ctx)
	}
	log.Printf("gateway listening on %s (TLS=%t)", bind, tlsConfig != nil)
	if err := serveGateway(ctx, server, listener, 30*time.Second); err != nil {
		log.Fatal(err)
	}
}
