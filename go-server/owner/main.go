package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const maxBody = 1 << 20

var tunnelPath = regexp.MustCompile(`^/tunnels/([a-f0-9-]{36})/([1-9][0-9]*)/(evaluation/|probe/|publication-probe/)?v1/(chat/completions|responses|messages|embeddings|rerank|audio/transcriptions|systemone|images/generations|images/edits|models)$`)

type owner struct {
	uploads, pluginChecks chan struct{}
	build                 runtimeBuild
	id, api, token        string
	control, upstream     *http.Client
}

func newOwner(id, api, token string) *owner {
	return &owner{uploads: make(chan struct{}, 4), pluginChecks: make(chan struct{}, 16), id: id, api: strings.TrimRight(api, "/"), token: token, control: &http.Client{Timeout: 8 * time.Second, Transport: instrumentOwnerClient(http.DefaultTransport, true), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, upstream: &http.Client{Transport: instrumentOwnerClient(ownerImageTimeout(&http.Transport{Proxy: nil, ResponseHeaderTimeout: 30 * time.Second, IdleConnTimeout: 60 * time.Second}), false), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (o *owner) call(ctx context.Context, path string, b any, out any) error {
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	r, err := http.NewRequestWithContext(ctx, "POST", o.api+"/api/v1/internal/frp/"+o.id+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	r.Header.Set("Authorization", "Bearer "+o.token)
	r.Header.Set("Content-Type", "application/json")
	res, err := o.control.Do(r)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("control rejected: %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err != nil || len(data) > maxBody {
		return fmt.Errorf("invalid control response")
	}
	return json.Unmarshal(data, out)
}
func (o *owner) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodGet {
		switch r.URL.Path {
		case "/internal/runtime":
			o.runtimeIdentity(w, r)
			return
		case "/health":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"up"}`))
			return
		case "/ready":
			w.Header().Set("Content-Type", "application/json")
			ready := o.ready(r)
			if !ready {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			json.NewEncoder(w).Encode(map[string]bool{"ready": ready})
			return
		}
	}
	if !o.build.matches() {
		if r.URL.Path == "/plugin" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"reject": true, "reject_reason": "owner build mismatch", "unchange": true})
			return
		}
		http.Error(w, "Owner build mismatch", http.StatusServiceUnavailable)
		return
	}
	slots := o.uploads
	if r.URL.Path == "/plugin" {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			http.Error(w, "local plugin only", 403)
			return
		}
		slots = o.pluginChecks
	} else {
		if len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+o.token)) != 1 {
			http.Error(w, "Unauthorized", 401)
			return
		}
	}
	match := tunnelPath.FindStringSubmatch(r.URL.Path)
	probe := match != nil && (match[3] == "probe/" || match[3] == "publication-probe/")
	models := match != nil && match[3] == "probe/" && match[4] == "models"
	if models && r.Method != "GET" || !models && r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	if match != nil && (match[4] == "models" && !models || match[4] == "images/edits" && match[3] != "") {
		http.Error(w, "route not allowed", 404)
		return
	}
	if probe {
		for _, beta := range r.Header.Values("Anthropic-Beta") {
			if beta != "" {
				http.Error(w, "Anthropic beta is not allowed for probes", 400)
				return
			}
		}
	}
	if r.URL.Path != "/plugin" && (match == nil || r.URL.RawQuery != "") {
		http.Error(w, "route not allowed", 404)
		return
	}
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		w.Header().Set("Retry-After", "1")
		w.Header().Set("X-Newnoa-Outcome", "not-started")
		http.Error(w, "owner is busy", http.StatusTooManyRequests)
		return
	}
	requestLimit := int64(maxBody)
	if match != nil && (match[4] == "chat/completions" || match[4] == "audio/transcriptions" || match[4] == "responses" || match[4] == "messages" || match[4] == "images/edits") {
		requestLimit = 32 << 20
	}
	if r.ContentLength > requestLimit {
		http.Error(w, "request too large", 413)
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, requestLimit))
	if err != nil {
		http.Error(w, "request too large", 413)
		return
	}
	if models && len(raw) != 0 {
		http.Error(w, "model listing probe requires an empty body", 400)
		return
	}
	if r.URL.Path == "/plugin" {
		var request map[string]any
		var response map[string]any
		decodeErr := json.Unmarshal(raw, &request)
		path := "/check"
		if content, ok := request["content"].(map[string]any); ok {
			user := content
			if request["op"] != "Login" {
				user, _ = content["user"].(map[string]any)
			}
			if metas, ok := user["metas"].(map[string]any); ok {
				if _, consumer := metas["consumerKey"]; consumer {
					path = "/consumer-check"
				}
			}
		}
		if decodeErr != nil || o.call(r.Context(), path, request, &response) != nil {
			response = map[string]any{"reject": true, "reject_reason": "node lease rejected", "unchange": true}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}
	beta := ""
	if match[4] == "messages" && !probe {
		var betaErr error
		beta, betaErr = messagesBeta(r.Header)
		if betaErr != nil {
			http.Error(w, "unsupported Anthropic beta", 400)
			return
		}
	}
	ep, err := strconv.ParseInt(match[2], 10, 64)
	if err != nil {
		http.Error(w, "invalid epoch", 400)
		return
	}
	var route struct {
		Port int `json:"port"`
	}
	purpose := "serving"
	if match[3] == "evaluation/" {
		if match[4] != "chat/completions" && match[4] != "embeddings" && match[4] != "rerank" && match[4] != "systemone" && match[4] != "audio/transcriptions" {
			http.Error(w, "evaluation route not allowed", 404)
			return
		}
		purpose = "evaluation"
	}
	if o.call(r.Context(), "/route", map[string]any{"nodeId": match[1], "epoch": ep, "purpose": purpose}, &route) != nil || route.Port < 20000 || route.Port > 29999 {
		http.Error(w, "node unavailable", 409)
		return
	}
	// Only the control plane can select a port; every TCP target stays on this owner host.
	up, err := http.NewRequestWithContext(r.Context(), r.Method, fmt.Sprintf("http://127.0.0.1:%d/%sv1/%s", route.Port, match[3], match[4]), bytes.NewReader(raw))
	if err != nil {
		http.Error(w, "invalid request", 400)
		return
	}
	up.Header.Set("Content-Type", "application/json")
	if match[4] == "messages" {
		up.Header.Set("Anthropic-Version", "2023-06-01")
		if beta != "" {
			up.Header.Set("Anthropic-Beta", beta)
		}
	}
	if match[4] == "audio/transcriptions" {
		up.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	}
	for _, name := range []string{"X-Newnoa-Request", "X-Newnoa-Permit", "X-Newnoa-Gateway"} {
		up.Header.Set(name, r.Header.Get(name))
	}
	up.Header.Set("X-Newnoa-Epoch", match[2])
	// A consumed permit must never be retried, including by the HTTP transport.
	up.GetBody = nil
	if models {
		// A non-nil empty body disables Transport's GET replay on reused sockets.
		up.Body = io.NopCloser(bytes.NewReader(raw))
	}
	if match[3] == "" {
		ctx, cancel := context.WithTimeout(up.Context(), 31*time.Minute)
		defer cancel()
		up = up.WithContext(context.WithValue(ctx, ownerExecutionContextKey{}, true))
	}
	res, err := o.upstream.Do(up)
	if err != nil {
		w.Header().Set("X-Newnoa-Outcome", "unknown")
		http.Error(w, "execution outcome unknown", 502)
		return
	}
	defer res.Body.Close()
	for _, name := range []string{"Content-Type", "X-Newnoa-Outcome", "Retry-After"} {
		if value := res.Header.Get(name); value != "" {
			w.Header().Set(name, value)
		}
	}
	if probe && res.Header.Get("X-Newnoa-Probe-Upstream") == "1" {
		w.Header().Set("X-Newnoa-Probe-Upstream", "1")
	}
	if probe {
		for _, name := range []string{"X-Newnoa-Probe-Ping-Status", "X-Newnoa-Probe-Ping-Ms", "X-Newnoa-Probe-Ping-Elapsed-Ms"} {
			if value := res.Header.Get(name); value != "" {
				w.Header().Set(name, value)
			}
		}
	}
	w.WriteHeader(res.StatusCode)
	buf := make([]byte, 32*1024)
	controller := http.NewResponseController(w)
	defer controller.SetWriteDeadline(time.Time{})
	for {
		n, readErr := res.Body.Read(buf)
		if n > 0 {
			if deadlineErr := controller.SetWriteDeadline(time.Now().Add(30 * time.Second)); deadlineErr != nil && !errors.Is(deadlineErr, http.ErrNotSupported) {
				return
			}
			if _, err = w.Write(buf[:n]); err != nil {
				return
			}
			if err = controller.Flush(); err != nil {
				return
			}
			if err = controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
				return
			}
		}
		if readErr != nil {
			if readErr != io.EOF && r.Context().Err() == nil {
				// Preserve truncation so a forwarded done event cannot become billable success.
				panic(http.ErrAbortHandler)
			}
			return
		}
	}
}
func loopbackBind(bind string) bool {
	host, _, err := net.SplitHostPort(bind)
	ip := net.ParseIP(host)
	return err == nil && ip != nil && ip.IsLoopback()
}
func (o *owner) publicOnly(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/plugin" {
		http.NotFound(w, r)
		return
	}
	o.ServeHTTP(w, r)
}
func (o *owner) pluginOnly(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/plugin" {
		http.NotFound(w, r)
		return
	}
	o.ServeHTTP(w, r)
}
func main() {
	id, token, api := os.Getenv("NEWNOA_FRP_OWNER_ID"), os.Getenv("NEWNOA_INTERNAL_TOKEN"), os.Getenv("NEWNOA_CONTROL_URL")
	if id == "" || len(token) < 32 {
		log.Fatal("owner ID and internal token required")
	}
	if api == "" {
		api = "http://127.0.0.1:8090"
	}
	bind := os.Getenv("NEWNOA_FRP_OWNER_BIND")
	if bind == "" {
		bind = "127.0.0.1:18101"
	}
	build, err := loadRuntimeBuild(os.Getenv("NEWNOA_EXPECTED_OWNER_SHA256"))
	if err != nil {
		log.Fatal(err)
	}
	o := newOwner(id, api, token)
	o.build = build
	shutdownTelemetry, err := initTelemetry("newnoa-owner")
	if err != nil {
		log.Fatal("OpenTelemetry initialization failed")
	}
	defer shutdownTelemetry()
	server := &http.Server{Addr: bind, Handler: instrumentHTTP(o, "newnoa-owner", ownerRoute), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Minute, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	cert, key := os.Getenv("NEWNOA_FRP_OWNER_TLS_CERT"), os.Getenv("NEWNOA_FRP_OWNER_TLS_KEY")
	if (cert == "") != (key == "") {
		log.Fatal("owner TLS certificate and key must be configured together")
	}
	pluginBind := os.Getenv("NEWNOA_FRP_PLUGIN_BIND")
	if cert != "" {
		if pluginBind == "" {
			log.Fatal("TLS owner requires a separate loopback FRP plugin listener")
		}
		pair, loadErr := tls.LoadX509KeyPair(cert, key)
		if loadErr != nil {
			log.Fatal("owner TLS certificate/key could not be loaded")
		}
		server.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
	} else if !loopbackBind(bind) {
		log.Fatal("non-loopback owner bridge requires TLS")
	}

	servers := []*http.Server{server}
	if pluginBind != "" {
		if !loopbackBind(pluginBind) {
			log.Fatal("FRP plugin listener must use loopback")
		}
		plugin := &http.Server{Addr: pluginBind, Handler: instrumentHTTP(http.HandlerFunc(o.pluginOnly), "newnoa-owner", ownerRoute), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, MaxHeaderBytes: 16384}
		server.Handler = instrumentHTTP(http.HandlerFunc(o.publicOnly), "newnoa-owner", ownerRoute)
		servers = append(servers, plugin)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serveOwner(ctx, servers, 30*time.Second); err != nil {
		log.Fatal(err)
	}
}
