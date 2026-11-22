package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pelletier/go-toml/v2"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	peernet "github.com/newnoa/newnoa-maas/go-server/network"
)

var errDeviceUnavailable = errors.New("device credential unavailable")
var errDeviceAdmission = errors.New("device admission temporarily unavailable")

var nodePattern = regexp.MustCompile(`^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$`)

type connector struct {
	serviceChanged                                      func()
	updateGate                                          *runtimeUpdateGate
	usageStore                                          *chatHistory
	usageMu                                             sync.Mutex
	usageReportError                                    string
	usageQueuePaused                                    bool
	overlay                                             *overlayProvider
	peerURL, peerPin, peerAddress, tunnelSecret         string
	stopPeer                                            func()
	stopping                                            bool
	stopCleanup                                         sync.Once
	mu                                                  sync.Mutex
	ctx                                                 context.Context
	cancel                                              context.CancelFunc
	done                                                chan struct{}
	nodeID, credential, ownerID, control, binary, state string
	assessmentMu                                        sync.Mutex
	assessments                                         map[string]cachedSupplyAssessment
	assessing                                           bool
	purpose                                             string
	stage, failureCode, failureAt                       string
	epoch                                               int64
	address                                             string
	server                                              *http.Server
	client, upstream, remoteUpstream                    *http.Client
}
type nodeLease struct {
	OwnerID    string `json:"ownerId"`
	Purpose    string `json:"lease_purpose"`
	Epoch      int64  `json:"epoch"`
	Credential string `json:"frpCredential"`
	FRP        struct {
		ServerAddr        string `json:"serverAddr"`
		ServerPort        int    `json:"serverPort"`
		ServerName        string `json:"serverName"`
		TransportProtocol string `json:"transportProtocol"`
		TrustedCA         string `json:"trustedCa"`
		ProxyName         string `json:"proxyName"`
		RemotePort        int    `json:"remotePort"`
	} `json:"frp"`
}

func controlBase(value string) (string, error) {
	u, err := url.Parse(value)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"))) {
		return "", errors.New("control URL must be HTTPS or loopback HTTP")
	}
	return strings.TrimRight(value, "/"), nil
}
func (c *core) startConnector(nodeID, credential, ownerID, purpose string) error {
	if purpose == "" {
		purpose = "serving"
	}
	if purpose != "serving" && purpose != "evaluation" {
		return errors.New("invalid connector lease purpose")
	}
	if !nodePattern.MatchString(nodeID) || !strings.HasPrefix(credential, "node_") || len(credential) < 35 || len(credential) > 100 || !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`).MatchString(ownerID) {
		return errors.New("valid node ID, device credential and FRP owner are required")
	}
	if c.connector != nil {
		select {
		case <-c.connector.done:
			c.connector = nil
		default:
			return errors.New("connector already running")
		}
	}
	control, err := controlBase(c.control)
	if err != nil {
		return err
	}
	binary, err := verifiedFRPC()
	if err != nil {
		return err
	}
	localPolicy, err := configuredSupplyPolicy("domestic")
	if err != nil {
		return err
	}
	usageStore, err := c.historyStore()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &connector{serviceChanged: c.historyEvents.publishService, updateGate: &c.updateGate, usageStore: usageStore, ctx: ctx, cancel: cancel, done: make(chan struct{}), nodeID: nodeID, credential: credential, ownerID: ownerID, purpose: purpose, control: control, binary: binary, state: "connecting", address: listener.Addr().String(), client: connectorControlClient()}
	n.upstream = localPolicy.client(0)
	n.upstream.Transport = telemetryTransport{next: n.upstream.Transport, propagate: false, route: "desktop.model"}
	remotePolicy := localPolicy
	remotePolicy.edition = "overseas"
	n.remoteUpstream = remotePolicy.client(0)
	n.remoteUpstream.Transport = telemetryTransport{next: n.remoteUpstream.Transport, propagate: false, route: "desktop.model"}
	if purpose == "serving" {
		address := os.Getenv("NEWNOA_PEER_ADDR")
		advertised := os.Getenv("NEWNOA_PEER_URL")
		stopPeer, pin, peerErr := n.startPeer(address, advertised)
		if peerErr != nil {
			listener.Close()
			cancel()
			return peerErr
		}
		n.peerPin, n.stopPeer = pin, stopPeer
	}
	// FRP membership pacing can make valid model uploads take longer than 15 seconds.
	n.server = &http.Server{Handler: n, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Minute, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384}
	n.checkUsageBacklog(ctx)
	c.connector = n
	c.historyEvents.publishService()
	go n.server.Serve(listener)
	go n.run()
	go n.runUsage()
	return nil
}
func (n *connector) status() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()
	return map[string]any{"state": n.state, "nodeId": n.nodeID, "ownerId": n.ownerID, "epoch": n.epoch, "purpose": n.purpose, "stage": n.stage, "failureCode": n.failureCode, "failureAt": n.failureAt, "overlay": n.overlay != nil, "usageReportError": publicStatusError("connector.usageReport", n.usageReportError)}
}
func (n *connector) setState(state string, epoch int64) {
	n.mu.Lock()
	changed := n.state != state
	n.state = state
	if epoch > 0 {
		n.epoch = epoch
	}
	n.mu.Unlock()
	if changed && n.serviceChanged != nil {
		n.serviceChanged()
	}
}

// Diagnostics contain only fixed codes, never upstream errors or FRPC output.
func (n *connector) setStage(stage string) {
	n.mu.Lock()
	n.stage = stage
	n.mu.Unlock()
}
func (n *connector) recordFailure(err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if errors.Is(err, context.Canceled) {
		return
	}
	code := "control_unavailable"
	switch {
	case errors.Is(err, errDeviceUnavailable):
		code = "device_unavailable"
	case errors.Is(err, errDeviceAdmission):
		code = "admission_unavailable"
	case n.stage == "configuration":
		code = "configuration_failed"
	case n.stage == "process":
		code = "process_failed"
	case n.stage == "tunnel":
		code = "tunnel_unavailable"
	}
	if code != n.failureCode && n.serviceChanged != nil {
		n.serviceChanged()
	}
	n.failureCode = code
	n.failureAt = time.Now().UTC().Format(time.RFC3339Nano)
}
func connectorControlClient() *http.Client {
	return &http.Client{Timeout: 8 * time.Second, Transport: telemetryTransport{next: http.DefaultTransport, propagate: true, route: "desktop.control"}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func frpProxyRunning(res *http.Response, names ...string) bool {
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return false
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, maxFrame+1))
	if err != nil || len(raw) > maxFrame {
		return false
	}
	var status map[string][]struct{ Name, Status string }
	if json.Unmarshal(raw, &status) != nil {
		return false
	}
	for _, name := range names {
		running := false
		for _, proxies := range status {
			for _, proxy := range proxies {
				if proxy.Name == name && proxy.Status == "running" {
					running = true
				}
			}
		}
		if !running {
			return false
		}
	}
	return true
}

func (n *connector) call(ctx context.Context, path string, body any, result any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", n.control+"/api/v1"+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	n.mu.Lock()
	credential := n.credential
	n.mu.Unlock()
	req.Header.Set("Authorization", "Bearer "+credential)
	req.Header.Set("Content-Type", "application/json")
	res, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden {
		return errDeviceUnavailable
	}
	if res.StatusCode == http.StatusTooManyRequests {
		return errDeviceAdmission
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("connector operation rejected: %d", res.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, maxFrame+1))
	if err != nil || len(data) > maxFrame {
		return errors.New("invalid connector response")
	}
	if result == nil {
		return nil
	}
	return json.Unmarshal(data, result)
}

// stop fences local admission immediately; cancellation stops heartbeats/reconnects.
// The attempt keeps FRPC alive until finishStop completes its bounded drain.
func (n *connector) stop() {
	n.mu.Lock()
	if n.stopping {
		n.mu.Unlock()
		return
	}
	n.stopping = true
	n.state = "stopping"
	n.mu.Unlock()
	n.cancel()
}
func (n *connector) isStopping() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stopping
}
func (n *connector) finishStop() {
	n.stopCleanup.Do(func() {
		if n.upstream != nil {
			defer n.upstream.CloseIdleConnections()
		}
		if n.remoteUpstream != nil {
			defer n.remoteUpstream.CloseIdleConnections()
		}
		n.mu.Lock()
		ep := n.epoch
		n.mu.Unlock()
		if ep > 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			n.call(ctx, "/connector/"+n.nodeID+"/drain", map[string]any{"epoch": ep}, nil)
			cancel()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		peerDone := make(chan struct{})
		go func() {
			if n.stopPeer != nil {
				n.stopPeer()
			}
			close(peerDone)
		}()
		n.server.Shutdown(ctx)
		<-peerDone
	})
}
func (n *connector) run() {
	if n.stopPeer != nil {
		defer n.stopPeer()
	}
	defer close(n.done)
	defer n.setState("stopped", 0)
	defer n.server.Close()
	defer n.upstream.CloseIdleConnections()
	defer func() { n.mu.Lock(); n.credential = ""; n.mu.Unlock() }()
	delay := time.Second
	failover := false
	for n.ctx.Err() == nil {
		var lease nodeLease
		n.setStage("lease")
		n.mu.Lock()
		ownerID := n.ownerID
		n.mu.Unlock()
		err := n.call(n.ctx, "/connector/"+n.nodeID+"/leases", map[string]any{"ownerId": ownerID, "purpose": n.purpose, "failover": failover}, &lease)
		if err == nil {
			if lease.OwnerID != "" {
				n.mu.Lock()
				n.ownerID = lease.OwnerID
				n.mu.Unlock()
			}
			n.setState("connecting", lease.Epoch)
			err = n.attempt(lease)
		}
		if n.ctx.Err() == nil {
			n.recordFailure(err)
		}
		if n.ctx.Err() != nil || errors.Is(err, errDeviceUnavailable) {
			break
		}
		if lease.Epoch > 0 {
			n.call(n.ctx, "/connector/"+n.nodeID+"/drain", map[string]any{"epoch": lease.Epoch}, nil)
			failover = true
		}
		n.setState("reconnecting", 0)
		// Reconnect changes the lease epoch; inference payloads are never replayed here.
		jitter := time.Duration(time.Now().UnixNano()%500) * time.Millisecond
		timer := time.NewTimer(delay + jitter)
		select {
		case <-n.ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		if delay < 16*time.Second {
			delay *= 2
		}
	}
	n.mu.Lock()
	ep := n.epoch
	n.mu.Unlock()
	if n.isStopping() {
		n.finishStop()
	} else if ep > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		n.call(ctx, "/connector/"+n.nodeID+"/drain", map[string]any{"epoch": ep}, nil)
	}
}

var frpDNSLabel = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)

func validFRPHost(host string) bool {
	if ip := net.ParseIP(host); ip != nil {
		return !ip.IsUnspecified() && !ip.IsMulticast()
	}
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if !frpDNSLabel.MatchString(label) {
			return false
		}
	}
	return true
}

func (n *connector) attempt(lease nodeLease) error {
	n.setStage("configuration")
	if lease.Purpose != n.purpose && !(lease.Purpose == "" && n.purpose == "serving") {
		return errors.New("lease purpose mismatch")
	}
	protocol := lease.FRP.TransportProtocol
	if protocol == "" {
		protocol = "tcp"
	}
	if protocol != "tcp" && protocol != "websocket" && protocol != "wss" {
		return errors.New("invalid FRP transport protocol")
	}
	if lease.Epoch < 1 || lease.FRP.ServerPort < 1 || lease.FRP.ServerPort > 65535 || lease.FRP.RemotePort < 20000 || lease.FRP.RemotePort > 29999 || !validFRPHost(lease.FRP.ServerName) || !validFRPHost(lease.FRP.ServerAddr) || lease.FRP.ProxyName != fmt.Sprintf("newnoa_%s_%d", n.nodeID, lease.Epoch) || !strings.HasPrefix(lease.Credential, "frp_") || len(lease.Credential) > 200 {
		return errors.New("invalid lease configuration")
	}
	if _, err := gatewayCertPool(lease.FRP.TrustedCA); err != nil {
		return errors.New("invalid FRP lease CA")
	}
	dir, err := os.MkdirTemp("", "newnoa-frp-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	os.Chmod(dir, 0700)
	ca := filepath.Join(dir, "ca.pem")
	if err = os.WriteFile(ca, []byte(lease.FRP.TrustedCA), 0600); err != nil {
		return err
	}
	statusListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	statusPort := statusListener.Addr().(*net.TCPAddr).Port
	statusListener.Close()
	_, localPort, _ := net.SplitHostPort(n.address)
	port, _ := strconv.Atoi(localPort)
	statusPassword := randomToken()
	config := map[string]any{
		"serverAddr": lease.FRP.ServerAddr, "serverPort": lease.FRP.ServerPort, "loginFailExit": true,
		"metadatas": map[string]any{"nodeId": n.nodeID, "epoch": strconv.FormatInt(lease.Epoch, 10), "credential": lease.Credential},
		"transport": map[string]any{"protocol": protocol, "tls": map[string]any{"enable": true, "trustedCaFile": ca, "serverName": lease.FRP.ServerName}, "heartbeatInterval": 10, "heartbeatTimeout": 30},
		"webServer": map[string]any{"addr": "127.0.0.1", "port": statusPort, "user": "newnoa", "password": statusPassword},
		"log":       map[string]any{"to": "console", "level": "error"},
		"proxies":   []any{map[string]any{"name": lease.FRP.ProxyName, "type": "tcp", "localIP": "127.0.0.1", "localPort": port, "remotePort": lease.FRP.RemotePort}},
	}
	proxyNames := []string{lease.FRP.ProxyName}
	if n.purpose == "serving" && n.peerAddress != "" {
		peerHost, peerPort, _ := net.SplitHostPort(n.peerAddress)
		if ip := net.ParseIP(peerHost); ip == nil || ip.IsUnspecified() {
			peerHost = "127.0.0.1"
		}
		peerNumber, _ := strconv.Atoi(peerPort)
		for _, kind := range []string{"xtcp", "stcp"} {
			suffix := "_peer"
			if kind == "stcp" {
				suffix = "_relay"
			}
			name := lease.FRP.ProxyName + suffix
			proxyNames = append(proxyNames, name)
			proxy := map[string]any{"name": name, "type": kind, "localIP": peerHost, "localPort": peerNumber, "secretKey": n.tunnelSecret}
			config["proxies"] = append(config["proxies"].([]any), proxy)
		}
		if stun := os.Getenv("NEWNOA_STUN_SERVER"); stun != "" {
			config["natHoleStunServer"] = stun
		}
	}
	raw, err := toml.Marshal(config)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "frpc.toml")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		return err
	}
	cmd := exec.Command(n.binary, "-c", path)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	n.setStage("process")
	if err = cmd.Start(); err != nil {
		return err
	}
	exited := make(chan struct{})
	var commandError error
	go func() { commandError = cmd.Wait(); close(exited) }()
	defer func() {
		cmd.Process.Kill()
		select {
		case <-exited:
		case <-time.After(2 * time.Second):
		}
	}()
	defer func() {
		if n.isStopping() {
			n.finishStop()
		}
	}()
	readyDeadline := time.NewTimer(15 * time.Second)
	defer readyDeadline.Stop()
	probe := time.NewTicker(250 * time.Millisecond)
	defer probe.Stop()
	statusClient := &http.Client{Timeout: time.Second}
	n.setStage("tunnel")
	online := false
	for !online {
		select {
		case <-n.ctx.Done():
			return n.ctx.Err()
		case <-exited:
			n.setStage("process")
			return commandError
		case <-readyDeadline.C:
			return errors.New("FRP tunnel startup timed out")
		case <-probe.C:
			req, _ := http.NewRequestWithContext(n.ctx, "GET", fmt.Sprintf("http://127.0.0.1:%d/api/status", statusPort), nil)
			req.SetBasicAuth("newnoa", statusPassword)
			res, err := statusClient.Do(req)
			if err != nil {
				continue
			}
			online = frpProxyRunning(res, proxyNames...)
		}
	}
	n.setStage("ready")
	if err = n.call(n.ctx, "/connector/"+n.nodeID+"/ready", map[string]any{"epoch": lease.Epoch}, nil); err != nil {
		return err
	}
	n.setState("online", lease.Epoch)
	if n.peerURL != "" {
		_, port, _ := net.SplitHostPort(n.peerAddress)
		number, _ := strconv.Atoi(port)
		provider, _ := startOverlayProvider(n.ctx, n.nodeID, lease.Epoch, n.tunnelSecret, number)
		n.mu.Lock()
		n.overlay = provider
		n.mu.Unlock()
		defer func() { provider.Stop(); n.mu.Lock(); n.overlay = nil; n.mu.Unlock() }()
		if err = n.call(n.ctx, "/connector/"+n.nodeID+"/peer", map[string]any{"epoch": lease.Epoch, "url": n.peerURL, "certificateSha256": n.peerPin, "tunnelSecret": n.tunnelSecret, "overlay": provider != nil}, nil); err != nil {
			return err
		}
	}
	n.mu.Lock()
	n.stage = "online"
	if n.failureCode != "" && n.serviceChanged != nil {
		n.serviceChanged()
	}
	n.failureCode = ""
	n.failureAt = ""
	n.mu.Unlock()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return n.ctx.Err()
		case <-exited:
			n.setStage("process")
			return commandError
		case <-ticker.C:
			req, _ := http.NewRequestWithContext(n.ctx, "GET", fmt.Sprintf("http://127.0.0.1:%d/api/status", statusPort), nil)
			req.SetBasicAuth("newnoa", statusPassword)
			res, probeError := statusClient.Do(req)
			running := false
			if probeError == nil {
				running = frpProxyRunning(res, proxyNames...)
			}
			if !running {
				n.setStage("tunnel")
				return errors.New("FRP tunnel unavailable")
			}
			n.setStage("heartbeat")
			if err = n.call(n.ctx, "/connector/"+n.nodeID+"/heartbeat", map[string]any{"epoch": lease.Epoch}, nil); err != nil {
				return err
			}
		}
	}
}
func (n *connector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !n.updateGate.begin() {
		http.Error(w, "Client update installation is starting", http.StatusServiceUnavailable)
		return
	}
	defer n.updateGate.end()
	if n.isStopping() {
		http.Error(w, "connector is draining", http.StatusServiceUnavailable)
		return
	}
	n.mu.Lock()
	purpose, usageQueuePaused := n.purpose, n.usageQueuePaused
	n.mu.Unlock()
	evaluation := r.URL.Path == "/evaluation/v1/chat/completions" || r.URL.Path == "/evaluation/v1/embeddings" || r.URL.Path == "/evaluation/v1/rerank" || r.URL.Path == "/evaluation/v1/systemone" || r.URL.Path == "/evaluation/v1/audio/transcriptions"
	publicationProbe := strings.HasPrefix(r.URL.Path, "/publication-probe/")
	probe := strings.HasPrefix(r.URL.Path, "/probe/") || publicationProbe
	wirePath := r.URL.Path
	if evaluation {
		wirePath = strings.TrimPrefix(wirePath, "/evaluation")
	} else if publicationProbe {
		wirePath = strings.TrimPrefix(wirePath, "/publication-probe")
	} else if probe {
		wirePath = strings.TrimPrefix(wirePath, "/probe")
	}
	models := probe && !publicationProbe && wirePath == "/v1/models"
	if (purpose == "evaluation") != evaluation || probe && purpose != "serving" {
		http.Error(w, "request purpose does not match lease", 403)
		return
	}
	if (models && r.Method != "GET" || !models && r.Method != "POST") || (wirePath == imageEditPath && (probe || evaluation)) || (!evaluation && !models && !historyInferencePath(wirePath)) || r.URL.RawQuery != "" || r.URL.ForceQuery {
		http.Error(w, "not allowed", 404)
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
	requestID := r.Header.Get("X-Newnoa-Request")
	permit := r.Header.Get("X-Newnoa-Permit")
	gateway := r.Header.Get("X-Newnoa-Gateway")
	ep, err := strconv.ParseInt(r.Header.Get("X-Newnoa-Epoch"), 10, 64)
	prefix, consume := "permit_", "/connector/dispatches/"
	if evaluation {
		prefix, consume = "eval_", "/connector/evaluations/"
	} else if probe {
		prefix, consume = "probe_", "/connector/model-checks/"
		if publicationProbe {
			prefix, consume = "pubprobe_", "/connector/publication-checks/"
		}
	}
	if !nodePattern.MatchString(requestID) || !strings.HasPrefix(permit, prefix) || len(permit) > 100 || gateway == "" || len(gateway) > 80 || err != nil || ep < 1 {
		http.Error(w, "invalid execution permit", 401)
		return
	}
	if !evaluation && !probe && usageQueuePaused {
		w.Header().Set("Retry-After", "10")
		w.Header().Set("X-Newnoa-Outcome", "not-started")
		http.Error(w, "usage reporting temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	requestLimit := inferenceRequestLimit(wirePath)
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, requestLimit))
	if err != nil {
		http.Error(w, "request too large", 413)
		return
	}
	var body map[string]any
	beta := ""
	if wirePath == "/v1/messages" && !probe {
		var betaErr error
		beta, betaErr = messagesBeta(r.Header)
		if betaErr != nil {
			http.Error(w, "unsupported Anthropic beta", 400)
			return
		}
	}
	asr := wirePath == "/v1/audio/transcriptions"
	contentType := "application/json"
	if asr {
		contentType = r.Header.Get("Content-Type")
		model, parseErr := transcriptionModel(raw, contentType)
		if parseErr != nil {
			http.Error(w, "invalid multipart body", 400)
			return
		}
		body = map[string]any{"model": model}
	} else if models {
		if len(raw) != 0 {
			http.Error(w, "model listing probe requires an empty body", 400)
			return
		}
	} else if json.Unmarshal(raw, &body) != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	if isImagePath(wirePath) && validateImageRequest(wirePath, raw) != nil {
		http.Error(w, "invalid image generation request", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	var target struct {
		Endpoint, Model, APIKey, Edition, DeploymentID string
		UsageReporting                                 bool `json:"usageReporting"`
		Revision                                       int64
		HTTPMethod                                     string          `json:"httpMethod"`
		InferencePath                                  string          `json:"inferencePath"`
		ExecutionDeadlineAt                            json.RawMessage `json:"executionDeadlineAt"`
	}
	bound := raw
	if !evaluation || r.URL.Path != "/evaluation/v1/chat/completions" {
		prefix := r.Method + " " + wirePath + "\n"
		if asr {
			prefix += contentType + "\n"
		}
		if wirePath == "/v1/messages" && !probe {
			prefix += messagesBinding(beta)
		}
		bound = append([]byte(prefix), raw...)
	}
	claim := map[string]any{"permit": permit, "gatewayId": gateway, "epoch": ep, "bodyDigest": hash(bound)}
	if probe {
		claim["httpMethod"], claim["inferencePath"] = r.Method, wirePath
	}
	if err = n.call(ctx, consume+requestID+"/consume", claim, &target); err != nil {
		if errors.Is(err, errDeviceAdmission) {
			w.Header().Set("Retry-After", "1")
			w.Header().Set("X-Newnoa-Outcome", "not-started")
			http.Error(w, "execution admission temporarily unavailable", 429)
			return
		}
		http.Error(w, "execution permit rejected", 409)
		return
	}
	var capture *usageCapture
	if target.UsageReporting && !evaluation && !probe && n.usageStore != nil {
		capture = newUsageCapture(false)
		capture.protocol = wirePath
		capture.report.Status = "failed"
		defer func() { n.saveUsage(capture, requestID, ep, gateway) }()
	}
	var cancel context.CancelFunc
	if evaluation || probe {
		ctx, cancel = context.WithTimeout(ctx, legacyExecutionTimeout)
	} else {
		var deadlineErr error
		ctx, cancel, deadlineErr = executionContext(ctx, target.ExecutionDeadlineAt, time.Now())
		if deadlineErr != nil {
			http.Error(w, "execution deadline invalid", http.StatusBadGateway)
			return
		}
	}
	defer cancel()
	if probe && (target.HTTPMethod != r.Method || target.InferencePath != wirePath) {
		http.Error(w, "probe target scope mismatch", 409)
		return
	}
	edition := target.Edition
	if edition == "" {
		edition = "domestic"
	}
	policy, policyErr := configuredSupplyPolicy(edition)
	if policyErr != nil {
		http.Error(w, "invalid supply policy", 409)
		return
	}
	u, err := policy.endpoint(target.Endpoint)
	if err != nil || !models && body["model"] != target.Model {
		http.Error(w, "local target mismatch", 409)
		return
	}
	var endpointPing peernet.EndpointPing
	if models {
		pingStart := time.Now()
		endpointPing = peernet.PingEndpoint(ctx, target.Endpoint)
		w.Header().Set("X-Newnoa-Probe-Ping-Elapsed-Ms", strconv.FormatFloat(float64(time.Since(pingStart))/float64(time.Millisecond), 'f', 3, 64))
		w.Header().Set("X-Newnoa-Probe-Ping-Status", endpointPing.Status)
		if endpointPing.Status == "succeeded" {
			w.Header().Set("X-Newnoa-Probe-Ping-Ms", strconv.FormatFloat(endpointPing.Milliseconds, 'f', 3, 64))
		}
	}
	if !evaluation && !probe {
		n.refreshAssessment(policy, target.DeploymentID, target.Revision, target.Endpoint, target.APIKey)
	}
	path := wirePath
	base := modelServiceBase(u.Path)
	if strings.HasSuffix(base, "/v1") && strings.HasPrefix(path, "/v1/") {
		path = strings.TrimPrefix(path, "/v1")
	}
	u.Path = base + path
	u.RawPath = ""
	up, err := http.NewRequestWithContext(ctx, r.Method, u.String(), bytes.NewReader(raw))
	if err != nil {
		http.Error(w, "invalid target", 502)
		return
	}
	up.GetBody = nil
	if models {
		// A non-nil empty body disables Transport's GET replay on reused sockets.
		up.Body = io.NopCloser(bytes.NewReader(raw))
	}
	up.Header.Set("Content-Type", contentType)
	if wirePath == "/v1/messages" {
		up.Header.Set("Anthropic-Version", "2023-06-01")
		if beta != "" {
			up.Header.Set("Anthropic-Beta", beta)
		}
	}
	if target.APIKey != "" {
		if wirePath == "/v1/messages" {
			up.Header.Set("X-Api-Key", target.APIKey)
		} else {
			up.Header.Set("Authorization", "Bearer "+target.APIKey)
		}
	}
	client := n.upstream
	if edition == "overseas" {
		client = n.remoteUpstream
		if client == nil {
			client = policy.client(0)
			defer client.CloseIdleConnections()
		}
	}
	if capture != nil {
		capture.report.Status = "unknown"
	}
	res, err := client.Do(up)
	if err != nil {
		w.Header().Set("X-Newnoa-Outcome", "unknown")
		http.Error(w, "execution outcome unknown", 502)
		return
	}
	defer res.Body.Close()
	if isImagePath(wirePath) && boundImageResponse(res) != nil {
		http.Error(w, "image response exceeds limit or is not supported", http.StatusBadGateway)
		return
	}
	if isImagePath(wirePath) && !probe && res.StatusCode >= 200 && res.StatusCode < 300 {
		if err := n.hostLocalImages(ctx, res, u.String(), client, requestID, permit, gateway, ep); err != nil {
			failure := localImageStorageFailure(res)
			if capture != nil {
				captureImageFailureUsage(capture, failure)
				capture.report.TransportConfirmed = true
				capture.report.Status = "failed"
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Newnoa-Image-Storage-Failed", "1")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write(failure)
			return
		}
	}
	if probe {
		w.Header().Set("X-Newnoa-Probe-Upstream", "1")
	}
	if capture != nil {
		capture.stream = strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream")
		capture.report.TransportConfirmed = true
		confirmCtx, confirmCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		if err := n.call(confirmCtx, "/connector/dispatches/"+requestID+"/private-transport", map[string]any{"nodeId": n.nodeID, "epoch": ep, "gatewayId": gateway}, nil); err != nil {
			n.usageFailure(errors.New("private_transport_confirmation_pending"))
		}
		confirmCancel()
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			capture.report.Status = "failed"
		}
	}
	w.Header().Set("Content-Type", res.Header.Get("Content-Type"))
	w.WriteHeader(res.StatusCode)
	buf := make([]byte, 32*1024)
	for {
		count, readErr := res.Body.Read(buf)
		if count > 0 {
			if capture != nil {
				capture.feed(buf[:count])
			}
			if _, err = w.Write(buf[:count]); err != nil {
				return
			}
			if err = http.NewResponseController(w).Flush(); err != nil {
				return
			}
		}
		if readErr != nil {
			if capture != nil {
				capture.counts()
				if readErr == io.EOF && !capture.invalid && res.StatusCode >= 200 && res.StatusCode < 300 && (!capture.stream || capture.completed) {
					capture.report.Status = "succeeded"
				}
			}
			return
		}
	}
}
func transcriptionModel(raw []byte, contentType string) (string, error) {
	kind, params, err := mime.ParseMediaType(contentType)
	if err != nil || kind != "multipart/form-data" || params["boundary"] == "" {
		return "", errors.New("multipart required")
	}
	reader := multipart.NewReader(bytes.NewReader(raw), params["boundary"])
	model := ""
	for count := 0; ; count++ {
		part, err := reader.NextRawPart()
		if err == io.EOF {
			break
		}
		if err != nil || count >= 6 {
			return "", errors.New("invalid multipart")
		}
		if part.FormName() == "model" {
			if model != "" || part.FileName() != "" || part.Header.Get("Content-Transfer-Encoding") != "" {
				return "", errors.New("ambiguous model")
			}
			value, err := io.ReadAll(io.LimitReader(part, 257))
			if err != nil || len(value) == 0 || len(value) > 256 {
				return "", errors.New("invalid model")
			}
			model = string(value)
		}
	}
	if model == "" {
		return "", errors.New("model required")
	}
	return model, nil
}

func replaceTranscriptionModel(raw []byte, contentType, model string) ([]byte, string, error) {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, "", err
	}
	reader := multipart.NewReader(bytes.NewReader(raw), params["boundary"])
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for {
		part, err := reader.NextRawPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", err
		}
		out, err := writer.CreatePart(part.Header)
		if err != nil {
			return nil, "", err
		}
		if part.FormName() == "model" {
			_, err = io.WriteString(out, model)
		} else {
			_, err = io.Copy(out, part)
		}
		if err != nil {
			return nil, "", err
		}
	}
	if err = writer.Close(); err != nil {
		return nil, "", err
	}
	return body.Bytes(), writer.FormDataContentType(), nil
}

func (c *core) shutdown() {
	c.cancelNoaTasks("")
	c.historyEvents.close()
	if c.publicStateless {
		if c.publicSVGCancel != nil {
			c.publicSVGCancel()
		}
		c.cancelSVGTests()
	}
	c.setProxySession("")
	c.svgWorkers.Wait()
	if c.hostedCancel != nil {
		c.hostedCancel()
		<-c.hostedDone
	}
	c.historyMu.Lock()
	history := c.history
	c.historyMu.Unlock()
	if history != nil && history.manager != nil {
		m := history.manager
		m.childrenMu.Lock()
		m.closing = true
		m.childrenMu.Unlock()
		m.childMu.Lock()
		m.childMu.Unlock()
		m.initWG.Wait()
		m.mu.Lock()
		var pending []chan struct{}
		for _, job := range m.jobs {
			if job.State == "running" {
				job.cancel()
				if job.done != nil {
					pending = append(pending, job.done)
				}
			}
		}
		m.mu.Unlock()
		for _, done := range pending {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
	}
	if c.modelTestServer != nil {
		c.modelTestServer.Close()
	}
	c.close()
	if c.proxyListener != nil {
		c.proxyListener.Close()
		c.proxyListener = nil
	}
	if c.connector != nil {
		c.connector.stop()
		select {
		case <-c.connector.done:
		case <-time.After(5 * time.Second):
		}
	}
}
