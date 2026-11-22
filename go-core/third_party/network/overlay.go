package network

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pelletier/go-toml/v2"
)

// Separate networks and a userspace /32 proxy expose only the pinned Core TLS port.
func overlayIdentity(node string, epoch int64, secret string) (string, error) {
	b, err := hex.DecodeString(secret)
	if !nodePattern.MatchString(node) || epoch < 1 || err != nil || len(b) != 32 || secret != strings.ToLower(secret) {
		return "", errors.New("invalid overlay identity")
	}
	return "newnoa-" + hash([]byte(fmt.Sprintf("overlay-v1\n%s\n%d\n%s", node, epoch, secret)))[:32], nil
}
func verifiedOverlayArtifact(name string) (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	suffix := ""
	if strings.HasSuffix(exe, ".exe") {
		suffix = ".exe"
	}
	path := filepath.Join(filepath.Dir(exe), name+suffix)
	digest, err := artifactDigest(path)
	if err != nil {
		return "", errors.New("EasyTier is not installed")
	}
	manifest, err := os.ReadFile(filepath.Join(filepath.Dir(exe), name+"-manifest.json"))
	if err != nil {
		return "", errors.New("EasyTier manifest missing")
	}
	var m struct {
		SHA     string `json:"sha256"`
		Version string `json:"version"`
	}
	if json.Unmarshal(manifest, &m) != nil || m.Version != "2.6.4" || m.SHA != digest {
		return "", errors.New("EasyTier integrity check failed")
	}
	return path, nil
}
func loopbackPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
func overlayFlags() map[string]any {
	return map[string]any{"no_tun": true, "use_smoltcp": true, "bind_device": false, "enable_ipv6": false, "disable_upnp": true, "disable_kcp_input": true, "disable_quic_input": true, "relay_network_whitelist": ""}
}
func overlayBase(name, secret, host string) map[string]any {
	return map[string]any{"hostname": host, "listeners": []string{}, "network_identity": map[string]any{"network_name": name, "network_secret": secret}, "stun_servers": []string{"stun.easyvoip.com:3478", "stun.miwifi.com:3478", "stun.chat.bilibili.com:3478", "stun.cloudflare.com:3478"}, "flags": overlayFlags()}
}
func overlayProviderConfigs(network, secret string, coordPort, peerPort int) (map[string]any, map[string]any) {
	coord := overlayBase(network+"-coord", hash([]byte("coord\n"+secret)), "newnoa-rendezvous")
	coord["listeners"] = []string{fmt.Sprintf("ws://127.0.0.1:%d", coordPort)}
	coord["flags"].(map[string]any)["relay_all_peer_rpc"] = true
	provider := overlayBase(network, hash([]byte("transport\n"+secret)), "newnoa-provider")
	provider["ipv4"] = "10.239.0.1"
	provider["peer"] = []any{map[string]any{"uri": fmt.Sprintf("ws://127.0.0.1:%d", coordPort)}}
	provider["proxy_network"] = []any{map[string]any{"cidr": "127.0.0.1/32", "mapped_cidr": "10.240.0.1/32"}}
	// Subnet proxy traffic uses the Forward chain; inbound whitelists alone do not protect it.
	provider["acl"] = map[string]any{"acl_v1": map[string]any{"chains": []any{map[string]any{
		"name": "model-port-only", "chain_type": 3, "enabled": true, "default_action": 2,
		"rules": []any{map[string]any{"name": "core-tls", "priority": 1000, "enabled": true, "protocol": 1, "ports": []string{strconv.Itoa(peerPort)}, "destination_ips": []string{"10.240.0.1/32"}, "action": 1, "stateful": true}},
	}}}}
	provider["tcp_whitelist"] = []string{strconv.Itoa(peerPort)}
	provider["udp_whitelist"] = []string{"0"}
	provider["flags"].(map[string]any)["p2p_only"] = true
	return coord, provider
}

type overlayProcess struct {
	dir     string
	cancel  context.CancelFunc
	done    chan struct{}
	mu      sync.Mutex
	process *os.Process
	once    sync.Once
	logs    *peerLogTail
}

func startOverlayProcess(parent context.Context, binary string, config map[string]any, rpcPort int) (*overlayProcess, error) {
	dir, err := os.MkdirTemp("", "newnoa-overlay-")
	if err != nil {
		return nil, err
	}
	raw, err := toml.Marshal(config)
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	path := filepath.Join(dir, "config.toml")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	p := &overlayProcess{dir: dir, cancel: cancel, done: make(chan struct{}), logs: &peerLogTail{}}
	start := func() (*exec.Cmd, error) {
		cmd := exec.CommandContext(ctx, binary, "-c", path, "--rpc-portal", fmt.Sprintf("127.0.0.1:%d", rpcPort), "--console-log-level", "warn")
		cmd.Stdout, cmd.Stderr = p.logs, p.logs
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		p.mu.Lock()
		p.process = cmd.Process
		p.mu.Unlock()
		return cmd, nil
	}
	cmd, err := start()
	if err != nil {
		cancel()
		os.RemoveAll(dir)
		return nil, err
	}
	go func() {
		defer close(p.done)
		defer os.RemoveAll(dir)
		for {
			if err := cmd.Wait(); err != nil {
				fmt.Fprintf(p.logs, "\nprocess exit: %v\n", err)
			}
			p.mu.Lock()
			p.process = nil
			p.mu.Unlock()
			timer := time.NewTimer(time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			for ctx.Err() == nil {
				cmd, err = start()
				if err == nil {
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
			if ctx.Err() != nil {
				if cmd != nil {
					cmd.Wait()
				}
				return
			}
		}
	}()
	return p, nil
}
func (p *overlayProcess) pid() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.process == nil {
		return 0
	}
	return p.process.Pid
}
func (p *overlayProcess) stop() {
	if p == nil {
		return
	}
	p.once.Do(func() { p.cancel(); <-p.done })
}

func overlayBootstrap(target, secret string) http.Handler {
	u, _ := url.Parse(target)
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.Transport = &http.Transport{Proxy: nil}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		http.Error(w, "coordination unavailable", 503)
	}
	slots := make(chan struct{}, 8)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Newnoa-Overlay")), []byte(secret)) != 1 {
			http.Error(w, "coordination denied", 403)
			return
		}
		if r.Method != "GET" || r.URL.Path != "/peer/p2p" || r.URL.RawQuery != "" || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "websocket required", 400)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			http.Error(w, "coordination busy", 429)
			return
		}
		r.Header.Del("X-Newnoa-Overlay")
		proxy.ServeHTTP(w, r)
	})
}

type overlayProvider struct {
	handler       http.Handler
	coord, worker *overlayProcess
	once          sync.Once
}

func startOverlayProvider(ctx context.Context, node string, epoch int64, secret string, peerPort int) (*overlayProvider, error) {
	if os.Getenv("NEWNOA_EASYTIER") == "off" {
		return nil, errors.New("EasyTier disabled")
	}
	binary, err := verifiedOverlayArtifact("easytier-core")
	if err != nil {
		return nil, err
	}
	network, err := overlayIdentity(node, epoch, secret)
	if err != nil {
		return nil, err
	}
	coordPort, err := loopbackPort()
	if err != nil {
		return nil, err
	}
	coordCfg, workerCfg := overlayProviderConfigs(network, secret, coordPort, peerPort)
	coord, err := startOverlayProcess(ctx, binary, coordCfg, 0)
	if err != nil {
		return nil, err
	}
	worker, err := startOverlayProcess(ctx, binary, workerCfg, 0)
	if err != nil {
		coord.stop()
		return nil, err
	}
	return &overlayProvider{handler: overlayBootstrap(fmt.Sprintf("http://127.0.0.1:%d", coordPort), hash([]byte(network+"\n"+secret))), coord: coord, worker: worker}, nil
}
func (p *overlayProvider) stop() {
	if p != nil {
		p.once.Do(func() { p.worker.stop(); p.coord.stop() })
	}
}

func overlayDirect(raw []byte) bool {
	var peers []struct {
		Route struct {
			Hostname string `json:"hostname"`
			Cost     int    `json:"cost"`
			PeerID   uint32 `json:"peer_id"`
			Next     uint32 `json:"next_hop_peer_id"`
		} `json:"route"`
		Peer struct {
			Conns []struct {
				Closed bool `json:"is_closed"`
				Tunnel struct {
					Type   string `json:"tunnel_type"`
					Remote struct {
						URL string `json:"url"`
					} `json:"remote_addr"`
				} `json:"tunnel"`
			} `json:"conns"`
		} `json:"peer"`
	}
	if json.Unmarshal(raw, &peers) != nil {
		return false
	}
	for _, p := range peers {
		if p.Route.Hostname != "newnoa-provider" || p.Route.Cost != 1 || p.Route.PeerID != p.Route.Next {
			continue
		}
		for _, c := range p.Peer.Conns {
			if !c.Closed && c.Tunnel.Type == "udp" && strings.HasPrefix(c.Tunnel.Remote.URL, "udp://") {
				return true
			}
		}
	}
	return false
}

func overlayPeerSummary(raw []byte) string {
	var peers []struct {
		Route struct {
			Hostname string `json:"hostname"`
			Cost     int    `json:"cost"`
			PeerID   uint32 `json:"peer_id"`
			Next     uint32 `json:"next_hop_peer_id"`
		} `json:"route"`
		Peer struct {
			Conns []struct {
				Closed bool `json:"is_closed"`
				Tunnel struct {
					Type string `json:"tunnel_type"`
				} `json:"tunnel"`
			} `json:"conns"`
		} `json:"peer"`
	}
	if err := json.Unmarshal(raw, &peers); err != nil {
		return "Invalid or truncated EasyTier peer status JSON: " + err.Error()
	}
	result := map[string]any{"peerCount": len(peers), "providerPresent": false, "udpDirect": overlayDirect(raw)}
	for _, p := range peers {
		if p.Route.Hostname == "newnoa-provider" {
			connections := []string{}
			for _, c := range p.Peer.Conns {
				if len(connections) < 16 {
					connections = append(connections, fmt.Sprintf("%s closed=%t", c.Tunnel.Type, c.Closed))
				}
			}
			result["providerPresent"] = true
			result["providerCost"] = p.Route.Cost
			result["nextHopMatches"] = p.Route.PeerID == p.Route.Next
			result["connections"] = connections
			break
		}
	}
	data, _ := json.Marshal(result)
	return string(data)
}

type overlaySession struct {
	mu         sync.Mutex
	identity   string
	cancel     context.CancelFunc
	done       chan struct{}
	state, url string
	lastUsed   time.Time
	lastDemand time.Time
	active     int
	retired    bool
	evidence   peerEvidenceEvent
	process    *overlayProcess
	coordLogs  *peerLogTail
}

func (s *overlaySession) directURL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == "direct" {
		return s.url
	}
	return ""
}
func (s *overlaySession) acquire() (string, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != "direct" || s.retired {
		return "", func() {}
	}
	s.active++
	var once sync.Once
	return s.url, func() { once.Do(func() { s.mu.Lock(); s.active--; s.lastUsed = time.Now(); s.mu.Unlock() }) }
}
func (s *overlaySession) retire(idle bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != 0 || (idle && time.Since(s.lastUsed) <= 2*time.Minute) {
		return false
	}
	s.retired = true
	s.cancel()
	return true
}
func (s *overlaySession) expired() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active == 0 && time.Since(s.lastUsed) > 2*time.Minute
}
func (s *overlaySession) setState(state string) { s.mu.Lock(); s.state = state; s.mu.Unlock() }

func (s *overlaySession) observe(code, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evidence = peerEvidenceEvent{At: time.Now().UTC(), Stage: "easytier", Code: code, Detail: scrubPeerEvidence(detail)}
}
func (s *overlaySession) diagnostic() []peerEvidenceEvent {
	s.mu.Lock()
	event, state, process, coordLogs := s.evidence, s.state, s.process, s.coordLogs
	s.mu.Unlock()
	if event.Code == "" {
		event = peerEvidenceEvent{At: time.Now().UTC(), Stage: "easytier", Code: "session_preparing", Detail: "No UDP direct status has been observed yet"}
	}
	event.Detail = "state=" + state + "; " + event.Detail
	result := []peerEvidenceEvent{event}
	if coordLogs != nil {
		if logs := coordLogs.snapshot(); logs != "" {
			result = append(result, peerEvidenceEvent{time.Now().UTC(), "easytier", "coordination_log", logs})
		}
	}
	if process != nil {
		if logs := process.logs.snapshot(); logs != "" {
			result = append(result, peerEvidenceEvent{time.Now().UTC(), "easytier", "process_log", logs})
		}
	}
	return result
}

type peerOverlayManager struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	entries map[string]*overlaySession
	path    string
}

func newPeerOverlayManager() *peerOverlayManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &peerOverlayManager{ctx: ctx, cancel: cancel, entries: map[string]*overlaySession{}}
}
func (m *peerOverlayManager) reset() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.cancel()
	entries := m.entries
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.entries = map[string]*overlaySession{}
	m.path = ""
	m.mu.Unlock()
	for _, s := range entries {
		s.cancel()
		<-s.done
	}
}
func (m *peerOverlayManager) selected(path string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.path = path
	m.mu.Unlock()
}
func (m *peerOverlayManager) status() map[string]any {
	if m == nil {
		return map[string]any{"state": "idle"}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	state := "idle"
	for _, s := range m.entries {
		s.mu.Lock()
		v := s.state
		s.mu.Unlock()
		if state != "direct" {
			state = v
		}
	}
	return map[string]any{"state": state, "path": m.path, "sessions": len(m.entries)}
}
func (m *peerOverlayManager) ensure(route peerRoute, deployment, key string, generation ...context.Context) *overlaySession {
	s, _ := m.prepare(route, deployment, key, generation...)
	return s
}
func (m *peerOverlayManager) prepare(route peerRoute, deployment, key string, generation ...context.Context) (*overlaySession, error) {
	if m == nil {
		return nil, errors.New("EasyTier manager unavailable")
	}
	if !route.Overlay {
		return nil, errors.New("Provider did not advertise EasyTier capability")
	}
	if route.Tunnel == nil {
		return nil, errors.New("EasyTier coordination tunnel unavailable")
	}
	if os.Getenv("NEWNOA_EASYTIER") == "off" {
		return nil, errors.New("EasyTier disabled locally")
	}
	if _, err := verifiedOverlayArtifact("easytier-core"); err != nil {
		return nil, fmt.Errorf("easytier-core: %w", err)
	}
	if _, err := verifiedOverlayArtifact("easytier-cli"); err != nil {
		return nil, fmt.Errorf("easytier-cli: %w", err)
	}
	network, err := overlayIdentity(route.NodeID, route.Epoch, route.Tunnel.Secret)
	if err != nil {
		return nil, err
	}
	cacheKey := hash([]byte(key + "\n" + deployment + "\n" + route.Tunnel.ConsumerSession))
	identity := hash([]byte(network + route.Pin + route.URL))
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(generation) > 0 && (generation[0] != m.ctx || generation[0].Err() != nil) {
		return nil, errors.New("EasyTier generation replaced")
	}
	for k, s := range m.entries {
		select {
		case <-s.done:
			delete(m.entries, k)
		default:
		}
	}
	if old := m.entries[cacheKey]; old != nil {
		old.mu.Lock()
		retired := old.retired
		old.mu.Unlock()
		if old.identity == identity && !retired {
			old.mu.Lock()
			if len(generation) == 0 {
				old.lastUsed = time.Now()
				old.lastDemand = old.lastUsed
			}
			old.mu.Unlock()
			return old, nil
		}
		old.cancel()
		delete(m.entries, cacheKey)
	}
	if len(m.entries) >= 4 {
		if len(generation) > 0 {
			return nil, errors.New("EasyTier session capacity reached")
		}
		var victim string
		var oldest time.Time
		for k, s := range m.entries {
			s.mu.Lock()
			demand, active := s.lastDemand, s.active
			s.mu.Unlock()
			if active == 0 && (victim == "" || demand.Before(oldest)) {
				victim, oldest = k, demand
			}
		}
		if victim == "" {
			return nil, errors.New("EasyTier sessions are busy")
		}
		if !m.entries[victim].retire(false) {
			return nil, errors.New("EasyTier session became busy")
		}
		delete(m.entries, victim)
	}
	ctx, cancel := context.WithCancel(m.ctx)
	s := &overlaySession{identity: identity, cancel: cancel, done: make(chan struct{}), state: "preparing", lastUsed: time.Now()}
	if len(generation) == 0 {
		s.lastDemand = s.lastUsed
	}
	m.entries[cacheKey] = s
	go func() {
		defer close(s.done)
		defer s.setState("stopped")
		for ctx.Err() == nil {
			expired := s.expired()
			if expired && s.retire(true) {
				return
			}
			s.run(ctx, route, deployment, key, network)
			if ctx.Err() != nil {
				return
			}
			s.setState("reconnecting")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return s, nil
}
func (s *overlaySession) run(ctx context.Context, route peerRoute, deployment, key, network string) {
	// Only coordination uses this encrypted FRP mapping; its EasyTier node refuses data forwarding.
	coordLogs := &peerLogTail{secrets: []string{key, route.Tunnel.Secret, route.Tunnel.ConsumerSession}}
	s.mu.Lock()
	s.coordLogs = coordLogs
	s.mu.Unlock()
	bootstrapURL, stop, bootstrapExited, err := startPeerTunnel(ctx, route, deployment, key, coordLogs)
	if err != nil {
		s.observe("coordination_start_failed", err.Error())
		return
	}
	defer stop()
	remote := route
	remote.URL = bootstrapURL
	remote.DirectDial = nil
	transport, err := peerTransport(remote)
	if err != nil {
		s.observe("coordination_transport_failed", err.Error())
		return
	}
	defer transport.CloseIdleConnections()
	transport.TLSHandshakeTimeout = 15 * time.Second
	target, _ := url.Parse(bootstrapURL)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = transport
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		director(r)
		r.Header.Set("X-Newnoa-Overlay", hash([]byte(network+"\n"+route.Tunnel.Secret)))
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		s.observe("coordination_failed", err.Error())
		http.Error(w, "coordination unavailable", 503)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.observe("local_listener_failed", err.Error())
		return
	}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: 5 * time.Second}
	go server.Serve(listener)
	defer server.Close()
	_, peerPort, err := net.SplitHostPort(targetHost(route.URL))
	if err != nil {
		s.observe("configuration_failed", err.Error())
		return
	}
	number, err := strconv.Atoi(peerPort)
	if err != nil {
		s.observe("configuration_failed", err.Error())
		return
	}
	port, err := loopbackPort()
	if err != nil {
		s.observe("local_listener_failed", err.Error())
		return
	}
	rpc, err := loopbackPort()
	if err != nil {
		s.observe("local_listener_failed", err.Error())
		return
	}
	cfg := overlayBase(network, hash([]byte("transport\n"+route.Tunnel.Secret)), "newnoa-consumer")
	// Addresses exist only inside userspace; no host routes or TUN privileges are created.
	random, _ := hex.DecodeString(randomToken()[:4])
	cfg["ipv4"] = fmt.Sprintf("10.239.%d.%d", random[0], int(random[1])%253+2)
	cfg["peer"] = []any{map[string]any{"uri": "ws://" + listener.Addr().String() + "/peer/p2p"}}
	cfg["port_forward"] = []any{map[string]any{"bind_addr": fmt.Sprintf("127.0.0.1:%d", port), "dst_addr": fmt.Sprintf("10.240.0.1:%d", number), "proto": "tcp"}}
	cfg["tcp_whitelist"], cfg["udp_whitelist"] = []string{"0"}, []string{"0"}
	cfg["flags"].(map[string]any)["p2p_only"] = true
	binary, err := verifiedOverlayArtifact("easytier-core")
	if err != nil {
		s.observe("artifact_unavailable", err.Error())
		return
	}
	cli, err := verifiedOverlayArtifact("easytier-cli")
	if err != nil {
		s.observe("artifact_unavailable", err.Error())
		return
	}
	process, err := startOverlayProcess(ctx, binary, cfg, rpc)
	if err != nil {
		s.observe("process_start_failed", err.Error())
		return
	}
	defer process.stop()
	s.mu.Lock()
	s.process = process
	s.url = fmt.Sprintf("https://127.0.0.1:%d", port)
	s.mu.Unlock()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	wasDirect := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-bootstrapExited:
			bootstrapExited = nil
			if s.directURL() == "" {
				s.observe("coordination_exited", "Coordination FRP process exited before UDP direct connection")
				return
			}
		case <-ticker.C:
			expired := s.expired()
			if expired && s.retire(true) {
				return
			}
			probe, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
			cmd := exec.CommandContext(probe, cli, "-p", fmt.Sprintf("127.0.0.1:%d", rpc), "-v", "-o", "json", "peer")
			output := &peerLogTail{limit: maxFrame}
			cmd.Stdout, cmd.Stderr = output, output
			err := cmd.Run()
			cancel()
			// Parse the bounded original JSON; redaction belongs only in the diagnostic copy.
			output.mu.Lock()
			raw := []byte(output.text)
			output.mu.Unlock()
			if err != nil {
				s.observe("status_probe_failed", err.Error()+"; "+output.snapshot())
			} else {
				s.observe("udp_status", overlayPeerSummary(raw))
			}
			if err == nil && overlayDirect(raw) {
				s.setState("direct")
				wasDirect = true
			} else if bootstrapExited == nil {
				return
			} else if wasDirect {
				s.setState("reconnecting")
			} else {
				s.setState("preparing")
			}
		}
	}
}
func targetHost(origin string) string {
	u, err := url.Parse(origin)
	if err != nil {
		return ""
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "443")
	}
	return host
}
