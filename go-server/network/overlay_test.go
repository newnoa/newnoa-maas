package network

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestOverlayIdentityFencesNodesEpochsAndSecrets(t *testing.T) {
	id := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	a, err := overlayIdentity(id, 1, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []struct {
		id     string
		epoch  int64
		secret string
	}{{id, 2, strings.Repeat("a", 64)}, {"bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", 1, strings.Repeat("a", 64)}, {id, 1, strings.Repeat("b", 64)}} {
		b, err := overlayIdentity(x.id, x.epoch, x.secret)
		if err != nil || a == b {
			t.Fatal("network identities overlap")
		}
	}
	for _, s := range []string{"", strings.Repeat("x", 64)} {
		if _, err := overlayIdentity(id, 1, s); err == nil {
			t.Fatal("invalid identity accepted")
		}
	}
}

func TestOverlayPeerStatusRequiresActualUDPNextHop(t *testing.T) {
	raw := `[{"route":{"hostname":"newnoa-provider","peer_id":7,"next_hop_peer_id":7,"cost":1},"peer":{"conns":[{"tunnel":{"tunnel_type":"udp","remote_addr":{"url":"udp://203.0.113.8:1234"}},"is_closed":false}]}}]`
	if !overlayDirect([]byte(raw)) {
		t.Fatal("direct UDP not recognized")
	}
	for _, s := range []string{strings.ReplaceAll(raw, `"udp"`, `"ws"`), strings.ReplaceAll(raw, `"cost":1`, `"cost":2`), strings.ReplaceAll(raw, `"next_hop_peer_id":7`, `"next_hop_peer_id":8`), strings.ReplaceAll(raw, `"is_closed":false`, `"is_closed":true`), `{}`} {
		if overlayDirect([]byte(s)) {
			t.Fatal("relay or invalid peer marked direct")
		}
	}
}

func TestOverlayBootstrapRequiresSecretAndUpgrade(t *testing.T) {
	calls := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(204) }))
	defer backend.Close()
	h := overlayBootstrap(backend.URL, strings.Repeat("a", 64))
	for _, secret := range []string{"", strings.Repeat("b", 64)} {
		r := httptest.NewRequest("GET", "https://peer/peer/p2p", nil)
		r.Header.Set("X-Newnoa-Overlay", secret)
		r.Header.Set("Upgrade", "websocket")
		r.Header.Set("Connection", "Upgrade")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 403 {
			t.Fatal(w.Code)
		}
	}
	r := httptest.NewRequest("GET", "https://peer/peer/p2p", nil)
	r.Header.Set("X-Newnoa-Overlay", strings.Repeat("a", 64))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 400 || calls != 0 {
		t.Fatal("unauthorized coordination reached backend")
	}
}

func TestOverlayProcessRecoversAndRemovesPrivateConfig(t *testing.T) {
	binary := os.Getenv("NEWNOA_TEST_EASYTIER")
	if binary == "" {
		t.Skip("set NEWNOA_TEST_EASYTIER to pinned binary")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p, err := startOverlayProcess(ctx, binary, map[string]any{"listeners": []string{}, "network_identity": map[string]any{"network_name": "test-" + randomToken(), "network_secret": randomToken()}, "flags": map[string]any{"no_tun": true, "bind_device": false, "enable_ipv6": false}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer p.stop()
	first := p.pid()
	if first == 0 {
		t.Fatal("process not started")
	}
	if st, err := os.Stat(p.dir); err != nil || st.Mode().Perm() != 0700 {
		t.Fatal("unsafe config directory")
	}
	proc, _ := os.FindProcess(first)
	proc.Kill()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) && (p.pid() == first || p.pid() == 0) {
		time.Sleep(50 * time.Millisecond)
	}
	if p.pid() == first || p.pid() == 0 {
		t.Fatal("process did not recover")
	}
	p.stop()
	if _, err := os.Stat(filepath.Join(p.dir, "config.toml")); !os.IsNotExist(err) {
		t.Fatal("secret config retained")
	}
}

func TestOverlayConfigDoesNotExportHostNetwork(t *testing.T) {
	coord, provider := overlayProviderConfigs("test", strings.Repeat("a", 64), 16211, 16218)
	raw, _ := json.Marshal(provider)
	if strings.Contains(string(raw), "192.168") {
		t.Fatal("LAN exported")
	}
	flags := provider["flags"].(map[string]any)
	if flags["no_tun"] != true || flags["p2p_only"] != true {
		t.Fatal("unbounded network")
	}
	if coord["flags"].(map[string]any)["relay_network_whitelist"] != "" {
		t.Fatal("coordinator may relay inference")
	}
	if len(provider["tcp_whitelist"].([]string)) != 1 || provider["tcp_whitelist"].([]string)[0] != "16218" {
		t.Fatal("extra service exposed")
	}
}

func TestOverlayUserspaceOnlyExportsWhitelistedPort(t *testing.T) {
	testOverlayUserspace(t, false)
}

func TestOverlayLoopbackUDPDataPath(t *testing.T) {
	testOverlayUserspace(t, true)
}

func testOverlayUserspace(t *testing.T, loopback bool) {
	binary := os.Getenv("NEWNOA_TEST_EASYTIER")
	if binary == "" {
		t.Skip("set NEWNOA_TEST_EASYTIER to pinned binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	serve := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("model-port-only")) }))
	}
	allowed, forbidden := serve(), serve()
	defer allowed.Close()
	defer forbidden.Close()
	portOf := func(origin string) int { u, _ := url.Parse(origin); p, _ := strconv.Atoi(u.Port()); return p }
	coordPort, _ := loopbackPort()
	rpc, _ := loopbackPort()
	forward, _ := loopbackPort()
	blocked, _ := loopbackPort()
	wrong, _ := loopbackPort()
	network, secret := "test-"+randomToken(), randomToken()
	coordCfg, providerCfg := overlayProviderConfigs(network, secret, coordPort, portOf(allowed.URL))
	var peerURI string
	if loopback {
		// This variant tests the data path, not discovery through public STUN.
		listener, err := net.ListenPacket("udp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		peerURI = "udp://" + listener.LocalAddr().String()
		if err = listener.Close(); err != nil {
			t.Fatal(err)
		}
		coordCfg["stun_servers"], providerCfg["stun_servers"] = []string{}, []string{}
		providerCfg["listeners"] = []string{peerURI}
	}
	coord, e := startOverlayProcess(ctx, binary, coordCfg, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer coord.stop()
	provider, e := startOverlayProcess(ctx, binary, providerCfg, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer provider.stop()
	cfg := overlayBase(network, hash([]byte("transport\n"+secret)), "newnoa-consumer")
	cfg["ipv4"] = "10.239.0.2"
	cfg["flags"].(map[string]any)["p2p_only"] = true
	cfg["peer"] = []any{map[string]any{"uri": fmt.Sprintf("ws://127.0.0.1:%d", coordPort)}}
	if loopback {
		cfg["stun_servers"] = []string{}
		cfg["peer"] = append(cfg["peer"].([]any), map[string]any{"uri": peerURI})
	}
	cfg["port_forward"] = []any{map[string]any{"bind_addr": fmt.Sprintf("127.0.0.1:%d", forward), "dst_addr": fmt.Sprintf("10.240.0.1:%d", portOf(allowed.URL)), "proto": "tcp"}, map[string]any{"bind_addr": fmt.Sprintf("127.0.0.1:%d", blocked), "dst_addr": fmt.Sprintf("10.240.0.1:%d", portOf(forbidden.URL)), "proto": "tcp"}}
	udp, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer udp.Close()
	go func() {
		b := make([]byte, 64)
		for {
			n, a, e := udp.ReadFrom(b)
			if e != nil {
				return
			}
			udp.WriteTo(b[:n], a)
		}
	}()
	udpForward, _ := loopbackPort()
	cfg["port_forward"] = append(cfg["port_forward"].([]any), map[string]any{"bind_addr": fmt.Sprintf("127.0.0.1:%d", udpForward), "dst_addr": fmt.Sprintf("10.240.0.1:%d", udp.LocalAddr().(*net.UDPAddr).Port), "proto": "udp"})
	consumer, e := startOverlayProcess(ctx, binary, cfg, rpc)
	if e != nil {
		t.Fatal(e)
	}
	defer consumer.stop()
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	get := func(port int) bool {
		res, e := client.Get(fmt.Sprintf("http://127.0.0.1:%d", port))
		if e != nil {
			return false
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return string(b) == "model-port-only"
	}
	cli := filepath.Join(filepath.Dir(binary), "easytier-cli"+filepath.Ext(binary))
	if _, err := os.Stat(cli); err != nil {
		t.Fatal(err)
	}
	peerState := "no peer status"
	for {
		probe, stop := context.WithTimeout(ctx, time.Second)
		out, err := exec.CommandContext(probe, cli, "-p", fmt.Sprintf("127.0.0.1:%d", rpc), "-v", "-o", "json", "peer").Output()
		stop()
		if err == nil {
			peerState = overlayPeerSummary(out)
			if overlayDirect(out) && get(forward) {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("direct UDP and allowed HTTP did not become ready:", peerState)
		case <-time.After(250 * time.Millisecond):
		}
	}
	if get(blocked) {
		t.Fatal("another host service was exposed")
	}
	datagram, e := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", udpForward))
	if e != nil {
		t.Fatal(e)
	}
	defer datagram.Close()
	datagram.SetDeadline(time.Now().Add(time.Second))
	datagram.Write([]byte("forbidden"))
	b := make([]byte, 64)
	if n, e := datagram.Read(b); e == nil && n > 0 {
		t.Fatal("host UDP service was exposed")
	}

	wrongCfg := overlayBase(network, randomToken(), "uninvited")
	if loopback {
		wrongCfg["stun_servers"] = []string{}
	}
	wrongCfg["ipv4"] = "10.239.0.3"
	wrongCfg["peer"] = cfg["peer"]
	wrongCfg["port_forward"] = []any{map[string]any{"bind_addr": fmt.Sprintf("127.0.0.1:%d", wrong), "dst_addr": fmt.Sprintf("10.240.0.1:%d", portOf(allowed.URL)), "proto": "tcp"}}
	outsider, e := startOverlayProcess(ctx, binary, wrongCfg, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer outsider.stop()
	for i := 0; i < 3; i++ {
		if get(wrong) {
			t.Fatal("wrong network secret accessed model port")
		}
	}
	coord.stop()
	if !get(forward) {
		t.Fatal("business data depends on coordinator")
	}
	coord, e = startOverlayProcess(ctx, binary, coordCfg, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer coord.stop()
	first := provider.pid()
	p, _ := os.FindProcess(first)
	p.Kill()
	for {
		if provider.pid() != 0 && provider.pid() != first && get(forward) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("provider recovery failed")
		case <-time.After(250 * time.Millisecond):
		}
	}
	t.Log("no TUN; allowed port succeeds; other port and wrong secret denied; inference path survives coordinator stop and worker restart")
}

func TestOverlayActiveInferenceCannotExpire(t *testing.T) {
	s := &overlaySession{state: "direct", url: "https://127.0.0.1:1234", lastUsed: time.Now().Add(-3 * time.Minute)}
	origin, release := s.acquire()
	if origin == "" || s.expired() {
		t.Fatal("active long inference expired")
	}
	release()
	release()
	if s.active != 0 || s.expired() {
		t.Fatal("release did not refresh idle timer or was not idempotent")
	}
	s.lastUsed = time.Now().Add(-3 * time.Minute)
	if !s.expired() {
		t.Fatal("idle session cannot expire")
	}
}

func TestOverlayPrewarmDoesNotRenewIdleOrBlockDemand(t *testing.T) {
	binary := os.Getenv("NEWNOA_TEST_EASYTIER")
	if binary == "" {
		t.Skip("set NEWNOA_TEST_EASYTIER to pinned binary")
	}
	executable, _ := os.Executable()
	dir := filepath.Dir(executable)
	for _, name := range []string{"easytier-core", "easytier-cli"} {
		path := filepath.Join(dir, name)
		manifest := filepath.Join(dir, name+"-manifest.json")
		if _, e := os.Stat(path); !os.IsNotExist(e) {
			t.Fatal("test artifact path exists")
		}
		raw, e := os.ReadFile(filepath.Join(filepath.Dir(binary), name))
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, raw, 0700); e != nil {
			t.Fatal(e)
		}
		m, _ := json.Marshal(map[string]any{"version": "2.6.4", "sha256": hash(raw)})
		os.WriteFile(manifest, m, 0600)
		defer os.Remove(path)
		defer os.Remove(manifest)
	}
	m := newPeerOverlayManager()
	defer m.reset()
	key := "nn_test"
	dep := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	route := peerRoute{NodeID: dep, Epoch: 1, Overlay: true, Pin: strings.Repeat("b", 64), URL: "https://127.0.0.1:1", Tunnel: &peerTunnel{Secret: strings.Repeat("a", 64)}}
	network, _ := overlayIdentity(route.NodeID, route.Epoch, route.Tunnel.Secret)
	old := time.Now().Add(-time.Minute)
	done := make(chan struct{})
	close(done)
	victim := &overlaySession{identity: hash([]byte(network + route.Pin + route.URL)), lastUsed: old, done: make(chan struct{}), cancel: func() {}}
	victim.cancel = func() { close(victim.done) }
	m.entries[hash([]byte(key+"\n"+dep+"\n"+route.Tunnel.ConsumerSession))] = victim
	if got := m.ensure(route, dep, key, m.ctx); got != victim || !got.lastUsed.Equal(old) {
		t.Fatal("prewarm renewed idle timer")
	}
	// Three active requests cannot be evicted; actual demand replaces the idle prewarm.
	for _, k := range []string{"1", "2", "3"} {
		m.entries[k] = &overlaySession{active: 1, done: done, cancel: func() {}}
	}
	// Keep the simulated active entries live until reset without starting processes.
	for _, k := range []string{"1", "2", "3"} {
		m.entries[k].done = make(chan struct{})
	}
	for _, k := range []string{"1", "2", "3"} {
		s := m.entries[k]
		s.cancel = func() { close(s.done) }
	}
	route.Epoch = 2
	next := m.ensure(route, "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", key)
	if next == nil || len(m.entries) != 4 {
		t.Fatal("actual demand was blocked by idle prewarm")
	}
	// reset must not wait for the synthetic victim after it has been evicted.
}

func TestOverlayRetirementAndAcquireAreExclusive(t *testing.T) {
	canceled := false
	s := &overlaySession{state: "direct", url: "https://127.0.0.1:1234", lastUsed: time.Now().Add(-3 * time.Minute), cancel: func() { canceled = true }}
	_, release := s.acquire()
	if s.retire(false) || canceled {
		t.Fatal("retirement canceled active inference")
	}
	release()
	if s.retire(true) {
		t.Fatal("freshly released session expired")
	}
	if !s.retire(false) || !canceled {
		t.Fatal("idle session not retired")
	}
	if origin, _ := s.acquire(); origin != "" {
		t.Fatal("retired session reacquired")
	}
}
