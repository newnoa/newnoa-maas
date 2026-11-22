package network

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pelletier/go-toml/v2"
)

type peerTunnel struct {
	TransportProtocol string `json:"transportProtocol"`
	ConsumerSession   string `json:"-"`
	ServerAddr        string `json:"serverAddr"`
	ServerPort        int    `json:"serverPort"`
	ServerName        string `json:"serverName"`
	TrustedCA         string `json:"trustedCa"`
	ProxyName         string `json:"proxyName"`
	RelayName         string `json:"relayName"`
	Secret            string `json:"secret"`
}

var peerProxyPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,160}$`)

// Bound local subprocesses before platform admission; each request owns its visitor.
var peerTunnelSlots = make(chan struct{}, 4)

func peerVisitorConfig(route peerRoute, deploymentID, consumerKey string, port int, ca string) (map[string]any, error) {
	tunnel := route.Tunnel
	if tunnel == nil || !nodePattern.MatchString(route.NodeID) || route.Epoch < 1 || !nodePattern.MatchString(deploymentID) || !validAPIKey(consumerKey) || port < 1 || port > 65535 {
		return nil, errors.New("invalid peer tunnel identity")
	}
	protocol := tunnel.TransportProtocol
	if protocol == "" {
		protocol = "tcp"
	}
	if protocol != "tcp" && protocol != "websocket" && protocol != "wss" {
		return nil, errors.New("invalid FRP transport protocol")
	}
	secret, err := hex.DecodeString(tunnel.Secret)
	if err != nil || len(secret) != 32 || tunnel.Secret != strings.ToLower(tunnel.Secret) || tunnel.ServerAddr == "" || len(tunnel.ServerAddr) > 255 || tunnel.ServerPort < 1 || tunnel.ServerPort > 65535 || tunnel.ServerName == "" || len(tunnel.ServerName) > 255 || !strings.Contains(tunnel.TrustedCA, "-----BEGIN CERTIFICATE-----") || !peerProxyPattern.MatchString(tunnel.ProxyName) || !strings.HasSuffix(tunnel.ProxyName, "_peer") || tunnel.RelayName != strings.TrimSuffix(tunnel.ProxyName, "_peer")+"_relay" {
		return nil, errors.New("invalid peer tunnel configuration")
	}
	config := map[string]any{
		"serverAddr": tunnel.ServerAddr, "serverPort": tunnel.ServerPort, "loginFailExit": true,
		"metadatas": map[string]any{"nodeId": route.NodeID, "epoch": strconv.FormatInt(route.Epoch, 10), "deploymentId": deploymentID, "consumerKey": consumerKey, "consumerSession": tunnel.ConsumerSession},
		"transport": map[string]any{"protocol": protocol, "tls": map[string]any{"enable": true, "trustedCaFile": ca, "serverName": tunnel.ServerName}},
		"log":       map[string]any{"to": "console", "level": "error"},
		"visitors": []any{
			map[string]any{"name": "peer-relay", "type": "stcp", "serverName": tunnel.RelayName, "secretKey": tunnel.Secret, "bindAddr": "127.0.0.1", "bindPort": port},
		},
	}
	if stun := os.Getenv("NEWNOA_STUN_SERVER"); stun != "" {
		config["natHoleStunServer"] = stun
	}
	return config, nil
}

func startPeerTunnel(ctx context.Context, route peerRoute, deploymentID, consumerKey string, logs ...*peerLogTail) (string, func(), <-chan struct{}, error) {
	binary, err := verifiedFRPC()
	if err != nil {
		return "", nil, nil, err
	}
	dir, err := os.MkdirTemp("", "newnoa-peer-")
	if err != nil {
		return "", nil, nil, err
	}
	cleanup := func() { os.RemoveAll(dir) }
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cleanup()
		return "", nil, nil, err
	}
	address := listener.Addr().String()
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	ca := filepath.Join(dir, "ca.pem")
	config, err := peerVisitorConfig(route, deploymentID, consumerKey, port, ca)
	if err != nil {
		cleanup()
		return "", nil, nil, err
	}
	if err = os.WriteFile(ca, []byte(route.Tunnel.TrustedCA), 0600); err != nil {
		cleanup()
		return "", nil, nil, err
	}
	raw, err := toml.Marshal(config)
	if err != nil {
		cleanup()
		return "", nil, nil, err
	}
	path := filepath.Join(dir, "frpc.toml")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		cleanup()
		return "", nil, nil, err
	}
	cmd := exec.CommandContext(ctx, binary, "-c", path)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if len(logs) > 0 && logs[0] != nil {
		cmd.Stdout, cmd.Stderr = logs[0], logs[0]
	}
	if err = cmd.Start(); err != nil {
		cleanup()
		return "", nil, nil, err
	}
	exited := make(chan struct{})
	go func() {
		if err := cmd.Wait(); err != nil && len(logs) > 0 && logs[0] != nil {
			fmt.Fprintf(logs[0], "\nprocess exit: %v\n", err)
		}
		close(exited)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cmd.Process.Kill()
			select {
			case <-exited:
			case <-time.After(2 * time.Second):
			}
			cleanup()
		})
	}
	return "https://" + address, stop, exited, nil
}

func probePeer(ctx context.Context, client *http.Client, route peerRoute) error {
	request, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(route.URL, "/")+"/peer/ready", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("peer readiness HTTP %d", response.StatusCode)
	}
	var ready struct {
		NodeID string `json:"nodeId"`
		Epoch  int64  `json:"epoch"`
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&ready); err != nil {
		return err
	}
	if ready.NodeID != route.NodeID || ready.Epoch != route.Epoch {
		return errors.New("peer epoch changed")
	}
	return nil
}

type peerConnection struct {
	client    *http.Client
	url, path string
	close     func()
	fallback  *peerFallback
}

func connectPeer(ctx context.Context, route peerRoute, deploymentID, consumerKey string, record func(*peerFallback) error, overlays ...*peerOverlayManager) (connection *peerConnection, resultErr error) {
	return connectPeerPolicy(ctx, route, deploymentID, consumerKey, record, "auto", overlays...)
}

func connectPeerPolicy(ctx context.Context, route peerRoute, deploymentID, consumerKey string, record func(*peerFallback) error, policy string, overlays ...*peerOverlayManager) (connection *peerConnection, resultErr error) {
	transport, err := peerTransport(route)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	err = errors.New("LAN disabled by transport policy")
	if policy == "auto" {
		err = probePeer(probeCtx, client, route)
	}
	cancel()
	if err == nil {
		return &peerConnection{client: client, url: route.URL, path: "direct", close: transport.CloseIdleConnections}, nil
	}
	transport.CloseIdleConnections()
	diagnostic := &peerFallback{ID: randomToken(), DeploymentID: deploymentID, NodeID: route.NodeID, Epoch: route.Epoch, StartedAt: time.Now().UTC(), Status: "attempting", Sequence: 1}
	diagnostic.event("direct", "probe_failed", err.Error())
	var session *overlaySession
	if policy != "frp" && len(overlays) > 0 && overlays[0] != nil {
		var setupErr error
		session, setupErr = overlays[0].prepare(route, deploymentID, consumerKey)
		if setupErr != nil {
			diagnostic.event("easytier", "setup_unavailable", setupErr.Error())
		}
		if session != nil {
			origin, release := session.acquire()
			if origin == "" {
				wait, cancel := context.WithTimeout(ctx, 5*time.Second)
				for origin == "" && wait.Err() == nil {
					select {
					case <-wait.Done():
					case <-time.After(100 * time.Millisecond):
						origin, release = session.acquire()
					}
				}
				if origin == "" {
					diagnostic.event("easytier", "direct_wait_expired", "No verified UDP direct session became ready within the 5-second selection budget: "+wait.Err().Error())
				}
				cancel()
			}
			if origin != "" {
				overlayRoute := route
				overlayRoute.URL = origin
				overlayRoute.DirectDial = nil
				tr, e := peerTransport(overlayRoute)
				if e == nil {
					cl := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
					probe, cancel := context.WithTimeout(ctx, 2*time.Second)
					e = probePeer(probe, cl, overlayRoute)
					cancel()
					if e == nil {
						return &peerConnection{client: cl, url: origin, path: "easytier-direct", close: func() { tr.CloseIdleConnections(); release() }}, nil
					}
					tr.CloseIdleConnections()
				}
				diagnostic.event("easytier", "probe_failed", e.Error())
			}
			release()
			diagnostic.Events = append(diagnostic.Events, session.diagnostic()...)
		}
	} else {
		diagnostic.event("easytier", "manager_unavailable", "EasyTier manager is not configured")
	}
	if policy == "easytier" || route.Tunnel == nil {
		return nil, err
	}
	// Retain the attempt before opening FRP, including failures before a request ID exists.
	if record == nil {
		return nil, errors.New("FRP fallback evidence recorder required")
	}
	if e := record(diagnostic); e != nil {
		return nil, fmt.Errorf("cannot retain FRP fallback evidence: %w", e)
	}
	logs := &peerLogTail{secrets: []string{consumerKey, route.Tunnel.Secret, route.Tunnel.ConsumerSession}}
	defer func() {
		diagnostic.Sequence = 2
		diagnostic.Status = "failed"
		if resultErr == nil {
			diagnostic.Status = "connected"
			diagnostic.event("frp", "connected", "Pinned TLS readiness probe succeeded over STCP")
		} else {
			diagnostic.event("frp", "failed", resultErr.Error())
		}
		if session != nil {
			for _, event := range session.diagnostic() {
				if len(diagnostic.Events) < 16 {
					diagnostic.Events = append(diagnostic.Events, event)
				}
			}
		}
		if tail := logs.snapshot(); tail != "" {
			diagnostic.event("frp", "process_log", tail)
		}
		if e := record(diagnostic); e != nil {
			if connection != nil {
				connection.close()
				connection = nil
			}
			resultErr = fmt.Errorf("cannot retain FRP fallback result: %w", e)
		}
	}()
	// Establish the encrypted FRP mapping before reserving or sending inference.
	probeCtx, cancel = context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	select {
	case peerTunnelSlots <- struct{}{}:
	case <-probeCtx.Done():
		return nil, probeCtx.Err()
	}
	url, stopProcess, exited, err := startPeerTunnel(ctx, route, deploymentID, consumerKey, logs)
	if err != nil {
		<-peerTunnelSlots
		return nil, err
	}
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { stopProcess(); <-peerTunnelSlots }) }
	route.URL = url
	route.DirectDial = nil
	transport, err = peerTransport(route)
	if err != nil {
		stop()
		return nil, err
	}
	transport.TLSHandshakeTimeout = 15 * time.Second
	client = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for probeCtx.Err() == nil {
		err = probePeer(probeCtx, client, route)
		if err == nil {
			return &peerConnection{client: client, url: url, path: "peer-tunnel", fallback: diagnostic, close: func() { transport.CloseIdleConnections(); stop() }}, nil
		}
		select {
		case <-probeCtx.Done():
		case <-exited:
			transport.CloseIdleConnections()
			stop()
			return nil, fmt.Errorf("FRP process exited; last readiness error: %w", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	transport.CloseIdleConnections()
	stop()
	return nil, fmt.Errorf("FRP readiness deadline: %v; last readiness error: %w", probeCtx.Err(), err)
}
