package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net"
	"net/http"
	"strings"
	"time"
)

// A fresh key per process prevents a stale registered endpoint being reused by another process.
func (n *connector) startPeer(address, advertised string) (func(), string, error) {
	if address == "" {
		address = "0.0.0.0:0"
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, "", err
	}
	template := &x509.Certificate{SerialNumber: serial, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(der)
	pin := hex.EncodeToString(digest[:])
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, "", err
	}
	if advertised == "" {
		host, port, _ := net.SplitHostPort(listener.Addr().String())
		if ip := net.ParseIP(host); ip == nil || ip.IsUnspecified() {
			host = peerLANAddress()
		}
		advertised = "https://" + net.JoinHostPort(host, port)
	}
	if _, err = peerTransport(peerRoute{URL: advertised, Pin: pin}); err != nil {
		listener.Close()
		return nil, "", err
	}
	n.peerURL, n.peerPin, n.peerAddress, n.tunnelSecret = advertised, pin, listener.Addr().String(), randomToken()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/peer/ready" && r.URL.RawQuery == "" {
			n.mu.Lock()
			online := !n.stopping && n.state == "online" && n.purpose == "serving"
			epoch := n.epoch
			n.mu.Unlock()
			if !online {
				http.Error(w, "peer unavailable", 503)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"nodeId": n.nodeID, "epoch": epoch})
			return
		}
		if r.URL.Path == "/peer/p2p" {
			n.mu.Lock()
			provider := n.overlay
			online := !n.stopping && n.state == "online"
			n.mu.Unlock()
			if !online || provider == nil {
				http.Error(w, "coordination unavailable", 503)
				return
			}
			provider.Handler().ServeHTTP(w, r)
			return
		}
		n.ServeHTTP(w, r)
	})
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Minute, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384}
	secured := tls.NewListener(listener, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}})
	go func() {
		if err := server.Serve(secured); err != nil && !errors.Is(err, http.ErrServerClosed) {
			n.recordFailure(err)
		}
	}()
	stop := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		server.Shutdown(ctx)
		server.Close()
	}
	return stop, pin, nil
}

// Prefer an active private LAN interface; a loopback advertisement still permits FRP-only use.
func peerLANAddress() string {
	interfaces, _ := net.Interfaces()
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		virtual := false
		for _, prefix := range []string{"docker", "br-", "veth", "virbr", "tun", "tap", "wg", "tailscale"} {
			if strings.HasPrefix(iface.Name, prefix) {
				virtual = true
				break
			}
		}
		if virtual {
			continue
		}
		addresses, _ := iface.Addrs()
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err == nil && ip.To4() != nil && ip.IsPrivate() {
				return ip.String()
			}
		}
	}
	return "127.0.0.1"
}
