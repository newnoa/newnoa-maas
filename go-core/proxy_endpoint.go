package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func validEndpointIP(value string) bool {
	ip := net.ParseIP(value)
	return ip != nil && ip.To4() != nil && (ip.IsPrivate() || value == "127.0.0.1" && !dockerRuntime())
}
func (c *core) endpointPath() string { return filepath.Join(c.home, ".newnoa", "proxy-endpoint.json") }
func (c *core) loadProxyEndpoint() {
	file := c.endpointPath()
	if c.safeConfigFile(file) != nil {
		return
	}
	raw, err := os.ReadFile(file)
	if err != nil || len(raw) > 256 {
		return
	}
	var setting struct {
		IP       string `json:"ip"`
		AllowLAN *bool  `json:"allowLAN"`
	}
	if json.Unmarshal(raw, &setting) == nil && (setting.IP == "" || validEndpointIP(setting.IP)) {
		c.endpointIP = setting.IP
		if setting.AllowLAN != nil {
			c.proxyLAN = *setting.AllowLAN
		}
	}
}
func (c *core) configureProxyEndpoint(ip string, testOnly bool) (map[string]any, error) {
	if ip != "" && !validEndpointIP(ip) {
		return nil, errors.New("LAN endpoint requires a private IPv4 address")
	}
	result := map[string]any{"ip": ip, "reachable": false, "durationMs": int64(0)}
	if ip != "" {
		if c.server == nil || (!c.proxyLAN && ip != "127.0.0.1") {
			return nil, errors.New("Enable LAN access in Proxy and connections before testing")
		}
		started := time.Now()
		transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
		if !net.ParseIP(ip).IsLoopback() {
			config := c.server.TLSConfig
			if config == nil || len(config.Certificates) == 0 {
				return nil, errors.New("LAN TLS certificate is unavailable")
			}
			leaf, err := x509.ParseCertificate(config.Certificates[0].Certificate[0])
			if err != nil {
				return nil, err
			}
			roots := x509.NewCertPool()
			roots.AddCert(leaf)
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
		}
		client := &http.Client{Timeout: 3 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		defer client.CloseIdleConnections()
		request, _ := http.NewRequest(http.MethodHead, proxyEndpointURL(ip, fmt.Sprint(c.proxyPortValue()))+"/models", nil)
		response, err := client.Do(request)
		if err != nil {
			return nil, errors.New("LAN endpoint connection timed out or is unavailable")
		}
		response.Body.Close()
		if response.Header.Get("X-Newnoa-Proxy") != hash([]byte(c.localKey)) {
			return nil, errors.New("LAN endpoint does not point to this Core proxy")
		}
		result["reachable"], result["durationMs"] = true, time.Since(started).Milliseconds()
	}
	if !testOnly {
		if err := c.saveProxyEndpoint(ip); err != nil {
			return nil, err
		}
		c.endpointIP = ip
	}
	return result, nil
}

func (c *core) saveProxyEndpoint(ip string) error {
	if err := c.safeConfigFile(c.endpointPath()); err != nil {
		return err
	}
	raw, _ := json.Marshal(map[string]any{"ip": ip, "allowLAN": c.proxyLAN})
	return atomicFile(c.endpointPath(), raw)
}
