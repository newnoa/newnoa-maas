package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLANProxyRequiresTrustedTLSWhileLoopbackRemainsHTTP(t *testing.T) {
	var ip string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, addr := range addrs {
		if private, _, err := net.ParseCIDR(addr.String()); err == nil && private.To4() != nil && private.IsPrivate() {
			ip = private.String()
			break
		}
	}
	if ip == "" {
		t.Skip("no private IPv4 address")
	}
	c := newCore(t.TempDir(), "https://server.example.test")
	defer c.shutdown()
	port := availableProxyPort(t)
	if err := c.startProxyOn("", port, true); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(c.home, ".newnoa", fmt.Sprintf("lan-proxy-%d-certificate.pem", port))
	pub, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	block, rest := pem.Decode(pub)
	if block == nil || block.Type != "CERTIFICATE" || len(rest) != 0 {
		t.Fatal("expected only the public leaf")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pub) {
		t.Fatal("could not load exported leaf")
	}
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, DisableKeepAlives: true}}
	defer client.CloseIdleConnections()
	for _, tc := range []struct {
		url string
		tls bool
	}{{fmt.Sprintf("https://%s:%d/v1/models", ip, port), true}, {c.proxyBaseURL() + "/v1/models", false}} {
		resp, err := client.Get(tc.url)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 401 || (resp.TLS != nil) != tc.tls {
			t.Fatalf("%s: status %d tls %t", tc.url, resp.StatusCode, resp.TLS != nil)
		}
	}
	raw, err := client.Get(fmt.Sprintf("http://%s:%d/v1/models", ip, port))
	if err == nil {
		raw.Body.Close()
		if raw.StatusCode != 400 || raw.Header.Get("X-Newnoa-Proxy") != "" {
			t.Fatal("plaintext LAN reached the HTTP handler")
		}
	}
	c.close()
	if err := c.startProxyOn("", port, true); err != nil {
		t.Fatal(err)
	}
	again, err := os.ReadFile(file)
	if err != nil || !bytes.Equal(pub, again) {
		t.Fatal("trusted certificate changed after restart", err)
	}
}

func TestLANProxyRenewsCertificateWhenAdvertisedAddressChanges(t *testing.T) {
	c := &core{home: t.TempDir(), endpointIP: "192.168.254.1"}
	const port = 18222
	first, err := c.lanCertificate(port)
	if err != nil {
		t.Fatal(err)
	}
	c.endpointIP = "192.168.254.2"
	next, err := c.lanCertificate(port)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first.Certificate[0], next.Certificate[0]) {
		t.Fatal("certificate without the new address was reused")
	}
	leaf, err := x509.ParseCertificate(next.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := leaf.VerifyHostname(c.endpointIP); err != nil {
		t.Fatal(err)
	}
	public := filepath.Join(c.home, ".newnoa", "lan-proxy-18222-certificate.pem")
	raw, err := os.ReadFile(public)
	if err != nil || bytes.Contains(raw, []byte("PRIVATE KEY")) {
		t.Fatalf("public certificate contains key material or is unavailable: %v", err)
	}
}

func TestLANProxyRejectsUnsafeCertificateStorage(t *testing.T) {
	const port = 18222
	for _, tc := range []string{"non-regular export", "invalid identity"} {
		t.Run(tc, func(t *testing.T) {
			c := &core{home: t.TempDir()}
			dir := filepath.Join(c.home, ".newnoa")
			switch tc {
			case "non-regular export":
				if err := os.MkdirAll(filepath.Join(dir, "lan-proxy-18222-certificate.pem"), 0700); err != nil {
					t.Fatal(err)
				}
			case "invalid identity":
				if err := atomicFile(filepath.Join(dir, "lan-proxy-18222-identity.pem"), []byte("invalid certificate fixture")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := c.lanCertificate(port); err == nil {
				t.Fatal("unsafe certificate storage was accepted")
			}
		})
	}
}
