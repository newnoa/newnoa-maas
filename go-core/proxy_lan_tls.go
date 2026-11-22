package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Native applications on this machine keep their loopback HTTP endpoint.
// TLS is selected from the socket addresses before any application data is read.
type lanTLSListener struct {
	net.Listener
	config *tls.Config
}

func (l lanTLSListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	local, localOK := conn.LocalAddr().(*net.TCPAddr)
	remote, remoteOK := conn.RemoteAddr().(*net.TCPAddr)
	if localOK && remoteOK && local.IP.IsLoopback() && remote.IP.IsLoopback() {
		return conn, nil
	}
	return tls.Server(conn, l.config), nil
}

func proxyEndpointURL(host string, port string) string {
	scheme := "https://"
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		scheme = "http://"
	}
	return scheme + net.JoinHostPort(host, port) + "/v1"
}

func (c *core) lanCertificate(port int) (tls.Certificate, error) {
	dir := filepath.Join(c.home, ".newnoa")
	bundle := filepath.Join(dir, fmt.Sprintf("lan-proxy-%d-identity.pem", port))
	public := filepath.Join(dir, fmt.Sprintf("lan-proxy-%d-certificate.pem", port))
	for _, path := range []string{bundle, public} {
		if err := c.safeConfigFile(path); err != nil {
			return tls.Certificate{}, err
		}
	}
	var ips []net.IP
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return tls.Certificate{}, err
	}
	for _, addr := range addrs {
		if ip, _, err := net.ParseCIDR(addr.String()); err == nil && ip.To4() != nil && ip.IsPrivate() {
			ips = append(ips, ip)
		}
	}
	if ip := net.ParseIP(c.endpointIP); ip != nil && ip.IsPrivate() {
		ips = append(ips, ip)
	}
	data, err := os.ReadFile(bundle)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return tls.Certificate{}, err
	}
	var cert tls.Certificate
	if err == nil {
		cert, err = tls.X509KeyPair(data, data)
		if err != nil {
			return tls.Certificate{}, err
		}
		cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return tls.Certificate{}, err
		}
	}
	renew := cert.Leaf == nil || time.Now().Add(24*time.Hour).After(cert.Leaf.NotAfter)
	for _, ip := range ips {
		if !renew && cert.Leaf.VerifyHostname(ip.String()) != nil {
			renew = true
		}
	}
	if renew {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return tls.Certificate{}, err
		}
		serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
		if err != nil {
			return tls.Certificate{}, err
		}
		now := time.Now()
		template := &x509.Certificate{SerialNumber: serial, IPAddresses: ips, NotBefore: now.Add(-5 * time.Minute), NotAfter: now.AddDate(1, 0, 0), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
		if err != nil {
			return tls.Certificate{}, err
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			return tls.Certificate{}, err
		}
		pub := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		data = append(pub, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})...)
		if err = atomicFile(bundle, data); err != nil {
			return tls.Certificate{}, err
		}
		cert = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	// Export only the leaf certificate for out-of-band trust on the LAN client.
	return cert, atomicFile(public, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}))
}
