package main

import (
	"crypto/tls"
	"errors"
)

// A partial or invalid TLS configuration must never fall back to plaintext.
func listenerTLS(certificate, key string) (*tls.Config, error) {
	if certificate == "" && key == "" {
		return nil, nil
	}
	if certificate == "" || key == "" {
		return nil, errors.New("gateway TLS certificate and private key must be configured together")
	}
	pair, err := tls.LoadX509KeyPair(certificate, key)
	if err != nil {
		return nil, errors.New("gateway TLS certificate or private key is invalid or unreadable")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}, nil
}
