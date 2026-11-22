package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	peernet "github.com/newnoa/newnoa-maas/go-server/network"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func gatewayCertPool(raw string) (*x509.CertPool, error) {
	if len(raw) > 16*1024 {
		return nil, errors.New("Go Server CA is too large")
	}
	pool := x509.NewCertPool()
	remaining := []byte(strings.TrimSpace(raw))
	count := 0
	for len(remaining) > 0 {
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, errors.New("invalid Go Server CA")
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, errors.New("invalid Go Server CA")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA || !cert.BasicConstraintsValid || cert.KeyUsage != 0 && cert.KeyUsage&x509.KeyUsageCertSign == 0 {
			return nil, errors.New("invalid Go Server CA")
		}
		count++
		if count > 4 {
			return nil, errors.New("too many Go Server CA certificates")
		}
		pool.AddCert(cert)
		remaining = bytes.TrimSpace(rest)
	}
	if count == 0 {
		return nil, errors.New("missing Go Server CA")
	}
	return pool, nil
}

func gatewayHTTPTransport(endpoint gatewayEndpoint) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if endpoint.dialAddress != "" {
		transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, endpoint.dialAddress)
		}
	}
	if endpoint.TrustedCA != "" {
		if !strings.HasPrefix(endpoint.Address, "https://") {
			return nil, errors.New("Go Server CA requires TLS")
		}
		roots, err := gatewayCertPool(endpoint.TrustedCA)
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	return transport, nil
}

type gatewayTransport struct{}

func devLoopbackGateway(endpoint gatewayEndpoint) bool {
	origin, err := url.Parse(gatewayBase(endpoint))
	return os.Getenv("NEWNOA_ENV") == "dev" && err == nil && origin.Scheme == "http" && net.ParseIP(origin.Hostname()) != nil && net.ParseIP(origin.Hostname()).IsLoopback()
}

func (gatewayTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	endpoint, selected := request.Context().Value(selectedGatewayContext{}).(gatewayEndpoint)
	if !selected {
		if _, public := request.Context().Value(publicCallerProofContext{}).(publicCallerProofInput); public {
			return nil, errors.New("public caller requires selected Go Server trust")
		}
		return http.DefaultTransport.RoundTrip(request)
	}
	origin, err := url.Parse(gatewayBase(endpoint))
	if err != nil || origin.Scheme != request.URL.Scheme || origin.Host != request.URL.Host {
		return nil, errors.New("Go Server trust origin mismatch")
	}
	if proof, ok := request.Context().Value(publicCallerProofContext{}).(publicCallerProofInput); ok && !(request.Method == http.MethodGet && request.URL.Path == "/ready") {
		if (endpoint.Transport != "grpc" || endpoint.TrustedCA == "") && !devLoopbackGateway(endpoint) {
			return nil, errors.New("public caller requires API-bound TLS gRPC Go Server trust")
		}
		signed, err := peernet.SignPublicCaller(proof.token, endpoint.ID, request, proof.bodySHA256, proof.callerIP, time.Now())
		if err != nil {
			return nil, err
		}
		request = request.Clone(request.Context())
		request.Header = request.Header.Clone()
		request.Header.Set(peernet.PublicCallerProofHeader, signed)
		request.Header.Set(peernet.PublicCallerIPHeader, proof.callerIP)
	}
	if endpoint.Transport == "grpc" {
		return grpcGatewayRoundTrip(request, endpoint)
	}
	if endpoint.TrustedCA == "" && endpoint.dialAddress == "" {
		return http.DefaultTransport.RoundTrip(request)
	}
	transport, err := gatewayHTTPTransport(endpoint)
	if err != nil {
		return nil, err
	}
	response, err := transport.RoundTrip(request)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	// Each request owns its endpoint trust; never share private roots across nodes.
	response.Body = &gatewayHTTPBody{ReadCloser: response.Body, transport: transport}
	return response, nil
}

type gatewayHTTPBody struct {
	io.ReadCloser
	transport *http.Transport
}

func (body *gatewayHTTPBody) Close() error {
	err := body.ReadCloser.Close()
	body.transport.CloseIdleConnections()
	return err
}
