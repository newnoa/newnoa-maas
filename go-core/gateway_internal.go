package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
)

func parsePublicGoServer(raw string) (*gatewayEndpoint, error) {
	if raw == "" {
		return nil, nil
	}
	invalid := errors.New("invalid NEWNOA_PUBLIC_GO_SERVER configuration")
	if len(raw) > 32*1024 {
		return nil, invalid
	}
	var config struct {
		ID              string `json:"id"`
		Address         string `json:"address"`
		InternalAddress string `json:"internalAddress"`
		TrustedCA       string `json:"trustedCa"`
		Transport       string `json:"transport"`
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, invalid
	}
	host, portText, err := net.SplitHostPort(config.InternalAddress)
	ip := net.ParseIP(host)
	port, portErr := strconv.Atoi(portText)
	if err != nil || ip == nil || (!ip.IsPrivate() && !ip.IsLoopback()) || portErr != nil || port < 1 || port > 65535 {
		return nil, invalid
	}
	endpoint := &gatewayEndpoint{ID: config.ID, Address: config.Address, TrustedCA: config.TrustedCA, Transport: config.Transport, dialAddress: config.InternalAddress}
	if !strings.HasPrefix(endpoint.Address, "https://") || !validGatewayEndpoint(*endpoint) {
		return nil, invalid
	}
	return endpoint, nil
}

func (s *gatewaySelector) selectPreferred(ctx context.Context) (gatewayEndpoint, bool) {
	if s.preferred == nil {
		return gatewayEndpoint{}, false
	}
	probeCtx, cancel := context.WithTimeout(ctx, gatewayProbeTimeout)
	defer cancel()
	latency, err := s.probe(probeCtx, *s.preferred)
	return *s.preferred, err == nil && latency > 0
}
