package network

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/url"
	"time"
)

type peerRoute struct {
	DirectDial func(context.Context, string, string) (net.Conn, error) `json:"-"`
	Overlay    bool                                                    `json:"overlay"`
	Private    bool                                                    `json:"private"`
	Tunnel     *peerTunnel                                             `json:"tunnel,omitempty"`
	Available  bool                                                    `json:"available"`
	URL        string                                                  `json:"url"`
	Pin        string                                                  `json:"certificateSha256"`
	Epoch      int64                                                   `json:"epoch"`
	NodeID     string                                                  `json:"nodeId"`
	Model      string                                                  `json:"model"`
	RequestID  string                                                  `json:"requestId"`
	Permit     string                                                  `json:"permit"`
	GatewayID  string                                                  `json:"gatewayId"`
}

func peerTransport(route peerRoute) (*http.Transport, error) {
	u, err := url.Parse(route.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || len(route.Pin) != 64 {
		return nil, errors.New("invalid peer route")
	}
	expected, err := hex.DecodeString(route.Pin)
	if err != nil {
		return nil, err
	}
	dial := route.DirectDial
	if dial == nil {
		dial = (&net.Dialer{Timeout: 3 * time.Second}).DialContext
	}
	return &http.Transport{Proxy: nil, DialContext: dial, ResponseHeaderTimeout: 30 * time.Second, TLSHandshakeTimeout: 3 * time.Second, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12,
		// The platform supplies a device-registered leaf pin; public CA/hostname trust is not used for this self-signed peer.
		InsecureSkipVerify: true, VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("peer certificate missing")
			}
			actual := sha256.Sum256(state.PeerCertificates[0].Raw)
			if !bytes.Equal(actual[:], expected) {
				return errors.New("peer certificate changed")
			}
			return nil
		}}}, nil
}
