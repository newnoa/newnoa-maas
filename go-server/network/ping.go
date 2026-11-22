package network

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

type EndpointPing struct {
	Milliseconds float64
	Status       string
	Error        string
}

// PingEndpoint accepts only an endpoint already authorized by the API and supply policy.
func PingEndpoint(parent context.Context, endpoint string) EndpointPing {
	failed := EndpointPing{Status: "unavailable", Error: "Endpoint ICMP probe unavailable"}
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return failed
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, u.Hostname())
	if err != nil || len(addresses) == 0 {
		return EndpointPing{Status: "unreachable", Error: "Endpoint DNS resolution failed"}
	}
	return PingIP(ctx, addresses[0])
}

// PingIP sends to a caller-authorized literal address without resolving DNS again.
func PingIP(parent context.Context, ip net.IPAddr) EndpointPing {
	failed := EndpointPing{Status: "unavailable", Error: "Endpoint ICMP probe unavailable"}
	if ip.IP == nil {
		return failed
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	network, bind, protocol, requestType, replyType := "udp4", "0.0.0.0", 1, icmp.Type(ipv4.ICMPTypeEcho), icmp.Type(ipv4.ICMPTypeEchoReply)
	rawNetwork := "ip4:icmp"
	if ip.IP.To4() == nil {
		network, bind, protocol, requestType, replyType, rawNetwork = "udp6", "::", 58, ipv6.ICMPTypeEchoRequest, ipv6.ICMPTypeEchoReply, "ip6:ipv6-icmp"
	}
	socket, err := icmp.ListenPacket(network, bind)
	raw := false
	if err != nil {
		socket, err = icmp.ListenPacket(rawNetwork, bind)
		raw = true
	}
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			return EndpointPing{Status: "permission-denied", Error: "ICMP socket permission denied"}
		}
		return failed
	}
	defer socket.Close()
	stop := context.AfterFunc(ctx, func() { _ = socket.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if socket.SetDeadline(deadline) != nil {
		return failed
	}
	nonce := make([]byte, 16)
	if _, err = rand.Read(nonce); err != nil {
		return failed
	}
	message := icmp.Message{Type: requestType, Code: 0, Body: &icmp.Echo{ID: os.Getpid() & 0xffff, Seq: 1, Data: nonce}}
	wire, err := message.Marshal(nil)
	if err != nil {
		return failed
	}
	var destination net.Addr = &net.UDPAddr{IP: ip.IP, Zone: ip.Zone}
	if raw {
		destination = &net.IPAddr{IP: ip.IP, Zone: ip.Zone}
	}
	start := time.Now()
	if _, err = socket.WriteTo(wire, destination); err != nil {
		return EndpointPing{Status: "unreachable", Error: "Endpoint ICMP request failed"}
	}
	buffer := make([]byte, 1500)
	for {
		count, peer, readErr := socket.ReadFrom(buffer)
		if readErr != nil {
			return EndpointPing{Status: "unreachable", Error: "Endpoint did not return an ICMP echo reply"}
		}
		peerHost := peer.String()
		if udp, ok := peer.(*net.UDPAddr); ok {
			peerHost = udp.IP.String()
		} else if address, ok := peer.(*net.IPAddr); ok {
			peerHost = address.IP.String()
		}
		if !strings.EqualFold(peerHost, ip.IP.String()) {
			continue
		}
		reply, parseErr := icmp.ParseMessage(protocol, buffer[:count])
		if parseErr != nil || reply.Type != replyType {
			continue
		}
		echo, ok := reply.Body.(*icmp.Echo)
		if ok && echo.Seq == 1 && bytes.Equal(echo.Data, nonce) {
			return EndpointPing{Milliseconds: float64(time.Since(start)) / float64(time.Millisecond), Status: "succeeded"}
		}
	}
}
