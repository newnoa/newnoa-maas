package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type supplyPolicy struct {
	edition    string
	dockerHost net.IP
	lookup     func(context.Context, string) ([]net.IPAddr, error)
	connect    func(context.Context, string, string) (net.Conn, error)
}

// Only a deployment-owned fixed hosts mapping can designate the Docker host.
func dockerHostMapping(hosts, configured string) (net.IP, error) {
	var result net.IP
	for _, line := range strings.Split(hosts, "\n") {
		fields := strings.Fields(strings.SplitN(line, "#", 2)[0])
		if len(fields) < 2 {
			continue
		}
		for _, name := range fields[1:] {
			if name != "host.docker.internal" {
				continue
			}
			ip := net.ParseIP(fields[0])
			if ip == nil || ip.IsUnspecified() || ip.IsMulticast() {
				return nil, errors.New("invalid Docker host mapping")
			}
			if result != nil && !result.Equal(ip) {
				return nil, errors.New("ambiguous Docker host mapping")
			}
			result = ip
		}
	}
	if configured != "" && (net.ParseIP(configured) == nil || result == nil || !result.Equal(net.ParseIP(configured))) {
		return nil, errors.New("Docker host IP must match the fixed host.docker.internal hosts mapping")
	}
	return result, nil
}
func configuredSupplyPolicy(edition string) (supplyPolicy, error) {
	p := supplyPolicy{edition: edition}
	if edition != "domestic" && edition != "overseas" {
		return p, errors.New("invalid supply edition")
	}
	if dockerRuntime() {
		hosts, err := os.ReadFile("/etc/hosts")
		if err != nil || len(hosts) > 65536 {
			return p, errors.New("Docker host mapping unavailable")
		}
		p.dockerHost, err = dockerHostMapping(string(hosts), os.Getenv("NEWNOA_DOCKER_HOST_IP"))
		if err != nil {
			return p, err
		}
	}
	return p, nil
}
func (p supplyPolicy) endpoint(endpoint string) (*url.URL, error) {
	if p.edition != "domestic" && p.edition != "overseas" && p.edition != "online" {
		return nil, errors.New("invalid supply edition")
	}
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("invalid model service address")
	}
	if port := u.Port(); port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return nil, errors.New("invalid model service port")
		}
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	if p.edition == "online" {
		if ip != nil && !p.allows(ip) || ip == nil && (strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") || strings.HasSuffix(strings.ToLower(host), ".local") || strings.Contains(host, "%")) {
			return nil, errors.New("online models require a public address")
		}
		return u, nil
	}
	if host == "0.0.0.0" {
		host = "127.0.0.1"
		ip = net.ParseIP(host)
	}
	if !p.allows(ip) && !(host == "host.docker.internal" && p.dockerHost != nil) {
		return nil, errors.New("local models require a loopback or private IP address; use an online model for public IPs or domain names")
	}
	if p.dockerHost != nil && ip != nil && p.dockerHost.Equal(ip) {
		host = "host.docker.internal"
	}
	if port := u.Port(); port != "" {
		u.Host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		u.Host = "[" + host + "]"
	} else {
		u.Host = host
	}
	return u, nil
}
func (p supplyPolicy) allows(ip net.IP) bool {
	if p.edition == "online" {
		return publicModelIP(ip)
	}
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate() || p.dockerHost != nil && p.dockerHost.Equal(ip))
}
func (p supplyPolicy) dial(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if p.edition == "online" {
		if ip := net.ParseIP(host); ip != nil && !p.allows(ip) {
			return nil, errors.New("online model connection requires a public address")
		}
	} else if host != "host.docker.internal" || p.dockerHost == nil {
		if !p.allows(net.ParseIP(host)) {
			return nil, errors.New("model connection requires a private IP address")
		}
	} else {
		host = p.dockerHost.String()
	}
	lookup := p.lookup
	if lookup == nil {
		lookup = net.DefaultResolver.LookupIPAddr
	}
	ips, err := lookup(ctx, host)
	if err != nil || len(ips) == 0 {
		return nil, errors.New("model endpoint resolution failed")
	}
	for _, ip := range ips {
		if !p.allows(ip.IP) {
			return nil, errors.New("model connection escaped the supply address policy")
		}
	}
	connect := p.connect
	if connect == nil {
		connect = (&net.Dialer{Timeout: 5 * time.Second}).DialContext
	}
	return connect(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
}
func (p supplyPolicy) client(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy, transport.DialContext = nil, p.dial
	transport.ResponseHeaderTimeout, transport.IdleConnTimeout = 30*time.Second, 30*time.Second
	return &http.Client{Timeout: timeout, Transport: withImageResponseTimeout(transport), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func (c *core) supplyPolicy(session string) (supplyPolicy, error) {
	edition := "domestic"
	if session != "" {
		var response struct {
			Edition string `json:"edition"`
		}
		if err := historyRemote(context.Background(), c.control, "/me/edition", "Bearer "+session, nil, &response); err != nil {
			return supplyPolicy{}, err
		}
		edition = response.Edition
	}
	return configuredSupplyPolicy(edition)
}

func publicModelIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.To4() == nil {
		_, global, _ := net.ParseCIDR("2000::/3")
		if !global.Contains(ip) {
			return false
		}
	}
	for _, block := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32", "2001::/23", "2002::/16", "64:ff9b::/96", "64:ff9b:1::/48"} {
		_, subnet, _ := net.ParseCIDR(block)
		if subnet.Contains(ip) {
			return false
		}
	}
	return true
}
