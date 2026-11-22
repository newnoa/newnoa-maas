package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
)

const defaultProxyPort = 18222

func (c *core) proxyPortValue() int {
	if c.proxyPort == 0 {
		return defaultProxyPort
	}
	return c.proxyPort
}
func (c *core) proxyAddress() string { return fmt.Sprintf("127.0.0.1:%d", c.proxyPortValue()) }
func (c *core) proxyBaseURL() string { return "http://" + c.proxyAddress() }

func (c *core) proxyStartParams(raw json.RawMessage) (string, int, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return "", 0, errors.New("invalid proxy settings")
	}
	for key := range fields {
		if key != "secret" && key != "port" && key != "allowLAN" && key != "endpointIP" {
			return "", 0, errors.New("invalid proxy settings")
		}
	}
	if value, exists := fields["allowLAN"]; exists {
		var enabled bool
		if string(value) == "null" || json.Unmarshal(value, &enabled) != nil {
			return "", 0, errors.New("allowLAN must be boolean")
		}
	}
	if value, exists := fields["endpointIP"]; exists {
		var ip string
		if string(value) == "null" || json.Unmarshal(value, &ip) != nil || (ip != "" && !validEndpointIP(ip)) {
			return "", 0, errors.New("LAN endpoint requires private IPv4 address")
		}
	}
	var secret string
	if value, exists := fields["secret"]; exists && (string(value) == "null" || json.Unmarshal(value, &secret) != nil) {
		return "", 0, errors.New("valid NEWNOA key required")
	}
	port := c.proxyPortValue()
	if value, exists := fields["port"]; exists {
		if string(value) == "null" || json.Unmarshal(value, &port) != nil || port < 1024 || port > 65535 {
			return "", 0, errors.New("proxy port must be an integer from 1024 to 65535")
		}
	}
	return secret, port, nil
}

// A managed configuration may still reference the last listener address. Require
// explicit restoration before changing it, including prepared/unknown journals.
func (c *core) canChangeProxyPort(port int) error {
	_, piJournal := c.piPaths()
	_, hanaJournal := c.hanaPaths()
	journals := []string{piJournal, hanaJournal}
	for _, agent := range []string{"codex", "claude-cli", "claude-desktop", "opencode", "openclaw", "hermes-agent"} {
		_, journal, err := c.agentPaths(agent)
		if err != nil {
			return err
		}
		journals = append(journals, journal)
	}
	for _, journal := range journals {
		if err := c.safeConfigFile(journal); err != nil {
			return errors.New("restore all managed agent configurations before changing the proxy port")
		}
		data, err := os.ReadFile(journal)
		if os.IsNotExist(err) {
			continue
		}
		var metadata struct {
			ProxyPort *int   `json:"proxyPort"`
			Stage     string `json:"stage"`
		}
		if err != nil || json.Unmarshal(data, &metadata) != nil || (metadata.Stage != "prepared" && metadata.Stage != "applied") {
			return errors.New("restore all managed agent configurations before changing the proxy port")
		}
		installedPort := 18080 // Journals without a port predate the 18222 default.
		if metadata.ProxyPort != nil {
			installedPort = *metadata.ProxyPort
		}
		if installedPort != port {
			return errors.New("restore all managed agent configurations before changing the proxy port")
		}
	}
	return nil
}

func (c *core) proxyListenAddress() string {
	host := "127.0.0.1"
	if c.proxyLAN {
		host = "0.0.0.0"
	}
	return fmt.Sprintf("%s:%d", host, c.proxyPortValue())
}
func (c *core) proxyEndpoints() []string {
	if c.proxyLAN && c.endpointIP != "" {
		return []string{proxyEndpointURL(c.endpointIP, fmt.Sprint(c.proxyPortValue()))}
	}
	if !c.proxyLAN {
		return []string{c.proxyBaseURL() + "/v1"}
	}
	conn, err := net.Dial("udp", "192.0.2.1:80")
	if err != nil {
		return nil
	}
	defer conn.Close()
	ip := conn.LocalAddr().(*net.UDPAddr).IP
	if ip.To4() == nil || !ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return nil
	}
	return []string{proxyEndpointURL(ip.String(), fmt.Sprint(c.proxyPortValue()))}
}
