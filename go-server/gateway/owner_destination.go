package main

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

func configuredOwnerOrigin() string {
	if origin := os.Getenv("NEWNOA_FRP_OWNER_ORIGIN"); origin != "" {
		return origin
	}
	var origin string
	for _, key := range []string{"NEWNOA_FRP_OWNERS_JSON", "NEWNOA_FRP_OWNER_CONFIG"} {
		if raw := os.Getenv(key); raw != "" {
			var owners []struct {
				ID         string `json:"id"`
				GatewayURL string `json:"gatewayUrl"`
			}
			if json.Unmarshal([]byte(raw), &owners) != nil {
				return ""
			}
			matches, selected := 0, ""
			for _, owner := range owners {
				if owner.ID != "" && owner.ID == os.Getenv("NEWNOA_FRP_OWNER_ID") {
					matches++
					u, err := url.Parse(owner.GatewayURL)
					if err != nil {
						return ""
					}
					selected, err = ownerOrigin(u)
					if err != nil {
						return ""
					}
				}
			}
			if matches != 1 || origin != "" && selected != origin {
				return ""
			}
			origin = selected
		}
	}
	if origin != "" {
		return origin
	}
	if address := os.Getenv("NEWNOA_GATEWAY_ADVERTISE_ADDR"); strings.HasPrefix(address, "https://") {
		return address
	}
	if bind := os.Getenv("NEWNOA_FRP_OWNER_BIND"); bind != "" {
		host, _, err := net.SplitHostPort(bind)
		ip := net.ParseIP(host)
		if err == nil && (host == "localhost" || ip != nil && ip.IsLoopback()) && os.Getenv("NEWNOA_FRP_OWNER_TLS_CERT") == "" {
			return "http://" + bind
		}
		return "https://" + bind
	}
	return "http://127.0.0.1:18101"
}

func ownerOrigin(u *url.URL) (string, error) {
	if u == nil || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", errors.New("invalid owner origin")
	}
	host := strings.ToLower(u.Hostname())
	ip := net.ParseIP(host)
	if u.Scheme == "http" && host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return "", errors.New("owner HTTP requires loopback")
	}
	port := u.Port()
	if port == "" {
		port = "443"
		if u.Scheme == "http" {
			port = "80"
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", errors.New("invalid owner port")
	}
	return u.Scheme + "://" + net.JoinHostPort(host, strconv.Itoa(n)), nil
}

func (g *gateway) authorizeOwnerRequest(request *http.Request, ownerID string) error {
	trusted, err := url.Parse(g.ownerOrigin)
	if err != nil || (trusted.Path != "" && trusted.Path != "/") || trusted.RawPath != "" {
		return errors.New("invalid configured owner origin")
	}
	origin, err := ownerOrigin(trusted)
	if err != nil {
		return err
	}
	destination, err := ownerOrigin(request.URL)
	if err != nil || !g.ownsNode(ownerID) || destination != origin {
		return errors.New("untrusted owner destination")
	}
	// Only a locally configured origin may receive the platform service credential.
	request.Header.Set("Authorization", "Bearer "+g.token)
	return nil
}
