package main

import (
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func newFRPWebsocketProxy(address string) (http.Handler, error) {
	if address == "" {
		return nil, nil
	}
	target, err := url.Parse(address)
	if err != nil {
		return nil, errors.New("invalid internal FRPS address")
	}
	ip := net.ParseIP(target.Hostname())
	port, portErr := strconv.Atoi(target.Port())
	if portErr != nil || port < 1 || port > 65535 {
		return nil, errors.New("invalid internal FRPS port")
	}
	if target.Scheme != "http" || ip == nil || !ip.IsLoopback() || target.Port() == "" || target.User != nil || target.RawQuery != "" || target.ForceQuery || target.Fragment != "" || (target.Path != "" && target.Path != "/") {
		return nil, errors.New("internal FRPS requires a fixed loopback HTTP origin")
	}
	if _, err = net.ResolveTCPAddr("tcp", target.Host); err != nil {
		return nil, errors.New("invalid internal FRPS port")
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var admission frpAdmission
	proxy.Transport = &frpHandshakeTransport{pending: make(chan struct{}, frpPendingHandshakes), upstream: &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, ResponseHeaderTimeout: 10 * time.Second}}
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		director(r)
		r.URL.Path = "/~!frp"
		// FRPS matches the literal HTTP preface before parsing the upgrade request.
		r.URL.RawPath = "/~!frp"
		r.URL.RawQuery = ""
		r.Host = target.Host
		for _, header := range []string{"Authorization", "X-Api-Key", "Cookie", "Proxy-Authorization"} {
			r.Header.Del(header)
		}
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		if errors.Is(err, errFRPBusy) {
			frpBusy(w)
			return
		}
		fail(w, 502, "FRP service unavailable")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/~!frp" || r.URL.RawQuery != "" || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			fail(w, 400, "FRP WebSocket upgrade required")
			return
		}
		peer, err := netip.ParseAddrPort(r.RemoteAddr)
		if err != nil {
			fail(w, 400, "Invalid FRP transport peer")
			return
		}
		ip := peer.Addr().Unmap()
		if !admission.accept(ip, time.Now()) {
			frpBusy(w)
			return
		}
		defer admission.release(ip)
		proxy.ServeHTTP(w, r)
	}), nil
}
