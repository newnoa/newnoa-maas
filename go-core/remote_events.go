package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/websocket"
)

const remoteRevisionWindow = 150 * time.Millisecond

const remoteEventsPath = "/api/local/remote/events"

type remoteEventsTicket struct {
	origin, session string
	until           time.Time
}
type remoteLiveState struct {
	changed     chan struct{}
	revoked     chan struct{}
	tickets     map[string]remoteEventsTicket
	desktopURL  string
	subscribers int
}

func (m *remoteManager) changesLocked() <-chan struct{} {
	if m.live.changed == nil {
		m.live.changed = make(chan struct{})
	}
	return m.live.changed
}

func (m *remoteManager) signalChangesLocked() {
	if m.live.changed != nil {
		close(m.live.changed)
		m.live.changed = nil
	}
}

func remoteBrowserOrigin(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.User != nil || u.Host == "" || origin != u.Scheme+"://"+u.Host {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") && (u.Hostname() == "localhost" || net.ParseIP(u.Hostname()).IsLoopback()) || u.Scheme == "https" && browserPublicHost(u.Host)
}

func (c *core) issueRemoteEventsTicket(m *remoteManager, session, origin string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errors.New("Remote runtime is closed")
	}
	if m.live.revoked == nil {
		m.live.revoked = make(chan struct{})
	}
	if m.live.tickets == nil {
		m.live.tickets = make(map[string]remoteEventsTicket)
	}
	now := time.Now()
	for id, t := range m.live.tickets {
		if !now.Before(t.until) {
			delete(m.live.tickets, id)
		}
	}
	if len(m.live.tickets) >= 32 || m.live.subscribers >= 16 {
		return nil, errors.New("Too many local event subscriptions")
	}
	ticket := randomToken()
	out := map[string]string{"ticket": ticket}
	if origin == "" {
		if m.live.desktopURL == "" {
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				return nil, err
			}
			server := &http.Server{Handler: http.HandlerFunc(c.remoteEventsHTTP), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
			m.live.desktopURL = "ws://" + listener.Addr().String() + remoteEventsPath
			context.AfterFunc(m.ctx, func() { _ = server.Close(); _ = listener.Close() })
			go server.Serve(listener)
		}
		out["url"] = m.live.desktopURL
	}
	m.live.tickets[ticket] = remoteEventsTicket{origin: origin, session: session, until: now.Add(30 * time.Second)}
	return out, nil
}

func (c *core) remoteEventsHTTP(w http.ResponseWriter, r *http.Request) {
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || !net.ParseIP(peer).IsLoopback() || r.Method != "GET" || r.URL.RequestURI() != remoteEventsPath || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || r.Header.Get("Authorization") != "" || dockerRuntime() || c.publicStateless {
		http.NotFound(w, r)
		return
	}
	c.remoteMu.Lock()
	m := c.remote
	c.remoteMu.Unlock()
	if m == nil {
		http.Error(w, "Unavailable", 403)
		return
	}
	// x/net's upgrader requires the underlying Hijacker.
	for {
		wrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			break
		}
		w = wrapper.Unwrap()
	}
	if _, ok := w.(http.Hijacker); !ok {
		http.Error(w, "Upgrade unavailable", 400)
		return
	}
	var session string
	var admitted bool
	defer func() {
		if admitted {
			m.mu.Lock()
			m.live.subscribers--
			m.mu.Unlock()
		}
	}()
	server := websocket.Server{Handshake: func(cfg *websocket.Config, req *http.Request) error {
		invalid := errors.New("Invalid local event ticket")
		if len(cfg.Protocol) != 2 || cfg.Protocol[0] != "newnoa.remote" || !strings.HasPrefix(cfg.Protocol[1], "ticket.") {
			return invalid
		}
		key := strings.TrimPrefix(cfg.Protocol[1], "ticket.")
		m.mu.Lock()
		defer m.mu.Unlock()
		t, ok := m.live.tickets[key]
		if !ok || m.closed || !time.Now().Before(t.until) || m.live.subscribers >= 16 || c.historySessionRevoked(t.session) {
			return invalid
		}
		origin := req.Header.Get("Origin")
		if t.origin != origin || (origin == "" && "ws://"+req.Host+remoteEventsPath != m.live.desktopURL) {
			return invalid
		}
		delete(m.live.tickets, key)
		cfg.Protocol = []string{"newnoa.remote"}
		session = t.session
		m.live.subscribers++
		admitted = true
		return nil
	}, Handler: func(ws *websocket.Conn) {
		ctx, cancel := context.WithCancel(m.ctx)
		defer cancel()
		ws.MaxPayloadBytes = 256
		go func() { var unused string; _ = websocket.Message.Receive(ws, &unused); cancel() }()
		var previous uint64
		first := true
		for {
			m.mu.Lock()
			changed := m.changesLocked()
			revoked := m.live.revoked
			revision, closed := m.revision, m.closed
			m.mu.Unlock()
			if closed || ctx.Err() != nil || c.historySessionRevoked(session) {
				return
			}
			if first || revision != previous {
				_ = ws.SetWriteDeadline(time.Now().Add(3 * time.Second))
				if websocket.JSON.Send(ws, map[string]any{"type": "remote.changed", "kind": "state", "hostId": m.hostID, "hostEpoch": m.epoch, "revision": revision}) != nil {
					return
				}
				previous, first = revision, false
			}
			var flush <-chan time.Time
		wait:
			for {
				select {
				case <-ctx.Done():
					return
				case <-revoked:
					m.mu.Lock()
					revoked = m.live.revoked
					m.mu.Unlock()
					if c.historySessionRevoked(session) {
						return
					}
				case <-changed:
					changed = nil
					flush = time.After(remoteRevisionWindow)
				case <-flush:
					break wait
				}
			}
		}
	}}
	server.ServeHTTP(w, r)
}
