package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

const historyEventsPath = "/api/local/history/events"

const (
	historyRecordsChanged = 1 << iota
	historyJobsChanged
	historySettingsChanged
	historyExportChanged
	historyCompressionChanged
	historySVGChanged
	historyServiceChanged
	historyAllChanged = historyRecordsChanged | historyJobsChanged | historySettingsChanged | historyExportChanged | historyCompressionChanged | historySVGChanged | historyServiceChanged
)

type historyEventTicket struct {
	account, origin string
	session         [32]byte
	until           time.Time
}

type historyObserver struct {
	historyEventTicket
	pending int
	wake    chan struct{}
	cancel  context.CancelFunc
}

type historyEventHub struct {
	mu        sync.Mutex
	tickets   map[string]historyEventTicket
	observers map[*historyObserver]struct{}
	listener  net.Listener
	closed    bool
}

// Only device service state is shared; every other change is account-scoped.
func (h *historyEventHub) publishService() {
	h.publish("", historyServiceChanged)
}

func (h *historyEventHub) publish(account string, mask int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for observer := range h.observers {
		if observer.account == account || (mask == historyServiceChanged && account == "") {
			observer.pending |= mask
			select {
			case observer.wake <- struct{}{}:
			default:
			}
		}
	}
}

func (h *historyEventHub) revoke(session [32]byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for key, t := range h.tickets {
		if t.session == session {
			delete(h.tickets, key)
		}
	}
	for observer := range h.observers {
		if observer.session == session {
			observer.cancel()
		}
	}
}

func (h *historyEventHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	if h.listener != nil {
		h.listener.Close()
	}
	h.tickets = nil
	for observer := range h.observers {
		observer.cancel()
	}
}

func (c *core) historyEventTicket(raw json.RawMessage, origin string) (map[string]string, error) {
	var input struct {
		Session string `json:"session"`
	}
	if c.publicStateless || json.Unmarshal(raw, &input) != nil {
		return nil, errors.New("History notifications unavailable")
	}
	account, err := c.historyIdentity(input.Session)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(input.Session))
	// Identity and registration use the same lock order as handshake. A revoked ticket cannot be reintroduced.
	c.historyIdentityMu.Lock()
	defer c.historyIdentityMu.Unlock()
	if c.historyIdentityHash != hash || c.historyIdentityAccount != account {
		return nil, errors.New("History session changed")
	}
	h := &c.historyEvents
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	for key, t := range h.tickets {
		if !now.Before(t.until) {
			delete(h.tickets, key)
		}
	}
	if h.closed || len(h.tickets) >= 32 || len(h.observers) >= 16 {
		return nil, errors.New("History event subscriptions unavailable")
	}
	if h.tickets == nil {
		h.tickets = map[string]historyEventTicket{}
	}
	key := randomToken()
	out := map[string]string{"ticket": key}
	if origin == "" {
		if h.listener == nil {
			ln, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				return nil, err
			}
			h.listener = ln
			server := &http.Server{Handler: http.HandlerFunc(c.historyEventsHTTP), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
			go server.Serve(ln)
		}
		out["url"] = "ws://" + h.listener.Addr().String() + historyEventsPath
	}
	h.tickets[key] = historyEventTicket{account, origin, hash, now.Add(30 * time.Second)}
	return out, nil
}

func (c *core) historyEventsHTTP(w http.ResponseWriter, r *http.Request) {
	if c.publicStateless || r.Method != "GET" || r.URL.RequestURI() != historyEventsPath || r.Header.Get("Authorization") != "" || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.NotFound(w, r)
		return
	}
	// The browser account middleware may wrap the server's connection upgrader.
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
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	h := &c.historyEvents
	o := &historyObserver{wake: make(chan struct{}, 1), cancel: cancel}
	defer func() { h.mu.Lock(); delete(h.observers, o); h.mu.Unlock() }()
	server := websocket.Server{Handshake: func(cfg *websocket.Config, req *http.Request) error {
		invalid := errors.New("Invalid history event ticket")
		if len(cfg.Protocol) != 2 || cfg.Protocol[0] != "newnoa.history" || !strings.HasPrefix(cfg.Protocol[1], "ticket.") {
			return invalid
		}
		key := strings.TrimPrefix(cfg.Protocol[1], "ticket.")
		c.historyIdentityMu.Lock()
		defer c.historyIdentityMu.Unlock()
		h.mu.Lock()
		defer h.mu.Unlock()
		t, ok := h.tickets[key]
		if !ok || h.closed || len(h.observers) >= 16 || !time.Now().Before(t.until) || c.historyIdentityHash != t.session || c.historyIdentityAccount != t.account || t.origin != req.Header.Get("Origin") {
			return invalid
		}
		localAddr, _ := req.Context().Value(http.LocalAddrContextKey).(net.Addr)
		if t.origin == "" && (h.listener == nil || localAddr == nil || req.Host != h.listener.Addr().String() || localAddr.String() != h.listener.Addr().String()) {
			return invalid
		}
		delete(h.tickets, key)
		o.historyEventTicket = t
		if h.observers == nil {
			h.observers = map[*historyObserver]struct{}{}
		}
		h.observers[o] = struct{}{}
		cfg.Protocol = []string{"newnoa.history"}
		return nil
	}, Handler: func(ws *websocket.Conn) {
		ws.MaxPayloadBytes = 128
		go func() { var unused string; _ = websocket.Message.Receive(ws, &unused); cancel() }()
		mask := historyAllChanged
		for {
			if ctx.Err() != nil {
				return
			}
			_ = ws.SetWriteDeadline(time.Now().Add(3 * time.Second))
			if websocket.JSON.Send(ws, map[string]any{"type": "history.changed", "mask": mask}) != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-o.wake:
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			h.mu.Lock()
			select {
			case <-o.wake:
			default:
			}
			mask = o.pending
			o.pending = 0
			h.mu.Unlock()
		}
	}}
	server.ServeHTTP(w, r)
}
