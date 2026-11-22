package main

import (
	"context"
	"errors"
	"math/rand/v2"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/net/websocket"
)

const (
	remoteHeartbeatInterval = 30 * time.Second
	remoteLease             = remoteHeartbeatInterval + 15*time.Second // Allow a delayed acknowledgement without waiting another full interval.
)

var remoteAPITicket = regexp.MustCompile("^ws_[A-Za-z0-9_-]{43}$")

func remoteDenied(err error) bool {
	var e *remoteHTTPError
	return errors.As(err, &e) && (e.Status == 401 || e.Status == 403 || e.Status == 404 || e.Status == 409)
}

func (c *core) remoteRelay(ctx context.Context, m *remoteManager) {
	m.mu.Lock()
	session := m.session
	m.mu.Unlock()
	c.runRemoteRelay(ctx, m, session)
}
func (c *core) runRemoteRelay(ctx context.Context, m *remoteManager, session string) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// A blocked command or publish must never extend the authority deadline.
	start := time.Now()
	var verified atomic.Int64

	go func() {
		timer := time.NewTimer(remoteLease)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				left := remoteLease - time.Since(start.Add(time.Duration(verified.Load())))
				if left <= 0 {
					cancel()
					c.stopExpiredRelay(m, session, false)
					return
				}
				timer.Reset(left)
			}
		}
	}()
	backoff := time.Second
	for ctx.Err() == nil {
		err := c.remoteRelayConnection(ctx, m, session, func() { verified.Store(int64(time.Since(start))) })
		if ctx.Err() != nil {
			return
		}
		if remoteDenied(err) {
			var denied *remoteHTTPError
			errors.As(err, &denied)
			c.stopExpiredRelay(m, session, denied.Status != 401)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff/2 + rand.N(backoff/2)):
		}
		backoff = min(5*time.Second, 2*backoff)
	}
}

func (c *core) stopExpiredRelay(m *remoteManager, session string, revoked bool) {
	m.mu.Lock()
	if m.closed || m.session != session {
		m.mu.Unlock()
		return
	}
	m.setFailureLocked("Remote authority expired or was revoked; owned tasks are stopping")
	m.revoked = m.revoked || revoked
	m.closed = true
	m.mu.Unlock()
	_ = m.shutdown()
}

func (c *core) remoteRelayConnection(parent context.Context, m *remoteManager, session string, verified func()) error {
	var ticket struct{ Ticket string }
	if err := c.remoteHTTP(parent, "POST", "/me/points/live-ticket", session, m, map[string]any{}, &ticket); err != nil {
		return err
	}
	if !remoteAPITicket.MatchString(ticket.Ticket) {
		return errors.New("Invalid host live ticket")
	}
	base := strings.TrimRight(c.control, "/")
	cfg, err := websocket.NewConfig("ws"+strings.TrimPrefix(base, "http")+"/api/v1/me/points/live", base)
	if err != nil {
		return err
	}
	cfg.Protocol = []string{"newnoa.points", "ticket." + ticket.Ticket}
	dial, stopDial := context.WithTimeout(parent, 5*time.Second)
	ws, err := cfg.DialContext(dial)
	stopDial()
	if err != nil {
		return errors.New("Host live connection unavailable")
	}
	defer ws.Close()
	if len(ws.Config().Protocol) != 1 || ws.Config().Protocol[0] != "newnoa.points" {
		return errors.New("Invalid host live protocol")
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stopClose := context.AfterFunc(ctx, func() { _ = ws.SetDeadline(time.Now()) })
	defer stopClose()
	ws.MaxPayloadBytes = 256 * 1024
	queue := make(chan struct{}, 1)
	ready := make(chan struct{})
	go func() {
		defer cancel()
		connected := false
		for {
			var msg map[string]any
			if websocket.JSON.Receive(ws, &msg) != nil {
				return
			}
			commands := msg["type"] == "remote.changed" && msg["hostId"] == m.hostID && msg["kind"] == "commands"
			heartbeat := len(msg) == 1 && msg["type"] == "remote.heartbeat"
			if !heartbeat && !commands {
				continue
			}
			if heartbeat {
				verified()
			}
			if commands {
				select {
				case queue <- struct{}{}:
				default:
				}
			}
			if !connected {
				connected = true
				close(ready)
			}
		}
	}()
	go func() {
		defer cancel()
		beat := time.NewTicker(remoteHeartbeatInterval)
		defer beat.Stop()
		for {
			_ = ws.SetWriteDeadline(time.Now().Add(3 * time.Second))
			if websocket.Message.Send(ws, `{"type":"remote.heartbeat"}`) != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-beat.C:
			}
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ready:
	}
	select {
	case <-queue:
		if err := c.fetchRemoteCommands(ctx, m, session); err != nil {
			return err
		}
	default:
	}
	c.refreshRemoteSiteState(ctx, m, session)
	previous := ^uint64(0)
	for {
		m.mu.Lock()
		changed := m.changesLocked()
		m.mu.Unlock()
		if err := c.publishChangedRemoteState(ctx, m, session, &previous); err != nil {
			return err
		}
		var flush <-chan time.Time
	wait:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-queue:
				if err := c.fetchRemoteCommands(ctx, m, session); err != nil {
					return err
				}
			case <-changed:
				changed = nil
				flush = time.After(remoteRevisionWindow)
			case <-flush:
				break wait
			}
		}
	}
}
