package main

import (
	"crypto/sha256"
	"strconv"
)

type historyCloudHTTPError struct{ status int }

func (e *historyCloudHTTPError) Error() string {
	return "history cloud rejected request: HTTP " + strconv.Itoa(e.status)
}
func (c *core) historySessionRevoked(session string) bool {
	hash := sha256.Sum256([]byte(session))
	c.historyIdentityMu.Lock()
	defer c.historyIdentityMu.Unlock()
	return c.historyRevokedHash == hash
}
func (c *core) revokeHistorySession(session string) {
	if session != "" {
		c.cancelNoaTasks(session)
	}
	hash := sha256.Sum256([]byte(session))
	c.historyIdentityMu.Lock()
	if c.historyIdentityHash != hash || c.historyIdentityAccount == "" {
		c.historyIdentityMu.Unlock()
		return
	}
	account := c.historyIdentityAccount
	c.historyRevokedHash = hash
	c.historyIdentityHash = [32]byte{}
	c.historyIdentityAccount = ""
	c.historyIdentityVersion++
	c.historyIdentityMu.Unlock()
	c.historyEvents.revoke(hash)
	c.remoteMu.Lock()
	m := c.remote
	c.remoteMu.Unlock()
	if m != nil {
		m.mu.Lock()
		if m.live.revoked != nil {
			close(m.live.revoked)
			m.live.revoked = make(chan struct{})
		}
		for id, t := range m.live.tickets {
			if t.session == session {
				delete(m.live.tickets, id)
			}
		}
		m.mu.Unlock()
	}
	c.proxySessionMu.Lock()
	if c.proxySession == session {
		c.proxySession = ""
		if c.proxySessionCancel != nil {
			c.proxySessionCancel()
		}
	}
	c.proxySessionMu.Unlock()
	c.historyMu.Lock()
	if c.history != nil && c.history.manager != nil {
		m := c.history.manager
		m.mu.Lock()
		for _, job := range m.jobs {
			if job.owner == account && job.State == "running" {
				job.cancel()
			}
		}
		m.mu.Unlock()
	}
	c.historyMu.Unlock()
}
