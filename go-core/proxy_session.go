package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

func (c *core) setProxySession(session string) {
	c.proxySessionMu.Lock()
	changed := c.proxySession != session
	if changed {
		c.nativeReferenceCache = nil
		if c.proxySessionCancel != nil {
			c.proxySessionCancel()
		}
		c.proxySessionContext, c.proxySessionCancel = context.WithCancel(context.Background())
	}
	c.proxySession = session
	c.proxySessionMu.Unlock()
	if changed {
		if err := c.stopRemote(); err != nil {
			c.remoteMu.Lock()
			if c.remote != nil {
				c.remote.mu.Lock()
				c.remote.failure = "Previous account's owned agent processes could not be stopped"
				c.remote.mu.Unlock()
			}
			c.remoteMu.Unlock()
		}
		c.cancelSVGTests()
		c.historyIdentityMu.Lock()
		previousHistory := c.historyIdentityHash
		c.historyIdentityHash = [32]byte{}
		c.historyIdentityAccount = ""
		c.historyIdentityVersion++
		c.historyIdentityMu.Unlock()
		c.historyEvents.revoke(previousHistory)
		c.historyMu.Lock()
		if c.history != nil && c.history.manager != nil {
			m := c.history.manager
			m.mu.Lock()
			for _, job := range m.jobs {
				if job.State == "running" {
					job.cancel()
				}
			}
			m.mu.Unlock()
		}
		c.historyMu.Unlock()
	}
}

// The application supplies only its API key. Login identity belongs to this Core.
func (c *core) attachProxySession(r *http.Request) {
	c.proxySessionMu.RLock()
	session := c.proxySession
	c.proxySessionMu.RUnlock()
	key := r.Header.Get("Authorization")
	r.Header.Del("X-Newnoa-Session")
	r.Header.Del("api-key")
	r.Header.Set("X-Api-Key", "")
	if len(key) >= 7 && key[:7] == "Bearer " {
		r.Header.Set("X-Api-Key", key[7:])
	}
	r.Header.Del("Authorization")
	if session != "" {
		r.Header.Set("Authorization", "Bearer "+session)
	}
}

// Normalize aliases once at the local boundary; never pick one contradictory identity.
func proxyAuthorization(header http.Header) (string, error) {
	auth := ""
	for _, name := range []string{"Authorization", "X-Api-Key", "api-key"} {
		for _, value := range header.Values(name) {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if name != "Authorization" {
				value = "Bearer " + value
			}
			if auth != "" && auth != value {
				return "", errors.New("conflicting credentials")
			}
			auth = value
		}
	}
	return auth, nil
}

// The caller has already verified that session rotation retains the same account.
func (c *core) renewProxySession(session string) {
	c.proxySessionMu.Lock()
	if c.proxySession == "" || c.proxySessionContext == nil {
		c.proxySessionMu.Unlock()
		c.setProxySession(session)
		return
	}
	c.nativeReferenceCache = nil
	c.proxySession = session
	c.proxySessionMu.Unlock()
}
