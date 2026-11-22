package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"net"
	"net/http"
	"strings"
)

// The provider can present this reference only to the logged-in loopback Core.
// The account key itself stays out of the installed native configuration.
func managedAgentKey(secret string) string {
	if (!strings.HasPrefix(secret, "nn_") && !strings.HasPrefix(secret, "sk-")) || !validAPIKey(secret) {
		return secret
	}
	return "nref_" + secret[:8] + "_" + hash([]byte(secret)) + "_" + hash([]byte("NEWNOA loopback native configuration v1\x00"+secret))
}

// Existing route journals use the account key fingerprint. Keeping that identity
// allows old routes and native references to resolve the same selected model.
func managedAgentKeyHash(value string) string {
	if len(value) == 143 && strings.HasPrefix(value, "nref_") && value[13] == '_' && value[78] == '_' {
		return value[14:78]
	}
	return hash([]byte(value))
}
func agentKeyMatches(configured, secret string) bool {
	candidate := secret
	if strings.HasPrefix(configured, "nref_") {
		candidate = managedAgentKey(secret)
	}
	return subtle.ConstantTimeCompare([]byte(candidate), []byte(configured)) == 1
}
func referencePrefixMatches(ref, prefix string) bool {
	if !strings.HasPrefix(ref, "nref_") {
		return strings.HasPrefix(ref, prefix)
	}
	if len(ref) != 143 || ref[13] != '_' || prefix == "" {
		return false
	}
	shown := ref[5:13]
	return strings.HasPrefix(shown, prefix) || strings.HasPrefix(prefix, shown)
}
func (c *core) localReferenceAuthorization(ctx context.Context, peer, ref string) (string, error) {
	host, _, err := net.SplitHostPort(peer)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return "", errors.New("Native configuration credential requires a loopback connection")
	}
	c.proxySessionMu.RLock()
	login := c.proxySession
	cached := c.nativeReferenceCache[ref]
	c.proxySessionMu.RUnlock()
	if login == "" {
		return "", errors.New("Sign in before using the native configuration")
	}
	keyHash := managedAgentKeyHash(ref)
	c.modelAliasMu.RLock()
	known := false
	for _, route := range c.modelAliases {
		if route.Keys[keyHash] {
			known = true
			break
		}
	}
	c.modelAliasMu.RUnlock()
	if !known {
		return "", errors.New("Native configuration is no longer active")
	}
	if cached != "" {
		return cached, nil
	}
	var keys []struct {
		ID, Prefix      string
		Paused, Expired bool
		Revoked         any `json:"revoked_at"`
	}
	if err = c.remoteHTTP(ctx, http.MethodGet, "/me/api-keys", login, nil, nil, &keys); err != nil {
		return "", err
	}
	for _, k := range keys {
		if k.Paused || k.Expired || k.Revoked != nil || !nodePattern.MatchString(k.ID) || !referencePrefixMatches(ref, k.Prefix) {
			continue
		}
		var credential struct{ Secret string }
		if err = c.remoteHTTP(ctx, http.MethodPost, "/me/api-keys/"+k.ID+"/copy-secret", login, nil, map[string]any{}, &credential); err != nil {
			return "", err
		}
		if agentKeyMatches(ref, credential.Secret) {
			c.proxySessionMu.Lock()
			defer c.proxySessionMu.Unlock()
			if c.proxySession != login {
				return "", errors.New("Account session changed during native authorization")
			}
			if c.nativeReferenceCache == nil || len(c.nativeReferenceCache) >= 64 {
				c.nativeReferenceCache = map[string]string{}
			}
			c.nativeReferenceCache[ref] = credential.Secret
			return credential.Secret, nil
		}
	}
	return "", errors.New("Native configuration credential is unavailable")
}
