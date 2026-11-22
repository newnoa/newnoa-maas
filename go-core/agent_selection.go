package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"time"
)

type agentSelectionAuthorization struct {
	Session, KeyID, Secret, Owner string
	Models                        []string
}

func (c *core) validateAgentSelection(ctx context.Context, session, keyID, apiKey string, models []string) error {
	if session == "" || !nodePattern.MatchString(keyID) || !validAPIKey(apiKey) {
		return errors.New("Current account, selected API key and authorized models are required")
	}
	var keys []struct {
		ID            string   `json:"id"`
		Paused        bool     `json:"paused"`
		Expired       bool     `json:"expired"`
		Revoked       any      `json:"revoked_at"`
		ExpiresAt     string   `json:"expires_at"`
		DeploymentIDs []string `json:"deploymentIds"`
	}
	if err := c.remoteHTTP(ctx, "GET", "/me/api-keys", session, nil, nil, &keys); err != nil {
		return err
	}
	allowed := map[string]bool{}
	found := false
	for _, key := range keys {
		if key.ID != keyID {
			continue
		}
		if key.Paused || key.Expired || key.Revoked != nil {
			return errors.New("Selected API key is paused, expired or revoked")
		}
		if key.ExpiresAt != "" {
			expires, err := time.Parse(time.RFC3339, key.ExpiresAt)
			if err != nil || !expires.After(time.Now()) {
				return errors.New("Selected API key has expired")
			}
		}
		found = true
		for _, id := range key.DeploymentIDs {
			allowed[id] = true
		}
	}
	if !found {
		return errors.New("Selected API key is not owned by the current account")
	}
	for _, id := range models {
		if !nodePattern.MatchString(id) || !allowed[id] {
			return errors.New("Selected API key no longer authorizes one or more models")
		}
	}
	var secret struct {
		Secret string `json:"secret"`
	}
	if err := c.remoteHTTP(ctx, "POST", "/me/api-keys/"+keyID+"/copy-secret", session, nil, map[string]any{}, &secret); err != nil {
		return err
	}
	if subtle.ConstantTimeCompare([]byte(apiKey), []byte(secret.Secret)) != 1 {
		return errors.New("API key does not match the selected account key")
	}
	return nil
}
