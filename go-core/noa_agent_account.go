package main

import (
	"context"
	"errors"
)

const noaModel = "spark-2.6-4b"

type noaBinding struct{ keyID, deploymentID, secret string }

func (c *core) resolveNoaBinding(ctx context.Context, session string) (noaBinding, error) {
	var result noaBinding
	if session == "" || c.historySessionRevoked(session) {
		return result, errors.New("login required")
	}
	var keys []struct {
		ID, SystemKind string
		Models         []struct {
			ID, Model, ModelType string
			Online               bool
		}
	}
	if err := c.remoteHTTP(ctx, "GET", "/me/model-tests/keys?activeOnly=true", session, nil, nil, &keys); err != nil {
		return result, err
	}
	for _, key := range keys {
		if key.SystemKind != "charity" || !nodePattern.MatchString(key.ID) {
			continue
		}
		for _, m := range key.Models {
			if m.Model != noaModel || m.ModelType != "llm" || !m.Online || !nodePattern.MatchString(m.ID) {
				continue
			}
			if result.keyID != "" {
				return noaBinding{}, errors.New("ambiguous charity model authorization")
			}
			result.keyID, result.deploymentID = key.ID, m.ID
		}
	}
	if result.keyID == "" {
		return noaBinding{}, errors.New("spark-2.6-4b unavailable on current charity key")
	}
	var value struct{ Secret string }
	if err := c.remoteHTTP(ctx, "POST", "/me/api-keys/"+result.keyID+"/copy-secret", session, nil, map[string]any{}, &value); err != nil {
		return noaBinding{}, err
	}
	if !validAPIKey(value.Secret) {
		return noaBinding{}, errors.New("invalid charity credential")
	}
	result.secret = value.Secret
	return result, nil
}
