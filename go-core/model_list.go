package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type listedModel struct {
	ContextLength any            `json:"context_length,omitempty"`
	MaxModelLen   any            `json:"max_model_len,omitempty"`
	Description   string         `json:"description,omitempty"`
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	OwnedBy       string         `json:"owned_by,omitempty"`
	Endpoint      string         `json:"endpoint,omitempty"`
	Meta          map[string]any `json:"meta,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
	Capabilities  []string       `json:"capabilities,omitempty"`
	Architecture  map[string]any `json:"architecture,omitempty"`
}

func modelListURL(endpoint string) (*url.URL, error) {
	return modelListURLWithPolicy(endpoint, supplyPolicy{edition: "domestic"})
}
func modelListURLWithPolicy(endpoint string, policy supplyPolicy) (*url.URL, error) {
	u, err := policy.endpoint(endpoint)
	if err != nil {
		return nil, err
	}
	u.Path = modelServiceBase(u.Path)
	if !strings.HasSuffix(u.Path, "/v1") {
		u.Path += "/v1"
	}
	u.Path += "/models"
	u.RawPath = ""
	return u, nil
}

func decodeListedModels(body []byte) ([]listedModel, error) {
	var result struct {
		Data   []listedModel `json:"data"`
		Models []listedModel `json:"models"`
	}
	if json.Unmarshal(body, &result) != nil {
		return nil, errors.New("invalid model list response")
	}
	models := result.Data
	if len(models) == 0 {
		models = result.Models
	}
	if len(models) > 500 {
		return nil, errors.New("model list exceeds limit")
	}
	filtered := models[:0]
	for _, model := range models {
		if model.ID == "" {
			model.ID = model.Name
		}
		if model.ID != "" && len(model.ID) <= 200 {
			filtered = append(filtered, model)
		}
	}
	if len(filtered) == 0 {
		return nil, errors.New("model service returned no models")
	}
	return filtered, nil
}

func listModels(endpoint, apiKey string) ([]listedModel, error) {
	policy, err := configuredSupplyPolicy("domestic")
	if err != nil {
		return nil, err
	}
	return listModelsWithPolicy(endpoint, apiKey, policy)
}
func listModelsWithPolicy(endpoint, apiKey string, policy supplyPolicy) ([]listedModel, error) {
	u, err := modelListURLWithPolicy(endpoint, policy)
	if err != nil {
		return nil, err
	}
	if len(apiKey) > 4096 {
		return nil, errors.New("invalid upstream API key")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, errors.New("invalid model service address")
	}
	if apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+apiKey)
	}
	client := policy.client(8 * time.Second)
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, errors.New("model connection timed out")
		}
		return nil, errors.New("model service is unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("model service rejected model discovery")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return nil, errors.New("model list response exceeds limit")
	}
	models, err := decodeListedModels(body)
	if err != nil {
		return nil, err
	}
	base := *u
	base.Path = strings.TrimSuffix(base.Path, "/models")
	for index := range models {
		models[index].Endpoint = base.String()
	}
	return models, nil
}

func localModelServiceAvailable(endpoint string) bool {
	policy, err := configuredSupplyPolicy("domestic")
	return err == nil && modelServiceAvailable(endpoint, policy)
}
func modelServiceAvailable(endpoint string, policy supplyPolicy) bool {
	u, err := modelListURLWithPolicy(endpoint, policy)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false
	}
	client := policy.client(2 * time.Second)
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return true
	}
	if response.StatusCode != http.StatusOK {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return false
	}
	_, err = decodeListedModels(body)
	return err == nil
}
