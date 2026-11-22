package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	gatewayProbeTimeout   = 1200 * time.Millisecond
	gatewayCacheTTL       = 2 * time.Second
	gatewayMaxNodes       = 64
	gatewayDiscoveryLimit = 128 * 1024
)

var gatewayIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)

type gatewayEndpoint struct {
	ID                      string `json:"id"`
	Address                 string `json:"address"`
	ConnectionCount         int64  `json:"connectionCount"`
	LatencyTolerancePercent int    `json:"latencyTolerancePercent"`
	TrustedCA               string `json:"trustedCa,omitempty"`
	Transport               string `json:"transport,omitempty"`
	dialAddress             string
}

type gatewayProbeResult struct {
	gatewayEndpoint
	latency time.Duration
}

type gatewaySelector struct {
	discover  func(context.Context, string, string) ([]gatewayEndpoint, error)
	probe     func(context.Context, gatewayEndpoint) (time.Duration, error)
	now       func() time.Time
	preferred *gatewayEndpoint

	mu        sync.Mutex
	currentID string
	caches    map[string]gatewayCacheEntry
}

type gatewayCacheEntry struct {
	until    time.Time
	endpoint gatewayEndpoint
	selected bool
	used     time.Time
}

func newGatewaySelector(control string) *gatewaySelector {
	selector := &gatewaySelector{now: time.Now}
	selector.discover = func(ctx context.Context, apiKey string, model string) ([]gatewayEndpoint, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(control, "/")+"/api/v1/gateways", nil)
		if err != nil {
			return nil, err
		}
		if model != "" {
			query := url.Values{"model": {model}}
			request.URL.RawQuery = query.Encode()
		}
		request.Header.Set("Authorization", "Bearer "+apiKey)
		if session, paired := ctx.Value(gatewaySessionContext{}).(string); paired {
			request.Header.Set("X-Api-Key", apiKey)
			request.Header.Set("Authorization", session)
		}
		client := &http.Client{Timeout: gatewayProbeTimeout, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("gateway discovery rejected: %d", response.StatusCode)
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, gatewayDiscoveryLimit+1))
		var endpoints []gatewayEndpoint
		if err != nil || len(data) > gatewayDiscoveryLimit || json.Unmarshal(data, &endpoints) != nil || len(endpoints) > gatewayMaxNodes {
			return nil, errors.New("invalid gateway discovery response")
		}
		for _, endpoint := range endpoints {
			if !validGatewayEndpoint(endpoint) {
				return nil, errors.New("invalid gateway discovery response")
			}
		}
		return endpoints, nil
	}
	selector.probe = probeGatewayReady
	return selector
}

func validGatewayEndpoint(endpoint gatewayEndpoint) bool {
	if !gatewayIDPattern.MatchString(endpoint.ID) || endpoint.ConnectionCount < 0 || endpoint.ConnectionCount > 1_000_000_000_000_000_000 || endpoint.LatencyTolerancePercent < 0 || endpoint.LatencyTolerancePercent > 100 {
		return false
	}
	if endpoint.Transport != "" && endpoint.Transport != "http" && endpoint.Transport != "grpc" {
		return false
	}
	if endpoint.Transport == "grpc" && !strings.HasPrefix(endpoint.Address, "https://") {
		return false
	}
	if endpoint.TrustedCA != "" {
		if !strings.HasPrefix(endpoint.Address, "https://") {
			return false
		}
		if _, err := gatewayCertPool(endpoint.TrustedCA); err != nil {
			return false
		}
	}
	host, portText, err := net.SplitHostPort(endpoint.Address)
	if strings.HasPrefix(endpoint.Address, "https://") {
		u, err := url.Parse(endpoint.Address)
		if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return false
		}
		if u.Port() == "" {
			return true
		}
		port, err := strconv.Atoi(u.Port())
		return err == nil && port > 0 && port <= 65535
	}
	ip := net.ParseIP(host)
	if err != nil || ip == nil || (!ip.IsPrivate() && !ip.IsLoopback()) {
		return false
	}
	port, err := strconv.Atoi(portText)
	return err == nil && port > 0 && port <= 65535
}

func gatewayBase(endpoint gatewayEndpoint) string {
	if strings.HasPrefix(endpoint.Address, "https://") || strings.HasPrefix(endpoint.Address, "http://") {
		return strings.TrimRight(endpoint.Address, "/")
	}
	return "http://" + endpoint.Address
}

func probeGatewayReady(ctx context.Context, endpoint gatewayEndpoint) (time.Duration, error) {
	probeCtx, cancel := context.WithTimeout(ctx, gatewayProbeTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, gatewayBase(endpoint)+"/ready", nil)
	if err != nil {
		return 0, err
	}
	request = request.WithContext(context.WithValue(request.Context(), selectedGatewayContext{}, endpoint))
	started := time.Now()
	client := &http.Client{Timeout: gatewayProbeTimeout, Transport: gatewayTransport{}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("gateway is not ready: %d", response.StatusCode)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	return time.Since(started), nil
}

func (s *gatewaySelector) selectGateway(ctx context.Context, apiKey string) (gatewayEndpoint, bool) {
	return s.selectGatewayForModel(ctx, apiKey, "")
}

func (s *gatewaySelector) selectGatewayForModel(ctx context.Context, apiKey, model string) (gatewayEndpoint, bool) {
	if s == nil || s.discover == nil || s.probe == nil || !validAPIKey(apiKey) {
		return gatewayEndpoint{}, false
	}
	session, _ := ctx.Value(gatewaySessionContext{}).(string)
	key := hash([]byte(apiKey + "\n" + model + "\n" + session))
	s.mu.Lock()
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	if cached, ok := s.caches[key]; ok && now().Before(cached.until) {
		s.mu.Unlock()
		return cached.endpoint, cached.selected
	}
	currentID := s.currentID
	s.mu.Unlock()
	if endpoint, ok := s.selectPreferred(ctx); ok {
		s.cache(key, gatewayCacheEntry{until: now().Add(gatewayCacheTTL), endpoint: endpoint, selected: true})
		return endpoint, true
	}
	probeCtx, cancel := context.WithTimeout(ctx, 3*gatewayProbeTimeout)
	defer cancel()
	endpoints, err := s.discover(probeCtx, apiKey, model)
	if err != nil || len(endpoints) == 0 || len(endpoints) > gatewayMaxNodes {
		s.cache(key, gatewayCacheEntry{until: now().Add(gatewayCacheTTL)})
		return gatewayEndpoint{}, false
	}
	results := make([]gatewayProbeResult, 0, len(endpoints))
	var resultsMu sync.Mutex
	var group sync.WaitGroup
	limit := make(chan struct{}, gatewayMaxNodes)
	for _, endpoint := range endpoints {
		group.Add(1)
		go func(endpoint gatewayEndpoint) {
			defer group.Done()
			select {
			case limit <- struct{}{}:
			case <-probeCtx.Done():
				return
			}
			defer func() { <-limit }()
			latency, err := s.probe(probeCtx, endpoint)
			if err != nil || latency <= 0 {
				return
			}
			resultsMu.Lock()
			results = append(results, gatewayProbeResult{gatewayEndpoint: endpoint, latency: latency})
			resultsMu.Unlock()
		}(endpoint)
	}
	group.Wait()
	if len(results) == 0 {
		s.cache(key, gatewayCacheEntry{until: now().Add(gatewayCacheTTL)})
		return gatewayEndpoint{}, false
	}
	s.mu.Lock()
	selected := chooseGateway(results, currentID)
	s.currentID = selected.ID
	s.mu.Unlock()
	s.cache(key, gatewayCacheEntry{until: now().Add(gatewayCacheTTL), endpoint: selected.gatewayEndpoint, selected: true})
	return selected.gatewayEndpoint, true
}

func (s *gatewaySelector) cache(key string, entry gatewayCacheEntry) {
	s.mu.Lock()
	if s.caches == nil {
		s.caches = make(map[string]gatewayCacheEntry)
	}
	entry.used = time.Now()
	if _, ok := s.caches[key]; !ok && len(s.caches) >= 64 {
		oldestKey := ""
		var oldest time.Time
		for candidate, cached := range s.caches {
			if oldestKey == "" || cached.used.Before(oldest) {
				oldestKey, oldest = candidate, cached.used
			}
		}
		delete(s.caches, oldestKey)
	}
	s.caches[key] = entry
	s.mu.Unlock()
}

func chooseGateway(results []gatewayProbeResult, currentID string) gatewayProbeResult {
	sort.Slice(results, func(i, j int) bool {
		if results[i].latency != results[j].latency {
			return results[i].latency < results[j].latency
		}
		if results[i].ConnectionCount != results[j].ConnectionCount {
			return results[i].ConnectionCount < results[j].ConnectionCount
		}
		return results[i].ID < results[j].ID
	})
	minimum := results[0].latency
	tolerance := results[0].LatencyTolerancePercent
	band := make([]gatewayProbeResult, 0, len(results))
	for _, result := range results {
		if result.latency <= minimum+minimum*time.Duration(tolerance)/100 {
			band = append(band, result)
		}
	}
	sort.Slice(band, func(i, j int) bool {
		if band[i].ConnectionCount != band[j].ConnectionCount {
			return band[i].ConnectionCount < band[j].ConnectionCount
		}
		if band[i].latency != band[j].latency {
			return band[i].latency < band[j].latency
		}
		return band[i].ID < band[j].ID
	})
	return band[0]
}
