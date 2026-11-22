package main

import (
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// Fixed authenticated catalog probe: no inference, caller URL, body or credential
// export. The proxy substitutes the platform credential through its normal path.
func (c *core) diagnoseProxy() map[string]any {
	started := time.Now()
	result := map[string]any{"address": c.proxyAddress(), "checkedAt": started.UTC().Format(time.RFC3339Nano), "status": "stopped", "durationMs": int64(0)}
	if c.server == nil {
		return result
	}
	client := &http.Client{Timeout: 4 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	request, _ := http.NewRequest("GET", c.proxyBaseURL()+"/v1/models", nil)
	request.Header.Set("Authorization", "Bearer "+c.localKey)
	response, err := client.Do(request)
	result["durationMs"] = time.Since(started).Milliseconds()
	result["status"] = "unavailable"
	if err != nil {
		return result
	}
	defer response.Body.Close()
	result["httpStatus"] = response.StatusCode
	if response.StatusCode == 401 || response.StatusCode == 403 {
		result["status"] = "authorization_rejected"
		return result
	}
	if response.StatusCode != 200 {
		return result
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxFrame+1))
	result["durationMs"] = time.Since(started).Milliseconds()
	result["status"] = "invalid_response"
	if err != nil || len(body) > maxFrame {
		return result
	}
	var catalog map[string]json.RawMessage
	if json.Unmarshal(body, &catalog) != nil || catalog == nil {
		return result
	}
	var models []json.RawMessage
	if data, ok := catalog["data"]; !ok || string(data) == "null" || json.Unmarshal(data, &models) != nil {
		return result
	}
	result["status"] = "reachable"
	return result
}
