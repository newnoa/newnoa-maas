package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/newnoa/newnoa-maas/go-server/network/configuredhost"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func imageOrigin(u *url.URL) string {
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return strings.ToLower(u.Scheme + "://" + u.Hostname() + ":" + port)
}

// Only the supplier can read its local model's images. Never turn this into an arbitrary LAN fetcher.
func (n *connector) hostLocalImages(ctx context.Context, response *http.Response, endpoint string, upstream *http.Client, requestID, permit, gateway string, epoch int64) error {
	raw, err := io.ReadAll(io.LimitReader(response.Body, imageResponseLimit+1))
	if err != nil || len(raw) > imageResponseLimit {
		return errors.New("invalid local image response")
	}
	response.Body = io.NopCloser(bytes.NewReader(raw))
	var result map[string]json.RawMessage
	var items []map[string]json.RawMessage
	if json.Unmarshal(raw, &result) != nil || json.Unmarshal(result["data"], &items) != nil {
		return errors.New("invalid local image response")
	}
	base, err := url.Parse(endpoint)
	if err != nil {
		return errors.New("invalid local image origin")
	}
	download := *upstream
	download.Timeout = 30 * time.Second
	download.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	type upload struct {
		Index     int    `json:"index"`
		Data      []byte `json:"data"`
		MediaType string `json:"mediaType"`
	}
	uploads := []upload{}
	total := 0
	for index, item := range items {
		var address string
		if value, ok := item["url"]; !ok {
			continue
		} else if json.Unmarshal(value, &address) != nil || len(address) > 8192 {
			return errors.New("invalid local image URL")
		}
		u, err := base.Parse(address)
		if err != nil || u.User != nil || u.Fragment != "" || imageOrigin(u) != imageOrigin(base) {
			return errors.New("local image escaped approved model origin")
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return errors.New("invalid local image request")
		}
		downloaded, err := download.Do(request)
		if err != nil {
			return errors.New("local image download failed")
		}
		if downloaded.StatusCode != 200 || downloaded.ContentLength > int64((24<<20)-total) {
			downloaded.Body.Close()
			return errors.New("local image download rejected")
		}
		data, err := io.ReadAll(io.LimitReader(downloaded.Body, int64((24<<20)-total)+1))
		downloaded.Body.Close()
		total += len(data)
		if err != nil || total > 24<<20 {
			return errors.New("local image exceeds size limit")
		}
		contentType := http.DetectContentType(data)
		if contentType != "image/webp" {
			config, _, decodeErr := image.DecodeConfig(bytes.NewReader(data))
			if decodeErr != nil || config.Width < 1 || config.Height < 1 || config.Width > 32768 || config.Height > 32768 || int64(config.Width)*int64(config.Height) > 100_000_000 {
				return errors.New("invalid local image")
			}
		}
		uploads = append(uploads, upload{index, data, contentType})
	}
	if len(uploads) > 0 {
		payload, err := json.Marshal(map[string]any{"nodeId": n.nodeID, "epoch": epoch, "gatewayId": gateway, "permit": permit, "images": uploads})
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, n.control+"/api/v1/connector/dispatches/"+requestID+"/images", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		n.mu.Lock()
		credential := n.credential
		n.mu.Unlock()
		request.Header.Set("Authorization", "Bearer "+credential)
		request.Header.Set("Content-Type", "application/json")
		client := *n.client
		client.Timeout = 60 * time.Second
		stored, err := client.Do(request)
		if err != nil {
			return errors.New("supplier image storage failed")
		}
		defer stored.Body.Close()
		data, err := io.ReadAll(io.LimitReader(stored.Body, 65537))
		if err != nil || len(data) > 65536 || stored.StatusCode != 200 {
			return errors.New("supplier image storage rejected")
		}
		var hosted struct {
			URLs []struct {
				Index int    `json:"index"`
				URL   string `json:"url"`
			} `json:"urls"`
		}
		if json.Unmarshal(data, &hosted) != nil || len(hosted.URLs) != len(uploads) {
			return errors.New("invalid supplier image storage response")
		}
		remaining := map[int]bool{}
		for _, image := range uploads {
			remaining[image.Index] = true
		}
		for _, image := range hosted.URLs {
			u, err := url.Parse(image.URL)
			if !remaining[image.Index] || err != nil || u.Scheme != "https" || !configuredhost.Allowed("NEWNOA_IMAGE_HOSTS", u.Host, "www.example.invalid,dev-www.example.invalid,sit-www.example.invalid") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !strings.HasPrefix(u.Path, "/v1/images/files/") {
				return errors.New("invalid supplier image URL")
			}
			delete(remaining, image.Index)
			items[image.Index]["url"], _ = json.Marshal(image.URL)
		}
		result["data"], _ = json.Marshal(items)
		raw, err = json.Marshal(result)
		if err != nil {
			return err
		}
	}
	response.Body = io.NopCloser(bytes.NewReader(raw))
	response.ContentLength = int64(len(raw))
	return nil
}

func localImageStorageFailure(response *http.Response) []byte {
	raw, _ := io.ReadAll(io.LimitReader(response.Body, imageResponseLimit+1))
	var original map[string]json.RawMessage
	_ = json.Unmarshal(raw, &original)
	result := map[string]any{"error": map[string]string{"message": "local image result storage unavailable"}}
	if usage := original["usage"]; len(usage) > 0 && len(usage) <= 4096 && json.Valid(usage) {
		result["usage"] = usage
	}
	encoded, _ := json.Marshal(result)
	return encoded
}

func captureImageFailureUsage(capture *usageCapture, failure []byte) {
	var body map[string]json.RawMessage
	if json.Unmarshal(failure, &body) != nil || len(body["usage"]) == 0 {
		return
	}
	evidence, _ := json.Marshal(map[string]json.RawMessage{"usage": body["usage"]})
	capture.feed(evidence)
	capture.counts()
}
