package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/newnoa/newnoa-maas/go-server/network/configuredhost"
	"github.com/newnoa/newnoa-maas/go-server/network/metering"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// URL results are measured through the existing SSRF-safe downloader before their URL is replaced.
func (g *gateway) measureImageURLs(ctx context.Context, raw []byte, metadata map[string]any) ([]byte, error) {
	var result map[string]json.RawMessage
	var items []map[string]json.RawMessage
	if json.Unmarshal(raw, &result) != nil || json.Unmarshal(result["data"], &items) != nil {
		return nil, errors.New("invalid image response")
	}
	if len(items) == 0 || metadata == nil || metadata["outputImages"] != nil {
		return raw, nil
	}
	client := g.imageDownload
	if client == nil {
		client = imageDownloadClient()
	}
	images := []map[string]any{}
	total := 0
	for _, item := range items {
		var data []byte
		if encoded := item["b64_json"]; encoded != nil {
			var text string
			if json.Unmarshal(encoded, &text) != nil {
				return nil, errors.New("invalid image")
			}
			var err error
			data, err = base64.StdEncoding.Strict().DecodeString(text)
			if err != nil {
				return nil, err
			}
		} else {
			var address string
			if json.Unmarshal(item["url"], &address) != nil {
				return nil, errors.New("invalid image URL")
			}
			u, err := url.Parse(address)
			if err != nil || !validDownloadURL(u) {
				return nil, errors.New("invalid image URL")
			}
			// Already-hosted files are verified by Java; do not download a second copy through the public route.
			if u.Scheme == "https" && configuredhost.Allowed("NEWNOA_IMAGE_HOSTS", u.Host, "www.example.invalid,dev-www.example.invalid,sit-www.example.invalid") && strings.HasPrefix(u.Path, "/v1/images/files/") {
				return raw, nil
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
			if err != nil {
				return nil, err
			}
			res, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			if res.StatusCode != http.StatusOK {
				res.Body.Close()
				return nil, errors.New("image download failed")
			}
			data, err = io.ReadAll(io.LimitReader(res.Body, int64(maxHostedImageBytes-total)+1))
			res.Body.Close()
			if err != nil {
				return nil, err
			}
		}
		total += len(data)
		if total > maxHostedImageBytes || !validImageBytes(data) {
			return nil, errors.New("invalid image transfer")
		}
		measured := metering.ImageTransfer(data)
		if measured == nil {
			return raw, nil
		}
		images = append(images, measured)
		// Reuse the bytes for hosting, so a URL is downloaded exactly once.
		item["b64_json"], _ = json.Marshal(base64.StdEncoding.EncodeToString(data))
	}
	metadata["outputImages"] = images
	metadata["outputBytes"] = int64(total)
	result["data"], _ = json.Marshal(items)
	return json.Marshal(result)
}

const maxHostedImageBytes = 24 << 20

func imageDownloadClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		Proxy: nil, DisableKeepAlives: true, ResponseHeaderTimeout: 10 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
		DialContext: safeDialer{timeout: 5 * time.Second}.DialContext,
	}, CheckRedirect: func(r *http.Request, via []*http.Request) error {
		if len(via) > 3 || !validDownloadURL(r.URL) {
			return errors.New("invalid image redirect")
		}
		return nil
	}}
}

func validDownloadURL(u *url.URL) bool {
	return u != nil && (u.Scheme == "https" || u.Scheme == "http") && u.Hostname() != "" && u.User == nil && u.Fragment == ""
}

// Downloads use no caller/upstream credentials and never inherit the model's private egress allowlist.
func (g *gateway) hostImageURLs(ctx context.Context, requestID, permit string, raw []byte) ([]byte, error) {
	var result map[string]json.RawMessage
	var items []map[string]json.RawMessage
	if json.Unmarshal(raw, &result) != nil || json.Unmarshal(result["data"], &items) != nil {
		return nil, errors.New("invalid image response")
	}
	type upload struct {
		Index     int    `json:"index"`
		Data      []byte `json:"data,omitempty"`
		MediaType string `json:"mediaType,omitempty"`
		URL       string `json:"url,omitempty"`
	}
	uploads := []upload{}
	total := 0
	client := g.imageDownload
	if client == nil {
		client = imageDownloadClient()
	}
	for index, item := range items {
		var address string
		if value, exists := item["url"]; !exists {
			continue
		} else if json.Unmarshal(value, &address) != nil {
			return nil, errors.New("invalid image URL")
		}
		u, err := url.Parse(address)
		if err != nil || !validDownloadURL(u) {
			return nil, errors.New("invalid image URL")
		}
		if u.Scheme == "https" && configuredhost.Allowed("NEWNOA_IMAGE_HOSTS", u.Host, "www.example.invalid,dev-www.example.invalid,sit-www.example.invalid") && u.RawQuery == "" && strings.HasPrefix(u.Path, "/v1/images/files/") {
			uploads = append(uploads, upload{Index: index, URL: address})
			continue // Java verifies the supplier's persisted request/index binding.
		}
		var data []byte
		var encoded string
		if value := item["b64_json"]; value != nil && json.Unmarshal(value, &encoded) == nil && encoded != "" {
			data, err = base64.StdEncoding.Strict().DecodeString(encoded)
		} else {
			request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
			if requestErr != nil {
				return nil, errors.New("invalid image request")
			}
			response, downloadErr := client.Do(request)
			if downloadErr != nil {
				return nil, errors.New("image download failed")
			}
			if response.StatusCode != http.StatusOK || response.ContentLength > int64(maxHostedImageBytes-total) {
				response.Body.Close()
				return nil, errors.New("image download rejected")
			}
			data, err = io.ReadAll(io.LimitReader(response.Body, int64(maxHostedImageBytes-total)+1))
			response.Body.Close()
		}
		total += len(data)
		if err != nil || total > maxHostedImageBytes || !validImageBytes(data) {
			return nil, errors.New("invalid downloaded image")
		}
		uploads = append(uploads, upload{Index: index, Data: data, MediaType: http.DetectContentType(data)})
	}
	if len(uploads) == 0 {
		return raw, nil
	}
	body, err := json.Marshal(map[string]any{"gatewayId": g.id, "permit": permit, "images": uploads})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, g.api+"/api/v1/internal/requests/"+requestID+"/images", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+g.token)
	request.Header.Set("Content-Type", "application/json")
	storageClient := *g.client
	storageClient.Timeout = 60 * time.Second
	response, err := storageClient.Do(request)
	if err != nil {
		return nil, errors.New("image storage failed")
	}
	defer response.Body.Close()
	stored, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(stored) > 65536 || response.StatusCode != 200 {
		return nil, errors.New("image storage rejected")
	}
	var hosted struct {
		URLs []struct {
			Index int    `json:"index"`
			URL   string `json:"url"`
		} `json:"urls"`
	}
	if json.Unmarshal(stored, &hosted) != nil || len(hosted.URLs) != len(uploads) {
		return nil, errors.New("invalid stored image response")
	}
	remaining := map[int]bool{}
	for _, image := range uploads {
		remaining[image.Index] = true
	}
	for _, image := range hosted.URLs {
		u, err := url.Parse(image.URL)
		if !remaining[image.Index] || err != nil || u.Scheme != "https" || !configuredhost.Allowed("NEWNOA_IMAGE_HOSTS", u.Host, "www.example.invalid,dev-www.example.invalid,sit-www.example.invalid") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || !strings.HasPrefix(u.Path, "/v1/images/files/") {
			return nil, errors.New("invalid stored image URL")
		}
		delete(remaining, image.Index)
		items[image.Index]["url"], _ = json.Marshal(image.URL)
		delete(items[image.Index], "b64_json")
	}
	result["data"], err = json.Marshal(items)
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}
