package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestImageMeasurementRejectsInvalidResponsesWithoutInventingUsage(t *testing.T) {
	g := newGateway("http://127.0.0.1:1", "internal", "server", nil)
	for _, raw := range []string{
		`{`, `{"data":{}}`, `{"data":[{"b64_json":1}]}`, `{"data":[{"b64_json":"invalid"}]}`,
		`{"data":[{"b64_json":""}]}`, `{"data":[{"url":1}]}`, `{"data":[{"url":"%"}]}`,
		`{"data":[{"url":"file:///private"}]}`, `{"data":[{"url":"https://user:secret@example.com/image"}]}`,
		`{"data":[{"url":"https://example.com/image#fragment"}]}`,
	} {
		meta := map[string]any{}
		if result, err := g.measureImageURLs(context.Background(), []byte(raw), meta); err == nil || result != nil || meta["outputImages"] != nil {
			t.Fatalf("invalid response acquired usage: %s %v %v", raw, meta, err)
		}
	}
	for _, meta := range []map[string]any{nil, {"outputImages": []any{}}} {
		raw := []byte(`{"data":[{"url":"https://example.com/image"}]}`)
		if result, err := g.measureImageURLs(context.Background(), raw, meta); err != nil || !bytes.Equal(result, raw) {
			t.Fatalf("existing or absent measurement changed: %s %v", result, err)
		}
	}
	raw := []byte(`{"data":[]}`)
	if result, err := g.measureImageURLs(context.Background(), raw, map[string]any{}); err != nil || !bytes.Equal(result, raw) {
		t.Fatalf("empty image response changed: %s %v", result, err)
	}
}

func TestImageMeasurementUsesValidBase64AndSkipsAlreadyHostedURLs(t *testing.T) {
	g := newGateway("http://127.0.0.1:1", "internal", "server", nil)
	meta := map[string]any{}
	raw := []byte(`{"data":[{"b64_json":"` + imagePNG + `"}]}`)
	if result, err := g.measureImageURLs(context.Background(), raw, meta); err != nil || len(result) == 0 || meta["outputBytes"] == nil {
		t.Fatalf("valid Base64 was not measured: %v %v", meta, err)
	}
	for _, host := range []string{"www.example.invalid", "dev-www.example.invalid", "sit-www.example.invalid"} {
		raw = []byte(`{"data":[{"url":"https://` + host + `/v1/images/files/id/signature"}]}`)
		meta = map[string]any{}
		if result, err := g.measureImageURLs(context.Background(), raw, meta); err != nil || !bytes.Equal(result, raw) || meta["outputImages"] != nil {
			t.Fatalf("already-hosted image was downloaded or fabricated: %v %v", meta, err)
		}
	}
}

func TestImageMeasurementDownloadFailuresCannotProduceTransferEvidence(t *testing.T) {
	png, _ := base64.StdEncoding.DecodeString(imagePNG)
	for _, tc := range []struct {
		name   string
		status int
		body   io.ReadCloser
		err    error
	}{
		{"transport", 0, nil, errors.New("fixture download failed")},
		{"status", http.StatusForbidden, io.NopCloser(bytes.NewReader(png)), nil},
		{"read", http.StatusOK, io.NopCloser(imageMeasurementErrorReader{}), nil},
		{"invalid-image", http.StatusOK, io.NopCloser(strings.NewReader("not an image")), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGateway("http://127.0.0.1:1", "internal", "server", nil)
			g.imageDownload = &http.Client{Transport: gatewayRoundTripFunc(func(*http.Request) (*http.Response, error) {
				if tc.err != nil {
					return nil, tc.err
				}
				return &http.Response{StatusCode: tc.status, Header: http.Header{}, Body: tc.body}, nil
			})}
			meta := map[string]any{}
			if result, err := g.measureImageURLs(context.Background(), []byte(`{"data":[{"url":"https://example.com/image"}]}`), meta); err == nil || result != nil || meta["outputImages"] != nil {
				t.Fatalf("failed download acquired usage: %s %v %v", result, meta, err)
			}
		})
	}
}

type imageMeasurementErrorReader struct{}

func (imageMeasurementErrorReader) Read([]byte) (int, error) {
	return 0, errors.New("fixture read failure")
}
