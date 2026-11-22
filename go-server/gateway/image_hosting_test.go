package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHostedImageURLsDownloadAndPersistBeforeReturning(t *testing.T) {
	png, _ := base64.StdEncoding.DecodeString(imagePNG)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("image download leaked credentials")
		}
		w.Write(png)
	}))
	defer source.Close()
	stored := false
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Gateway string `json:"gatewayId"`
			Permit  string `json:"permit"`
			Images  []struct {
				Index     int    `json:"index"`
				Data      []byte `json:"data"`
				MediaType string `json:"mediaType"`
			} `json:"images"`
		}
		if r.URL.Path != "/api/v1/internal/requests/request-fixture/images" || r.Header.Get("Authorization") != "Bearer internal-secret" {
			t.Error("unbound image storage request")
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Fatal(err)
		}
		if input.Gateway != "server" || input.Permit != "permit-fixture" || len(input.Images) != 1 || input.Images[0].Index != 0 || !bytes.Equal(input.Images[0].Data, png) || input.Images[0].MediaType != "image/png" {
			t.Errorf("wrong stored image: %+v", input)
		}
		stored = true
		io.WriteString(w, `{"urls":[{"index":0,"url":"https://www.example.invalid/v1/images/files/id/signature"}]}`)
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal-secret", "server", nil)
	g.imageDownload = source.Client()
	raw := []byte(`{"created":1,"data":[{"url":"` + source.URL + `/result.png?token=secret","revised_prompt":"circle"}],"usage":{"input_tokens":17,"output_tokens":1056}}`)
	result, err := g.hostImageURLs(context.Background(), "request-fixture", "permit-fixture", raw)
	if err != nil {
		t.Fatal(err)
	}
	if !stored || strings.Contains(string(result), source.URL) || strings.Contains(string(result), "token=secret") || !strings.Contains(string(result), "https://www.example.invalid/v1/images/files/") {
		t.Fatalf("upstream URL escaped: %s", result)
	}
	var body map[string]json.RawMessage
	json.Unmarshal(result, &body)
	if string(body["usage"]) != `{"input_tokens":17,"output_tokens":1056}` {
		t.Fatalf("usage changed: %s", body["usage"])
	}
}

func TestHostedImageResultAllowsFastImageHostAndSafeCrossHostRedirect(t *testing.T) {
	for _, address := range []string{"https://fast-img.ns.tisoz.com/result.png", "https://fast.ns.tisoz.com/result.png"} {
		t.Run(address, func(t *testing.T) {
			png, _ := base64.StdEncoding.DecodeString(imagePNG)
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, `{"urls":[{"index":0,"url":"https://dev-www.example.invalid/v1/images/files/id/signature"}]}`)
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "server", nil)
			g.imageDownload = &http.Client{CheckRedirect: imageDownloadClient().CheckRedirect, Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
					t.Error("image host received credentials")
				}
				if r.URL.Host == "fast.ns.tisoz.com" {
					return &http.Response{StatusCode: 302, Header: http.Header{"Location": []string{"https://fast-img.ns.tisoz.com/result.png"}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
				}
				if r.URL.Host != "fast-img.ns.tisoz.com" {
					t.Error("unexpected image host")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(png)), Request: r}, nil
			})}
			raw, _ := json.Marshal(map[string]any{"data": []any{map[string]any{"url": address}}})
			result, err := g.hostImageURLs(context.Background(), "request", "permit", raw)
			if err != nil || !strings.Contains(string(result), "https://dev-www.example.invalid/v1/images/files/") || strings.Contains(string(result), "tisoz.com") {
				t.Fatalf("result host was not safely hosted: %v", err)
			}
		})
	}
}

func TestHostedImagesRejectUnsafeDownloadsAndStorageFailure(t *testing.T) {
	g := newGateway("http://127.0.0.1:1", "internal", "server", []string{"127.0.0.1"})
	for _, address := range []string{"http://127.0.0.1:1/private.png", "http://169.254.169.254/latest", "file:///secret", "https://user:password@example.com/image.png"} {
		raw, _ := json.Marshal(map[string]any{"data": []any{map[string]any{"url": address}}})
		result, err := g.hostImageURLs(context.Background(), "request", "permit", raw)
		if err == nil || len(result) != 0 {
			t.Fatalf("unsafe download returned result: %s", result)
		}
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "<html>not an image</html>") }))
	defer source.Close()
	g.imageDownload = source.Client()
	raw := []byte(`{"data":[{"url":"` + source.URL + `"}]}`)
	if result, err := g.hostImageURLs(context.Background(), "request", "permit", raw); err == nil || len(result) != 0 {
		t.Fatal("invalid image was hosted")
	}
	// A valid image followed by failed storage must never fall back to its original URL.
	image := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		png, _ := base64.StdEncoding.DecodeString(imagePNG)
		w.Write(png)
	}))
	defer image.Close()
	raw = []byte(`{"data":[{"url":"` + image.URL + `"}]}`)
	if result, err := g.hostImageURLs(context.Background(), "request", "permit", raw); err == nil || len(result) != 0 {
		t.Fatal("storage failure leaked URL")
	}
}

func TestImageBase64ResponseNeedsNoURLStorage(t *testing.T) {
	g := newGateway("http://127.0.0.1:1", "internal", "server", nil)
	raw := []byte(`{"data":[{"b64_json":"` + imagePNG + `"}]}`)
	result, err := g.hostImageURLs(context.Background(), "request", "permit", raw)
	if err != nil || !bytes.Equal(raw, result) {
		t.Fatalf("Base64 response changed: %s %v", result, err)
	}
}

func TestImageHostingPolicyInRealInferenceResponse(t *testing.T) {
	for _, tc := range []struct {
		name, policy string
		wantStatus   int
		hosted       bool
	}{
		{"public", `,"imageResultHosting":true`, 200, true},
		{"private", `,"imageResultHosting":false`, 200, false},
		{"missing-policy", "", 502, false},
		{"storage-unavailable", `,"imageResultHosting":true`, 502, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			png, _ := base64.StdEncoding.DecodeString(imagePNG)
			downloadCalls, storeCalls := 0, 0
			file := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { downloadCalls++; w.Write(png) }))
			defer file.Close()
			response := `{"created":1,"data":[{"url":"` + file.URL + `/upstream-only.png"}],"usage":{"input_tokens":17,"output_tokens":1056}}`
			model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, response) }))
			defer model.Close()
			var finalized string
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"upstream-image"`+tc.policy+`}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": "image-fixture", "permit": "permit-fixture", "endpoint": model.URL})
				case strings.HasSuffix(r.URL.Path, "/images"):
					storeCalls++
					if tc.name == "storage-unavailable" {
						http.Error(w, "storage failed", 503)
						return
					}
					io.WriteString(w, `{"urls":[{"index":0,"url":"https://www.example.invalid/v1/images/files/id/signature"}]}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					body = meterFixtureSelectedEvidence(r, body)
					finalized, _ = body["status"].(string)
					if body["inputTokens"] != float64(17) || body["outputTokens"] != float64(1056) {
						t.Errorf("usage lost: %v", body)
					}
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			g.imageDownload = file.Client()
			req := httptest.NewRequest("POST", "/v1/images/generations", strings.NewReader(`{"model":"image","prompt":"circle"}`))
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_login")
			out := httptest.NewRecorder()
			g.ServeHTTP(out, req)
			if out.Code != tc.wantStatus {
				t.Fatalf("status %d body %s", out.Code, out.Body.String())
			}
			if tc.name == "private" && out.Body.String() != response {
				t.Fatal("private model response changed")
			}
			if tc.name != "private" && strings.Contains(out.Body.String(), file.URL) {
				t.Fatal("upstream image domain leaked")
			}
			if tc.hosted && (storeCalls != 1 || downloadCalls != 1) {
				t.Fatalf("hosting not executed: stores=%d downloads=%d", storeCalls, downloadCalls)
			}
			if !tc.hosted && (storeCalls != 0 || downloadCalls != 0) {
				t.Fatal("private or untrusted response downloaded")
			}
			if (finalized == "succeeded") != (tc.wantStatus == 200) {
				t.Fatalf("incorrect final state %s", finalized)
			}
		})
	}
}

func TestImageTransferObservationDownloadsOnlyOnceAndNeverReturnsBase64AlongsideHostedURL(t *testing.T) {
	png, _ := base64.StdEncoding.DecodeString(imagePNG)
	calls := 0
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.Write(png) }))
	defer source.Close()
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"urls":[{"index":0,"url":"https://dev-www.example.invalid/v1/images/files/id/signature"}]}`)
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "gateway", nil)
	g.imageDownload = source.Client()
	raw := []byte(`{"data":[{"url":"` + source.URL + `/image"}],"usage":{"input_tokens":0,"output_tokens":0}}`)
	meta := map[string]any{}
	measured, err := g.measureImageURLs(context.Background(), raw, meta)
	if err != nil {
		t.Fatal(err)
	}
	hosted, err := g.hostImageURLs(context.Background(), "id", "permit", measured)
	if err != nil || calls != 1 || meta["outputBytes"] != int64(len(png)) || strings.Contains(string(hosted), "b64_json") || !strings.Contains(string(hosted), "dev-www.example.invalid") {
		t.Fatalf("transfer observation failed calls=%d metadata=%v err=%v", calls, meta, err)
	}
}
