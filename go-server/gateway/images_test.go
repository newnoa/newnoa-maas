package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/newnoa/newnoa-maas/go-server/network/metering"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const imagePNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j5c8AAAAASUVORK5CYII="

func TestImageInputValidation(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{`{"prompt":"a red circle"}`, true},
		{`{"prompt":"a red circle","n":1,"size":"1024x1024","stream":false}`, true},
		{`{"prompt":" "}`, false}, {`{"prompt":[]}`, false}, {`{}`, false},
		{`{"prompt":"circle","n":0}`, false}, {`{"prompt":"circle","n":1.5}`, false},
		{`{"prompt":"circle","n":11}`, false}, {`{"prompt":"circle","n":"1"}`, false},
		{`{"prompt":"circle","stream":true}`, false}, {`{"prompt":"circle","stream":"false"}`, false},
	} {
		var body map[string]any
		_ = json.Unmarshal([]byte(tc.raw), &body)
		if validImageInput(body) != tc.valid {
			t.Fatalf("input %s", tc.raw)
		}
	}
}

func TestImageResponseValidation(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		valid bool
	}{
		{fmt.Sprintf(`{"data":[{"b64_json":%q}]}`, imagePNG), true},
		{`{"data":[{"url":"https://images.example/output.png?expires=1"}]}`, true},
		{`{"data":[{"url":"http://images.example/output.png"}]}`, true},
		{`{"data":[]}`, false}, {`{"data":[{}]}`, false}, {`{"data":[{"b64_json":"ZmFrZQ=="}]}`, false},
		{`{"data":[{"url":"javascript:alert(1)"}]}`, false},
		{`{"data":[{"url":"https://secret@images.example/output.png"}]}`, false},
		{`{"data":[{"url":"https://images.example/output.png#fragment"}]}`, false},
		{`{"data":[{"url":"https://images.example/output.png"}],"error":{"message":"failed"}}`, false},
	} {
		if validImages([]byte(tc.raw)) != tc.valid {
			t.Fatalf("response %s", tc.raw)
		}
	}
}

func TestImageGenerationUsesAuthorizedModelAndPreservesUsage(t *testing.T) {
	for _, path := range []string{"/v1/images/generations", "/v1/images/edits"} {
		for _, usage := range []string{`,"usage":{"input_tokens":17,"input_tokens_details":{"text_tokens":17,"image_tokens":0},"output_tokens":1056}`, `,"usage":{"input_tokens":17,"output_tokens":1056}`, `,"usage":{"input_tokens":0,"output_tokens":0}`, ""} {
			t.Run(fmt.Sprintf("usage-%d", len(usage)), func(t *testing.T) {
				calls, finalized := 0, false
				response := fmt.Sprintf(`{"data":[{"b64_json":%q}]%s}`, imagePNG, usage)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					var body map[string]any
					_ = json.NewDecoder(r.Body).Decode(&body)
					if r.URL.Path != path || body["model"] != "upstream-image" || body["prompt"] != "red circle" || r.Header.Get("Authorization") != "Bearer upstream-only" {
						t.Error("image request or credential changed")
					}
					if path == "/v1/images/edits" && !metering.ValidEditImages(body) {
						t.Error("edit lost uploaded image")
					}
					if body["messages"] != nil {
						t.Error("chat fields invented")
					}
					_, _ = w.Write([]byte(response))
				}))
				defer upstream.Close()
				api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var body map[string]any
					_ = json.NewDecoder(r.Body).Decode(&body)
					body = meterFixtureSelectedEvidence(r, body)
					switch {
					case strings.HasSuffix(r.URL.Path, "/authorize"):
						_, _ = w.Write([]byte(`{"model":"upstream-image"}`))
					case strings.HasSuffix(r.URL.Path, "/dispatches"):
						if body["inferencePath"] != path {
							t.Error("wrong authorized image path")
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"requestId": "image-test", "permit": "fixture", "endpoint": upstream.URL, "apiKey": "upstream-only"})
					case strings.HasSuffix(r.URL.Path, "/finalize"):
						finalized = true
						if body["status"] != "succeeded" {
							t.Errorf("not succeeded: %v", body["status"])
						}
						if usage == "" && (body["inputTokens"] != nil || body["outputTokens"] != nil) {
							t.Error("missing usage converted into zero")
						}
						if usage != "" && (body["inputTokens"] == nil || body["outputTokens"] == nil) {
							t.Error("explicit usage dropped")
						}
						metadata, ok := body["imageMetadata"].(map[string]any)
						if !ok || metadata["operation"] != map[string]string{"/v1/images/generations": "generation", "/v1/images/edits": "edit"}[path] || metadata["count"] != float64(1) {
							t.Errorf("image metadata missing: %v", body)
						}
						if strings.Contains(usage, "input_tokens_details") {
							image, ok := body["imageUsage"].(map[string]any)
							if !ok || image["textInputTokens"] != float64(17) || image["cachedImageInputTokens"] != float64(0) {
								t.Errorf("five counters missing: %v", body)
							}
						} else if usage == "" && body["imageUsage"] != nil {
							t.Error("missing image usage invented")
						}
						_, _ = w.Write([]byte(`{}`))
					default:
						_, _ = w.Write([]byte(`{}`))
					}
				}))
				defer api.Close()
				g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
				payload := map[string]any{"model": "deployment", "prompt": "red circle", "n": 1}
				if path == "/v1/images/edits" {
					payload["images"] = []map[string]string{{"image_url": "data:image/png;base64," + imagePNG}}
				}
				raw, _ := json.Marshal(payload)
				r := httptest.NewRequest("POST", path, bytes.NewReader(raw))
				r.Header.Set("X-Api-Key", "nn_fixture")
				r.Header.Set("Authorization", "Bearer ses_login")
				out := httptest.NewRecorder()
				g.ServeHTTP(out, r)
				if out.Code != 200 || calls != 1 || !finalized || out.Body.String() != response {
					t.Fatalf("status=%d calls=%d finalized=%v response=%s", out.Code, calls, finalized, out.Body.String())
				}
			})
		}
	}
}

func TestImagePublicationProbeUsesGenerationAndRejectsEmptySuccess(t *testing.T) {
	for _, success := range []bool{true, false} {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if r.URL.Path != "/v1/images/generations" || r.Method != "POST" || body["model"] != "image" || body["n"] != float64(1) || body["prompt"] == nil || body["response_format"] != nil || body["quality"] != nil {
				t.Error("not a minimal image inference")
			}
			if success {
				_, _ = fmt.Fprintf(w, `{"data":[{"b64_json":%q}]}`, imagePNG)
			} else {
				_, _ = w.Write([]byte(`{"data":[]}`))
			}
		}))
		g := newGateway("", "internal", "fixture", []string{"127.0.0.1"})
		result := g.checkModel(context.Background(), modelCheck{Endpoint: upstream.URL + "/v1/images/generations", Model: "image", Path: "/v1/images/generations", Kind: "publication"})
		upstream.Close()
		if (result.Status == "succeeded") != success {
			t.Fatalf("unexpected result: %#v", result)
		}
	}
}

func TestAutomaticHostedImageHealthUsesCatalogWithoutGeneratingImages(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/models" || r.ContentLength > 0 || r.Header.Get("Authorization") != "Bearer image-catalog-key" {
			t.Error("hosted image health must only read the authenticated catalog")
		}
		fmt.Fprint(w, `{"data":[{"id":"image"}]}`)
	}))
	defer upstream.Close()
	g := newGateway("", "internal", "fixture", []string{"127.0.0.1"})
	result := g.checkModel(context.Background(), modelCheck{Endpoint: upstream.URL, Model: "image", Credential: "image-catalog-key", Path: "/v1/images/generations", Kind: "health"})
	if calls != 1 || result.Status != "succeeded" {
		t.Fatalf("image health calls=%d result=%+v", calls, result)
	}
}

func TestHostedImageCatalogHealthRejectsMissingModelsAndInvalidResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"missing", 200, `{"data":[{"id":"other"}]}`},
		{"malformed", 200, `{"data":"image"}`},
		{"error", 200, `{"data":[{"id":"image"}],"error":{"message":"failed"}}`},
		{"unauthorized", 401, `{"error":"unauthorized"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/v1/models" {
					t.Error("catalog failure must never fall back to generation")
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer upstream.Close()
			g := newGateway("", "internal", "fixture", []string{"127.0.0.1"})
			result := g.checkModel(context.Background(), modelCheck{Endpoint: upstream.URL, Model: "image", Path: "/v1/images/generations", Kind: "health"})
			if result.Status != "failed" {
				t.Fatalf("invalid catalog marked healthy: %+v", result)
			}
		})
	}
}

func TestImageLatencyUsesCatalogWithoutGeneratingImages(t *testing.T) {
	calls := 0
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "GET" || r.URL.Path != "/v1/models" {
			t.Error("latency measurement generated an image")
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"image"}]}`))
	}))
	defer s.Close()
	g := newGateway("", "internal", "fixture", []string{"127.0.0.1"})
	result := g.checkModel(context.Background(), modelCheck{Endpoint: s.URL, Model: "image", Path: "/v1/images/generations", Kind: "latency"})
	if result.Status != "succeeded" || calls != 1 {
		t.Fatalf("image catalog latency failed: %#v calls=%d", result, calls)
	}
}

func TestGeneratedImageHistoryKeepsMetadataWithoutImageContentOrAccessURL(t *testing.T) {
	raw := fmt.Sprintf(`{"created":1,"data":[{"b64_json":%q,"revised_prompt":"a circle"},{"url":"https://images.example/private.png?token=secret"}],"usage":{"input_tokens":17,"output_tokens":1056}}`, imagePNG)
	saved := imageHistoryResponse(raw)
	if strings.Contains(saved, imagePNG) || strings.Contains(saved, "private.png") || strings.Contains(saved, "token=secret") {
		t.Fatal("image content or access URL leaked into history")
	}
	var value map[string]any
	if json.Unmarshal([]byte(saved), &value) != nil || value["usage"] == nil || value["created"] != float64(1) {
		t.Fatal("image metadata lost")
	}
	for _, item := range value["data"].([]any) {
		if item.(map[string]any)["contentSaved"] != false {
			t.Fatal("omitted image falsely marked saved")
		}
	}
}
