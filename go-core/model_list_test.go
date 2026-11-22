package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestModelListPreservesPrivateInterfaceAddress(t *testing.T) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range addresses {
		local, ok := address.(*net.IPNet)
		if !ok || local.IP.IsLoopback() || !local.IP.IsPrivate() || local.IP.To4() == nil {
			continue
		}
		modelURL, err := modelListURL("http://" + net.JoinHostPort(local.IP.String(), "8080") + "/v1")
		if err != nil || modelURL.Host != net.JoinHostPort(local.IP.String(), "8080") {
			t.Fatalf("url=%v err=%v", modelURL, err)
		}
		return
	}
	t.Skip("No non-loopback IPv4 interface")
}

func TestListModelsReadsOpenAIAndOllamaShapes(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		want       string
		apiKey     string
		wantHeader string
	}{
		{"openai with optional key", `{"data":[{"id":"model-a"}]}`, "model-a", "upstream", "Bearer upstream"},
		{"ollama without key", `{"models":[{"name":"model-b"}]}`, "model-b", "", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != test.wantHeader {
					t.Fatalf("unexpected model request: %s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			models, err := listModels(server.URL+"/v1", test.apiKey)
			if err != nil || len(models) != 1 || models[0].ID != test.want {
				t.Fatalf("models=%v err=%v", models, err)
			}
		})
	}
}

func TestListModelsPreservesPublishedMetadataForAutofill(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"qwen","meta":{"n_ctx":65536},"details":{"format":"gguf","quantization_level":"Q3_K - Medium"},"architecture":{"input_modalities":["text","image"]}}]}`))
	}))
	defer server.Close()
	models, err := listModels(server.URL+"/v1", "")
	if err != nil || len(models) != 1 || models[0].Meta["n_ctx"] != float64(65536) || models[0].Details["format"] != "gguf" {
		t.Fatalf("models=%#v err=%v", models, err)
	}
}

func TestListModelsReadsOnlineHTTPSWithTrustedCertificate(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != "Bearer online-key" {
			t.Error("online discovery request changed")
		}
		w.Write([]byte(`{"data":[{"id":"hosted-model"}]}`))
	}))
	defer server.Close()
	transport := http.DefaultTransport
	http.DefaultTransport = server.Client().Transport
	defer func() { http.DefaultTransport = transport }()
	models, err := listModels(server.URL+"/v1", "online-key")
	if err != nil || len(models) != 1 || models[0].ID != "hosted-model" {
		t.Fatalf("models=%v err=%v", models, err)
	}
}

func TestListModelsRejectsUnsafeEndpointsAndRedirects(t *testing.T) {
	for _, endpoint := range []string{"https://user:pass@example.test/v1", "http://192.0.2.1/v1", "file:///etc/passwd"} {
		if _, err := listModels(endpoint, ""); err == nil {
			t.Fatalf("accepted unsafe endpoint %q", endpoint)
		}
	}
	server := httptest.NewServer(http.RedirectHandler("http://127.0.0.1/secret", http.StatusFound))
	defer server.Close()
	if _, err := listModels(server.URL, ""); err == nil {
		t.Fatal("accepted model discovery redirect")
	}
}

func TestLocalModelServiceAvailabilityUsesOnlyConfiguredLoopbackAddress(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Fatalf("unexpected local model path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"configured-model"}]}`))
	}))
	defer server.Close()
	if !localModelServiceAvailable(server.URL + "/v1") {
		t.Fatal("configured loopback model service was not detected")
	}
	if localModelServiceAvailable("https://model.example.test/v1") {
		t.Fatal("probed a remote model service")
	}
}

func TestLocalModelServiceAvailabilityTreatsAuthenticationAsAReachableService(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()
	if !localModelServiceAvailable(server.URL + "/v1") {
		t.Fatal("authenticated local service was treated as unavailable")
	}
}

func TestModelListURLAcceptsOnlineHTTPSAndOnlyConfiguredLoopbackHTTP(t *testing.T) {
	if _, err := modelListURLWithPolicy("https://api.example.com/v1", supplyPolicy{edition: "overseas"}); err == nil {
		t.Fatal("online discovery must not connect directly from Core")
	}
	for _, endpoint := range []string{"http://192.0.2.10/v1", "https://user:pass@example.com/v1", "http://127.0.0.1/v1?key=secret"} {
		if _, err := modelListURL(endpoint); err == nil {
			t.Fatalf("accepted invalid endpoint %q", endpoint)
		}
	}
}
