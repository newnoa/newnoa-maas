package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProbeContextRequiresCompleteBoundedJSON(t *testing.T) {
	valid := `{"n_ctx":32768}`
	for _, test := range []struct {
		name, body string
		detected   bool
	}{
		{"valid", valid, true},
		{"whitespace", " \n" + valid + "\n ", true},
		{"boundary", valid + strings.Repeat(" ", 256*1024-len(valid)), true},
		{"second-object", valid + `{"n_ctx":65536}`, false},
		{"garbage", valid + "broken", false},
		{"oversized", valid + strings.Repeat(" ", 256*1024-len(valid)+1), false},
		{"truncated", `{"n_ctx":32768`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/props" {
					http.NotFound(w, r)
					return
				}
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			result, err := probeContext(server.URL, "llama.cpp", "", "demo")
			if err != nil || result["detected"] != test.detected {
				t.Fatalf("result=%#v error=%v", result, err)
			}
		})
	}
}

func TestFindContextRejectsValuesOutsidePublicationRange(t *testing.T) {
	for _, value := range []any{float64(2000001), "2000001", json.Number("2000001"), float64(-1), "0"} {
		if length, _ := findContext(map[string]any{"n_ctx": value}); length != 0 {
			t.Errorf("accepted unpublishable context %v: %d", value, length)
		}
	}
	for _, value := range []any{float64(2000000), "2000000", json.Number("2000000")} {
		if length, _ := findContext(map[string]any{"n_ctx": value}); length != 2000000 {
			t.Errorf("rejected publication boundary %v: %d", value, length)
		}
	}
}

func TestProbeContextRejectsIncompleteHTTPBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/props" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte(`{"n_ctx":32768}`))
	}))
	defer server.Close()
	result, err := probeContext(server.URL, "llama.cpp", "", "demo")
	if err != nil || result["detected"] != false {
		t.Fatalf("incomplete transfer accepted: result=%#v error=%v", result, err)
	}
}

func TestProbeContextReadsCommonMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/props" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"default_generation_settings":{"n_ctx":131072}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	// httptest uses a loopback address and is therefore accepted by the probe.
	result, err := probeContext(server.URL, "llama.cpp", "", "demo")
	if err != nil {
		t.Fatal(err)
	}
	if result["detected"] != true || result["contextLength"] != int64(131072) {
		t.Fatalf("unexpected result: %#v", result)
	}
}

func TestProbeContextAcceptsVersionedAPIBase(t *testing.T) {
	for _, prefix := range []string{"", "/model-proxy"} {
		t.Run(prefix, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == prefix+"/v1/models" {
					_, _ = w.Write([]byte(`{"data":[{"id":"demo","max_model_len":8192}]}`))
					return
				}
				http.NotFound(w, r)
			}))
			defer server.Close()
			result, err := probeContext(server.URL+prefix+"/v1/", "vllm", "", "demo")
			if err != nil || result["detected"] != true || result["contextLength"] != int64(8192) {
				t.Fatalf("versioned API base failed: %#v, %v", result, err)
			}
		})
	}
}

func TestProbeContextRejectsRemoteEndpoint(t *testing.T) {
	if _, err := probeContext("https://models.example.com", "ollama", "", "demo"); err == nil {
		t.Fatal("expected remote endpoint rejection")
	}
}

func TestGenericProbeDoesNotUseOllamaModelCapability(t *testing.T) {
	showCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/show" {
			showCalls++
			_, _ = w.Write([]byte(`{"model_info":{"demo.context_length":131072}}`))
			return
		}
		if r.URL.Path == "/v1/models" {
			_, _ = w.Write([]byte(`{"data":[{"id":"demo"}]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	result, err := probeContext(server.URL, "llama.cpp", "", "demo")
	if err != nil || result["detected"] != false || showCalls != 0 {
		t.Fatalf("generic probe used model capability: result=%#v, showCalls=%d, error=%v", result, showCalls, err)
	}
}

func TestProbeOllamaUsesLoadedContextOnly(t *testing.T) {
	for _, loaded := range []bool{true, false} {
		t.Run(map[bool]string{true: "loaded", false: "unloaded"}[loaded], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/ps" {
					t.Errorf("unexpected metadata endpoint: %s", r.URL.Path)
				}
				body := `{"models":[{"name":"other:latest","context_length":131072}]}`
				if loaded {
					body = `{"models":[{"name":"other:latest","context_length":131072},{"name":"demo:latest","context_length":8192,"model_info":{"demo.context_length":131072}}]}`
				}
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			result, err := probeContext(server.URL, "ollama", "", "demo")
			if err != nil || result["detected"] != loaded {
				t.Fatalf("unexpected detection: %#v, %v", result, err)
			}
			if loaded && result["contextLength"] != int64(8192) {
				t.Fatalf("used theoretical context: %#v", result)
			}
		})
	}
}

func TestFindOllamaRunningContextRegistryPortIsNotTag(t *testing.T) {
	for _, test := range []struct {
		selected, running string
		want              int64
	}{
		{"registry:5000/team/demo", "registry:5000/team/demo:latest", 8192},
		{"registry:5000/team/demo:q4", "registry:5000/team/demo:latest", 0},
		{"registry:5000/team/demo:q4", "registry:5000/team/demo:q4", 8192},
	} {
		body := map[string]any{"models": []any{map[string]any{"name": test.running, "context_length": float64(8192)}}}
		if got, _ := findOllamaRunningContext(body, test.selected); got != test.want {
			t.Errorf("selected=%s running=%s: got %d want %d", test.selected, test.running, got, test.want)
		}
	}
}

func TestProbeContextDoesNotFollowRedirects(t *testing.T) {
	redirected := false
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected = true
		_, _ = w.Write([]byte(`{"n_ctx":32768}`))
	}))
	defer destination.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL+"/props", http.StatusFound)
	}))
	defer source.Close()
	result, err := probeContext(source.URL, "llama.cpp", "", "demo")
	if err != nil {
		t.Fatal(err)
	}
	if result["detected"] != false || redirected {
		t.Fatalf("redirect was followed: result=%#v", result)
	}
}

func TestFindContextIgnoresTrainingLengthAndFractionalValues(t *testing.T) {
	value, _ := findContext(map[string]any{"n_ctx_train": float64(131072), "context_length": float64(32768.5)})
	if value != 0 {
		t.Fatalf("unexpected context length: %d", value)
	}
}

func TestFindContextAcceptsOllamaSuffixedField(t *testing.T) {
	value, key := findContext(map[string]any{"details": map[string]any{"demo.context_length": float64(32768)}})
	if value != 32768 || key != "demo.context_length" {
		t.Fatalf("unexpected context length: %d (%s)", value, key)
	}
}

func TestFindContextUsesStablePreferredField(t *testing.T) {
	value, key := findContext(map[string]any{"max_model_len": float64(131072), "n_ctx": float64(32768)})
	if value != 32768 || key != "n_ctx" {
		t.Fatalf("unexpected preferred context length: %d (%s)", value, key)
	}
}

func TestFindContextPrefersDirectRuntimeFieldOverNestedMetadata(t *testing.T) {
	value, key := findContext(map[string]any{
		"n_ctx":         map[string]any{"context_length": float64(131072)},
		"max_model_len": float64(8192),
	})
	if value != 8192 || key != "max_model_len" {
		t.Fatalf("nested metadata overrode direct runtime field: %d (%s)", value, key)
	}
}

func TestFindContextForModelSelectsRequestedModel(t *testing.T) {
	value, key := findContextForModel(map[string]any{"data": []any{
		map[string]any{"id": "other", "max_model_len": float64(4096)},
		map[string]any{"id": "demo", "max_model_len": float64(131072)},
	}}, "demo")
	if value != 131072 || key != "max_model_len" {
		t.Fatalf("unexpected model context length: %d (%s)", value, key)
	}
}

func TestFindContextForModelRejectsUnexpectedTopLevelShape(t *testing.T) {
	value, key := findContextForModel([]any{map[string]any{"n_ctx": float64(4096)}}, "demo")
	if value != 0 || key != "" {
		t.Fatalf("used unbound fallback context length: %d (%s)", value, key)
	}
}

func TestFindContextForModelRequiresModelAndStandardEnvelope(t *testing.T) {
	for _, value := range []any{
		map[string]any{"n_ctx": float64(4096)},
		map[string]any{"models": []any{map[string]any{"id": "other", "n_ctx": float64(4096)}}},
	} {
		if n, _ := findContextForModel(value, "demo"); n != 0 {
			t.Fatalf("accepted unexpected envelope: %#v", value)
		}
	}
	if n, _ := findContextForModel(map[string]any{"data": []any{map[string]any{"id": "other", "n_ctx": float64(4096)}}}, ""); n != 0 {
		t.Fatal("guessed context without a selected model")
	}
}

func TestFindContextForModelRejectsUnmatchedModelList(t *testing.T) {
	value, key := findContextForModel(map[string]any{"data": []any{
		map[string]any{"id": "other", "max_model_len": float64(4096)},
	}}, "demo")
	if value != 0 || key != "" {
		t.Fatalf("used another model's context: %d (%s)", value, key)
	}
}

func TestFindContextForModelDoesNotMatchAliasOfDifferentID(t *testing.T) {
	value, _ := findContextForModel(map[string]any{"data": []any{
		map[string]any{"id": "other", "name": "demo", "max_model_len": float64(131072)},
		map[string]any{"id": "demo", "max_model_len": float64(8192)},
	}}, "demo")
	if value != 8192 {
		t.Fatalf("used context from a different canonical model: %d", value)
	}
}

func TestProbeContextRejectsInvalidEndpointShape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("invalid endpoint reached metadata service")
		_, _ = w.Write([]byte(`{"n_ctx":32768}`))
	}))
	defer server.Close()
	for _, endpoint := range []string{
		strings.Replace(server.URL, "http://", "ftp://", 1), strings.TrimPrefix(server.URL, "http:"),
		server.URL + "?token=private", server.URL + "#fragment",
		server.URL + "?", server.URL + "/#fragment",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if _, err := probeContext(endpoint, "llama.cpp", "", "demo"); err == nil {
				t.Fatal("expected invalid endpoint rejection")
			}
		})
	}
}
