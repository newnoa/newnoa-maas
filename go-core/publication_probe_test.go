package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func publicationProbeRequest(path string, raw []byte) *http.Request {
	r := probeRequest("POST", path, raw)
	r.URL.Path = "/publication-probe" + path
	r.Header.Set("X-Newnoa-Permit", "pubprobe_fixture")
	return r
}

func TestPublicationProbeOnlyConsumesItsBoundSingleUsePermit(t *testing.T) {
	raw := []byte(`{"model":"image-model","prompt":"A red circle","n":1}`)
	var executions, consumes atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executions.Add(1)
		received, _ := io.ReadAll(r.Body)
		if r.URL.Path != imagePath || !bytes.Equal(received, raw) || r.Header.Get("Authorization") != "Bearer upstream-fixture" || r.Header.Get("X-Newnoa-Permit") != "" {
			t.Error("publication wire or credentials changed")
		}
		w.Header().Set("X-Newnoa-Probe-Upstream", "forged")
		io.WriteString(w, `{"data":[{"url":"https://images.example.test/result.png"}]}`)
	}))
	defer upstream.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/connector/publication-checks/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/consume" || r.Header.Get("Authorization") != "Bearer node_fixture" {
			t.Error("wrong publication consumer identity")
		}
		var claim map[string]any
		json.NewDecoder(r.Body).Decode(&claim)
		if claim["bodyDigest"] != hash(append([]byte("POST "+imagePath+"\n"), raw...)) || claim["httpMethod"] != "POST" || claim["inferencePath"] != imagePath || claim["permit"] != "pubprobe_fixture" || claim["gatewayId"] != "gateway-a" || claim["epoch"] != float64(4) {
			t.Error("publication scope unbound", claim)
		}
		if consumes.Add(1) > 1 {
			w.WriteHeader(409)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"endpoint": upstream.URL, "model": "image-model", "apiKey": "upstream-fixture", "edition": "domestic", "httpMethod": "POST", "inferencePath": imagePath})
	}))
	defer api.Close()
	n := &connector{purpose: "serving", control: api.URL, credential: "node_fixture", client: api.Client(), upstream: upstream.Client()}
	for i, want := range []int{200, 409} {
		out := httptest.NewRecorder()
		n.ServeHTTP(out, publicationProbeRequest(imagePath, raw))
		if out.Code != want {
			t.Fatalf("attempt %d: %d %s", i, out.Code, out.Body.String())
		}
		if i == 0 && out.Header().Get("X-Newnoa-Probe-Upstream") != "1" {
			t.Fatal("actual response not marked")
		}
	}
	if executions.Load() != 1 || consumes.Load() != 2 {
		t.Fatalf("replayed publication: executions=%d consumes=%d", executions.Load(), consumes.Load())
	}
}

func TestPublicationProbeRejectsWrongPurposePermitAndScope(t *testing.T) {
	raw := []byte(`{"model":"image-model","prompt":"test"}`)
	for _, variant := range []string{"evaluation-lease", "ordinary-permit", "health-permit", "models-get", "beta", "target-method", "target-path", "target-model", "epoch", "digest"} {
		t.Run(variant, func(t *testing.T) {
			executions := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { executions++; io.WriteString(w, `{}`) }))
			defer upstream.Close()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var claim map[string]any
				json.NewDecoder(r.Body).Decode(&claim)
				if claim["epoch"] != float64(4) || claim["bodyDigest"] != hash(append([]byte("POST "+imagePath+"\n"), raw...)) {
					w.WriteHeader(409)
					return
				}
				target := map[string]any{"endpoint": upstream.URL, "model": "image-model", "httpMethod": "POST", "inferencePath": imagePath}
				switch variant {
				case "target-method":
					target["httpMethod"] = "GET"
				case "target-path":
					target["inferencePath"] = "/v1/responses"
				case "target-model":
					target["model"] = "another-model"
				}
				json.NewEncoder(w).Encode(target)
			}))
			defer api.Close()
			n := &connector{purpose: "serving", control: api.URL, credential: "node_fixture", client: api.Client(), upstream: upstream.Client()}
			r := publicationProbeRequest(imagePath, raw)
			switch variant {
			case "evaluation-lease":
				n.purpose = "evaluation"
			case "ordinary-permit":
				r.Header.Set("X-Newnoa-Permit", "permit_fixture")
			case "health-permit":
				r.Header.Set("X-Newnoa-Permit", "probe_fixture")
			case "models-get":
				r.Method = "GET"
				r.URL.Path = "/publication-probe/v1/models"
			case "beta":
				r.Header.Set("Anthropic-Beta", "untrusted")
			case "epoch":
				r.Header.Set("X-Newnoa-Epoch", "5")
			case "digest":
				r.Body = io.NopCloser(bytes.NewReader([]byte(`{"model":"image-model","prompt":"tampered"}`)))
			}
			out := httptest.NewRecorder()
			n.ServeHTTP(out, r)
			if out.Code < 400 || executions != 0 || out.Header().Get("X-Newnoa-Probe-Upstream") != "" {
				t.Fatalf("unsafe publication %s executed: %d %d", variant, out.Code, executions)
			}
		})
	}
}
