package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestModelDiscoveryUsesCallableNamesAndKeepsAmbiguousIDs(t *testing.T) {
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[{"id":"astra-id","name":"Astra display","model":"gpt-6-astra"},{"id":"terra-id","model":"gpt-6.1-terra"},{"id":"luna-id","model":"gpt-6-luna"},{"id":"sol-id","model":"gpt-6.1-sol"},{"id":"same-1","model":"shared"},{"id":"same-2","model":"shared"},{"id":"legacy-id"},{"id":"uuid-named","model":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}]`))
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
	request := httptest.NewRequest("GET", "/v1/models", nil)
	request.Header.Set("X-Api-Key", "application-key")
	request.Header.Set("Authorization", "Bearer ses_client_login")
	response := httptest.NewRecorder()
	g.ServeHTTP(response, request)
	var listing struct {
		Data []struct {
			ID           string `json:"id"`
			DeploymentID string `json:"deployment_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &listing); err != nil || response.Code != 200 {
		t.Fatalf("listing: %d %s", response.Code, response.Body.String())
	}
	want := []string{"gpt-6-astra", "gpt-6.1-terra", "gpt-6-luna", "gpt-6.1-sol", "same-1", "same-2", "legacy-id", "uuid-named"}
	if len(listing.Data) != len(want) {
		t.Fatalf("models: %+v", listing.Data)
	}
	for i, id := range want {
		if listing.Data[i].ID != id || listing.Data[i].DeploymentID == "" {
			t.Errorf("model %d: %+v, want %s and deployment_id", i, listing.Data[i], id)
		}
	}
}

func TestNamedNodeModelKeepsDeploymentIDInPeerEvidence(t *testing.T) {
	t.Setenv("NEWNOA_FRP_OWNER_ID", "fixture")
	const deployment = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
	const node = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	evidenceID := ""
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/authorize") {
			w.Write([]byte(`{"deploymentId":"` + deployment + `","model":"upstream","nodeId":"` + node + `","epoch":1,"ownerId":"fixture","peer":{"available":true,"private":true,"url":"https://127.0.0.1:1","certificateSha256":"` + strings.Repeat("a", 64) + `","nodeId":"` + node + `","epoch":1,"tunnel":{}}}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/peer-fallbacks") {
			var body struct {
				Evidence struct {
					DeploymentID string `json:"deploymentId"`
				} `json:"evidence"`
			}
			json.NewDecoder(r.Body).Decode(&body)
			evidenceID = body.Evidence.DeploymentID
			w.Write([]byte(`{}`))
			return
		}
		t.Errorf("unexpected request: %s", r.URL.Path)
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
	request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"named-node-model","messages":[{"role":"user","content":"hello"}],"max_tokens":16}`))
	request.Header.Set("X-Api-Key", "nn_"+strings.Repeat("k", 40))
	request.Header.Set("Authorization", "Bearer ses_fixture")
	response := httptest.NewRecorder()
	g.ServeHTTP(response, request)
	if response.Code != 503 || evidenceID != deployment {
		t.Fatalf("status %d, evidence deployment %q", response.Code, evidenceID)
	}
}
