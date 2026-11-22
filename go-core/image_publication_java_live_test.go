package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The root wire fixture owns disposable API/DB/node state and the upstream HTTP model.
func TestCoreImagePublicationJavaLive(t *testing.T) {
	api := os.Getenv("NEWNOA_IMAGES_CORE_API")
	if api == "" {
		t.Skip("set NEWNOA_IMAGES_CORE_* from the disposable root image wire fixture")
	}
	base, err := url.Parse(api)
	if err != nil || base.Scheme != "http" || net.ParseIP(base.Hostname()) == nil || !net.ParseIP(base.Hostname()).IsLoopback() {
		t.Fatal("image publication live test requires an explicit loopback API fixture")
	}
	session, internal := os.Getenv("NEWNOA_IMAGES_CORE_SESSION"), os.Getenv("NEWNOA_IMAGES_CORE_INTERNAL")
	node, device, gateway := os.Getenv("NEWNOA_IMAGES_CORE_NODE"), os.Getenv("NEWNOA_IMAGES_CORE_DEVICE"), os.Getenv("NEWNOA_IMAGES_CORE_GATEWAY")
	epoch, err := strconv.ParseInt(os.Getenv("NEWNOA_IMAGES_CORE_EPOCH"), 10, 64)
	upstream := os.Getenv("NEWNOA_IMAGES_CORE_UPSTREAM")
	if !strings.HasPrefix(session, "ses_") || len(internal) < 32 || !nodePattern.MatchString(node) || device == "" || gateway == "" || err != nil || epoch < 1 {
		t.Fatal("invalid disposable image publication credentials")
	}
	if _, err = (supplyPolicy{edition: "overseas"}).endpoint(upstream); err != nil {
		t.Fatal("image publication upstream must remain local")
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	call := func(method, path, credential string, body any, want int) map[string]any {
		t.Helper()
		var encoded []byte
		if body != nil {
			encoded, err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		request, requestErr := http.NewRequestWithContext(ctx, method, strings.TrimRight(api, "/")+"/api/v1"+path, bytes.NewReader(encoded))
		if requestErr != nil {
			t.Fatal("invalid live fixture request")
		}
		request.Header.Set("Authorization", "Bearer "+credential)
		request.Header.Set("Content-Type", "application/json")
		response, requestErr := client.Do(request)
		if requestErr != nil {
			t.Fatal("live publication API unavailable")
		}
		defer response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("live publication API %s %s returned %d; expected %d", method, path, response.StatusCode, want)
		}
		var result map[string]any
		if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil {
			t.Fatal("invalid live publication API JSON")
		}
		return result
	}
	const model = "wire-local-image"
	raw := []byte(`{"model":"wire-local-image","prompt":"A simple red circle on a plain white background.","n":1,"size":"1024x1024","stream":false}`)
	config := map[string]any{"requestId": uuid.NewString(), "name": "[TEST] local image publication", "model": model, "modelType": "image", "inferencePath": imageGenerationPath, "endpoint": upstream + "/v1", "apiKey": "fixture-upstream-only", "nodeId": node, "visibility": "public", "concurrencyLimit": 1}
	check := call("POST", "/me/publication-checks", session, config, 200)
	id, ok := check["id"].(string)
	if !ok || !nodePattern.MatchString(id) || check["status"] != "pending" {
		t.Fatal("live local publication did not become pending")
	}
	claimRequest, _ := http.NewRequest("POST", api+"/api/v1/internal/publication-checks/claim", strings.NewReader(`{"gatewayId":"`+gateway+`","limit":8}`))
	claimRequest.Header.Set("Authorization", "Bearer "+internal)
	claimRequest.Header.Set("Content-Type", "application/json")
	claimResponse, err := client.Do(claimRequest)
	if err != nil {
		t.Fatal("live local publication claim unavailable")
	}
	defer claimResponse.Body.Close()
	var jobs []map[string]any
	if claimResponse.StatusCode != 200 || json.NewDecoder(io.LimitReader(claimResponse.Body, 1<<20)).Decode(&jobs) != nil {
		t.Fatal("invalid live local publication claim")
	}
	found := false
	for _, job := range jobs {
		if job["id"] == id {
			found = true
			if job["nodeId"] != node || job["credential"] != "" || job["inferencePath"] != imageGenerationPath {
				t.Fatal("live publication claim leaked key or lost scope")
			}
		}
	}
	if !found {
		t.Fatal("live local publication not claimed by its assigned Go Server")
	}
	binding := map[string]any{"gatewayId": gateway, "httpMethod": "POST", "inferencePath": imageGenerationPath, "bodyDigest": hash(append([]byte("POST "+imageGenerationPath+"\n"), raw...))}
	prepared := call("POST", "/internal/publication-checks/"+id+"/prepare", internal, binding, 200)
	permit, ok := prepared["permit"].(string)
	if !ok || !strings.HasPrefix(permit, "pubprobe_") {
		t.Fatal("live local publication did not receive a scoped permit")
	}
	upstreamClient := (supplyPolicy{edition: "overseas"}).client(10 * time.Second)
	defer upstreamClient.CloseIdleConnections()
	n := &connector{purpose: "serving", nodeID: node, control: api, credential: device, client: client, upstream: upstreamClient, remoteUpstream: upstreamClient}
	execute := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest("POST", "/publication-probe"+imageGenerationPath, bytes.NewReader(raw))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Newnoa-Request", id)
		request.Header.Set("X-Newnoa-Permit", permit)
		request.Header.Set("X-Newnoa-Gateway", gateway)
		request.Header.Set("X-Newnoa-Epoch", strconv.FormatInt(epoch, 10))
		out := httptest.NewRecorder()
		n.ServeHTTP(out, request)
		return out
	}
	response := execute()
	if response.Code != 200 || response.Header().Get("X-Newnoa-Probe-Upstream") != "1" {
		t.Fatalf("Core/live Java/local upstream probe failed: status %d", response.Code)
	}
	var image struct {
		Data []struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	if json.Unmarshal(response.Body.Bytes(), &image) != nil || len(image.Data) != 1 || image.Data[0].URL == "" {
		t.Fatal("live upstream image response not preserved")
	}
	replay := execute()
	if replay.Code != 409 || replay.Header().Get("X-Newnoa-Probe-Upstream") != "" {
		t.Fatal("real Java publication permit replay reached upstream")
	}
	call("POST", "/internal/publication-checks/"+id+"/result", internal, map[string]any{"gatewayId": gateway, "status": "succeeded", "networkLatencyMs": 1}, 200)
	current := call("GET", "/me/publication-checks/"+id, session, nil, 200)
	if current["status"] != "succeeded" {
		t.Fatal("trusted live publication result was not persisted")
	}
	t.Log("CORE_IMAGE_PUBLICATION_JAVA liveApi=true actualConnector=true actualUpstream=true permitSingleUse=true credentialsScoped=true")
}
