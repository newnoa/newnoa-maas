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

const hostingPNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j5c8AAAAASUVORK5CYII="

func TestLocalImageURLIsReadBySupplierAndUploadedToJava(t *testing.T) {
	png, _ := base64.StdEncoding.DecodeString(hostingPNG)
	downloads, uploads := 0, 0
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloads++
		if r.Header.Get("Authorization") != "" {
			t.Error("upstream key leaked to image download")
		}
		w.Write(png)
	}))
	defer local.Close()
	java := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/connector/dispatches/request/images" || r.Header.Get("Authorization") != "Bearer device-fixture" {
			t.Error("wrong supplier upload identity")
		}
		var body struct {
			NodeID string `json:"nodeId"`
			Epoch  int    `json:"epoch"`
			Permit string `json:"permit"`
			Images []struct {
				Data []byte `json:"data"`
			} `json:"images"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if body.NodeID != "node" || body.Epoch != 2 || body.Permit != "permit-fixture" || len(body.Images) != 1 || !bytes.Equal(body.Images[0].Data, png) {
			t.Errorf("wrong upload %+v", body)
		}
		uploads++
		io.WriteString(w, `{"urls":[{"index":0,"url":"https://dev-www.example.invalid/v1/images/files/id/digest/signature"}]}`)
	}))
	defer java.Close()
	n := &connector{control: java.URL, credential: "device-fixture", nodeID: "node", client: java.Client()}
	raw := `{"data":[{"url":"` + local.URL + `/private.png"}],"usage":{"input_tokens":17,"output_tokens":1056}}`
	response := &http.Response{Body: io.NopCloser(strings.NewReader(raw)), Header: make(http.Header)}
	if err := n.hostLocalImages(context.Background(), response, local.URL+"/v1", local.Client(), "request", "permit-fixture", "server", 2); err != nil {
		t.Fatal(err)
	}
	result, _ := io.ReadAll(response.Body)
	if downloads != 1 || uploads != 1 || strings.Contains(string(result), local.URL) || !strings.Contains(string(result), "https://dev-www.example.invalid/v1/images/files/") {
		t.Fatalf("supplier image not hosted: %s", result)
	}
	if !strings.Contains(string(result), `"input_tokens":17`) {
		t.Fatal("usage lost")
	}
}

func TestLocalImageDownloadCannotEscapeApprovedModelOrigin(t *testing.T) {
	calls := 0
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; io.WriteString(w, hostingPNG) }))
	defer other.Close()
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", other.URL)
		w.WriteHeader(302)
	}))
	defer model.Close()
	n := &connector{}
	for _, address := range []string{other.URL + "/secret", model.URL + "/redirect"} {
		response := &http.Response{Body: io.NopCloser(strings.NewReader(`{"data":[{"url":"` + address + `"}]}`)), Header: make(http.Header)}
		if err := n.hostLocalImages(context.Background(), response, model.URL, model.Client(), "request", "permit", "server", 1); err == nil {
			t.Fatal("unsafe local image accepted")
		}
	}
	if calls != 0 {
		t.Fatal("local image escaped model origin")
	}
}

func TestLocalImageStorageFailureRetainsUsageWithoutAccessURLs(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "failed", 502) }))
	defer source.Close()
	raw := `{"data":[{"url":"` + source.URL + `/private.png?token=secret"}],"usage":{"input_tokens":17,"output_tokens":1056}}`
	res := &http.Response{Body: io.NopCloser(strings.NewReader(raw)), Header: make(http.Header)}
	n := &connector{}
	if err := n.hostLocalImages(context.Background(), res, source.URL, source.Client(), "request", "permit", "server", 1); err == nil {
		t.Fatal("download unexpectedly succeeded")
	}
	failure := localImageStorageFailure(res)
	if strings.Contains(string(failure), source.URL) || strings.Contains(string(failure), "token=secret") || !strings.Contains(string(failure), `"input_tokens":17`) {
		t.Fatalf("failure lost usage or leaked URL: %s", failure)
	}
	capture := newUsageCapture(false)
	capture.protocol = imageGenerationPath
	captureImageFailureUsage(capture, failure)
	report := capture.counts()
	if report.Input == nil || *report.Input != 17 || report.Output == nil || *report.Output != 1056 {
		t.Fatalf("usage missing %+v", report)
	}
}
