package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCoreLocalImageResultJavaLive(t *testing.T) {
	base := os.Getenv("NEWNOA_IMAGES_CORE_API")
	if base == "" {
		t.Skip("isolated Java fixture not configured")
	}
	requestID, permit := os.Getenv("NEWNOA_IMAGES_LOCAL_REQUEST"), os.Getenv("NEWNOA_IMAGES_LOCAL_PERMIT")
	if !nodePattern.MatchString(requestID) || !strings.HasPrefix(permit, "permit_") {
		t.Fatal("invalid isolated image storage fixture")
	}
	png, _ := base64.StdEncoding.DecodeString(hostingPNG)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("download leaked model credentials")
		}
		w.Write(png)
	}))
	defer model.Close()
	n := &connector{nodeID: os.Getenv("NEWNOA_IMAGES_CORE_NODE"), credential: os.Getenv("NEWNOA_IMAGES_CORE_DEVICE"), control: base, client: &http.Client{Timeout: 30 * time.Second}}
	original := `{"data":[{"url":"` + model.URL + `/private.png"}],"usage":{"input_tokens":17,"output_tokens":1056}}`
	response := &http.Response{Body: io.NopCloser(strings.NewReader(original)), Header: make(http.Header)}
	if err := n.hostLocalImages(context.Background(), response, model.URL+"/v1", model.Client(), requestID, permit, os.Getenv("NEWNOA_IMAGES_CORE_GATEWAY"), 1); err != nil {
		t.Fatal(err)
	}
	result, _ := io.ReadAll(response.Body)
	var body struct {
		Data []struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	json.Unmarshal(result, &body)
	if len(body.Data) != 1 || strings.Contains(string(result), model.URL) {
		t.Fatal("private origin escaped")
	}
	address, err := url.Parse(body.Data[0].URL)
	if err != nil || address.Host != "www.example.invalid" {
		t.Fatal("not a platform URL")
	}
	stored, err := http.Get(base + address.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer stored.Body.Close()
	data, _ := io.ReadAll(stored.Body)
	if stored.StatusCode != 200 || !bytes.Equal(png, data) || stored.Header.Get("Location") != "" {
		t.Fatalf("stored image status=%d bytes=%d", stored.StatusCode, len(data))
	}
	t.Log("CORE_LOCAL_IMAGE_JAVA_RUSTFS_PROTOCOL_OK")
}
