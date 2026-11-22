package main

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUnaliasedDeploymentRequestRetainsBody(t *testing.T) {
	c := newCore(t.TempDir(), "https://gateway.example.test")
	raw := `{"model":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee","messages":[{"role":"user","content":"preserve me"}]}`
	request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(raw))
	if err := c.resolveManagedModel(request, "Bearer nn_abcdefghijklmnopqrstuvwxyz0123456789"); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(request.Body)
	if err != nil || string(body) != raw || request.ContentLength != int64(len(raw)) {
		t.Fatalf("unaliased body was consumed: %q", body)
	}
}
