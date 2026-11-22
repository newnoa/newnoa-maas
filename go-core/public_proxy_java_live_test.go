package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

// The parent isolated Java fixture owns accounts, VIP policy, deployments and cleanup.
func TestPublicProxyJavaLive(t *testing.T) {
	api := os.Getenv("NEWNOA_PUBLIC_TEST_API")
	if api == "" {
		t.Skip("set NEWNOA_PUBLIC_TEST_API/BEARER/MODEL/INTERNAL_TOKEN from the isolated Java fixture")
	}
	bearer, model, internal := os.Getenv("NEWNOA_PUBLIC_TEST_BEARER"), os.Getenv("NEWNOA_PUBLIC_TEST_MODEL"), os.Getenv("NEWNOA_PUBLIC_TEST_INTERNAL_TOKEN")
	if !validAPIKey(bearer) || model == "" || internal == "" {
		t.Fatal("isolated public fixture credentials missing")
	}
	secondBearer, secondModel := os.Getenv("NEWNOA_PUBLIC_TEST_SECOND_BEARER"), os.Getenv("NEWNOA_PUBLIC_TEST_SECOND_MODEL")
	if (secondBearer != "" || secondModel != "") && (!validAPIKey(secondBearer) || secondModel == "" || secondBearer == bearer) {
		t.Fatal("isolated second public fixture identity invalid")
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prefix/v1/chat/completions" || (r.Header.Get("Authorization") == "Bearer "+bearer || secondBearer != "" && r.Header.Get("Authorization") == "Bearer "+secondBearer) {
			t.Error("hosted upstream identity not bound")
		}
		io.WriteString(w, `{"choices":[{"message":{"content":"public isolated fixture"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":2}}`)
	}))
	defer upstream.Close()
	home := t.TempDir()
	c := &core{home: home, control: api, gateway: "http://127.0.0.1:1", onlineSupply: fixturePublicPolicy(upstream)}
	h, err := c.publicProxyHandler(publicTestSecret, internal)
	if err != nil {
		t.Fatal(err)
	}
	request := func(identity, method, path, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+identity)
		r.Header.Set("X-Newnoa-Public-Proxy", publicTestSecret)
		r.Header.Set("X-Newnoa-Caller-IP", "203.0.113.19")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "public-java-"+randomToken())
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	models := request(bearer, "GET", "/v1/models", "")
	if models.Code != 200 {
		t.Fatalf("real API model discovery status=%d body=%s", models.Code, models.Body.String())
	}
	var listing struct {
		Data []struct {
			ID           string `json:"id"`
			DeploymentID string `json:"deploymentId"`
		}
	}
	if json.Unmarshal(models.Body.Bytes(), &listing) != nil {
		t.Fatal("invalid public models JSON")
	}
	found := false
	for _, row := range listing.Data {
		if row.ID == model || row.DeploymentID == model {
			found = true
		}
	}
	if !found {
		t.Fatal("real API private model missing")
	}
	response := request(bearer, "POST", "/v1/chat/completions", `{"model":"`+model+`","messages":[{"role":"user","content":"[TEST] official website fixture"}]}`)
	if response.Code != 200 || response.Header().Get("X-Newnoa-Path") != "core-hosted-direct" || !strings.Contains(response.Body.String(), "public isolated fixture") {
		t.Fatalf("real API public dispatch status=%d body=%s", response.Code, response.Body.String())
	}
	if !nodePattern.MatchString(response.Header().Get("X-Newnoa-Request")) {
		t.Fatal("real API request identity missing")
	}
	seen := map[string]bool{response.Header().Get("X-Newnoa-Request"): true}
	if secondBearer != "" {
		var group sync.WaitGroup
		responses := make(chan *httptest.ResponseRecorder, 2)
		for _, identity := range []struct{ bearer, model string }{{bearer, model}, {secondBearer, secondModel}} {
			group.Add(1)
			go func(identity struct{ bearer, model string }) {
				defer group.Done()
				responses <- request(identity.bearer, "POST", "/v1/chat/completions", `{"model":"`+identity.model+`","messages":[{"role":"user","content":"[TEST] concurrent official website fixture"}]}`)
			}(identity)
		}
		group.Wait()
		close(responses)
		for concurrent := range responses {
			id := concurrent.Header().Get("X-Newnoa-Request")
			if concurrent.Code != 200 || concurrent.Header().Get("X-Newnoa-Path") != "core-hosted-direct" || !strings.Contains(concurrent.Body.String(), "public isolated fixture") || !nodePattern.MatchString(id) || seen[id] {
				t.Fatalf("concurrent real API identity failed: status=%d request=%s body=%s", concurrent.Code, id, concurrent.Body.String())
			}
			seen[id] = true
			t.Logf("PUBLIC_JAVA_REQUEST_ID=%s route=core-hosted-direct concurrent=true", id)
		}
	}
	entries, _ := os.ReadDir(home)
	if len(entries) != 0 || c.history != nil || c.proxySession != "" {
		t.Fatal("real API call created local state")
	}
	t.Logf("PUBLIC_JAVA_REQUEST_ID=%s route=core-hosted-direct noLocalState=true", response.Header().Get("X-Newnoa-Request"))
}
