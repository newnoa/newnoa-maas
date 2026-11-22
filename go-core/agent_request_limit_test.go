package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLocalAgentProxyBodyLimits(t *testing.T) {
	for _, path := range []string{"/v1/responses", "/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			calls := 0
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				raw, err := io.ReadAll(r.Body)
				if err != nil || len(raw) <= maxFrame || !strings.Contains(string(raw), `"model":"fixture"`) {
					t.Error("large agent request changed")
				}
				io.WriteString(w, `{}`)
			}))
			defer gateway.Close()
			c := fixedGatewayTestCore(t.TempDir(), gateway.URL)
			if err := c.startProxyAt("nn_"+strings.Repeat("x", 40), availableProxyPort(t)); err != nil {
				t.Fatal(err)
			}
			defer c.shutdown()
			for _, tc := range []struct{ size, status int }{{2 << 20, 200}, {modelTestRequestLimit + 1, 413}} {
				body := `{"model":"fixture","input":"[TEST]"}` + strings.Repeat(" ", tc.size)
				req, _ := http.NewRequest("POST", c.proxyBaseURL()+path, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
				if res.StatusCode != tc.status {
					t.Fatalf("body=%d status=%d want=%d", len(body), res.StatusCode, tc.status)
				}
			}
			if calls != 1 {
				t.Fatalf("upstream calls=%d want=1", calls)
			}
		})
	}
}
