package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOwnerInferenceBodyLimitsAndForwarding(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"port":21001}`) }))
	defer control.Close()
	for _, tc := range []struct {
		path       string
		size, code int
	}{
		{"audio/transcriptions", 2 << 20, 200}, {"chat/completions", 2 << 20, 200},
		{"audio/transcriptions", (32 << 20) + 1, 413}, {"chat/completions", (32 << 20) + 1, 413},
		{"responses", 2 << 20, 200}, {"messages", 2 << 20, 200},
		{"responses", (32 << 20) + 1, 413}, {"messages", (32 << 20) + 1, 413},
		{"embeddings", 2 << 20, 413},
	} {
		t.Run(tc.path, func(t *testing.T) {
			o := newOwner("owner", control.URL, "internal")
			calls := 0
			o.upstream.Transport = embeddingTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				raw, _ := io.ReadAll(r.Body)
				if len(raw) != tc.size || string(raw) != strings.Repeat("a", tc.size) {
					t.Error("body changed")
				}
				if tc.path == "audio/transcriptions" && r.Header.Get("Content-Type") != "multipart/form-data; boundary=fixture" {
					t.Error("content type changed")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: fixture\n\n"))}, nil
			})
			r := httptest.NewRequest("POST", "/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/1/v1/"+tc.path, strings.NewReader(strings.Repeat("a", tc.size)))
			r.Header.Set("Authorization", "Bearer internal")
			r.Header.Set("Content-Type", "multipart/form-data; boundary=fixture")
			w := httptest.NewRecorder()
			o.ServeHTTP(w, r)
			if w.Code != tc.code || (tc.code == 200 && (calls != 1 || w.Body.String() != "data: fixture\n\n")) || (tc.code == 413 && calls != 0) {
				t.Fatalf("code=%d calls=%d", w.Code, calls)
			}
		})
	}
}
