package main

import "testing"

func TestUpstreamURLJoinsOpenAIBasePath(t *testing.T) {
	for _, test := range []struct {
		endpoint, path, want string
	}{
		{"http://127.0.0.1:8082", "/v1/chat/completions", "http://127.0.0.1:8082/v1/chat/completions"},
		{"http://127.0.0.1:8082/v1", "/v1/chat/completions", "http://127.0.0.1:8082/v1/chat/completions"},
		{"https://models.example/base", "/v1/responses", "https://models.example/base/v1/responses"},
		{"https://models.example/base/v1/images/generations", "/v1/images/generations", "https://models.example/base/v1/images/generations"},
	} {
		got, err := upstreamURL(test.endpoint, test.path)
		if err != nil || got != test.want {
			t.Fatalf("upstreamURL(%q, %q) = %q, %v; want %q", test.endpoint, test.path, got, err, test.want)
		}
	}
}
