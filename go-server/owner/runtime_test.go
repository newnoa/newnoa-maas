package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRuntimeBuildSnapshot(t *testing.T) {
	b, err := loadRuntimeBuild("")
	if err != nil || len(b.actual) != 64 || !b.matches() {
		t.Fatalf("snapshot failed: %v", err)
	}
	pinned, err := loadRuntimeBuild(b.actual)
	if err != nil || !pinned.matches() {
		t.Fatal("matching artifact rejected")
	}
	bad, err := loadRuntimeBuild(strings.Repeat("0", 64))
	if err != nil || bad.matches() {
		t.Fatal("wrong artifact accepted")
	}
	for _, value := range []string{"bad", strings.Repeat("A", 64), " " + b.actual} {
		if _, err := loadRuntimeBuild(value); err == nil {
			t.Fatal("invalid pin accepted")
		}
	}
}
func TestOwnerMismatchGatesServingAndKeepsAuthenticatedIdentity(t *testing.T) {
	g := newOwner("owner-a", "http://127.0.0.1:1", "private-token")
	g.build = runtimeBuild{actual: strings.Repeat("a", 64), expected: strings.Repeat("b", 64)}
	for _, path := range []string{"/ready", "/v1/models", "/v1/chat/completions", "/tunnels/00000000-0000-0000-0000-000000000001/1/v1/chat/completions"} {
		w := httptest.NewRecorder()
		g.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 503 {
			t.Fatalf("%s got %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("GET", "/health", nil))
	if w.Code != 200 {
		t.Fatal("health depends on build")
	}
	for _, token := range []string{"", "Bearer wrong", "Bearer private-token"} {
		r := httptest.NewRequest("GET", "/internal/runtime", nil)
		r.Header.Set("Authorization", token)
		w = httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if token != "Bearer private-token" {
			if w.Code != 401 || strings.Contains(w.Body.String(), g.build.actual) {
				t.Fatal("runtime identity leaked")
			}
			continue
		}
		var result map[string]any
		if json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result) != 4 || result["instance"] != "owner-a" || result["buildMatches"] != false || result["buildPinned"] != true || result["artifactSha256"] != g.build.actual || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("invalid runtime identity")
		}
	}
}
