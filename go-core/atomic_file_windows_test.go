package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAtomicFileWindowsPublishesAndReplacesLongPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), strings.Repeat("a", 140), strings.Repeat("b", 140))
	path := filepath.Join(dir, "配置.json")
	for _, value := range []string{"original", "replacement"} {
		if err := atomicFile(path, []byte(value)); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(path)
		if err != nil || string(got) != value {
			t.Fatalf("read published file: %q, %v", got, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatalf("unexpected files after replacement: %v, %v", entries, err)
	}
}
