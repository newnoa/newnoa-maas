package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemoteOpenCodeWindowsResolvesNativeNPMEntry(t *testing.T) {
	modules := filepath.Join(t.TempDir(), "node_modules")
	shims := filepath.Join(modules, ".bin")
	exe := filepath.Join(modules, "opencode-ai", "bin", "opencode.exe")
	for _, path := range []string{filepath.Join(shims, "opencode.cmd"), exe} {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("fixture: never execute"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", shims)
	t.Setenv("PATHEXT", ".EXE;.CMD")
	got, err := remoteExecutorCommand("opencode")
	if err != nil || got.path != exe || got.entry != "" {
		t.Fatalf("expected native package executable without a shell: %+v, %v", got, err)
	}
	if err = os.Remove(exe); err != nil {
		t.Fatal(err)
	}
	if _, err := remoteExecutorCommand("opencode"); err == nil {
		t.Fatal("fell back to invoking the batch shim")
	}
}
