package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAtomicFileLeavesIdenticalPrivateFileUntouched(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	if err := atomicFile(path, []byte("private config")); err != nil {
		t.Fatal(err)
	}
	past := time.Unix(1600000000, 0)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(path)
	if err := atomicFile(path, []byte("private config")); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(path)
	if !os.SameFile(before, after) || !after.ModTime().Equal(before.ModTime()) || after.Mode().Perm() != 0600 {
		t.Fatal("unchanged safe config was replaced, touched or made public")
	}
}

func TestAtomicFileStillReplacesChangedOrUnsafeFiles(t *testing.T) {
	for _, mode := range []string{"changed", "public", "symlink"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config")
			if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			want := []byte("new")
			if mode == "public" {
				want = []byte("old")
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "symlink" {
				want = []byte("old")
				target := filepath.Join(dir, "target")
				if err := os.Rename(path, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := os.Lstat(path)
			if err := atomicFile(path, want); err != nil {
				t.Fatal(err)
			}
			after, _ := os.Lstat(path)
			data, err := os.ReadFile(path)
			if err != nil || string(data) != string(want) || !after.Mode().IsRegular() || after.Mode().Perm() != 0600 || os.SameFile(before, after) {
				t.Fatalf("unsafe replacement: %q %v mode=%v", data, err, after.Mode())
			}
		})
	}
}
