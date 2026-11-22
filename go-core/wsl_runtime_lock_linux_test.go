package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestWSLRuntimeLockOwnsCanonicalPrivateScope(t *testing.T) {
	scope := t.TempDir()
	home := filepath.Join(scope, "home")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(scope, ".run-lock")
	if err := os.WriteFile(lockPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	lock, err := lockWSLRuntime(home)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if flags, err := unix.FcntlInt(lock.Fd(), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("lock may be inherited by Agent: flags=%d err=%v", flags, err)
	}
	if second, err := lockWSLRuntime(home); !errors.Is(err, unix.EWOULDBLOCK) {
		if second != nil {
			second.Close()
		}
		t.Fatalf("second writer was not refused: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := lockWSLRuntime(home)
	if err != nil {
		t.Fatal(err)
	}
	next.Close()
	if err := os.Rename(lockPath, lockPath+".target"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(lockPath+".target", lockPath); err != nil {
		t.Fatal(err)
	}
	if symlink, err := lockWSLRuntime(home); err == nil {
		symlink.Close()
		t.Fatal("accepted a symlink lock")
	}
}
