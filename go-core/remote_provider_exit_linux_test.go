//go:build linux

package main

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestRemoteNativeCloseRejectsSurvivingOwnedGroup(t *testing.T) {
	owned, child, done := remoteProcessGroupFixture(t)
	if err := syscall.Kill(owned.PID, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("native group leader did not exit")
	}
	if !remoteTestProcessExecuting(child) {
		t.Fatal("owned child fixture did not survive leader")
	}
	input, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	terminated := make(chan struct{})
	close(terminated)
	provider := &remoteNativeProvider{cancel: func() {}, stdin: input, done: terminated, owned: owned}
	if err = provider.Close(); err == nil {
		t.Fatal("native Close reported success while an owned descendant remained alive")
	}
	if !remoteTestProcessExecuting(child) {
		t.Fatal("unverifiable orphan group was signaled by Close")
	}
}
