package main

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestVersionCommandReapsGroupBeforeReleasingLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "/bin/sleep 30 >/dev/null 2>&1 & echo $!")
	raw, err := versionCommandOutput(cmd)
	if err != nil {
		t.Fatal(err)
	}
	child, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || child <= 1 {
		t.Fatalf("invalid fixture child %q", raw)
	}
	until := time.Now().Add(time.Second)
	for remoteTestProcessExecuting(child) {
		if time.Now().After(until) {
			t.Fatal("descendant survived successful version probe")
		}
		time.Sleep(time.Millisecond)
	}
}
