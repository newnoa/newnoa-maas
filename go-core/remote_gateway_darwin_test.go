package main

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
	"testing"
	"time"
)

func TestRemoteGatewayDarwinRecordsOwnershipAndCloses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var owned remoteOwnedProcess
	ctx = context.WithValue(ctx, remoteGatewayOwnerKey{}, func(process remoteOwnedProcess) error {
		owned = process
		return nil
	})
	g, err := launchRemoteGateway(ctx, "/bin/sleep", t.TempDir(), []string{"30"}, []string{})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if owned.PID <= 1 || owned.Identity == "" || owned.Executable != "/bin/sleep" {
		t.Fatalf("incomplete ownership: %+v", owned)
	}
	wrong := owned
	wrong.Identity += ":wrong"
	if err := stopRemoteOwnedProcess(wrong); err == nil {
		t.Fatal("foreign process identity accepted")
	}
	if identity, err := remoteProcessIdentity(owned.PID); err != nil || identity != owned.Identity {
		t.Fatalf("unverified record or foreign cleanup stopped the owned process: %q %v", identity, err)
	}
	if err := stopRemoteOwnedProcess(owned); err == nil {
		t.Fatal("reattached process was signaled without a stable OS handle")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := remoteProcessIdentity(owned.PID); !os.IsNotExist(err) {
		t.Fatalf("gateway still present or PID lookup was inconclusive: %v", err)
	}
}

func TestDarwinVersionProbeCleansGroupBeforeReapingExitedLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "/bin/sleep 20 >/dev/null 2>&1 & echo $!")
	raw, err := versionCommandOutput(cmd)
	if err != nil {
		t.Fatal(err)
	}
	child, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || child <= 1 {
		t.Fatal("missing fixture child")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		rows, err := unix.SysctlKinfoProcSlice("kern.proc.pid", child)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("version command left a child")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
