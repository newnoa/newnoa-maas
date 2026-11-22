//go:build linux

package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func remoteProcessGroupFixture(t *testing.T) (remoteOwnedProcess, int, <-chan struct{}) {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "sh", "-c", `sh -c 'trap "" TERM; echo $$; exec sleep 30' & trap 'exit 0' TERM; wait`)
	prepareVersionCommand(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() {
		s := bufio.NewScanner(stdout)
		if s.Scan() {
			ready <- s.Text()
		} else {
			ready <- ""
		}
	}()
	var line string
	select {
	case line = <-ready:
	case <-time.After(3 * time.Second):
		cleanupVersionCommand(cmd)
		cmd.Wait()
		t.Fatal("owned child did not initialize")
	}
	child, err := strconv.Atoi(line)
	if err != nil {
		cleanupVersionCommand(cmd)
		cmd.Wait()
		t.Fatal("owned child PID missing")
	}
	identity, err := remoteProcessIdentity(cmd.Process.Pid)
	if err != nil {
		cleanupVersionCommand(cmd)
		cmd.Wait()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		cleanupVersionCommand(cmd)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("owned process test cleanup timed out")
		}
	})
	return remoteOwnedProcess{PID: cmd.Process.Pid, Identity: identity, Executable: cmd.Path}, child, done
}
func remoteTestProcessExecuting(pid int) bool {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	end := strings.LastIndex(string(raw), ")")
	if end < 0 {
		return true
	}
	fields := strings.Fields(string(raw)[end+1:])
	return len(fields) == 0 || fields[0] != "Z" && fields[0] != "X"
}

func TestRemoteOwnedProcessStopsChildAfterLeaderExits(t *testing.T) {
	owned, child, _ := remoteProcessGroupFixture(t)
	if err := stopRemoteOwnedProcess(owned); err != nil {
		t.Fatal(err)
	}
	if remoteTestProcessExecuting(child) {
		t.Fatal("cleanup succeeded while owned child ignored TERM and remained alive")
	}
	if alive, err := remoteOwnedProcessAlive(owned); err != nil || alive {
		t.Fatalf("terminated owned process group still reported live: %v %v", alive, err)
	}
}
func TestRemoteOwnedProcessOrphanGroupRemainsFenced(t *testing.T) {
	owned, child, done := remoteProcessGroupFixture(t)
	if err := syscall.Kill(owned.PID, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("leader did not exit")
	}
	alive, err := remoteOwnedProcessAlive(owned)
	if !alive || err == nil {
		t.Fatalf("orphaned group was declared safe without a live birth anchor: %v %v", alive, err)
	}
	if err = stopRemoteOwnedProcess(owned); err == nil {
		t.Fatal("unverified orphan group ownership allowed cleanup")
	}
	if !remoteTestProcessExecuting(child) {
		t.Fatal("unverified group child was killed")
	}
}
