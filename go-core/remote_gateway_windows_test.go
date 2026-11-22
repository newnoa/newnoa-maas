package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestRemoteGatewayWindowsContainsChildren(t *testing.T) {
	if mode := os.Getenv("NEWNOA_GATEWAY_FIXTURE"); mode != "" {
		if mode == "parent" {
			child := exec.Command(os.Args[0], "-test.run=^TestRemoteGatewayWindowsContainsChildren$")
			child.Env = append(os.Environ(), "NEWNOA_GATEWAY_FIXTURE=child")
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile("child.pid", []byte(strconv.Itoa(child.Process.Pid)), 0600); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(15 * time.Second)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dir := t.TempDir()
	var owned remoteOwnedProcess
	ctx = context.WithValue(ctx, remoteGatewayOwnerKey{}, func(p remoteOwnedProcess) error { owned = p; return nil })
	g, err := launchRemoteGateway(ctx, os.Args[0], dir, []string{"-test.run=^TestRemoteGatewayWindowsContainsChildren$"}, append(os.Environ(), "NEWNOA_GATEWAY_FIXTURE=parent"))
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if owned.PID <= 1 || owned.Identity == "" || owned.Job == "" {
		t.Fatal("missing identity")
	}
	if alive, err := remoteOwnedProcessAlive(owned); err != nil || !alive {
		t.Fatalf("gateway ownership not verifiable: %v %v", alive, err)
	}
	var pid int
	for pid == 0 {
		if raw, err := os.ReadFile(filepath.Join(dir, "child.pid")); err == nil {
			pid, _ = strconv.Atoi(string(raw))
		}
		select {
		case <-ctx.Done():
			t.Fatal("child never started")
		case <-time.After(15 * time.Millisecond):
		}
	}
	opened, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(opened)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if alive, err := remoteOwnedProcessAlive(owned); err != nil || alive {
		t.Fatalf("gateway job still active: %v %v", alive, err)
	}
	if result, err := windows.WaitForSingleObject(opened, 2000); err != nil || result != windows.WAIT_OBJECT_0 {
		t.Fatal(fmt.Errorf("child is not confirmed exited: %d %v", result, err))
	}
}
