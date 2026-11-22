//go:build linux

package main

import (
	"context"
	"os/exec"
	"testing"
)

func TestRemoteOwnedProcessRejectsWrongBirthAndStopsOnlyOwnedGroup(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), "sleep", "30")
	prepareVersionCommand(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cleanupVersionCommand(cmd)
	identity, err := remoteProcessIdentity(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	owned := remoteOwnedProcess{PID: cmd.Process.Pid, Identity: identity, Executable: cmd.Path}
	wrong := owned
	wrong.Identity = "another-birth"
	if stopRemoteOwnedProcess(wrong) == nil {
		t.Fatal("wrong process birth accepted")
	}
	alive, err := remoteOwnedProcessAlive(owned)
	if err != nil || !alive {
		t.Fatal("unrelated failed identity check killed owned process")
	}
	if err := stopRemoteOwnedProcess(owned); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	alive, err = remoteOwnedProcessAlive(owned)
	if err != nil || alive {
		t.Fatalf("owned process still live: %v %v", alive, err)
	}
}
