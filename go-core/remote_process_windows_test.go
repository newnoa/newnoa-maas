package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestRemoteWindowsIdentityRecordsUserAndBirth(t *testing.T) {
	first, err := remoteProcessIdentity(os.Getpid())
	if err != nil || !strings.HasPrefix(first, "windows:S-1-") {
		t.Fatalf("missing Windows process identity: %q %v", first, err)
	}
	second, err := remoteProcessIdentity(os.Getpid())
	if err != nil || second != first {
		t.Fatalf("live process identity changed: %q %v", second, err)
	}
	for _, pid := range []int{-1, 0, 1} {
		if _, err := remoteProcessIdentity(pid); err == nil {
			t.Fatalf("invalid process ID accepted: %d", pid)
		}
	}
}

func TestRemoteWindowsNativeOwnerCrash(t *testing.T) {
	if os.Getenv("NEWNOA_NATIVE_OWNER_FIXTURE") == "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRemoteWindowsNativeTreeCleanup$")
		cmd.Env = append(os.Environ(), "NEWNOA_NATIVE_TREE_FIXTURE=parent")
		cleanup, err := startRemoteNativeProcess(cmd)
		if err != nil {
			t.Fatal(err)
		}
		defer cleanup()
		identity, err := remoteProcessIdentity(cmd.Process.Pid)
		if err != nil {
			t.Fatal(err)
		}
		owned := remoteOwnedProcess{PID: cmd.Process.Pid, Identity: identity, Executable: cmd.Path, Job: remoteProcessJob(cmd.Process.Pid)}
		data, err := json.Marshal(owned)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile("owned.json", data, 0600); err != nil {
			t.Fatal(err)
		}
		time.Sleep(15 * time.Second)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	owner := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRemoteWindowsNativeOwnerCrash$")
	owner.Dir = t.TempDir()
	owner.Env = append(os.Environ(), "NEWNOA_NATIVE_OWNER_FIXTURE=1")
	if err := owner.Start(); err != nil {
		t.Fatal(err)
	}
	defer owner.Process.Kill()
	var record remoteOwnedProcess
	var childPID int
	for record.Job == "" || childPID == 0 {
		if raw, err := os.ReadFile(filepath.Join(owner.Dir, "owned.json")); err == nil {
			_ = json.Unmarshal(raw, &record)
		}
		if raw, err := os.ReadFile(filepath.Join(owner.Dir, "child.pid")); err == nil {
			childPID, _ = strconv.Atoi(string(raw))
		}
		if ctx.Err() != nil {
			t.Fatal("owned process tree did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	child, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(childPID))
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(child)
	if err = owner.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = owner.Wait()
	if state, err := windows.WaitForSingleObject(child, 2000); err != nil || state != windows.WAIT_OBJECT_0 {
		t.Fatalf("orphaned child remains: %d %v", state, err)
	}
	if alive, err := remoteOwnedProcessAlive(record); err != nil || alive {
		t.Fatalf("crashed owner left job: %v %v", alive, err)
	}
	if _, err := remoteOwnedProcessAlive(remoteOwnedProcess{PID: record.PID}); err == nil {
		t.Fatal("unverified legacy process accepted")
	}
}

func TestRemoteWindowsNativeTreeCleanup(t *testing.T) {
	if mode := os.Getenv("NEWNOA_NATIVE_TREE_FIXTURE"); mode != "" {
		if mode == "parent" {
			child := exec.Command(os.Args[0], "-test.run=^TestRemoteWindowsNativeTreeCleanup$")
			child.Env = append(os.Environ(), "NEWNOA_NATIVE_TREE_FIXTURE=child")
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
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRemoteWindowsNativeTreeCleanup$")
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), "NEWNOA_NATIVE_TREE_FIXTURE=parent")
	cleanup, err := startRemoteNativeProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	identity, err := remoteProcessIdentity(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	owned := remoteOwnedProcess{PID: cmd.Process.Pid, Identity: identity, Executable: cmd.Path, Job: remoteProcessJob(cmd.Process.Pid)}
	if owned.Job == "" {
		t.Fatal("missing private process job")
	}
	var childPID int
	for childPID == 0 {
		if raw, err := os.ReadFile(filepath.Join(cmd.Dir, "child.pid")); err == nil {
			childPID, _ = strconv.Atoi(string(raw))
		}
		if ctx.Err() != nil {
			t.Fatal("child did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	child, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(childPID))
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(child)
	wrong := owned
	wrong.Identity, err = remoteProcessIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if stopRemoteOwnedProcess(wrong) == nil {
		t.Fatal("wrong process identity accepted")
	}
	if alive, err := remoteOwnedProcessAlive(owned); err != nil || !alive {
		t.Fatalf("valid job not live: %v %v", alive, err)
	}
	if err := stopRemoteOwnedProcess(owned); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if state, err := windows.WaitForSingleObject(child, 2000); err != nil || state != windows.WAIT_OBJECT_0 {
		t.Fatalf("child remains: %d %v", state, err)
	}
	if alive, err := remoteOwnedProcessAlive(owned); err != nil || alive {
		t.Fatalf("job remains: %v %v", alive, err)
	}
}
