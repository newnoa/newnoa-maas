//go:build !windows

package main

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func prepareVersionCommand(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if err == syscall.ESRCH {
			return os.ErrProcessDone
		}
		return err
	}
}

func cleanupVersionCommand(command *exec.Cmd) {
	if command.Process != nil {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
	}
}

func runVersionCommand(command *exec.Cmd) error {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		return err
	}
	return waitVersionCommand(command)
}

func waitVersionCommand(command *exec.Cmd) error {
	if !versionQueriesSupported() {
		return command.Wait()
	}
	pid := command.Process.Pid
	// Keep the exited leader unreaped until its process group is cleaned up.
	observed := waitVersionExit(pid)
	killed := syscall.Kill(-pid, syscall.SIGKILL)
	if killed == syscall.ESRCH {
		killed = nil
	}
	if observed == nil && killed == syscall.EPERM && exitedVersionGroupHasNoChildren(pid) {
		killed = nil
	}
	return errors.Join(observed, killed, command.Wait())
}
