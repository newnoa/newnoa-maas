package main

import (
	"os/exec"
	"syscall"
)

func prepareRemoteProviderCommand(command *exec.Cmd) {
	prepareVersionCommand(command)
	command.SysProcAttr.Pdeathsig = syscall.SIGKILL
}
func cleanupRemoteProviderCommand(command *exec.Cmd) { cleanupVersionCommand(command) }
