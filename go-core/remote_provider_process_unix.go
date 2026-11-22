//go:build !windows && !linux

package main

import "os/exec"

func prepareRemoteProviderCommand(command *exec.Cmd) { prepareVersionCommand(command) }
func cleanupRemoteProviderCommand(command *exec.Cmd) { cleanupVersionCommand(command) }
