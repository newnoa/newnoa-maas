//go:build !windows

package main

import "os/exec"

func startRemoteNativeProcess(cmd *exec.Cmd) (func(), error) {
	return func() {}, cmd.Start()
}

func remoteProcessJob(int) string { return "" }
