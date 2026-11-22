package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func prepareRemoteProviderCommand(command *exec.Cmd) {
	command.Cancel = func() error {
		if command.Process == nil {
			return os.ErrProcessDone
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		// Only the process created by this adapter and its children belong to this cancellation.
		if err := exec.CommandContext(ctx, "taskkill", "/PID", strconv.Itoa(command.Process.Pid), "/T", "/F").Run(); err != nil {
			if killErr := command.Process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				return killErr
			}
		}
		return nil
	}
}
func cleanupRemoteProviderCommand(command *exec.Cmd) {}
