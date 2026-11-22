//go:build !windows

package main

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"runtime"
	"time"
)

func launchRemoteGateway(ctx context.Context, binary, cwd string, args, env []string) (*remoteGatewayProcess, error) {
	lifetime := ctx
	if host, ok := ctx.Value(remoteProviderLifetimeKey{}).(context.Context); ok {
		lifetime = host
	}
	life, cancel := context.WithCancel(lifetime)
	cmd := exec.CommandContext(life, binary, args...)
	cmd.Dir, cmd.Env, cmd.Stdin, cmd.Stdout, cmd.Stderr = cwd, env, nil, io.Discard, io.Discard
	cmd.WaitDelay = 2 * time.Second
	prepareRemoteProviderCommand(cmd)
	started := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		if runtime.GOOS == "linux" {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
		}
		err := remoteCheckAuthority(ctx)
		if err == nil {
			err = cmd.Start()
		}
		started <- err
		if err == nil {
			_ = waitVersionCommand(cmd)
		}
		close(done)
	}()
	if err := <-started; err != nil {
		cancel()
		return nil, errors.New("Cannot start the owned native Gateway")
	}
	identity, err := remoteProcessIdentity(cmd.Process.Pid)
	if err != nil || identity == "" {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			return nil, errors.New("Unverified native Gateway cleanup timed out")
		}
		return nil, errors.New("Native Gateway ownership could not be verified")
	}
	g := &remoteGatewayProcess{done: done, cancel: cancel, owned: remoteOwnedProcess{PID: cmd.Process.Pid, Identity: identity, Executable: cmd.Path}}
	if err := remoteRecordGatewayProcess(ctx, g.owned); err != nil {
		return nil, errors.Join(err, g.Close())
	}
	return g, nil
}
