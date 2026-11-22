package main

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func remoteProcessIdentity(pid int) (string, error) {
	if pid <= 1 {
		return "", errors.New("Invalid owned process identity")
	}
	entries, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "", os.ErrNotExist
	}
	p := entries[0]
	if len(entries) != 1 || int(p.Proc.P_pid) != pid || int(p.Eproc.Pgid) != pid ||
		int(p.Eproc.Pcred.P_ruid) != os.Getuid() || int(p.Eproc.Ucred.Uid) != os.Geteuid() || p.Proc.P_starttime.Sec <= 0 {
		return "", errors.New("Owned macOS process user, group or birth could not be verified")
	}
	birth := p.Proc.P_starttime
	return fmt.Sprintf("%d:%d.%06d:darwin", os.Getuid(), birth.Sec, birth.Usec), nil
}

func remoteOwnedProcessAlive(p remoteOwnedProcess) (bool, error) {
	if p.PID <= 1 || p.Identity == "" {
		return false, errors.New("Owned process identity cannot be verified")
	}
	identity, err := remoteProcessIdentity(p.PID)
	if err == nil {
		if identity != p.Identity {
			return false, errors.New("Owned process birth identity changed")
		}
		return true, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	// Probe the group without sending a signal to a possibly reused PID.
	if err = unix.Kill(-p.PID, 0); err == unix.ESRCH {
		return false, nil
	}
	return true, errors.New("Owned macOS process group may remain without a verified leader")
}

func stopRemoteOwnedProcess(p remoteOwnedProcess) error {
	alive, err := remoteOwnedProcessAlive(p)
	if err != nil {
		return err
	}
	if alive {
		// macOS has no pidfd-style handle for safe crash-recovery signaling.
		return errors.New("Recovered macOS process cleanup requires review")
	}
	return nil
}
