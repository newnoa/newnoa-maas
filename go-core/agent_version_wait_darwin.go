package main

import (
	"golang.org/x/sys/unix"
)

func waitVersionExit(pid int) error {
	fd, err := unix.Kqueue()
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	var event unix.Kevent_t
	unix.SetKevent(&event, pid, unix.EVFILT_PROC, unix.EV_ADD|unix.EV_ONESHOT)
	event.Fflags = unix.NOTE_EXIT
	ready := make([]unix.Kevent_t, 1)
	for {
		n, err := unix.Kevent(fd, []unix.Kevent_t{event}, ready, nil)
		if err == unix.EINTR {
			continue
		}
		if err == unix.ESRCH {
			return nil
		}
		if err != nil {
			return err
		}
		if n == 0 {
			continue
		}
		if ready[0].Flags&unix.EV_ERROR != 0 && ready[0].Data != 0 && unix.Errno(ready[0].Data) != unix.ESRCH {
			return unix.Errno(ready[0].Data)
		}
		return nil
	}
}

func versionQueriesSupported() bool { return true }

// Darwin can report EPERM when only the exited, unreaped group leader remains.
// NOTE_EXIT has already confirmed its exit and Cmd.Wait still owns its PID.
func exitedVersionGroupHasNoChildren(pid int) bool {
	members, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", pid)
	if err != nil {
		return false
	}
	for _, member := range members {
		if int(member.Proc.P_pid) != pid || int(member.Eproc.Pgid) != pid {
			return false
		}
	}
	return true
}
