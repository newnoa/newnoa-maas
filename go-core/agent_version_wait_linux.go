package main

import "golang.org/x/sys/unix"

func waitVersionExit(pid int) error {
	var info unix.Siginfo
	for {
		err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if err != unix.EINTR {
			return err
		}
	}
}

func versionQueriesSupported() bool { return true }

func exitedVersionGroupHasNoChildren(int) bool { return false }
