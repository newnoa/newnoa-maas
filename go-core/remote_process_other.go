//go:build !linux && !darwin && !windows

package main

import "errors"

func remoteProcessIdentity(pid int) (string, error) {
	return "", errors.New("Crash recovery process identity is not verified on this platform")
}
func remoteOwnedProcessAlive(p remoteOwnedProcess) (bool, error) {
	return false, errors.New("Previous native process ownership must be verified before recovery on this platform")
}
func stopRemoteOwnedProcess(p remoteOwnedProcess) error {
	return errors.New("Recovered process cleanup is not verified on this platform")
}
