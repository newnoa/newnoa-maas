//go:build !windows

package main

import (
	"errors"
	"os"
)

func checkRemoteJournalPrivacy(_, _ string, info os.FileInfo) error {
	if info.Mode().Perm() != 0600 {
		return errors.New("remote state file must be private (0600)")
	}
	return nil
}

func writeRemoteJournal(path, _ string, data []byte) error {
	return atomicFile(path, data)
}
