//go:build !windows && !linux && !darwin

package main

import "errors"

func versionQueriesSupported() bool { return false }
func waitVersionExit(pid int) error { return errors.New("version probe unavailable") }

func exitedVersionGroupHasNoChildren(int) bool { return false }
