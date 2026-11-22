package main

import "os/exec"

func prepareVersionCommand(command *exec.Cmd) {}

// Windows CLI wrappers need a dedicated process-tree lifecycle before enabling probes.
func versionQueriesSupported() bool           { return false }
func cleanupVersionCommand(command *exec.Cmd) {}

func runVersionCommand(command *exec.Cmd) error { return command.Run() }

func waitVersionCommand(command *exec.Cmd) error { return command.Wait() }
