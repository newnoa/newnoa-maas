package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Resolve npm's fixed layout without evaluating its batch shim or package metadata.
func windowsOpenClawCommand(shim string) (remoteCommand, error) {
	if filepath.IsAbs(shim) && strings.EqualFold(filepath.Ext(shim), ".exe") {
		return remoteCommand{path: shim}, nil
	}
	if !filepath.IsAbs(shim) || !strings.EqualFold(filepath.Base(shim), "openclaw.cmd") {
		return remoteCommand{}, errors.New("OpenClaw requires a native executable or npm installation")
	}
	dir := filepath.Dir(shim)
	modules := filepath.Join(dir, "node_modules")
	if strings.EqualFold(filepath.Base(dir), ".bin") && strings.EqualFold(filepath.Base(filepath.Dir(dir)), "node_modules") {
		modules = filepath.Dir(dir)
	}
	entry := filepath.Join(modules, "openclaw", "openclaw.mjs")
	if !openClawRegularFile(entry) {
		return remoteCommand{}, errors.New("OpenClaw npm entry is missing")
	}
	node := filepath.Join(dir, "node.exe")
	if !openClawRegularFile(node) {
		var err error
		node, err = exec.LookPath("node.exe")
		if err != nil || !filepath.IsAbs(node) || !openClawRegularFile(node) {
			return remoteCommand{}, errors.New("Native Node executable is missing")
		}
	}
	return remoteCommand{path: node, entry: entry}, nil
}

func openClawRegularFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
