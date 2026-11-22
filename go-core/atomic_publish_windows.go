package main

import (
	"os"
	"strings"

	"golang.org/x/sys/windows"
)

func publishAtomicFile(src, dst string) error {
	from, err := atomicWindowsPath(src)
	if err != nil {
		return err
	}
	to, err := atomicWindowsPath(dst)
	if err != nil {
		return err
	}
	// FlushFileBuffers cannot sync the parent directory; request write-through replacement instead.
	if err := windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return &os.LinkError{Op: "rename", Old: src, New: dst, Err: err}
	}
	return nil
}

func atomicWindowsPath(path string) (*uint16, error) {
	full, err := windowsFilePath(path)
	if err != nil {
		return nil, err
	}
	return windows.UTF16PtrFromString(full)
}

func windowsFilePath(path string) (string, error) {
	full, err := windows.FullPath(path)
	if err != nil {
		return "", err
	}
	if len(full) >= 248 && !strings.HasPrefix(full, `\\?\`) && !strings.HasPrefix(full, `\\.\`) {
		if tail, ok := strings.CutPrefix(full, `\\`); ok {
			full = `\\?\UNC\` + tail
		} else {
			full = `\\?\` + full
		}
	}
	return full, nil
}
