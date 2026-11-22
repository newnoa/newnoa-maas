package main

import (
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func lockWSLRuntime(home string) (*os.File, error) {
	if !filepath.IsAbs(home) || filepath.Base(home) != "home" {
		return nil, errors.New("invalid WSL state home")
	}
	path := filepath.Join(filepath.Dir(home), ".run-lock")
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), path)
	var info unix.Stat_t
	if err = unix.Fstat(fd, &info); err == nil {
		if info.Mode != unix.S_IFREG|0600 || info.Uid != uint32(os.Geteuid()) || info.Nlink != 1 {
			err = errors.New("invalid WSL state lock")
		} else {
			err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		}
	}
	if err != nil {
		lock.Close()
		return nil, err
	}
	return lock, nil
}
