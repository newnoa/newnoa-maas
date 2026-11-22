//go:build !linux

package main

import (
	"errors"
	"os"
)

func lockWSLRuntime(string) (*os.File, error) {
	return nil, errors.New("WSL runtime requires Linux")
}
