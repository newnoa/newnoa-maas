//go:build !noa_pibolt || !linux || !amd64

package main

import (
	"context"
	"errors"
	"io"
)

func noaAgentCLI(_ string, _ io.Writer) error {
	return errors.New("this Core build does not include Noa Agent")
}

const noaBundled = false

func (c *core) runNoa(_ context.Context, _ string, _ noaBinding, _ []noaMessage) (string, error) {
	return "", errors.New("Noa Agent unavailable")
}
