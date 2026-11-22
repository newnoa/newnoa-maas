package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
)

type remoteCommandAuthorityKey struct{}

func remoteCheckAuthority(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if validate, ok := ctx.Value(remoteCommandAuthorityKey{}).(func(context.Context) error); ok && validate != nil {
		return validate(ctx)
	}
	return nil
}

func (c *core) remoteExecutorFingerprint(agent string) (string, error) {
	binary := remoteProviderBinary(agent)
	switch agent {
	case "codex":
		if err := c.checkCodexLocation(); err != nil {
			return "", err
		}
	case "claude-cli":
		if err := c.checkClaudeLocation(); err != nil {
			return "", err
		}
	case "pi-agent":
		if err := c.checkPiLocation(); err != nil {
			return "", err
		}
	case "hermes-agent":
		if err := c.checkHermesLocation(); err != nil {
			return "", err
		}
	case "opencode":
		if err := c.checkOpenCodeLocation(); err != nil {
			return "", err
		}
	case "openclaw":
		if err := c.checkOpenClawLocation(); err != nil {
			return "", err
		}
	default:
		return "", errors.New("No verified remote adapter for executor")
	}
	command, err := remoteExecutorCommand(binary)
	if err != nil {
		return "", errors.New("Executor is no longer installed")
	}
	h := sha256.New()
	for _, path := range command.args(command.path) {
		executable, err := filepath.EvalSymlinks(path)
		if err != nil {
			return "", err
		}
		info, err := os.Stat(executable)
		if err != nil {
			return "", err
		}
		if !info.Mode().IsRegular() {
			return "", errors.New("Executor is not a regular file")
		}
		_, _ = h.Write([]byte(executable + "\n" + versionIdentity(info)))
	}
	var paths []string
	if agent == "pi-agent" {
		paths, _ = c.piPaths()
	} else {
		config, _, err := c.agentPaths(agent)
		if err != nil {
			return "", err
		}
		paths = []string{config}
		if agent == "codex" {
			paths = append(paths, filepath.Join(c.home, ".codex", "auth.json"))
		} else if agent == "claude-cli" {
			paths = append(paths, filepath.Join(c.home, ".claude", ".credentials.json"))
		}
	}
	for _, p := range paths {
		if err = c.safeConfigFile(p); err != nil {
			return "", err
		}
		data, e := os.ReadFile(p)
		if e != nil && !os.IsNotExist(e) {
			return "", e
		}
		_, _ = h.Write([]byte(p))
		_, _ = h.Write(data)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
