package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type agentVersionResult struct {
	State          string `json:"state"`
	ProductVersion string `json:"productVersion,omitempty"`
	Executable     string `json:"executable,omitempty"`
	Identity       string `json:"identity,omitempty"`
}

var agentVersionCommands = map[string]string{"codex": "codex", "claude-cli": "claude", "opencode": "opencode", "openclaw": "openclaw", "pi-agent": "pi"}
var agentVersionPatterns = map[string]*regexp.Regexp{
	"codex":      regexp.MustCompile(`^codex-cli [0-9][a-zA-Z0-9.+_-]*$`),
	"claude-cli": regexp.MustCompile(`^[0-9][a-zA-Z0-9.+_-]* \(Claude Code\)$`),
	"opencode":   regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+[a-zA-Z0-9.+_-]*$`),
	"openclaw":   regexp.MustCompile(`^OpenClaw [0-9][a-zA-Z0-9.+_-]*( \([a-f0-9]+\))?$`),
	"pi-agent":   regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+[a-zA-Z0-9.+_-]*$`),
}

func validAgentVersion(agent, output string) bool {
	pattern := agentVersionPatterns[agent]
	return len(output) <= 120 && pattern != nil && pattern.MatchString(output)
}

type versionOutput struct{ bytes.Buffer }

func (b *versionOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 1024 {
		return 0, io.ErrShortWrite
	}
	return b.Buffer.Write(p)
}
func versionIdentity(info os.FileInfo) string {
	return fmt.Sprintf("%d:%d:%s", info.Size(), info.ModTime().UnixNano(), info.Mode())
}
func (c *core) agentVersion(agent string) agentVersionResult {
	if !versionQueriesSupported() {
		return agentVersionResult{State: "unsupported"}
	}
	name, ok := agentVersionCommands[agent]
	if !ok {
		return agentVersionResult{State: "unsupported"}
	}
	unavailable := agentVersionResult{State: "unavailable"}
	binary, err := exec.LookPath(name)
	if err != nil {
		return unavailable
	}
	binary, err = filepath.Abs(binary)
	if err != nil {
		return unavailable
	}
	binary, err = filepath.EvalSymlinks(binary)
	if err != nil {
		return unavailable
	}
	before, err := os.Stat(binary)
	if err != nil || !before.Mode().IsRegular() {
		return unavailable
	}
	home, err := os.MkdirTemp("", "newnoa-agent-version-")
	if err != nil {
		return unavailable
	}
	defer os.RemoveAll(home)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "--version")
	command.Dir = home
	// No inherited credentials, agent config overrides, hooks, or preload variables.
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "USERPROFILE=" + home, "XDG_CONFIG_HOME=" + filepath.Join(home, ".config"), "TMPDIR=" + home, "TMP=" + home, "TEMP=" + home, "DISABLE_TELEMETRY=1", "DISABLE_ERROR_REPORTING=1", "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1", "HERMES_DISABLE_LAZY_INSTALLS=1", "NO_COLOR=1"}
	if root := os.Getenv("SystemRoot"); root != "" {
		command.Env = append(command.Env, "SystemRoot="+root)
	}
	command.WaitDelay = 100 * time.Millisecond
	output := new(versionOutput)
	command.Stdout = output
	command.Stderr = io.Discard
	if err = runVersionCommand(command); err != nil {
		return unavailable
	}
	after, err := os.Stat(binary)
	if err != nil || !os.SameFile(before, after) || versionIdentity(before) != versionIdentity(after) {
		return unavailable
	}
	value := strings.TrimSpace(output.String())
	if !validAgentVersion(agent, value) {
		return unavailable
	}
	return agentVersionResult{State: "detected", ProductVersion: value, Executable: binary, Identity: versionIdentity(after)}
}

func versionCommandOutput(command *exec.Cmd) ([]byte, error) {
	var output bytes.Buffer
	command.Stdout = &output
	err := runVersionCommand(command)
	return output.Bytes(), err
}
