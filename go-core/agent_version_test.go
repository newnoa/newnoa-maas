//go:build !windows

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func versionFixture(t *testing.T, name, script string) string {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, name)
	if err := os.WriteFile(binary, []byte("#!/bin/sh\n"+script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	return binary
}
func TestAgentVersionUsesIsolatedFixedCommand(t *testing.T) {
	binary := versionFixture(t, "codex", `test "$#" = 1 && test "$1" = --version || exit 5
 test "$HOME" != "" && test "$HOME" != "$REAL_HOME" || exit 6
 test -z "$OPENAI_API_KEY" || exit 7
 test -z "$CODEX_HOME" || exit 8
 printf 'codex-cli 1.2.3\n'
 `)
	t.Setenv("REAL_HOME", "private-home")
	t.Setenv("OPENAI_API_KEY", "private-token")
	t.Setenv("CODEX_HOME", "private-config")
	c := newCore(t.TempDir(), "")
	result := c.agentVersion("codex")
	if result.State != "detected" || result.ProductVersion != "codex-cli 1.2.3" || result.Executable != binary {
		t.Fatalf("unexpected version metadata: %+v", result)
	}
}
func TestAgentVersionRejectsAmbiguousFailedAndUnboundedOutput(t *testing.T) {
	for _, script := range []string{"printf 'login required'", "printf 'codex-cli 1.2.3\\nextra output'", "printf 'codex-cli 1.2.3'; exit 1", `i=0;while [ "$i" -lt 2000 ];do printf x;i=$((i+1));done`} {
		t.Run(script[:8], func(t *testing.T) {
			versionFixture(t, "codex", script)
			result := newCore(t.TempDir(), "").agentVersion("codex")
			if result.State == "detected" || result.ProductVersion != "" {
				t.Fatal("unreliable output was accepted")
			}
		})
	}
}
func TestAgentVersionDeadlineAndUnsupportedProducts(t *testing.T) {
	versionFixture(t, "codex", "while :;do :;done")
	start := time.Now()
	result := newCore(t.TempDir(), "").agentVersion("codex")
	if result.State == "detected" || time.Since(start) > 4*time.Second {
		t.Fatal("version command was not bounded")
	}
	for _, agent := range []string{"hermes-agent", "claude-desktop", "../../codex", ""} {
		if result := newCore(t.TempDir(), "").agentVersion(agent); result.State != "unsupported" {
			t.Fatalf("unexpected executable attempt: %s", agent)
		}
	}
}
func TestAgentVersionMissingAndKnownFormats(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if result := newCore(t.TempDir(), "").agentVersion("codex"); result.State != "unavailable" {
		t.Fatal(result)
	}
	for agent, output := range map[string]string{"codex": "codex-cli 0.100.0-alpha.1", "claude-cli": "2.1.163 (Claude Code)", "opencode": "1.19.0", "openclaw": "OpenClaw 2026.10.3 (59cb447)", "pi-agent": "1.2.0"} {
		if !validAgentVersion(agent, output) {
			t.Fatalf("known output rejected for %s", agent)
		}
		if validAgentVersion(agent, output+"\nwarning") {
			t.Fatal("multiple lines accepted")
		}
	}
}
