package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRemoteOpenClawResolvesNPMWithoutExecutingShim(t *testing.T) {
	for _, local := range []bool{false, true} {
		name := "global"
		if local {
			name = "local"
		}
		t.Run(name, func(t *testing.T) {
			prefix := t.TempDir()
			shims := prefix
			if local {
				shims = filepath.Join(prefix, "node_modules", ".bin")
			}
			entry := filepath.Join(prefix, "node_modules", "openclaw", "openclaw.mjs")
			node := filepath.Join(prefix, "node runtime", "node.exe")
			fixture := func(path string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("not an executable fixture"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			shim := filepath.Join(shims, "openclaw.cmd")
			fixture(shim)
			fixture(node)
			t.Setenv("PATH", shims+string(os.PathListSeparator)+filepath.Dir(node))
			t.Setenv("PATHEXT", ".EXE;.CMD")
			if _, err := remoteExecutorCommand("openclaw"); err == nil {
				t.Fatal("accepted shim without its package entry")
			}
			fixture(entry)
			cmd, err := remoteExecutorCommand("openclaw")
			if err != nil {
				t.Fatal(err)
			}
			if cmd.path != node || !reflect.DeepEqual(cmd.args("acp"), []string{entry, "acp"}) {
				t.Fatalf("unexpected native invocation: %+v", cmd)
			}
			if _, err := os.Stat(shim); err != nil {
				t.Fatal("resolver must not modify the npm shim", err)
			}
		})
	}
}

func TestRemoteOpenClawFingerprintTracksNodeAndEntry(t *testing.T) {
	for _, update := range []string{"node.exe", "node_modules/openclaw/openclaw.mjs"} {
		t.Run(update, func(t *testing.T) {
			prefix := t.TempDir()
			for _, name := range []string{"openclaw.cmd", "node.exe", "node_modules/openclaw/openclaw.mjs"} {
				path := filepath.Join(prefix, filepath.FromSlash(name))
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("PATH", prefix)
			t.Setenv("PATHEXT", ".EXE;.CMD")
			for _, key := range []string{"OPENCLAW_HOME", "OPENCLAW_STATE_DIR", "OPENCLAW_CONFIG_PATH", "OPENCLAW_PROFILE"} {
				t.Setenv(key, "")
			}
			c := &core{home: t.TempDir()}
			before, err := c.remoteExecutorFingerprint("openclaw")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(prefix, filepath.FromSlash(update)), []byte("replaced executable input"), 0600); err != nil {
				t.Fatal(err)
			}
			after, err := c.remoteExecutorFingerprint("openclaw")
			if err != nil || after == before {
				t.Fatal("changed Node or OpenClaw entry retained authorization identity", err)
			}
		})
	}
}
