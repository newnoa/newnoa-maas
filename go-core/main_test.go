package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFrameLimits(t *testing.T) {
	var b bytes.Buffer
	binary.Write(&b, binary.BigEndian, uint32(maxFrame+1))
	if _, err := readFrame(&b); err == nil {
		t.Fatal("oversized frame accepted")
	}
	b.Reset()
	b.Write([]byte{0, 0, 0, 4, '{'})
	if _, err := readFrame(&b); err == nil {
		t.Fatal("truncated frame accepted")
	}
}
func TestRPCCapabilities(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	defer c.close()
	if _, err := c.handle("shell.exec", json.RawMessage(`{}`)); err == nil {
		t.Fatal("arbitrary method accepted")
	}
	if _, err := c.handle("proxy.start", json.RawMessage(`{"secret":"invalid"}`)); err == nil {
		t.Fatal("invalid credential accepted")
	}
}
func TestConfigConflictAndRestore(t *testing.T) {
	home := t.TempDir()
	c := newCore(home, "http://127.0.0.1:8091")
	p := filepath.Join(home, ".codex", "config.toml")
	os.MkdirAll(filepath.Dir(p), 0700)
	original := []byte("# user comment\nmodel = \"old\"\n")
	os.WriteFile(p, original, 0600)
	preview, err := c.preview("codex", "deployment-id")
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(p, []byte("user changed file"), 0600)
	if err = c.apply(preview.Digest, preview.Content); err == nil {
		t.Fatal("concurrent edit overwritten")
	}
	os.WriteFile(p, original, 0600)
	if err = c.apply(preview.Digest, preview.Content); err != nil {
		t.Fatal(err)
	}
	if err = c.restore(); err != nil {
		t.Fatal(err)
	}
	restored, _ := os.ReadFile(p)
	if !bytes.Equal(restored, original) {
		t.Fatalf("restore=%s", restored)
	}
}
func TestRoundTripFrame(t *testing.T) {
	var b bytes.Buffer
	if err := writeFrame(&b, []byte(`{"id":"1"}`)); err != nil {
		t.Fatal(err)
	}
	frame, err := readFrame(&b)
	if err != nil || string(frame) != `{"id":"1"}` {
		t.Fatal(string(frame), err)
	}
	if _, err = readFrame(&b); err != io.EOF {
		t.Fatal(err)
	}
}
func TestConfigDirectorySymlinksAreRejected(t *testing.T) {
	for _, directory := range []string{".codex", ".newnoa"} {
		t.Run(directory, func(t *testing.T) {
			home, outside := t.TempDir(), t.TempDir()
			c := newCore(home, "http://127.0.0.1:8091")
			if err := os.Symlink(outside, filepath.Join(home, directory)); err != nil {
				t.Skip(err)
			}
			if _, err := c.preview("codex", "deployment-id"); err == nil {
				t.Fatal("preview followed configuration directory symlink")
			}
			if directory == ".newnoa" && c.restore() == nil {
				t.Fatal("restore followed configuration directory symlink")
			}
			entries, err := os.ReadDir(outside)
			if err != nil || len(entries) != 0 {
				t.Fatal("external directory changed", err)
			}
		})
	}
}
func TestConfigExistenceChangeRequiresNewPreview(t *testing.T) {
	for _, existed := range []bool{false, true} {
		t.Run(map[bool]string{false: "created-empty", true: "removed-empty"}[existed], func(t *testing.T) {
			c := newCore(t.TempDir(), "http://127.0.0.1:8091")
			if err := os.MkdirAll(filepath.Dir(c.configPath()), 0700); err != nil {
				t.Fatal(err)
			}
			if existed {
				if err := os.WriteFile(c.configPath(), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			pr, err := c.preview("codex", "deployment-id")
			if err != nil {
				t.Fatal(err)
			}
			if existed {
				err = os.Remove(c.configPath())
			} else {
				err = os.WriteFile(c.configPath(), nil, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.apply(pr.Digest, pr.Content) == nil {
				t.Fatal("file existence change overwritten")
			}
			if _, err = os.Stat(c.journalPath()); !os.IsNotExist(err) {
				t.Fatal("rejected write created journal", err)
			}
		})
	}
}
func TestConfigParentChangedAfterPreviewIsRejected(t *testing.T) {
	home, outside := t.TempDir(), t.TempDir()
	c := newCore(home, "http://127.0.0.1:8091")
	pr, err := c.preview("codex", "deployment-id")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(outside, filepath.Join(home, ".codex")); err != nil {
		t.Skip(err)
	}
	if c.apply(pr.Digest, pr.Content) == nil {
		t.Fatal("apply followed parent changed after preview")
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatal("external directory changed", err)
	}
}
func TestLargeConfigJournalCanBeRestored(t *testing.T) {
	c := newCore(t.TempDir(), "http://127.0.0.1:8091")
	original := []byte("#" + strings.Repeat("a", 800000) + "\nmodel = \"old\"\n")
	if err := atomicFile(c.configPath(), original); err != nil {
		t.Fatal(err)
	}
	pr, err := c.preview("codex", "deployment-id")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.apply(pr.Digest, pr.Content); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(c.journalPath())
	if err != nil || info.Size() <= maxFrame {
		t.Fatal("fixture must exercise base64 journal expansion", err)
	}
	if err = c.restore(); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(c.configPath())
	if err != nil || !bytes.Equal(actual, original) {
		t.Fatal("original config not restored", err)
	}
}
