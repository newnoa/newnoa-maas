package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"time"
)

// External CLI login, installation and config edits do not pass through Core RPC.
// Sample only local metadata; authority lookups happen on a change or an explicit read.
type remoteFileStamp struct {
	name     string
	size     int64
	modified time.Time
	mode     os.FileMode
}

func (c *core) remoteLocalFiles() []remoteFileStamp {
	var stamps []remoteFileStamp
	add := func(path string) {
		stamp := remoteFileStamp{name: path}
		if info, err := os.Stat(path); err == nil {
			stamp.size = info.Size()
			stamp.modified = info.ModTime()
			stamp.mode = info.Mode()
		}
		stamps = append(stamps, stamp)
	}
	for _, agent := range []string{"codex", "claude-cli", "pi-agent", "openclaw", "hermes-agent", "opencode"} {
		config, journal, err := c.agentPaths(agent)
		if err == nil {
			add(config)
			add(journal)
		}
		binary, _ := exec.LookPath(remoteProviderBinary(agent))
		add(binary)
	}
	pi, _ := c.piPaths()
	for _, path := range pi[1:] {
		add(path)
	}
	add(filepath.Join(c.home, ".codex", "auth.json"))
	add(filepath.Join(c.home, ".claude", ".credentials.json"))
	return stamps
}

func (c *core) watchRemoteLocal(ctx context.Context, m *remoteManager, initial []remoteFileStamp) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		files := c.remoteLocalFiles()
		if slices.Equal(files, initial) {
			continue
		}
		initial = files
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return
		}
		session, enabled := m.session, m.enabled
		m.changedLocked(nil, "", "executor.files", nil)
		m.mu.Unlock()
		if enabled {
			c.refreshRemoteSiteState(ctx, m, session)
		}
	}
}
