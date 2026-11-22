package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestRemoteJournalRejectsTamperedIdentityAndUnsafeFiles(t *testing.T) {
	for _, kind := range []string{"invalidJSON", "version", "owner", "uid", "host", "secret", "nilSession", "wrongSession", "permissions", "symlink", "directory"} {
		t.Run(kind, func(t *testing.T) {
			if runtime.GOOS == "windows" && kind == "permissions" {
				t.Skip("Windows DACL covered by TestRemoteJournalWindowsRejectsSharedPermissions")
			}
			m, _, _ := remoteFixture(t)
			j := remoteJournal{Version: 1, Owner: m.owner, UID: m.uid, HostID: m.hostID, Secret: m.secret, Sessions: map[string]*remoteSession{}}
			switch kind {
			case "version":
				j.Version = 2
			case "owner":
				j.Owner = "other"
			case "uid":
				j.UID = "other"
			case "host":
				j.HostID = ""
			case "secret":
				j.Secret = "short"
			case "nilSession":
				j.Sessions["bad"] = nil
			case "wrongSession":
				j.Sessions["bad"] = &remoteSession{ID: "different"}
			}
			raw, _ := json.Marshal(j)
			if kind == "invalidJSON" {
				raw = []byte("{")
			}
			if err := writeRemoteJournal(m.path, m.uid, raw); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "permissions":
				if err := os.Chmod(m.path, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				other := filepath.Join(t.TempDir(), "private")
				os.Rename(m.path, other)
				if err := os.Symlink(other, m.path); err != nil {
					t.Skip(err)
				}
			case "directory":
				if err := os.Remove(m.path); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(m.path, 0700); err != nil {
					t.Fatal(err)
				}
				defer os.Remove(m.path)
			}
			if err := m.load(); err == nil {
				t.Fatal("unsafe or tampered journal was loaded")
			}
		})
	}
}

func TestRemoteJournalExpiresHistoryAndInvalidatesCrashWork(t *testing.T) {
	m, _, _ := remoteFixture(t)
	old := time.Now().Add(-8 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)
	j := remoteJournal{Version: 1, Owner: m.owner, UID: m.uid, HostID: m.hostID, Secret: m.secret, Revision: 9, Sessions: map[string]*remoteSession{}, Operations: map[string]remoteStoredOperation{}}
	for _, state := range []string{"idle", "stopped", "failed", "running", "starting", "awaitingApproval", "stopping", "recovering"} {
		j.Sessions[state] = &remoteSession{ID: state, State: state, UpdatedAt: old, Approvals: []*remoteApproval{{ID: "approval", State: "open"}}}
	}
	j.Operations["expired"] = remoteStoredOperation{Receipt: remoteReceipt{Status: "providerAccepted"}, CreatedAt: time.Now().Add(-8 * 24 * time.Hour)}
	j.Operations["pending"] = remoteStoredOperation{Receipt: remoteReceipt{Status: "accepted"}, CreatedAt: time.Now(), Digest: "retained-digest"}
	raw, _ := json.Marshal(j)
	if err := writeRemoteJournal(m.path, m.uid, raw); err != nil {
		t.Fatal(err)
	}
	upload := filepath.Join(filepath.Dir(m.path), "attachments", "ephemeral")
	if err := atomicFile(upload, []byte("private upload")); err != nil {
		t.Fatal(err)
	}
	if err := m.load(); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"idle", "stopped", "failed"} {
		if m.sessions[state] != nil {
			t.Fatal("expired terminal history retained")
		}
	}
	for _, state := range []string{"running", "starting", "awaitingApproval", "stopping", "recovering"} {
		if m.sessions[state].State != "unknown" || m.sessions[state].Approvals[0].State != "invalidated" {
			t.Fatal("crash work was claimed complete")
		}
	}
	if m.operations["expired"] != nil || m.operations["pending"].Status != "unknown" || m.operations["pending"].Digest != "retained-digest" {
		t.Fatal("operation retention or uncertainty lost")
	}
	if _, err := os.Stat(upload); !os.IsNotExist(err) {
		t.Fatal("ephemeral image survived journal recovery")
	}
}
