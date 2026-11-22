package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type remoteStoredOperation struct {
	Receipt   remoteReceipt `json:"receipt"`
	Digest    string        `json:"digest"`
	CreatedAt time.Time     `json:"createdAt"`
}
type remoteJournal struct {
	History    []*remoteSession                 `json:"historySessions,omitempty"`
	Revoked    bool                             `json:"revoked,omitempty"`
	Version    int                              `json:"version"`
	Owner      string                           `json:"owner"`
	UID        string                           `json:"uid"`
	HostID     string                           `json:"hostId"`
	Secret     string                           `json:"hostSecret"`
	Revision   uint64                           `json:"revision"`
	Sessions   map[string]*remoteSession        `json:"sessions"`
	Operations map[string]remoteStoredOperation `json:"operations"`
}

func (m *remoteManager) safeJournal() error {
	for p := filepath.Dir(m.path); ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return errors.New("unsafe remote state directory")
		}
		if p == filepath.Clean(m.home) {
			break
		}
		if p == filepath.Dir(p) {
			return errors.New("remote state is outside operating system home")
		}
	}
	info, err := os.Lstat(m.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > remoteMaxJournalBytes {
		return errors.New("unsafe or oversized remote state file")
	}
	return checkRemoteJournalPrivacy(m.path, m.uid, info)
}
func (m *remoteManager) load() error {
	if err := m.safeJournal(); err != nil {
		return err
	}
	raw, err := os.ReadFile(m.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var j remoteJournal
	if json.Unmarshal(raw, &j) != nil || j.Version != 1 || j.Owner != m.owner || j.UID != m.uid || j.HostID == "" || len(j.Secret) != 64 {
		return errors.New("remote state identity or version is invalid")
	}
	if !j.Revoked {
		m.hostID = j.HostID
		m.secret = j.Secret
	}
	if len(j.History) > remoteHistoryLimit {
		return errors.New("invalid remote history length")
	}
	seen := map[string]bool{}
	for _, s := range j.History {
		if s == nil || s.ID == "" || seen[s.ID] || j.Sessions[s.ID] != nil {
			return errors.New("invalid archived remote session")
		}
		seen[s.ID] = true
		s.redactStoredOutput()
	}
	m.history = j.History
	m.revision = j.Revision
	cutoff := time.Now().Add(-7 * 24 * time.Hour)
	for id, s := range j.Sessions {
		if s == nil || s.ID != id {
			return errors.New("invalid remote session projection")
		}
		s.redactStoredOutput()
		updated, err := time.Parse(time.RFC3339Nano, s.UpdatedAt)
		if err == nil && updated.Before(cutoff) && s.OwnedProcess.PID <= 1 && s.OwnedGateway == nil && (s.State == "idle" || s.State == "stopped" || s.State == "failed") {
			m.archiveSessionLocked(s)
			continue
		}
		switch s.State {
		case "idle", "starting", "running", "awaitingApproval", "stopping", "recovering":
			s.State = "unknown"
			s.Error = "Core restarted; explicitly resume this native session"
		}
		for _, a := range s.Approvals {
			if a.State == "open" {
				a.State = "invalidated"
			}
		}
		m.sessions[id] = s
	}
	for id, stored := range j.Operations {
		if stored.CreatedAt.Before(cutoff) {
			continue
		}
		r := stored.Receipt
		r.Digest = stored.Digest
		r.created = stored.CreatedAt
		if r.Status == "accepted" {
			r.Status = "unknown"
			r.Error = "Core restarted before native acceptance could be confirmed"
		}
		m.operations[id] = &r
	}
	// Uploads are ephemeral and never restored as ready after a crash.
	dir := filepath.Join(filepath.Dir(m.path), "attachments")
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			_ = os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
	return nil
}
func (m *remoteManager) saveLocked() error {
	if err := m.safeJournal(); err != nil {
		return err
	}
	stored := map[string]remoteStoredOperation{}

	for id, r := range m.operations {
		if r.Transient {
			continue
		}
		stored[id] = remoteStoredOperation{Receipt: *r, Digest: r.Digest, CreatedAt: r.created}
	}
	raw, err := json.Marshal(remoteJournal{Version: 1, Revoked: m.revoked, Owner: m.owner, UID: m.uid, HostID: m.hostID, Secret: m.secret, Revision: m.revision, Sessions: m.sessions, History: m.history, Operations: stored})
	if err != nil {
		return err
	}
	if len(raw) > remoteMaxJournalBytes {
		return errors.New("remote state storage quota reached")
	}
	return writeRemoteJournal(m.path, m.uid, raw)
}
