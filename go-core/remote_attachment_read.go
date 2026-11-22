package main

import (
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

func (m *remoteManager) readAttachment(sessionID, attachmentID string, offset int) (map[string]any, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readAttachmentLocked(sessionID, attachmentID, offset)
}
func (m *remoteManager) readAttachmentLocked(sessionID, attachmentID string, offset int) (map[string]any, error) {
	unavailable := func() (map[string]any, error) {
		return nil, errors.New("Image is unavailable or expired; upload it again")
	}
	if m.closed || m.sessions[sessionID] == nil {
		return unavailable()
	}
	a := m.attachments[attachmentID]
	if a == nil || a.ID != attachmentID || a.SessionID != sessionID || !a.Ready || a.Size <= 0 || a.Size > remoteMaxImageBytes || a.Received != a.Size || a.CreatedAt.IsZero() || a.CreatedAt.After(time.Now()) || !time.Now().Before(a.CreatedAt.Add(7*24*time.Hour)) {
		return unavailable()
	}
	if _, err := uuid.Parse(attachmentID); err != nil {
		return unavailable()
	}
	if offset < 0 || offset > a.Size {
		return nil, errors.New("Invalid image chunk offset")
	}
	expected := filepath.Join(filepath.Dir(m.path), "attachments", attachmentID)
	if a.Path != expected {
		return unavailable()
	}
	before, err := os.Lstat(expected)
	if err != nil || !before.Mode().IsRegular() || before.Size() != int64(a.Size) {
		return unavailable()
	}
	for parent := filepath.Dir(expected); ; parent = filepath.Dir(parent) {
		info, e := os.Lstat(parent)
		if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return unavailable()
		}
		if parent == filepath.Clean(m.home) {
			break
		}
		if parent == filepath.Dir(parent) {
			return unavailable()
		}
	}
	relative, err := filepath.Rel(m.home, expected)
	if err != nil {
		return unavailable()
	}
	// Anchored access prevents a concurrently replaced parent from escaping this host home.
	file, err := os.OpenInRoot(m.home, relative)
	if err != nil {
		return unavailable()
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || opened.Size() != int64(a.Size) || !os.SameFile(before, opened) {
		return unavailable()
	}
	count := min(16384, a.Size-offset)
	data := make([]byte, count)
	if count > 0 {
		n, e := file.ReadAt(data, int64(offset))
		if n != count || e != nil && e != io.EOF {
			return unavailable()
		}
	}
	after, err := os.Lstat(expected)
	if err != nil || !after.Mode().IsRegular() || after.Size() != int64(a.Size) || !os.SameFile(opened, after) || !after.ModTime().Equal(opened.ModTime()) {
		return unavailable()
	}
	return map[string]any{"attachmentId": a.ID, "name": a.Name, "mime": a.Mime, "size": a.Size, "content": base64.StdEncoding.EncodeToString(data), "offset": offset, "nextOffset": offset + count, "final": offset+count == a.Size}, nil
}
