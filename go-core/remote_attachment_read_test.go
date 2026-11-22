package main

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func remoteReadFixture(t *testing.T) (*remoteManager, *remoteAttachment, []byte) {
	t.Helper()
	m, _, cwd := remoteFixture(t)
	sid := createRemote(t, m, cwd)
	id := uuid.NewString()
	data := bytes.Repeat([]byte{1, 2, 3, 4, 5}, 10000)
	a := &remoteAttachment{ID: id, SessionID: sid, Name: "actual.png", Mime: "image/png", Path: filepath.Join(filepath.Dir(m.path), "attachments", id), Size: len(data), Received: len(data), Ready: true, CreatedAt: time.Now()}
	if err := atomicFile(a.Path, data); err != nil {
		t.Fatal(err)
	}
	m.attachments[id] = a
	return m, a, data
}
func TestRemoteAttachmentReadRoundTrip(t *testing.T) {
	m, a, want := remoteReadFixture(t)
	var got []byte
	offset := 0
	for {
		r, err := m.readAttachment(a.SessionID, a.ID, offset)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := base64.StdEncoding.DecodeString(r["content"].(string))
		if err != nil || len(raw) > 16384 {
			t.Fatal("invalid bounded content")
		}
		if r["attachmentId"] != a.ID || r["name"] != a.Name || r["mime"] != a.Mime || r["size"] != a.Size || r["offset"] != offset {
			t.Fatal("attachment identity or offset changed")
		}
		got = append(got, raw...)
		offset = r["nextOffset"].(int)
		if r["final"].(bool) {
			break
		}
	}
	if !bytes.Equal(want, got) {
		t.Fatal("original image content was not preserved")
	}
}
func TestRemoteAttachmentReadRejectsInvalidOwnershipAndFiles(t *testing.T) {
	for _, test := range []string{"crossSession", "missingSession", "offsetNegative", "offsetBeyond", "unready", "expired", "future", "symlink", "parentSymlink", "wrongPath", "sizeChanged", "closed"} {
		t.Run(test, func(t *testing.T) {
			m, a, _ := remoteReadFixture(t)
			sid, id, offset := a.SessionID, a.ID, 0
			switch test {
			case "crossSession":
				sid = "another-session"
				m.sessions[sid] = &remoteSession{ID: sid}
			case "missingSession":
				original := m.sessions[sid]
				delete(m.sessions, sid)
				t.Cleanup(func() { m.sessions[sid] = original })
			case "offsetNegative":
				offset = -1
			case "offsetBeyond":
				offset = a.Size + 1
			case "unready":
				a.Ready = false
			case "expired":
				a.CreatedAt = time.Now().Add(-7*24*time.Hour - time.Second)
			case "future":
				a.CreatedAt = time.Now().Add(time.Minute)
			case "symlink":
				original := a.Path + "-other"
				os.Rename(a.Path, original)
				if err := os.Symlink(original, a.Path); err != nil {
					t.Skip(err)
				}
			case "parentSymlink":
				dir := filepath.Dir(a.Path)
				other := dir + "-elsewhere"
				os.Rename(dir, other)
				if err := os.Symlink(other, dir); err != nil {
					t.Skip(err)
				}
			case "wrongPath":
				a.Path = filepath.Join(t.TempDir(), id)
			case "sizeChanged":
				os.WriteFile(a.Path, []byte("different"), 0600)
			case "closed":
				m.mu.Lock()
				m.closed = true
				m.mu.Unlock()
			}
			if _, err := m.readAttachment(sid, id, offset); err == nil {
				t.Fatal("invalid image read accepted")
			}
		})
	}
}
