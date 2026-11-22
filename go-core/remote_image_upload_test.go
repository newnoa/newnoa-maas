package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"image"
	"image/color"
	"image/gif"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func remoteUploadCommand(t *testing.T, m *remoteManager, kind, sid string, payload any) remoteReceipt {
	t.Helper()
	id := randomToken()
	if _, err := m.command(remoteEnvelope(id, kind, sid, payload)); err != nil {
		t.Fatal(err)
	}
	return waitRemote(t, m, id)
}

func remotePrepareImage(t *testing.T, m *remoteManager, sid, mime string, size int) string {
	t.Helper()
	r := remoteUploadCommand(t, m, "attachment.prepare", sid, map[string]any{"name": "original.image", "mime": mime, "size": size})
	if r.Status != "providerAccepted" {
		t.Fatal(r)
	}
	id, ok := r.Result["attachmentId"].(string)
	if !ok || id == "" {
		t.Fatal("prepare returned no attachment identity")
	}
	return id
}

func remotePNGBytes(t *testing.T) []byte {
	t.Helper()
	im := image.NewNRGBA(image.Rect(0, 0, 128, 128))
	var value uint32 = 7
	for i := range im.Pix {
		value ^= value << 13
		value ^= value >> 17
		value ^= value << 5
		im.Pix[i] = byte(value)
	}
	var data bytes.Buffer
	if err := png.Encode(&data, im); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

type remoteUploadCapture struct {
	*remoteTestProvider
	inputs chan remoteProviderInput
}

func (p *remoteUploadCapture) StartTurn(ctx context.Context, input remoteProviderInput) error {
	p.inputs <- input
	return p.remoteTestProvider.StartTurn(ctx, input)
}
func (p *remoteUploadCapture) Steer(ctx context.Context, input remoteProviderInput) error {
	p.inputs <- input
	return p.remoteTestProvider.Steer(ctx, input)
}

func TestRemoteUploadFullImageFlowPreservesOriginalBytesAndMessage(t *testing.T) {
	for _, format := range []string{"png", "gif"} {
		t.Run(format, func(t *testing.T) {
			m, original, cwd := remoteFixture(t)
			sid := createRemote(t, m, cwd)
			data := remotePNGBytes(t)
			if format == "gif" {
				var encoded bytes.Buffer
				im := image.NewPaletted(image.Rect(0, 0, 2, 2), color.Palette{color.Black, color.White})
				im.Pix = []byte{0, 1, 1, 0}
				if err := gif.Encode(&encoded, im, nil); err != nil {
					t.Fatal(err)
				}
				data = encoded.Bytes()
			}
			mime := "image/" + format
			id := remotePrepareImage(t, m, sid, mime, len(data))
			for offset := 0; offset < len(data); {
				next := min(offset+12288, len(data))
				r := remoteUploadCommand(t, m, "attachment.upload", sid, map[string]any{"attachmentId": id, "offset": offset, "content": base64.StdEncoding.EncodeToString(data[offset:next]), "final": next == len(data)})
				if r.Status != "providerAccepted" {
					t.Fatal(r)
				}
				offset = next
			}
			m.mu.Lock()
			path, ready, received := m.attachments[id].Path, m.attachments[id].Ready, m.attachments[id].Received
			capture := &remoteUploadCapture{remoteTestProvider: original, inputs: make(chan remoteProviderInput, 2)}
			m.providers[sid] = capture
			m.mu.Unlock()
			stored, err := os.ReadFile(path)
			if err != nil || !ready || received != len(data) || !bytes.Equal(stored, data) {
				t.Fatal("upload changed original bytes or did not become ready", err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("uploaded image was not private", err)
			}
			var restored []byte
			for offset := 0; ; {
				r := remoteUploadCommand(t, m, "attachment.read", sid, map[string]any{"attachmentId": id, "offset": offset})
				if r.Status != "providerAccepted" {
					t.Fatal(r)
				}
				chunk, err := base64.StdEncoding.DecodeString(r.Result["content"].(string))
				if err != nil || len(chunk) > 16384 {
					t.Fatal("invalid read chunk", err)
				}
				restored = append(restored, chunk...)
				offset = r.Result["nextOffset"].(int)
				if r.Result["final"].(bool) {
					break
				}
			}
			if !bytes.Equal(restored, data) {
				t.Fatal("authenticated reads did not reconstruct original image")
			}
			for _, text := range []string{"describe this image", "focus on the corner"} {
				r := remoteUploadCommand(t, m, "message.send", sid, map[string]any{"text": text, "attachments": []string{id}})
				if r.Status != "providerAccepted" {
					t.Fatal(r)
				}
				input := <-capture.inputs
				if input.Text != text || len(input.Images) != 1 || input.Images[0].URL != "data:"+mime+";base64,"+base64.StdEncoding.EncodeToString(data) {
					t.Fatal("provider input lost text, image MIME, or original bytes")
				}
			}
			journal, err := os.ReadFile(m.path)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := json.Marshal(m.snapshot())
			if err != nil {
				t.Fatal(err)
			}
			for _, raw := range [][]byte{journal, projection} {
				if bytes.Contains(raw, []byte(base64.StdEncoding.EncodeToString(data))) || bytes.Contains(raw, []byte("data:image/")) {
					t.Fatal("image content leaked into journal or snapshot")
				}
			}
			m.mu.Lock()
			messages := append([]remoteMessage(nil), m.sessions[sid].Messages...)
			m.mu.Unlock()
			if len(messages) != 2 || len(messages[0].Attachments) != 1 || messages[0].Attachments[0] != id {
				t.Fatal("message history lost attachment reference")
			}
			if r := remoteUploadCommand(t, m, "attachment.discard", sid, map[string]any{"attachmentId": id}); r.Status != "providerAccepted" {
				t.Fatal(r)
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatal("discard left image file", err)
			}
			if _, err := m.command(remoteEnvelope(randomToken(), "attachment.read", sid, map[string]any{"attachmentId": id})); err == nil {
				t.Fatal("discarded image remained readable")
			}
		})
	}
}

func TestRemoteUploadRejectsInvalidChunksAndImageContent(t *testing.T) {
	var gifData bytes.Buffer
	if err := gif.Encode(&gifData, image.NewPaletted(image.Rect(0, 0, 1, 1), color.Palette{color.Black}), nil); err != nil {
		t.Fatal(err)
	}
	hugeGIF := append([]byte(nil), gifData.Bytes()...)
	binary.LittleEndian.PutUint16(hugeGIF[6:8], 10000)
	binary.LittleEndian.PutUint16(hugeGIF[8:10], 10000)
	for _, test := range []struct {
		name, mime, content string
		size                int
		final               bool
		errorPart           string
	}{
		{"base64", "image/png", "%%%", 3, false, "invalid image chunk"},
		{"chunkTooLarge", "image/png", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 16385)), 16385, false, "invalid image chunk"},
		{"exceedsPreparedSize", "image/png", base64.StdEncoding.EncodeToString([]byte{1, 2, 3}), 2, false, "conflicts"},
		{"incompleteFinal", "image/png", base64.StdEncoding.EncodeToString([]byte{1}), 2, true, "incomplete"},
		{"mimeMismatch", "image/png", base64.StdEncoding.EncodeToString(gifData.Bytes()), gifData.Len(), true, "media type"},
		{"brokenPNG", "image/png", base64.StdEncoding.EncodeToString([]byte("\x89PNG\r\n\x1a\ncorrupt")), 15, true, "dimensions"},
		{"hugeDimensions", "image/gif", base64.StdEncoding.EncodeToString(hugeGIF), len(hugeGIF), true, "dimensions"},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, _, cwd := remoteFixture(t)
			sid := createRemote(t, m, cwd)
			id := remotePrepareImage(t, m, sid, test.mime, test.size)
			r := remoteUploadCommand(t, m, "attachment.upload", sid, map[string]any{"attachmentId": id, "offset": 0, "content": test.content, "final": test.final})
			if r.Status != "rejected" || !strings.Contains(r.Error, test.errorPart) {
				t.Fatalf("malformed image accepted or wrong failure: %+v", r)
			}
			if _, err := m.command(remoteEnvelope(randomToken(), "message.send", sid, map[string]any{"attachments": []string{id}})); err == nil {
				t.Fatal("failed image was accepted for native input")
			}
			if _, err := m.readAttachment(sid, id, 0); err == nil {
				t.Fatal("failed image was made readable")
			}
		})
	}
}

func TestRemoteUploadProtectsFileBoundaryAndRejectsOutOfOrderChunks(t *testing.T) {
	for _, scenario := range []string{"offset", "symlink", "readOnlyDirectory", "discardMissingFile"} {
		t.Run(scenario, func(t *testing.T) {
			m, _, cwd := remoteFixture(t)
			sid := createRemote(t, m, cwd)
			id := remotePrepareImage(t, m, sid, "image/png", 10)
			m.mu.Lock()
			path := m.attachments[id].Path
			m.mu.Unlock()
			payload := map[string]any{"attachmentId": id, "offset": 0, "content": base64.StdEncoding.EncodeToString([]byte{1})}
			if scenario == "discardMissingFile" {
				if r := remoteUploadCommand(t, m, "attachment.discard", sid, map[string]any{"attachmentId": id}); r.Status != "providerAccepted" {
					t.Fatal(r)
				}
				return
			}
			var protected string
			switch scenario {
			case "offset":
				payload["offset"] = 1
			case "symlink":
				protected = filepath.Join(t.TempDir(), "protected")
				if err := os.WriteFile(protected, []byte("untouched"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(protected, path); err != nil {
					t.Skip(err)
				}
			case "readOnlyDirectory":
				if err := os.MkdirAll(filepath.Dir(path), 0500); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(filepath.Dir(path), 0700) })
			}
			operation := randomToken()
			_, err := m.command(remoteEnvelope(operation, "attachment.upload", sid, payload))
			if scenario == "offset" {
				if err == nil {
					t.Fatal("out-of-order upload admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if r := waitRemote(t, m, operation); r.Status != "rejected" {
				t.Fatal("unsafe image write succeeded", r)
			}
			if protected != "" {
				raw, err := os.ReadFile(protected)
				if err != nil || string(raw) != "untouched" {
					t.Fatal("upload followed symlink", err)
				}
			}
		})
	}
}
