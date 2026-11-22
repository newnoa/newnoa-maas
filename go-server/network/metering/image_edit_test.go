package metering

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func TestImageEditUploadedReferenceContract(t *testing.T) {
	png := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0}, 32)...)
	encoded := base64.StdEncoding.EncodeToString(png)
	valid := "data:image/png;base64," + encoded
	for _, tc := range []struct {
		name   string
		images any
		want   bool
	}{
		{"uploaded PNG", []any{map[string]any{"image_url": valid}}, true},
		{"forged JPEG label", []any{map[string]any{"image_url": "data:image/jpeg;base64," + encoded}}, false},
		{"undecodable bytes", []any{map[string]any{"image_url": "data:image/png;base64,not-base64"}}, false},
		{"line break in encoded data", []any{map[string]any{"image_url": valid[:25] + "\r\n" + valid[25:]}}, false},
		{"remote URL", []any{map[string]any{"image_url": "https://example.invalid/photo.png"}}, false},
		{"ambiguous reference", []any{map[string]any{"image_url": valid, "file_id": "fixture"}}, false},
		{"second reference", []any{map[string]any{"image_url": valid}, map[string]any{"image_url": valid}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := map[string]any{"images": tc.images}
			if got := ValidEditImages(body); got != tc.want {
				t.Fatalf("ValidEditImages = %t, want %t", got, tc.want)
			}
		})
	}
}
