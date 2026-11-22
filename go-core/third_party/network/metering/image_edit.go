package metering

import (
	"encoding/base64"
	"net/http"
	"strings"
)

const ImageEditPath = "/v1/images/edits"

// The JSON edit transport accepts uploaded bytes. Remote URLs and file IDs are not resolved.
func ValidEditReference(value string) bool {
	header, data, ok := strings.Cut(value, ",")
	if !ok || len(data) == 0 || len(data) > base64.StdEncoding.EncodedLen(4<<20) {
		return false
	}
	if header != "data:image/png;base64" && header != "data:image/jpeg;base64" && header != "data:image/webp;base64" {
		return false
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(data)
	return err == nil && len(raw) <= 4<<20 && !strings.ContainsAny(data, "\r\n") && header == "data:"+http.DetectContentType(raw)+";base64"
}

func ValidEditImages(body map[string]any) bool {
	images, ok := body["images"].([]any)
	if !ok || len(images) != 1 {
		return false
	}
	entry, ok := images[0].(map[string]any)
	value, okURL := entry["image_url"].(string)
	return ok && len(entry) == 1 && okURL && ValidEditReference(value)
}
