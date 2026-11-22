package metering

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/png"
	"testing"
)

func TestObserveImagePayloadWithoutLeakingPromptOrURL(t *testing.T) {
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 1024, 1024))); err != nil {
		t.Fatal(err)
	}
	req := []byte(`{"prompt":"A clean flat illustration of a green mountain at sunrise, pale ivory background, no text, square image.","size":"1024x1024"}`)
	res := []byte(`{"data":[{"b64_json":"` + base64.StdEncoding.EncodeToString(b.Bytes()) + `"}],"usage":{"input_tokens":0,"output_tokens":0}}`)
	meta := ObserveImagePayload("/v1/images/generations", req, res)
	if meta["promptTokens"] != int64(21) || meta["inputImageCount"] != 0 || meta["outputBytes"] != int64(b.Len()) {
		t.Fatalf("missing measurements: %v", meta)
	}
	items, ok := meta["outputImages"].([]map[string]any)
	if !ok || len(items) != 1 || items[0]["width"] != 1024 || items[0]["height"] != 1024 {
		t.Fatalf("dimensions %v", meta)
	}
	raw, _ := json.Marshal(meta)
	if bytes.Contains(raw, []byte("sunrise")) || bytes.Contains(raw, []byte("b64_json")) {
		t.Fatal("content leaked")
	}
}

func TestObserveImageURLsRemainUnmeasuredUntilDownloaded(t *testing.T) {
	meta := ObserveImagePayload("/v1/images/generations", []byte(`{"prompt":"test","image":"https://private/input"}`), []byte(`{"data":[{"url":"https://private/result"}]}`))
	if meta["inputImageCount"] != 1 || meta["outputBytes"] != nil || meta["outputImages"] != nil {
		t.Fatalf("fabricated URL measurement %v", meta)
	}
}
