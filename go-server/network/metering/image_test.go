package metering

import "testing"

func TestImageUsageProtocol(t *testing.T) {
	for _, tc := range []struct {
		name, usage      string
		complete, absent bool
	}{
		{"standard", `{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2},"output_tokens":4,"total_tokens":16}`, true, false},
		{"zero", `{"input_tokens":0,"input_tokens_details":{"text_tokens":0,"image_tokens":0},"output_tokens":0,"total_tokens":0}`, true, false},
		{"missing", `null`, false, true},
		{"incomplete", `{"input_tokens":12,"output_tokens":4}`, false, false},
		{"contradiction", `{"input_tokens":13,"input_tokens_details":{"text_tokens":10,"image_tokens":2},"output_tokens":4}`, false, false},
		{"cache total", `{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2,"cached_tokens":3},"output_tokens":4}`, false, false},
		{"cache split", `{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2,"cached_tokens":3,"cached_tokens_details":{"text_tokens":2,"image_tokens":1}},"output_tokens":4}`, true, false},
		{"text output", `{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2},"output_tokens":4,"output_tokens_details":{"text_tokens":1,"image_tokens":3}}`, false, false},
		{"malformed output", `{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2},"output_tokens":4,"output_tokens_details":[]}`, false, false},
		{"malformed cache", `{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2,"cached_tokens_details":[]},"output_tokens":4}`, false, false},
		{"unsupported cache", `{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2},"cached_tokens":3,"output_tokens":4}`, false, false},
		{"extra output modality", `{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2},"output_tokens":4,"output_tokens_details":{"image_tokens":4,"text_tokens":0,"audio_tokens":2}}`, false, false},
		{"illegal output modality", `{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2},"output_tokens":4,"output_tokens_details":{"image_tokens":4,"text_tokens":0,"audio_tokens":-1}}`, false, false},
		{"zero extra output modality", `{"input_tokens":12,"input_tokens_details":{"text_tokens":10,"image_tokens":2},"output_tokens":4,"output_tokens_details":{"image_tokens":4,"text_tokens":0,"audio_tokens":0}}`, true, false},
		{"negative", `{"input_tokens":12,"input_tokens_details":{"text_tokens":-1,"image_tokens":13},"output_tokens":4}`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := ParseImageUsage([]byte(`{"usage":` + tc.usage + `}`))
			if (u == nil) != tc.absent {
				t.Fatalf("absent %v", u)
			}
			if u != nil && u.Complete() != tc.complete {
				t.Fatalf("complete %v: %+v", u.Complete(), u)
			}
		})
	}
}

func TestImageMetadataExcludesContent(t *testing.T) {
	meta := ImageMetadata("/v1/images/generations", []byte(`{"size":"1024x1024","quality":"high","prompt":"secret","url":"https://private/?token=secret"}`), []byte(`{"data":[{"url":"https://private/?token=secret","b64_json":"secret"}]}`))
	if len(meta) != 4 || meta["count"] != 1 || meta["size"] != "1024x1024" || meta["quality"] != "high" {
		t.Fatalf("metadata: %+v", meta)
	}
}
