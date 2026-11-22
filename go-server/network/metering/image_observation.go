package metering

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	_ "image/jpeg"
	_ "image/png"
)

// ObserveImagePayload records transfer evidence without retaining prompts, URLs or image bodies.
func ObserveImagePayload(path string, request, response []byte) map[string]any {
	meta := ImageMetadata(path, request, response)
	var req, res map[string]json.RawMessage
	_ = json.Unmarshal(request, &req)
	_ = json.Unmarshal(response, &res)
	var prompt string
	if json.Unmarshal(req["prompt"], &prompt) == nil && prompt != "" {
		if n, err := countText("o200k_base", prompt); err == nil {
			meta["promptTokens"] = n
			meta["promptEncoding"] = "o200k_base"
		}
	}
	inputs := 0
	for _, key := range []string{"image", "images", "input_image", "input_images"} {
		raw := req[key]
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var list []json.RawMessage
		if json.Unmarshal(raw, &list) == nil {
			inputs += len(list)
		} else {
			inputs++
		}
	}
	meta["inputImageCount"] = inputs
	if inputs == 0 && path == "/v1/images/edits" {
		delete(meta, "inputImageCount")
	}
	var items []map[string]json.RawMessage
	_ = json.Unmarshal(res["data"], &items)
	images := []map[string]any{}
	var total int64
	for _, item := range items {
		var encoded string
		if json.Unmarshal(item["b64_json"], &encoded) != nil {
			continue
		}
		raw, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil || len(raw) == 0 || len(raw) > 24<<20 {
			continue
		}
		if measured := ImageTransfer(raw); measured != nil {
			images = append(images, measured)
			total += int64(len(raw))
		}
	}
	if len(images) > 0 && len(images) == len(items) {
		meta["outputImages"] = images
		meta["outputBytes"] = total
	}
	usage := obj(res["usage"])
	if usage == nil {
		meta["usageStatus"] = "absent"
	} else {
		input, ie := number(usage["input_tokens"])
		output, oe := number(usage["output_tokens"])
		if ie == nil && oe == nil && input != nil && output != nil && *input == 0 && *output == 0 {
			meta["usageStatus"] = "zero"
		} else {
			meta["usageStatus"] = "reported"
		}
	}
	return meta
}

// ImageTransfer observes decoded file dimensions; compressed byte counts are never Token counts.
func ImageTransfer(raw []byte) map[string]any {
	config, _, err := image.DecodeConfig(bytes.NewReader(raw))
	if err != nil || config.Width <= 0 || config.Height <= 0 || config.Width > 32768 || config.Height > 32768 || int64(config.Width)*int64(config.Height) > 100000000 {
		return nil
	}
	return map[string]any{"width": config.Width, "height": config.Height, "bytes": int64(len(raw))}
}
