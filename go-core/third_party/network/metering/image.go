package metering

import (
	"encoding/json"
	"strconv"
	"strings"
)

// ImageUsage totals include their respective cached subsets. Nil means unknown.
type ImageUsage struct {
	TextInputTokens        *int64 `json:"textInputTokens"`
	CachedTextInputTokens  *int64 `json:"cachedTextInputTokens"`
	ImageInputTokens       *int64 `json:"imageInputTokens"`
	CachedImageInputTokens *int64 `json:"cachedImageInputTokens"`
	ImageOutputTokens      *int64 `json:"imageOutputTokens"`
}

func (u *ImageUsage) Complete() bool {
	return u != nil && u.TextInputTokens != nil && u.CachedTextInputTokens != nil && u.ImageInputTokens != nil && u.CachedImageInputTokens != nil && u.ImageOutputTokens != nil
}

// ParseImageUsage follows the official OpenAI SDK schema:
// https://github.com/openai/openai-python/blob/main/src/openai/types/images_response.py
// Standard image
// usage omits optional cache fields: absence means zero only with complete totals.
// An aggregate cache count cannot establish its modality and stays unknown.
func ParseImageUsage(raw []byte) *ImageUsage {
	var root map[string]json.RawMessage
	if json.Unmarshal(raw, &root) != nil {
		return nil
	}
	usage := obj(root["usage"])
	if usage == nil {
		return nil
	}
	u := &ImageUsage{}
	read := func(m map[string]json.RawMessage, k string) *int64 {
		n, e := number(m[k])
		if e != nil {
			return nil
		}
		return n
	}
	details := obj(usage["input_tokens_details"])
	u.TextInputTokens = read(details, "text_tokens")
	u.ImageInputTokens = read(details, "image_tokens")
	input, output := read(usage, "input_tokens"), read(usage, "output_tokens")
	if input == nil || u.TextInputTokens == nil || u.ImageInputTokens == nil || *input != *u.TextInputTokens+*u.ImageInputTokens {
		return &ImageUsage{}
	}
	if total, exists := usage["total_tokens"]; exists {
		n, e := number(total)
		if e != nil || n == nil || output == nil || *n != *input+*output {
			return &ImageUsage{}
		}
	}
	out := obj(usage["output_tokens_details"])
	if raw, present := usage["output_tokens_details"]; present && string(raw) != "null" && out == nil {
		return &ImageUsage{}
	}
	for _, key := range []string{"cached_tokens", "cache_read_input_tokens", "cache_creation_input_tokens", "cache_write_tokens"} {
		if _, present := usage[key]; present {
			return &ImageUsage{}
		}
	}
	if out != nil {
		for key, raw := range out {
			if key == "image_tokens" || key == "text_tokens" {
				continue
			}
			n, e := number(raw)
			if e != nil || n == nil || *n != 0 {
				return &ImageUsage{}
			}
		}
		image, text := read(out, "image_tokens"), read(out, "text_tokens")
		if image == nil || text == nil || *text != 0 || output == nil || *image != *output {
			return &ImageUsage{}
		}
		u.ImageOutputTokens = image
	} else {
		u.ImageOutputTokens = output
	}
	cache := obj(details["cached_tokens_details"])
	if raw, present := details["cached_tokens_details"]; present && string(raw) != "null" && cache == nil {
		return &ImageUsage{}
	}
	aggregate, hasAggregate := details["cached_tokens"]
	if cache != nil {
		u.CachedTextInputTokens = read(cache, "text_tokens")
		u.CachedImageInputTokens = read(cache, "image_tokens")
		if u.CachedTextInputTokens == nil || u.CachedImageInputTokens == nil {
			return u
		}
		if *u.CachedTextInputTokens > *u.TextInputTokens || *u.CachedImageInputTokens > *u.ImageInputTokens {
			return &ImageUsage{}
		}
		if hasAggregate {
			n, e := number(aggregate)
			if e != nil || n == nil || *n != *u.CachedTextInputTokens+*u.CachedImageInputTokens {
				return &ImageUsage{}
			}
		}
	} else if hasAggregate {
		n, e := number(aggregate)
		if e != nil || n == nil || *n != 0 {
			return u
		}
		u.CachedTextInputTokens = ptr(0)
		u.CachedImageInputTokens = ptr(0)
	} else if _, present := details["cached_tokens_details"]; !present && u.ImageOutputTokens != nil {
		u.CachedTextInputTokens = ptr(0)
		u.CachedImageInputTokens = ptr(0)
	}
	// Separate SVG/text authoring has no approved rate and must not settle for free.
	if v, ok := root["svg_usage"]; ok && string(v) != "null" {
		return &ImageUsage{}
	}
	return u
}

// ImageMetadata deliberately excludes image bodies, prompts, and result URLs.
func ImageMetadata(path string, request, response []byte) map[string]any {
	var req, res map[string]json.RawMessage
	_ = json.Unmarshal(request, &req)
	_ = json.Unmarshal(response, &res)
	operation := "generation"
	if path == "/v1/images/edits" {
		operation = "edit"
	}
	metadata := map[string]any{"operation": operation}
	var items []json.RawMessage
	if json.Unmarshal(res["data"], &items) == nil && items != nil {
		metadata["count"] = len(items)
	}
	for _, key := range []string{"size", "quality"} {
		var value string
		if json.Unmarshal(res[key], &value) != nil || value == "" {
			_ = json.Unmarshal(req[key], &value)
		}
		valid := false
		if key == "quality" {
			switch value {
			case "auto", "low", "medium", "high", "xhigh", "max", "standard", "hd":
				valid = true
			}
		} else if value == "auto" {
			valid = true
		} else {
			parts := strings.Split(value, "x")
			if len(parts) == 2 {
				width, e1 := strconv.Atoi(parts[0])
				height, e2 := strconv.Atoi(parts[1])
				valid = e1 == nil && e2 == nil && width > 0 && height > 0 && width <= 32768 && height <= 32768
			}
		}
		if valid {
			metadata[key] = value
		}
	}
	return metadata
}
