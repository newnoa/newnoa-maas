package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"strings"
)

func historyInferencePath(path string) bool {
	switch path {
	case "/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/embeddings", "/v1/rerank", "/v1/audio/transcriptions", "/v1/systemone", imageGenerationPath, imageEditPath:
		return true
	}
	return false
}

func historyModelType(value string) string {
	switch value {
	case "llm", "image", "asr", "decision", "embedding", "reranking", "reranker", "recommendation", "predictive", "world", "other":
		return value
	}
	return ""
}

// Capture a separate representation so file bytes never enter history or alter inference.
func historyPayload(raw []byte, contentType string) (map[string]any, []byte, []map[string]any, error) {
	var body map[string]any
	attachments := []map[string]any{}
	kind, params, _ := mime.ParseMediaType(contentType)
	if kind == "multipart/form-data" {
		if params["boundary"] == "" {
			return nil, nil, nil, errors.New("missing multipart boundary")
		}
		body = map[string]any{}
		reader := multipart.NewReader(bytes.NewReader(raw), params["boundary"])
		for count := 0; ; count++ {
			part, err := reader.NextRawPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, nil, nil, err
			}
			if count >= 100 {
				return nil, nil, nil, errors.New("too many multipart fields")
			}
			if part.FileName() != "" {
				size, err := io.Copy(io.Discard, part)
				if err != nil {
					return nil, nil, nil, err
				}
				attachment := map[string]any{"type": "audio", "name": historyMetadata(part.FileName()), "mime": historyMetadata(part.Header.Get("Content-Type")), "size": size, "contentSaved": false}
				attachments = append(attachments, attachment)
			} else {
				data, err := io.ReadAll(io.LimitReader(part, 65537))
				if err != nil || len(data) > 65536 {
					return nil, nil, nil, errors.New("multipart text too large")
				}
				name := part.FormName()
				if _, exists := body[name]; exists {
					return nil, nil, nil, errors.New("duplicate multipart field")
				}
				body[name] = string(data)
			}
			part.Close()
		}
		body["files"] = attachments
	} else {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&body); err != nil {
			return nil, nil, nil, err
		}
		if decoder.Decode(new(any)) != io.EOF {
			return nil, nil, nil, errors.New("invalid trailing JSON")
		}
		if body == nil {
			return nil, nil, nil, errors.New("object required")
		}
		collectHistoryAttachments(body, &attachments)
	}
	encoded, err := json.Marshal(body)
	return body, encoded, attachments, err
}

func collectHistoryAttachments(value any, attachments *[]map[string]any) {
	switch item := value.(type) {
	case map[string]any:
		typ, _ := item["type"].(string)
		kind := ""
		switch typ {
		case "input_audio", "audio", "audio_url":
			kind = "audio"
		case "image", "image_url", "input_image":
			kind = "image"
		case "video", "video_url", "input_video":
			kind = "video"
		case "document", "file", "input_file":
			kind = "document"
		}
		if kind != "" && len(*attachments) < 100 {
			item["contentSaved"] = false
			attachment := map[string]any{"type": kind, "contentSaved": false}
			for _, field := range []string{"name", "filename", "file_name"} {
				if name, ok := item[field].(string); ok {
					attachment["name"] = historyMetadata(name)
					break
				}
			}
			if mime, ok := item["mime_type"].(string); ok {
				attachment["mime"] = historyMetadata(mime)
			}
			if source, ok := item["source"].(map[string]any); ok {
				if mime, ok := source["media_type"].(string); ok {
					attachment["mime"] = historyMetadata(mime)
				}
			}
			*attachments = append(*attachments, attachment)
		}
		for key, child := range item {
			if text, ok := child.(string); ok && (key == "file_data" || key == "data" || (strings.HasPrefix(text, "data:") && (key == "url" || key == "image_url" || key == "video_url" || key == "audio_url"))) {
				// Inline file content is excluded even when nested inside a native protocol block.
				if kind != "" || key == "file_data" || strings.HasPrefix(text, "data:") {
					item[key] = "[content not saved]"
					continue
				}
			}
			if key == "input_audio" || key == "source" && kind != "" {
				if block, ok := child.(map[string]any); ok {
					if _, exists := block["data"]; exists {
						block["data"] = "[content not saved]"
					}
				}
			}
			collectHistoryAttachments(child, attachments)
		}
	case []any:
		for _, child := range item {
			collectHistoryAttachments(child, attachments)
		}
	}
}

func historyMetadata(value string) string {
	if len(value) > 200 {
		value = value[:200]
	}
	return strings.ReplaceAll(strings.ToValidUTF8(value, "?"), "\x00", "?")
}
