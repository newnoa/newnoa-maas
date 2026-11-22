package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime"
	"mime/multipart"
	"strconv"
	"strings"
	"unicode/utf8"
)

type audioPart struct {
	header map[string][]string
	data   []byte
	name   string
}
type transcription struct {
	parts           []audioPart
	boundary, model string
	stream          bool
}

func parseTranscription(raw []byte, contentType string) (*transcription, error) {
	kind, params, err := mime.ParseMediaType(contentType)
	if err != nil || kind != "multipart/form-data" || params["boundary"] == "" {
		return nil, fmt.Errorf("multipart/form-data required")
	}
	form := &transcription{boundary: params["boundary"]}
	reader := multipart.NewReader(bytes.NewReader(raw), form.boundary)
	seen := map[string]bool{}
	for {
		part, err := reader.NextRawPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("invalid multipart body")
		}
		name := part.FormName()
		for key, values := range part.Header {
			if len(values) != 1 || (key != "Content-Disposition" && key != "Content-Type") {
				return nil, fmt.Errorf("unsupported multipart headers")
			}
		}
		if seen[name] || len(seen) >= 7 || part.Header.Get("Content-Transfer-Encoding") != "" {
			return nil, fmt.Errorf("duplicate or unsupported multipart field")
		}
		seen[name] = true
		data, err := io.ReadAll(part)
		if err != nil {
			return nil, fmt.Errorf("invalid multipart field")
		}
		if name != "file" && (!utf8.Valid(data) || part.FileName() != "") {
			return nil, fmt.Errorf("invalid text field")
		}
		value := string(data)
		switch name {
		case "model":
			if strings.TrimSpace(value) == "" || len(data) > 256 {
				return nil, fmt.Errorf("model required")
			}
			form.model = value
		case "file":
			if part.FileName() == "" || len(data) == 0 || len(data) > 20<<20 {
				return nil, fmt.Errorf("audio file must be nonempty and at most 20 MiB")
			}
		case "stream":
			if value != "true" && value != "false" {
				return nil, fmt.Errorf("stream must be boolean")
			}
			form.stream = value == "true"
		case "response_format":
			if value != "json" {
				return nil, fmt.Errorf("only JSON transcription is supported")
			}
		case "language":
			if len(data) < 1 || len(data) > 64 {
				return nil, fmt.Errorf("invalid language")
			}
		case "prompt":
			if len(data) > 65536 {
				return nil, fmt.Errorf("prompt too large")
			}
		case "temperature":
			n, e := strconv.ParseFloat(value, 64)
			if e != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > 1 {
				return nil, fmt.Errorf("invalid temperature")
			}
		default:
			return nil, fmt.Errorf("unsupported transcription field")
		}
		// Canonical names prevent different multipart parsers selecting different model fields.
		disposition := map[string]string{"name": name}
		if part.FileName() != "" {
			disposition["filename"] = part.FileName()
		}
		header := map[string][]string{"Content-Disposition": {mime.FormatMediaType("form-data", disposition)}}
		if value := part.Header.Get("Content-Type"); value != "" {
			header["Content-Type"] = []string{value}
		}
		form.parts = append(form.parts, audioPart{header, data, name})
	}
	if !seen["model"] || !seen["file"] {
		return nil, fmt.Errorf("model and file required")
	}
	return form, nil
}

func (form *transcription) encode(model string) ([]byte, string, error) {
	var out bytes.Buffer
	writer := multipart.NewWriter(&out)
	if err := writer.SetBoundary(form.boundary); err != nil {
		return nil, "", err
	}
	for _, part := range form.parts {
		dst, err := writer.CreatePart(part.header)
		if err != nil {
			return nil, "", err
		}
		data := part.data
		if part.name == "model" {
			data = []byte(model)
		}
		if _, err = dst.Write(data); err != nil {
			return nil, "", err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	return out.Bytes(), writer.FormDataContentType(), nil
}

func validTranscription(data []byte) bool {
	var body map[string]json.RawMessage
	if json.Unmarshal(data, &body) != nil || len(body["error"]) > 0 && string(body["error"]) != "null" {
		return false
	}
	var text string
	return len(body["text"]) > 0 && string(body["text"]) != "null" && json.Unmarshal(body["text"], &text) == nil
}

func (e *evidence) transcriptionEvent(data string) {
	if data == "[DONE]" {
		if !e.done {
			e.invalid = true
		}
		return
	}
	var event struct {
		Type  string          `json:"type"`
		Delta json.RawMessage `json:"delta"`
		Error json.RawMessage `json:"error"`
	}
	if e.done || json.Unmarshal([]byte(data), &event) != nil || len(event.Error) > 0 && string(event.Error) != "null" {
		e.invalid = true
		return
	}
	switch event.Type {
	case "transcript.text.delta":
		var delta string
		if len(event.Delta) == 0 || string(event.Delta) == "null" || json.Unmarshal(event.Delta, &delta) != nil {
			e.invalid = true
		}
	case "transcript.text.done":
		if !validTranscription([]byte(data)) {
			e.invalid = true
			return
		}
		e.done = true
		e.usage([]byte(data))
	default:
		e.invalid = true
	}
}
