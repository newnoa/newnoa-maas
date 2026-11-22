package main

import (
	"bytes"
	"encoding/json"
)

// Buffer one event so an error split across network reads never reaches the caller.
type hostedSSEFilter struct {
	protocol  string
	pending   []byte
	lineStart int
	skipLF    bool
	failed    bool
	uncertain bool
}

const hostedSSEEventLimit = 256 * 1024

func (f *hostedSSEFilter) failure() []byte {
	f.failed = true
	f.pending = nil
	if f.protocol == "/v1/responses" || f.protocol == "/v1/audio/transcriptions" {
		return []byte("event: error\ndata: {\"type\":\"error\",\"code\":\"upstream_error\",\"message\":\"Hosted upstream stream failed\"}\n\n")
	}
	return []byte("event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Hosted upstream stream failed\"}}\n\n")
}

func (f *hostedSSEFilter) feed(raw []byte) []byte {
	var output []byte
	for len(raw) > 0 && !f.failed {
		if f.skipLF {
			f.skipLF = false
			if raw[0] == '\n' {
				if len(f.pending) == 0 {
					output = append(output, '\n')
				} else {
					if len(f.pending) == usageStreamEventLimit(f.protocol) {
						return append(output, f.failure()...)
					}
					f.pending = append(f.pending, '\n')
					f.lineStart = len(f.pending)
				}
				raw = raw[1:]
				continue
			}
		}
		end := bytes.IndexAny(raw, "\r\n")
		n := len(raw)
		if end >= 0 {
			n = end + 1
		}
		if len(f.pending)+n > usageStreamEventLimit(f.protocol) {
			return append(output, f.failure()...)
		}
		f.pending = append(f.pending, raw[:n]...)
		f.skipLF = end >= 0 && raw[end] == '\r'
		raw = raw[n:]
		if end < 0 {
			break
		}
		line := f.pending[f.lineStart : len(f.pending)-1]
		f.lineStart = len(f.pending)
		if len(line) != 0 {
			continue
		}
		if hostedSSEProtocolError(f.pending, f.protocol) {
			return append(output, f.failure()...)
		}
		output = append(output, f.pending...)
		f.pending = nil
		f.lineStart = 0
	}
	return output
}

func (f *hostedSSEFilter) finish() []byte {
	if f.failed {
		return nil
	}
	if hostedSSEProtocolError(f.pending, f.protocol) {
		f.uncertain = true
		return f.failure()
	}
	output := f.pending
	f.pending = nil
	return output
}

func hostedSSEProtocolError(event []byte, protocol string) bool {
	event = bytes.TrimPrefix(event, []byte("\xef\xbb\xbf"))
	var payload []byte
	var name string
	for _, line := range bytes.FieldsFunc(event, func(r rune) bool { return r == '\r' || r == '\n' }) {
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "event":
			name = string(value)
		case "data":
			payload = append(payload, value...)
			payload = append(payload, '\n')
		case "", "id", "retry":
		default:
			return true
		}
	}
	if name == "error" || name == "response.failed" || name == "response.incomplete" {
		return true
	}
	if len(payload) == 0 || bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) {
		return false
	}
	return hostedJSONProtocolError(payload, protocol)
}

func hostedJSONProtocolError(payload []byte, protocol string) bool {
	payload = bytes.TrimPrefix(bytes.TrimSpace(payload), []byte("\xef\xbb\xbf"))
	var value *struct {
		Type     string          `json:"type"`
		Status   string          `json:"status"`
		Error    json.RawMessage `json:"error"`
		Response struct {
			Error  json.RawMessage `json:"error"`
			Status string          `json:"status"`
		} `json:"response"`
	}
	if json.Unmarshal(payload, &value) != nil || value == nil {
		return true
	}
	return value.Type == "error" || value.Type == "response.failed" || value.Type == "response.incomplete" ||
		protocol == "/v1/responses" && (value.Status == "failed" || value.Status == "incomplete") ||
		value.Type == "response.completed" && value.Response.Status != "completed" ||
		len(value.Error) > 0 && string(value.Error) != "null" ||
		len(value.Response.Error) > 0 && string(value.Response.Error) != "null"
}
