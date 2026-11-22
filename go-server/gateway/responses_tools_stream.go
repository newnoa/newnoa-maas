package main

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/newnoa/newnoa-maas/go-server/network/metering"
)

type responsesFunctionStream struct {
	aliases map[string]responsesToolName
	custom  map[string]bool
	pending []byte
	skipLF  bool
}

// Meter has validated and checkpointed this data. Preserve untouched SSE frames verbatim.
func (b *responsesFunctionStream) feed(chunk []byte, eof bool) ([]byte, error) {
	var out []byte
	for _, c := range chunk {
		if b.skipLF {
			b.skipLF = false
			if c == '\n' {
				if len(b.pending) == 0 {
					out = append(out, c)
				} else {
					b.pending = append(b.pending, c)
				}
				continue
			}
		}
		if len(b.pending) >= metering.MaxEventBytes {
			return nil, errResponsesTools
		}
		b.pending = append(b.pending, c)
		if c != '\r' && c != '\n' {
			continue
		}
		end := len(b.pending) - 1
		blank := end == 0 || b.pending[end-1] == '\n' || b.pending[end-1] == '\r'
		b.skipLF = c == '\r'
		if blank {
			frame, err := b.frame(b.pending)
			if err != nil {
				return nil, err
			}
			out = append(out, frame...)
			b.pending = nil
		}
	}
	if eof && len(bytes.TrimSpace(b.pending)) > 0 {
		return nil, errResponsesTools
	}
	return out, nil
}

func (b *responsesFunctionStream) frame(raw []byte) ([]byte, error) {
	lines := bytes.FieldsFunc(bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf")), func(r rune) bool { return r == '\n' || r == '\r' })
	var data []byte
	for _, line := range lines {
		field, value, _ := bytes.Cut(line, []byte(":"))
		if string(field) == "data" {
			data = append(data, bytes.TrimPrefix(value, []byte(" "))...)
			data = append(data, '\n')
		}
	}
	var event map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if decoder.Decode(&event) != nil {
		return raw, nil
	}
	kind, _ := event["type"].(string)
	item := responseMap(event["item"])
	name, _ := item["name"].(string)
	itemID, _ := item["id"].(string)
	if kind == "response.output_item.added" && b.aliases[name].custom {
		if itemID == "" || len(b.custom) >= 1024 {
			return nil, errResponsesTools
		}
		if b.custom == nil {
			b.custom = make(map[string]bool)
		}
		b.custom[itemID] = true
	}
	if strings.HasPrefix(kind, "response.function_call_arguments.") {
		id, _ := event["item_id"].(string)
		if b.custom[id] {
			// Escaped JSON fragments cannot be forwarded as freeform input. The final item carries it intact.
			return nil, nil
		}
	}
	changed, err := restoreResponsesFunctions(event, b.aliases)
	if err != nil {
		return nil, err
	}
	if !changed {
		return raw, nil
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	var out []byte
	for _, line := range lines {
		field, _, _ := bytes.Cut(line, []byte(":"))
		if string(field) != "data" {
			out = append(out, line...)
			out = append(out, '\n')
		}
	}
	out = append(out, "data: "...)
	out = append(out, encoded...)
	return append(out, '\n', '\n'), nil
}
