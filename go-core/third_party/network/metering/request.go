package metering

import (
	"encoding/json"
)

func (m *Meter) request(raw []byte) error {
	b, err := decode(raw)
	if err != nil {
		return err
	}
	var texts []string
	complete := true
	var content func(json.RawMessage)
	content = func(raw json.RawMessage) {
		if len(raw) == 0 || string(raw) == "null" {
			return
		}
		var s string
		if json.Unmarshal(raw, &s) == nil {
			texts = append(texts, s)
			return
		}
		a := arr(raw)
		if a == nil {
			complete = false
			return
		}
		for _, v := range a {
			part := obj(v)
			switch str(part["type"]) {
			case "text", "input_text", "output_text":
				content(part["text"])
			default:
				complete = false
			}
		}
	}
	for _, f := range []string{"system", "instructions"} {
		content(b[f])
	}
	if a := arr(b["messages"]); a != nil {
		for _, raw := range a {
			msg := obj(raw)
			content(msg["content"])
			for _, rawTool := range arr(msg["tool_calls"]) {
				f := obj(obj(rawTool)["function"])
				content(f["name"])
				content(f["arguments"])
			}
		}
	}
	if input, ok := b["input"]; ok {
		if a := arr(input); a != nil {
			for _, v := range a {
				if o := obj(v); o != nil {
					content(o["content"])
				} else {
					content(v)
				}
			}
		} else {
			content(input)
		}
	}
	if tools := b["tools"]; len(tools) > 0 && string(tools) != "null" {
		a := arr(tools)
		if a == nil {
			complete = false
		}
		for _, raw := range a {
			tool := obj(raw)
			if kind := str(tool["type"]); kind != "" && kind != "function" {
				complete = false
				continue
			}
			f := obj(tool["function"])
			if f == nil {
				f = tool
			}
			for _, key := range []string{"name", "description"} {
				content(f[key])
			}
			if schema := f["parameters"]; len(schema) > 0 {
				texts = append(texts, string(schema))
			}
		}
	}
	if complete && len(texts) > 0 {
		var total int64
		for _, text := range texts {
			n, err := countText(m.encoding, text)
			if err != nil {
				return err
			}
			total += n
		}
		m.snapshot.EstimatedInputTokens = ptr(total)
	}
	return nil
}
