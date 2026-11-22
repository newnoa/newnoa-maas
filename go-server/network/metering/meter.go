package metering

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

const MaxContentBytes = 8 << 20
const MaxEventBytes = 8 << 20
const MaxLanes = 4096

// ResponsesProviderMetadata is a nonstandard upstream notification, not caller content.
const ResponsesProviderMetadata = "response.metadata"

var ErrLimit = errors.New("meter: content limit")
var ErrProtocol = errors.New("meter: invalid protocol content")
var ErrTruncated = errors.New("meter: incomplete response")
var ErrFinished = errors.New("meter: already finished")

var errProviderMetadata = errors.New("meter: omit upstream metadata")

type lane struct {
	text                 strings.Builder
	count, stableCount   int64
	stableBytes, scanned int
	wordStart            int
	wordCount            int64
	wordToken            string
	wordBlocked          bool
}

// Meter is single-owner; serialize calls. Never forward ready bytes before an
// acknowledged checkpoint of Snapshot. Errors latch and withhold the whole batch.
type Meter struct {
	protocol, encoding  string
	mode                byte
	pending             []byte
	prefixStart         int
	prefixBOM           bool
	lineStart           int
	skipLF, discardLF   bool
	lanes               map[string]*lane
	dirty               map[string]bool
	retained            int
	snapshot            Snapshot
	terminal, finished  bool
	outputUsageEvidence bool
	err                 error
	currentEvent        string
	rejection           Rejection
}

func New(model, protocol string, requestBytes []byte) (*Meter, error) {
	switch protocol {
	case "/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/audio/transcriptions":
	default:
		return nil, ErrProtocol
	}
	m := &Meter{protocol: protocol, encoding: encodingForModel(model), lanes: make(map[string]*lane), dirty: make(map[string]bool)}
	m.snapshot = Snapshot{Estimator: "tiktoken-visible-v1", Encoding: m.encoding}
	if m.encoding == "unknown" {
		m.snapshot.Estimator = "reference-o200k-v1"
	}
	if len(requestBytes) > MaxContentBytes {
		return nil, ErrLimit
	}
	if len(requestBytes) > 0 {
		if err := m.request(requestBytes); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *Meter) Snapshot() Snapshot {
	s := m.snapshot
	s.EstimatedInputTokens = copyCount(s.EstimatedInputTokens)
	s.EstimatedOutputTokens = copyCount(s.EstimatedOutputTokens)
	if s.Reported != nil {
		u := *s.Reported
		u.InputTokens = copyCount(u.InputTokens)
		u.OutputTokens = copyCount(u.OutputTokens)
		u.CacheWriteTokens = copyCount(u.CacheWriteTokens)
		u.CachedInputTokens = copyCount(u.CachedInputTokens)
		s.Reported = &u
	}
	return s
}
func copyCount(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
func ptr(n int64) *int64 { return &n }
func (m *Meter) fail(err error) ([]byte, error) {
	m.reject("other")
	if countErr := m.recount(); countErr != nil {
		err = countErr
	}
	m.err = err
	return nil, err
}

func (m *Meter) Feed(raw []byte) ([]byte, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.finished {
		m.currentEvent = "none"
		m.reject("other")
		return nil, ErrFinished
	}
	var ready []byte
	for _, b := range raw {
		if m.discardLF {
			m.discardLF = false
			if b == '\n' {
				continue
			}
		}
		if len(m.pending) >= MaxEventBytes {
			return m.fail(ErrLimit)
		}
		m.pending = append(m.pending, b)
		if m.mode == 0 {
			// Previously skipped whitespace stays in the wire payload but never needs rescanning.
			switch b {
			case ' ', '\t', '\r', '\n', '\v', '\f':
				if m.prefixStart == len(m.pending)-1 {
					m.prefixStart++
				}
				continue
			}
			prefix := m.pending[m.prefixStart:]
			const bom = "\xef\xbb\xbf"
			if !m.prefixBOM && len(prefix) <= len(bom) && bytes.HasPrefix([]byte(bom), prefix) {
				if len(prefix) == len(bom) {
					m.prefixBOM = true
					m.prefixStart = len(m.pending)
				}
				continue
			}
			if prefix[0] == '{' || prefix[0] == '[' {
				m.mode = 'j'
			} else {
				m.mode = 's'
			}
		}
		if m.mode == 'j' {
			continue
		}
		if m.skipLF {
			m.skipLF = false
			if b == '\n' {
				m.lineStart = len(m.pending)
				if m.lineStart == 1 {
					ready = append(ready, m.pending...)
					m.pending = nil
					m.lineStart = 0
				}
				continue
			}
		}
		if b != '\n' && b != '\r' {
			continue
		}
		line := m.pending[m.lineStart : len(m.pending)-1]
		m.lineStart = len(m.pending)
		m.skipLF = b == '\r'
		if len(bytes.TrimPrefix(line, []byte("\xef\xbb\xbf"))) != 0 {
			continue
		}
		switch err := m.event(m.pending); err {
		case nil:
			ready = append(ready, m.pending...)
		case errProviderMetadata:
			m.discardLF, m.skipLF = m.skipLF, false
		default:
			return m.fail(err)
		}
		m.pending = nil
		m.lineStart = 0
		m.currentEvent = "none"
	}
	if err := m.recount(); err != nil {
		return m.fail(err)
	}
	return ready, nil
}

func (m *Meter) Finish() ([]byte, error) {
	m.currentEvent = "none"
	if m.err != nil {
		return nil, m.err
	}
	if m.finished {
		return nil, nil
	}
	m.finished = true
	if m.mode == 'j' {
		if err := m.document(m.pending, true); err != nil {
			return m.fail(err)
		}
		if err := m.recount(); err != nil {
			return m.fail(err)
		}
		out := m.pending
		m.pending = nil
		return out, nil
	}
	if len(bytes.TrimSpace(bytes.TrimPrefix(m.pending, []byte("\xef\xbb\xbf")))) > 0 {
		return m.fail(ErrTruncated)
	}
	out := m.pending
	m.pending = nil
	return out, nil
}

func (m *Meter) event(raw []byte) error {
	m.currentEvent = "none"
	if !utf8.Valid(raw) {
		return ErrProtocol
	}
	var data []byte
	name := ""
	for _, line := range bytes.FieldsFunc(bytes.TrimPrefix(raw, []byte("\xef\xbb\xbf")), func(r rune) bool { return r == '\n' || r == '\r' }) {
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "":
			continue
		case "data":
			data = append(data, value...)
			data = append(data, '\n')
		case "event":
			name = string(value)
		case "id", "retry":
		default:
			return ErrProtocol
		}
	}
	if name == "error" || name == "response.failed" || name == "response.incomplete" {
		m.observeFailureUsage(data)
		return ErrProtocol
	}
	if len(data) == 0 {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
		if m.terminal {
			return nil
		}
		m.terminal = true
		m.finalUsage()
		return nil
	}
	if m.terminal {
		return ErrProtocol
	}
	return m.document(data, false)
}

func decode(raw []byte) (map[string]json.RawMessage, error) {
	if !utf8.Valid(raw) || !validJSONUnicode(raw) {
		return nil, ErrProtocol
	}
	raw = bytes.TrimPrefix(bytes.TrimSpace(raw), []byte("\xef\xbb\xbf"))
	var b map[string]json.RawMessage
	if json.Unmarshal(raw, &b) != nil || b == nil {
		return nil, ErrProtocol
	}
	return b, nil
}

// encoding/json replaces unpaired escaped surrogates; accounting must not count
// that replacement as the original model content.
func validJSONUnicode(raw []byte) bool {
	inString := false
	for i := 0; i < len(raw); i++ {
		if raw[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}
func str(raw json.RawMessage) string { var s string; _ = json.Unmarshal(raw, &s); return s }
func obj(raw json.RawMessage) map[string]json.RawMessage {
	var b map[string]json.RawMessage
	_ = json.Unmarshal(raw, &b)
	return b
}
func arr(raw json.RawMessage) []json.RawMessage {
	var a []json.RawMessage
	_ = json.Unmarshal(raw, &a)
	return a
}
func index(raw json.RawMessage) (int, error) {
	if len(raw) == 0 {
		return 0, nil
	}
	n, err := strconv.Atoi(string(raw))
	if err != nil || n < 0 || n >= MaxLanes {
		return 0, ErrProtocol
	}
	return n, nil
}

func (m *Meter) appendLane(key, text string, snapshot bool) error {
	if text == "" {
		return nil
	}
	if !utf8.ValidString(text) {
		return ErrProtocol
	}
	old, exists := m.lanes[key]
	if snapshot && exists {
		if old.text.String() != text {
			m.reject("snapshot-mismatch")
			return ErrProtocol
		}
		return nil
	}
	if !exists && len(m.lanes) >= MaxLanes {
		return ErrLimit
	}
	if m.retained+len(text) > MaxContentBytes {
		return ErrLimit
	}
	if !exists {
		old = &lane{}
		m.lanes[key] = old
	}
	old.text.WriteString(text)
	m.retained += len(text)
	m.dirty[key] = true
	m.snapshot.ResponseStarted = true
	return nil
}

func (m *Meter) finalUsage() {
	u := m.snapshot.Reported
	m.snapshot.TerminalUsage = m.terminal && m.outputUsageEvidence && u != nil && u.InputTokens != nil && u.OutputTokens != nil
}

func (m *Meter) document(raw []byte, whole bool) error {
	m.currentEvent = "none"
	b, err := decode(raw)
	if err != nil {
		return err
	}
	if value := b["error"]; len(value) > 0 && string(value) != "null" {
		m.observeFailureUsage(raw)
		return ErrProtocol
	}
	kind := str(b["type"])
	m.currentEvent = rejectionEvent(kind)
	if kind == "error" || kind == "response.failed" || kind == "response.incomplete" || str(b["status"]) == "failed" || str(b["status"]) == "incomplete" {
		m.observeFailureUsage(raw)
		return ErrProtocol
	}
	if !whole && m.protocol == "/v1/responses" && kind == ResponsesProviderMetadata {
		return errProviderMetadata
	}
	valid := false
	switch m.protocol {
	case "/v1/chat/completions":
		choices := arr(b["choices"])
		if choices != nil {
			valid = true
		}
		for i, rawChoice := range choices {
			c := obj(rawChoice)
			n, err := index(c["index"])
			if err != nil {
				return err
			}
			if whole && len(c["index"]) == 0 {
				n = i
			}
			d := obj(c["delta"])
			if whole {
				d = obj(c["message"])
			}
			key := fmt.Sprintf("choice/%d/", n)
			for _, field := range []string{"content", "reasoning_content", "reasoning"} {
				if err := m.appendLane(key+field, str(d[field]), whole); err != nil {
					return err
				}
			}
			for ti, rawTool := range arr(d["tool_calls"]) {
				tool := obj(rawTool)
				tn, err := index(tool["index"])
				if err != nil {
					return err
				}
				if whole && len(tool["index"]) == 0 {
					tn = ti
				}
				f := obj(tool["function"])
				for _, field := range []string{"name", "arguments"} {
					if err := m.appendLane(fmt.Sprintf("%stool/%d/%s", key, tn, field), str(f[field]), whole); err != nil {
						return err
					}
				}
			}
			if f := obj(d["function_call"]); f != nil {
				for _, field := range []string{"name", "arguments"} {
					if err := m.appendLane(key+"function/"+field, str(f[field]), whole); err != nil {
						return err
					}
				}
			}
		}
	case "/v1/responses":
		valid, err = m.responses(b, kind, whole)
		if err != nil {
			return err
		}
	case "/v1/messages":
		valid, err = m.messages(b, kind, whole)
		if err != nil {
			return err
		}
	case "/v1/audio/transcriptions":
		if whole {
			_, valid = b["text"]
			err = m.appendLane("transcript", str(b["text"]), true)
		} else if kind == "transcript.text.delta" {
			valid = true
			err = m.appendLane("transcript", str(b["delta"]), false)
		} else if kind == "transcript.text.done" {
			valid = true
			err = m.appendLane("transcript", str(b["text"]), true)
			m.terminal = true
		}
		if err != nil {
			return err
		}
	}
	if usage := obj(b["usage"]); usage != nil {
		if whole {
			m.terminal = true
		}
		if m.protocol == "/v1/messages" && kind == "message_delta" {
			if _, ok := usage["output_tokens"]; ok {
				m.outputUsageEvidence = true
			}
		}
		if err := m.usage(usage, m.protocol == "/v1/messages"); err != nil {
			m.reject("invalid-usage")
			return err
		}
		valid = true
	}
	if !valid {
		return ErrProtocol
	}
	if whole {
		m.terminal = true
	}
	m.finalUsage()
	return nil
}

func (m *Meter) responses(b map[string]json.RawMessage, kind string, whole bool) (bool, error) {
	response := b
	if !whole {
		response = obj(b["response"])
	}
	if whole || kind == "response.completed" || kind == "response.done" {
		if response == nil {
			return false, ErrProtocol
		}
		if status := str(response["status"]); status != "" && status != "completed" {
			return false, ErrProtocol
		}
		for oi, rawItem := range arr(response["output"]) {
			item := obj(rawItem)
			if err := m.responseItem(item, oi); err != nil {
				return false, err
			}
		}
		if u := obj(response["usage"]); u != nil {
			if err := m.usage(u, false); err != nil {
				m.reject("invalid-usage")
				return false, err
			}
		}
		m.terminal = true
		return true, nil
	}
	oi, err := index(b["output_index"])
	if err != nil {
		m.reject("invalid-index")
		return false, err
	}
	ci, err := index(b["content_index"])
	if err != nil {
		m.reject("invalid-index")
		return false, err
	}
	if strings.HasPrefix(kind, "response.reasoning_summary_text.") {
		ci, err = index(b["summary_index"])
		if err != nil {
			m.reject("invalid-index")
			return false, err
		}
	}
	key := fmt.Sprintf("response/%d/%d/", oi, ci)
	switch kind {
	case "response.output_text.delta", "response.reasoning_text.delta", "response.reasoning_summary_text.delta", "response.refusal.delta":
		return true, m.appendLane(key+strings.TrimSuffix(kind, ".delta"), str(b["delta"]), false)
	case "response.output_text.done", "response.reasoning_text.done", "response.reasoning_summary_text.done", "response.refusal.done":
		return true, m.appendLane(key+strings.TrimSuffix(kind, ".done"), str(b["text"]), true)
	case "response.function_call_arguments.delta":
		return true, m.appendLane(fmt.Sprintf("response/%d/arguments", oi), str(b["delta"]), false)
	case "response.function_call_arguments.done":
		return true, m.appendLane(fmt.Sprintf("response/%d/arguments", oi), str(b["arguments"]), true)
	case "response.output_item.added":
		item := obj(b["item"])
		if str(item["type"]) == "function_call" {
			return true, m.appendLane(fmt.Sprintf("response/%d/name", oi), str(item["name"]), true)
		}
		return true, nil
	case "response.output_item.done":
		return true, m.responseItem(obj(b["item"]), oi)
	case "response.created", "response.in_progress", "response.content_part.added", "response.content_part.done":
		return true, nil
	default:
		m.reject("unsupported-event")
		return false, ErrProtocol
	}
}
func (m *Meter) responseItem(item map[string]json.RawMessage, oi int) error {
	if str(item["type"]) == "reasoning" {
		for si, raw := range arr(item["summary"]) {
			if err := m.appendLane(fmt.Sprintf("response/%d/%d/response.reasoning_summary_text", oi, si), str(obj(raw)["text"]), true); err != nil {
				return err
			}
		}
	}
	if str(item["type"]) == "function_call" {
		for _, field := range []string{"name", "arguments"} {
			if err := m.appendLane(fmt.Sprintf("response/%d/%s", oi, field), str(item[field]), true); err != nil {
				return err
			}
		}
		return nil
	}
	for ci, raw := range arr(item["content"]) {
		part := obj(raw)
		field := "response.output_text"
		text := str(part["text"])
		if str(part["type"]) == "refusal" {
			field = "response.refusal"
			text = str(part["refusal"])
		}
		if err := m.appendLane(fmt.Sprintf("response/%d/%d/%s", oi, ci, field), text, true); err != nil {
			return err
		}
	}
	return nil
}
func (m *Meter) messages(b map[string]json.RawMessage, kind string, whole bool) (bool, error) {
	if whole {
		if arr(b["content"]) == nil {
			return false, ErrProtocol
		}
		for i, raw := range arr(b["content"]) {
			if err := m.messageBlock(obj(raw), i, true); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	n, err := index(b["index"])
	if err != nil {
		return false, err
	}
	switch kind {
	case "message_start":
		msg := obj(b["message"])
		if u := obj(msg["usage"]); u != nil {
			if err := m.usage(u, true); err != nil {
				return false, err
			}
		}
		return true, nil
	case "content_block_start":
		return true, m.messageBlock(obj(b["content_block"]), n, false)
	case "content_block_delta":
		d := obj(b["delta"])
		field := "text"
		switch str(d["type"]) {
		case "text_delta":
		case "thinking_delta":
			field = "thinking"
		case "input_json_delta":
			field = "partial_json"
		case "signature_delta":
			return true, nil
		default:
			return false, ErrProtocol
		}
		return true, m.appendLane(fmt.Sprintf("message/%d/%s", n, field), str(d[field]), false)
	case "message_delta", "content_block_stop", "ping":
		return true, nil
	case "message_stop":
		m.terminal = true
		return true, nil
	default:
		return false, ErrProtocol
	}
}
func (m *Meter) messageBlock(b map[string]json.RawMessage, n int, whole bool) error {
	for _, field := range []string{"text", "thinking", "name"} {
		if err := m.appendLane(fmt.Sprintf("message/%d/%s", n, field), str(b[field]), whole); err != nil {
			return err
		}
	}
	if input := b["input"]; whole && len(input) > 0 && string(input) != "null" {
		return m.appendLane(fmt.Sprintf("message/%d/partial_json", n), string(input), true)
	}
	return nil
}

// Recount once per changed lane per Feed batch, never once per event or Snapshot.
func (m *Meter) recount() error {
	total := int64(0)
	if m.snapshot.EstimatedOutputTokens != nil {
		total = *m.snapshot.EstimatedOutputTokens
	}
	for key := range m.dirty {
		l := m.lanes[key]
		text := l.text.String()
		// Both pinned regexes split ASCII letter | space+ASCII letter here. No BPE
		// merge crosses a regex piece, even if more text extends the right word.
		cut := l.stableBytes
		for end := len(text); end > max(cut, l.scanned-1); {
			r, width := utf8.DecodeLastRuneInString(text[:end])
			i := end - width
			if i > cut {
				if r == ' ' && end < len(text) && asciiLetter(text[i-1]) && asciiLetter(text[end]) {
					cut = i
					break
				}
				if r >= utf8.RuneSelf && strings.ContainsRune("，。！？；：、", r) {
					prev, _ := utf8.DecodeLastRuneInString(text[:i])
					if prev >= '\u4e00' && prev <= '\u9fff' {
						// Neither regex can attach this punctuation to the preceding Han.
						cut = i
						break
					}
				}
			}
			end = i
		}
		if cut > l.stableBytes {
			n, err := countText(m.encoding, text[l.stableBytes:cut])
			if err != nil {
				return err
			}
			l.stableCount += n
			l.stableBytes = cut
			l.wordToken, l.wordBlocked = "", false
			l.scanned = cut
		}
		n, err := l.countOpen(m.encoding, text)
		l.scanned = len(text)
		if err != nil {
			return err
		}
		n += l.stableCount
		total += n - l.count
		l.count = n
		delete(m.dirty, key)
	}
	if len(m.lanes) > 0 {
		m.snapshot.EstimatedOutputTokens = ptr(total)
	}
	return nil
}

func asciiLetter(b byte) bool { return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') }
