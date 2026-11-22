package metering

import (
	"encoding/json"
	"strconv"
)

const maxTokens int64 = 1_000_000_000

func number(raw json.RawMessage) (*int64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || n < 0 || n > maxTokens {
		return nil, ErrProtocol
	}
	return ptr(n), nil
}
func (m *Meter) usage(b map[string]json.RawMessage, messages bool) error {
	old := m.snapshot.Reported
	if old == nil {
		old = &Usage{}
	}
	next := *old
	for _, field := range []struct {
		keys []string
		dst  **int64
	}{
		{[]string{"input_tokens", "prompt_tokens"}, &next.InputTokens}, {[]string{"output_tokens", "completion_tokens"}, &next.OutputTokens}, {[]string{"cache_creation_input_tokens"}, &next.CacheWriteTokens}, {[]string{"cache_read_input_tokens"}, &next.CachedInputTokens},
	} {
		for _, key := range field.keys {
			if raw, ok := b[key]; ok {
				value, err := number(raw)
				if err != nil {
					return err
				}
				if value != nil {
					*field.dst = value
				}
				break
			}
		}
	}
	details := obj(b["input_tokens_details"])
	if details == nil {
		details = obj(b["prompt_tokens_details"])
	}
	for _, f := range []struct {
		key string
		dst **int64
	}{{"cached_tokens", &next.CachedInputTokens}, {"cache_write_tokens", &next.CacheWriteTokens}, {"cache_creation_tokens", &next.CacheWriteTokens}} {
		if raw, ok := details[f.key]; ok {
			n, err := number(raw)
			if err != nil {
				return err
			}
			if n != nil {
				*f.dst = n
			}
		}
	}
	if messages {
		if base, ok := b["input_tokens"]; ok {
			n, err := number(base)
			if err != nil {
				return err
			}
			if n != nil {
				total := *n
				if next.CachedInputTokens != nil {
					total += *next.CachedInputTokens
				}
				if next.CacheWriteTokens != nil {
					total += *next.CacheWriteTokens
				}
				if total > maxTokens {
					return ErrProtocol
				}
				next.InputTokens = ptr(total)
			}
		}
	}
	if next.InputTokens == nil && next.OutputTokens == nil && next.CacheWriteTokens == nil && next.CachedInputTokens == nil {
		return nil
	}
	m.snapshot.Reported = &next
	if !messages || next.OutputTokens != nil && *next.OutputTokens > 0 || m.terminal {
		if _, ok := b["output_tokens"]; ok {
			m.outputUsageEvidence = true
		}
		if _, ok := b["completion_tokens"]; ok {
			m.outputUsageEvidence = true
		}
	}
	m.snapshot.ResponseStarted = true
	return nil
}

// Error payload bytes are never released, but explicit valid usage remains evidence.
func (m *Meter) observeFailureUsage(raw []byte) {
	b, err := decode(raw)
	if err != nil {
		return
	}
	u := obj(b["usage"])
	if response := obj(b["response"]); response != nil {
		if nested := obj(response["usage"]); nested != nil {
			u = nested
		}
	}
	if u != nil {
		m.terminal = true
		if m.usage(u, m.protocol == "/v1/messages") != nil {
			return
		}
		m.finalUsage()
	}
}
