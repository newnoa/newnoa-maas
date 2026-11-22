package metering

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func BenchmarkCumulative128KiB(b *testing.B) {
	text := strings.Repeat("hello world 中文 code. ", 8192)[:128<<10]
	// Keep UTF-8 valid at the fixed corpus boundary.
	text = strings.ToValidUTF8(text, "")
	for _, eventBytes := range []int{256, 8192} {
		b.Run(fmt.Sprintf("events-%d", eventBytes), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(text)))
			for i := 0; i < b.N; i++ {
				m, err := New("gpt-4o", "/v1/chat/completions", nil)
				if err != nil {
					b.Fatal(err)
				}
				var wire bytes.Buffer
				for p := 0; p < len(text); {
					end := min(p+eventBytes, len(text))
					for end < len(text) && text[end]&0xc0 == 0x80 {
						end--
					}
					raw, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": text[p:end]}}}})
					wire.WriteString("data: ")
					wire.Write(raw)
					wire.WriteString("\n\n")
					if _, err = m.Feed(wire.Bytes()); err != nil {
						b.Fatal(err)
					}
					_ = m.Snapshot()
					wire.Reset()
					p = end
				}
				if _, err = m.Finish(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCachedSnapshot(b *testing.B) {
	m, _ := New("gpt-4o", "/v1/chat/completions", nil)
	_, _ = m.Feed([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Snapshot()
	}
}

func BenchmarkSmallEventsCoalescedRead128KiB(b *testing.B) {
	text := strings.ToValidUTF8(strings.Repeat("hello world 中文 code. ", 8192)[:128<<10], "")
	var wire bytes.Buffer
	for p := 0; p < len(text); {
		end := min(p+256, len(text))
		for end < len(text) && text[end]&0xc0 == 0x80 {
			end--
		}
		raw, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": text[p:end]}}}})
		wire.WriteString("data: ")
		wire.Write(raw)
		wire.WriteString("\n\n")
		p = end
	}
	raw := wire.Bytes()
	b.SetBytes(int64(len(text)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m, _ := New("gpt-4o", "/v1/chat/completions", nil)
		for remaining := raw; len(remaining) > 0; {
			n := min(8192, len(remaining))
			if _, err := m.Feed(remaining[:n]); err != nil {
				b.Fatal(err)
			}
			_ = m.Snapshot()
			remaining = remaining[n:]
		}
		if _, err := m.Finish(); err != nil {
			b.Fatal(err)
		}
	}
}
