package metering

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
)

func TestStreamCountStablePrefixAndEveryCheckpoint(t *testing.T) {
	for _, model := range []string{"gpt-4", "gpt-4o", "unknown-alias"} {
		m, _ := New(model, "/v1/responses", nil)
		text := ""
		for _, part := range []string{"abc", " ", "a", "'s", "  ", "b", "\r\n", "  b C", "ode中\u0301", "👩‍💻\x7f", " D e", "f", "01234567890", "_ A"} {
			text += part
			if err := m.appendLane("x", part, false); err != nil {
				t.Fatal(err)
			}
			if err := m.recount(); err != nil {
				t.Fatal(err)
			}
			want, err := countText(m.encoding, text)
			if err != nil {
				t.Fatal(err)
			}
			requireCount(t, m.Snapshot().EstimatedOutputTokens, want)
		}
		if m.lanes["x"].stableBytes == 0 {
			t.Fatal("stable prefix never retired from recount")
		}
		if err := m.appendLane("x", text, true); err != nil {
			t.Fatal(err)
		}
		if err := m.appendLane("x", strings.ToUpper(text), true); err != ErrProtocol {
			t.Fatal("changed snapshot must still be rejected", err)
		}
	}
}

func TestStreamCountPartitionedOfficialOracle(t *testing.T) {
	raw, err := os.ReadFile("testdata/tokenizer-golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden struct {
		Vectors []struct {
			ID, Text  string
			Encodings map[string][]uint
		}
	}
	if err = json.Unmarshal(raw, &golden); err != nil {
		t.Fatal(err)
	}
	models := map[string]string{"cl100k_base": "gpt-4", "o200k_base": "gpt-4o"}
	for _, v := range golden.Vectors {
		for enc, want := range v.Encodings {
			model, ok := models[enc]
			if !ok {
				t.Fatalf("unexpected oracle encoding %q", enc)
			}
			m, _ := New(model, "/v1/messages", nil)
			start := 0
			for end := range v.Text {
				if end-start < 61 {
					continue
				}
				if err := m.appendLane("x", v.Text[start:end], false); err != nil {
					t.Fatal(err)
				}
				if err := m.recount(); err != nil {
					t.Fatal(err)
				}
				whole, err := countText(enc, v.Text[:end])
				if err != nil {
					t.Fatal(err)
				}
				if got := *m.Snapshot().EstimatedOutputTokens; got != whole {
					t.Fatalf("%s %s prefix %d: got %d want %d", enc, v.ID, end, got, whole)
				}
				start = end
			}
			if v.Text == "" {
				continue
			}
			if err := m.appendLane("x", v.Text[start:], false); err != nil {
				t.Fatal(err)
			}
			if err := m.recount(); err != nil {
				t.Fatal(err)
			}
			if got := *m.Snapshot().EstimatedOutputTokens; got != int64(len(want)) {
				t.Fatalf("%s %s got %d want %d", enc, v.ID, got, len(want))
			}
		}
	}
}

func TestWordStreamCheckpoints(t *testing.T) {
	for _, model := range []string{"gpt-4", "gpt-4o", "unknown-alias"} {
		for _, unit := range []string{"本次测试记录文本", "普通中文，包含标点。", "a", "testing", "汉abc字"} {
			m, _ := New(model, "/v1/chat/completions", nil)
			var text strings.Builder
			for i := 0; i < 220; i++ {
				part := unit
				if i == 160 {
					part += "ABC's_1\u0301，"
				}
				wire := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"" + part + "\"}}]}\n\n")
				got, err := m.Feed(wire)
				if err != nil || string(got) != string(wire) {
					t.Fatalf("%s event %d: changed output, err=%v", model, i, err)
				}
				text.WriteString(part)
				want, err := countText(m.encoding, text.String())
				if err != nil {
					t.Fatal(err)
				}
				requireCount(t, m.Snapshot().EstimatedOutputTokens, want)
			}
			if _, err := m.Feed([]byte("data: {")); err != nil {
				t.Fatal(err)
			}
			if ready, err := m.Finish(); err != ErrTruncated || len(ready) != 0 {
				t.Fatalf("incomplete event: ready=%q err=%v", ready, err)
			}
			want, _ := countText(m.encoding, text.String())
			requireCount(t, m.Snapshot().EstimatedOutputTokens, want)
		}
	}
}

func BenchmarkWordStream(b *testing.B) {
	for _, unit := range []string{"本次测试记录文本", "普通中文，包含标点。", "continuous", "aaaaaaaa"} {
		for _, steps := range []int{256, 1024, 4096} {
			b.Run(fmt.Sprintf("%s/%d", unit, steps), func(b *testing.B) {
				b.SetBytes(int64(len(unit) * steps))
				b.ReportAllocs()
				for b.Loop() {
					m, _ := New("gpt-4o", "/v1/chat/completions", nil)
					for range steps {
						if err := m.appendLane("text", unit, false); err != nil {
							b.Fatal(err)
						}
						if err := m.recount(); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

func TestWordSuffixMatchesWholeEncoding(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	bits := []string{"a", "aaaa", "in", "g", "test", "甲", "编", "芈", "程"}
	for _, model := range []string{"gpt-4o", "gpt-4"} {
		m, _ := New(model, "/v1/responses", nil)
		var text strings.Builder
		for range 350 {
			part := bits[rng.Intn(len(bits))]
			text.WriteString(part)
			if err := m.appendLane("x", part, false); err != nil {
				t.Fatal(err)
			}
			if err := m.recount(); err != nil {
				t.Fatal(err)
			}
			want, err := countText(m.encoding, text.String())
			if err != nil {
				t.Fatal(err)
			}
			requireCount(t, m.Snapshot().EstimatedOutputTokens, want)
		}
		if m.lanes["x"].wordStart < text.Len()/2 {
			t.Fatal("word suffix cache did not advance")
		}
	}
}
