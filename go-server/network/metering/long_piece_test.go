package metering

import (
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestOfficialLongPieceTokenIDs(t *testing.T) {
	raw, err := os.ReadFile("testdata/long-piece-golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Oracle  struct{ Package, Version, Mode string }
		Vectors []struct {
			ID, Text  string
			Encodings map[string][]uint
		}
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Oracle.Package != "tiktoken" || fixture.Oracle.Version != "0.14.0" || fixture.Oracle.Mode != "encode_ordinary" || len(fixture.Vectors) != 10 {
		t.Fatal("unexpected long-piece oracle")
	}
	for _, v := range fixture.Vectors {
		for encoding, want := range v.Encodings {
			t.Run(encoding+"/"+v.ID, func(t *testing.T) {
				got, err := encodeOrdinary(encoding, v.Text)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("long-piece IDs differ: error=%v got=%d want=%d", err, len(got), len(want))
				}
				n, err := countText(encoding, v.Text)
				if err != nil || n != int64(len(want)) {
					t.Fatalf("long-piece count=%d want=%d error=%v", n, len(want), err)
				}
			})
		}
	}
}

func TestConcurrentLongPieceCounts(t *testing.T) {
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for _, encoding := range []string{"cl100k_base", "o200k_base"} {
				n, err := countText(encoding, strings.Repeat("a", 70000))
				// Full IDs for this input are independently checked above.
				if err != nil || n != 8750 {
					t.Errorf("%s long count=%d error=%v", encoding, n, err)
				}
			}
		})
	}
	wg.Wait()
}

func TestLongPieceTransportStillAccepted(t *testing.T) {
	for _, size := range []int{70000, 250000} {
		for _, stream := range []bool{false, true} {
			t.Run(strconv.Itoa(size)+"/stream="+strconv.FormatBool(stream), func(t *testing.T) {
				text := strings.Repeat("a", size)
				var wire []byte
				if stream {
					raw, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"content": text}}}})
					wire = append(append([]byte("data: "), raw...), []byte("\n\ndata: [DONE]\n\n")...)
				} else {
					wire, _ = json.Marshal(map[string]any{"choices": []any{map[string]any{"index": 0, "message": map[string]string{"content": text}, "finish_reason": "stop"}}})
				}
				m, err := New("gpt-4o", "/v1/chat/completions", nil)
				if err != nil {
					t.Fatal(err)
				}
				ready, err := m.Feed(wire)
				if err != nil {
					t.Fatal(err)
				}
				tail, err := m.Finish()
				if err != nil {
					t.Fatal(err)
				}
				if string(append(ready, tail...)) != string(wire) {
					t.Fatal("valid long response changed")
				}
				s := m.Snapshot()
				if s.EstimatedOutputTokens == nil || *s.EstimatedOutputTokens != int64(size/8) || !s.ResponseStarted {
					t.Fatalf("wrong long response count: %+v", s)
				}
			})
		}
	}
}

func BenchmarkRepeatedLongPiece(b *testing.B) {
	for _, encoding := range []string{"cl100k_base", "o200k_base"} {
		for _, size := range []int{70000, 250000, 524288} {
			text := strings.Repeat("a", size)
			b.Run(encoding+"/"+strconv.Itoa(size), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for i := 0; i < b.N; i++ {
					if _, err := countText(encoding, text); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
