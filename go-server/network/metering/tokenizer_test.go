package metering

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestOfficialPythonGoldenTokenIDs(t *testing.T) {
	raw, err := os.ReadFile("testdata/tokenizer-golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Oracle  struct{ Version, Mode string }
		Vectors []struct {
			ID, Text  string
			Encodings map[string][]uint
		}
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Oracle.Version != "0.14.0" || fixture.Oracle.Mode != "encode_ordinary" || len(fixture.Vectors) != 506 {
		t.Fatal("unexpected independent oracle provenance")
	}
	for _, v := range fixture.Vectors {
		for encoding, want := range v.Encodings {
			t.Run(encoding+"/"+v.ID, func(t *testing.T) {
				got, err := encodeOrdinary(encoding, v.Text)
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(want) || len(got) > 0 && !reflect.DeepEqual(got, want) {
					t.Fatalf("token IDs differ: got %v want %v", got[:min(10, len(got))], want[:min(10, len(want))])
				}
			})
		}
	}
}

func TestModelMappingDoesNotGuessAliases(t *testing.T) {
	for _, tc := range []struct{ model, want string }{{"gpt-4", "cl100k_base"}, {"gpt-4o", "o200k_base"}, {"gpt-4o-mini", "o200k_base"}, {"text-embedding-3-small", "cl100k_base"}, {"my-gpt-4o-alias", "unknown"}, {"gpt-6-future", "unknown"}, {"qwen", "unknown"}, {"gpt-oss-120b", "unknown"}, {"gpt-4o-custom", "unknown"}} {
		if got := encodingForModel(tc.model); got != tc.want {
			t.Fatalf("model %s got %s want %s", tc.model, got, tc.want)
		}
	}
}

func TestUnknownLongIdentifierUsesReferenceBPE(t *testing.T) {
	text := strings.Repeat("qzj_vx9_", 4096)
	exact, err := encodeOrdinary("o200k_base", text)
	if err != nil {
		t.Fatal(err)
	}
	got, err := countText("unknown", text)
	if err != nil || got != int64(len(exact)) || got < 1000 {
		t.Fatalf("reference long identifier undercounted: %d err %v", got, err)
	}
}

func TestIndependentMetersUseCodecsConcurrently(t *testing.T) {
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Go(func() {
			for i := 0; i < 20; i++ {
				for _, encoding := range []string{"cl100k_base", "o200k_base"} {
					ids, err := encodeOrdinary(encoding, "hello\x7f你好👩‍💻")
					if err != nil || len(ids) == 0 {
						t.Error("concurrent codec failure", err)
					}
				}
			}
		})
	}
	wg.Wait()
}
