package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestHistoryBodyCodecLosslessAndVerified(t *testing.T) {
	value := map[string]any{"id": "bytes", "request": strings.Repeat("汉字 e\u0301\r\n🚀 \" ", 40000), "response": "data: {\"x\":\"a\\nb\"}\r\n\r\ndata: [DONE]\n", "extra": map[string]any{"nested": true}, "null": nil}
	raw, blocks, err := historyBodyEncode(value)
	if err != nil {
		t.Fatal(err)
	}
	byHash := map[string]historyBodyBlock{}
	for _, b := range blocks {
		if b.RawSize > historyBodyChunkMax {
			t.Fatal("oversized chunk", b.RawSize)
		}
		byHash[b.Hash] = b
	}
	load := func(hash string) (historyBodyBlock, error) {
		b, ok := byHash[hash]
		if !ok {
			return b, fmt.Errorf("missing block")
		}
		return b, nil
	}
	got, err := historyBodyDecode(raw, load)
	if err != nil || !reflect.DeepEqual(value, got) {
		t.Fatal("lossless roundtrip failed", err)
	}
	if !historyBodyIsPacked(raw) || !json.Valid([]byte(raw)) {
		t.Fatal("header must be marked valid JSON")
	}
	if len(raw) >= len(historyText(value["request"]))/10 {
		t.Fatal("header retained body")
	}
	for hash, b := range byHash {
		b.Data = []byte("corrupt")
		byHash[hash] = b
		break
	}
	if _, err = historyBodyDecode(raw, load); err == nil {
		t.Fatal("corrupt block accepted")
	}
}

func TestHistoryBodyCodecRejectsLengthHashAndExpansion(t *testing.T) {
	raw, blocks, err := historyBodyEncode(map[string]any{"request": strings.Repeat("original", 100)})
	if err != nil {
		t.Fatal(err)
	}
	load := func(hash string) (historyBodyBlock, error) { return blocks[0], nil }
	var header map[string]any
	if err = json.Unmarshal([]byte(raw), &header); err != nil {
		t.Fatal(err)
	}
	marker := header["_newnoaBody"].(map[string]any)
	fields := marker["fields"].(map[string]any)
	request := fields["request"].(map[string]any)
	for _, change := range []struct {
		field string
		value any
	}{{"size", float64(historyBodyMaxBytes + 1)}, {"size", float64(1)}, {"sha256", strings.Repeat("0", 64)}} {
		old := request[change.field]
		request[change.field] = change.value
		forged, _ := json.Marshal(header)
		if _, err = historyBodyDecode(string(forged), load); err == nil {
			t.Fatal("forged manifest accepted", change.field)
		}
		request[change.field] = old
	}
	legacy := `{"request":"旧格式","response":null,"n":1}`
	got, err := historyBodyDecode(legacy, nil)
	if err != nil || got["request"] != "旧格式" || got["response"] != nil {
		t.Fatal("legacy decode failed", got, err)
	}
}

func TestHistoryBodyCodecContentDefinedChunksReuseAfterInsertion(t *testing.T) {
	var body strings.Builder
	for i := 0; i < 40000; i++ {
		fmt.Fprintf(&body, "message-%08d: some unique conversation content\n", i)
	}
	_, a, err := historyBodyEncode(map[string]any{"request": body.String()})
	if err != nil {
		t.Fatal(err)
	}
	_, b, err := historyBodyEncode(map[string]any{"request": "a new message inserted first\n" + body.String()})
	if err != nil {
		t.Fatal(err)
	}
	hashes := map[string]bool{}
	for _, chunk := range a {
		hashes[chunk.Hash] = true
	}
	reused := 0
	for _, chunk := range b {
		if hashes[chunk.Hash] {
			reused++
		}
	}
	if reused < 2 {
		t.Fatal("content-defined chunk boundaries did not resynchronize", len(a), len(b), reused)
	}
}
