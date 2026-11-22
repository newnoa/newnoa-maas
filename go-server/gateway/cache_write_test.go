package main

import (
	"encoding/json"
	"testing"
)

func TestCacheWriteUsage(t *testing.T) {
	var e evidence
	e.usage([]byte(`{"usage":{"prompt_tokens":12,"completion_tokens":2,"prompt_tokens_details":{"cache_write_tokens":4,"cached_tokens":5}}}`))
	raw, _ := json.Marshal(e.write)
	if string(raw) != "4" {
		t.Fatalf("write=%s", raw)
	}
	if !e.messagesUsage(&messagesUsage{Input: cacheNumber(2), Output: cacheNumber(3), Creation: cacheNumber(4), Read: cacheNumber(5)}, true) || *e.input != 11 || e.write == nil || *e.write != 4 {
		t.Fatal("native cache write lost")
	}
	if !e.messagesUsage(&messagesUsage{Output: cacheNumber(6), Creation: cacheNumber(4)}, false) || *e.input != 11 || *e.write != 4 {
		t.Fatal("cumulative cache write double counted")
	}
	e.usage([]byte(`{"usage":{"prompt_tokens":12,"completion_tokens":2}}`))
	if e.write != nil {
		t.Fatal("missing write must remain unknown")
	}
}

func TestOpenAIExplicitWriteAliasesKeepTotalInput(t *testing.T) {
	for _, usage := range []string{
		`"prompt_tokens":12,"completion_tokens":3,"cache_creation_input_tokens":4`,
		`"input_tokens":12,"output_tokens":3,"cache_creation_input_tokens":4`,
		`"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_creation_tokens":4}`,
		`"input_tokens":12,"output_tokens":3,"input_tokens_details":{"cache_creation_tokens":4}`,
	} {
		var e evidence
		e.usage([]byte(`{"usage":{` + usage + `}}`))
		if e.input == nil || *e.input != 12 || e.write == nil || *e.write != 4 {
			t.Fatalf("usage=%s", usage)
		}
	}
}
