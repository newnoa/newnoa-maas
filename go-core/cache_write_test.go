package main

import (
	"encoding/json"
	"testing"
)

func TestCacheWriteUsageSnapshots(t *testing.T) {
	for _, tc := range []struct {
		body  string
		input int64
		write any
	}{
		{`{"usage":{"input_tokens":2,"output_tokens":3,"cache_creation_input_tokens":4,"cache_read_input_tokens":5}}`, 11, float64(4)},
		{`{"usage":{"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_write_tokens":4,"cached_tokens":5}}}`, 12, float64(4)},
		{`{"usage":{"input_tokens":12,"output_tokens":3,"input_tokens_details":{"cache_write_tokens":4,"cached_tokens":5}}}`, 12, float64(4)},
		{`{"usage":{"prompt_tokens":12,"completion_tokens":3}}`, 12, nil},
	} {
		c := newUsageCapture(false)
		c.feed([]byte(tc.body))
		v := c.counts()
		raw, _ := json.Marshal(v)
		var fields map[string]any
		json.Unmarshal(raw, &fields)
		if v.Input == nil || *v.Input != tc.input || fields["cacheWriteTokens"] != tc.write {
			t.Fatalf("usage=%s", raw)
		}
	}
	c := newUsageCapture(true)
	c.feed([]byte("data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":0,\"cache_creation_input_tokens\":4,\"cache_read_input_tokens\":5}}}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":3,\"cache_creation_input_tokens\":4}}\n\n"))
	raw, _ := json.Marshal(c.counts())
	var v map[string]any
	json.Unmarshal(raw, &v)
	if v["inputTokens"] != float64(11) || v["cacheWriteTokens"] != float64(4) {
		t.Fatalf("stream=%s", raw)
	}
}

func TestOpenAIExplicitWriteAliasesDoNotAddToTotalInput(t *testing.T) {
	for _, usage := range []string{
		`"prompt_tokens":12,"completion_tokens":3,"cache_creation_input_tokens":4`,
		`"input_tokens":12,"output_tokens":3,"cache_creation_input_tokens":4`,
		`"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cache_creation_tokens":4}`,
		`"input_tokens":12,"output_tokens":3,"input_tokens_details":{"cache_creation_tokens":4}`,
	} {
		c := newUsageCapture(false)
		c.protocol = "/v1/responses"
		c.feed([]byte(`{"usage":{` + usage + `}}`))
		v := c.counts()
		if v.Input == nil || *v.Input != 12 || v.Write == nil || *v.Write != 4 {
			t.Fatalf("usage=%s report=%+v", usage, v)
		}
	}
}
func TestUsageSnapshotsDoNotRetainOldCacheAndStreamErrors(t *testing.T) {
	c := newUsageCapture(true)
	c.feed([]byte("data: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cache_write_tokens\":4,\"cached_tokens\":5}}}\n\ndata: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\ndata: [DONE]\n\ndata: {\"error\":{\"message\":\"failed\"}}\n\n"))
	v := c.counts()
	if v.Write != nil || v.Cached != nil || c.completed {
		t.Fatalf("stale cache or stream succeeded: %+v completed=%v", v, c.completed)
	}
}
