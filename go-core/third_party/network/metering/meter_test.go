package metering

import (
	"encoding/json"
	"errors"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func requireCount(t *testing.T, n *int64, want int64) {
	t.Helper()
	if n == nil || *n != want {
		t.Fatalf("count %v want %d", n, want)
	}
}
func newTest(t *testing.T, model, protocol string) *Meter {
	t.Helper()
	m, err := New(model, protocol, []byte(`{"messages":[{"role":"user","content":"hello"}],"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestChatBatchCheckpointBarrierAndLanes(t *testing.T) {
	m := newTest(t, "gpt-4", "/v1/chat/completions")
	s := m.Snapshot()
	requireCount(t, s.EstimatedInputTokens, 1)
	if s.ResponseStarted || s.EstimatedOutputTokens != nil {
		t.Fatal("input estimate claimed execution")
	}
	first := `data: {"choices":[{"index":0,"delta":{"content":"hel"}}]}` + "\n\n"
	ready, err := m.Feed([]byte(first[:len(first)-1]))
	if err != nil || len(ready) != 0 || m.Snapshot().ResponseStarted {
		t.Fatal("incomplete event released or counted")
	}
	second := `data: {"choices":[{"index":0,"delta":{"content":"lo"}},{"index":1,"delta":{"content":"hello"}}]}` + "\n\n"
	ready, err = m.Feed([]byte(first[len(first)-1:] + second))
	if err != nil {
		t.Fatal(err)
	}
	if string(ready) != first+second {
		t.Fatal("complete batch changed")
	}
	s = m.Snapshot()
	requireCount(t, s.EstimatedOutputTokens, 2)
	if !s.ResponseStarted {
		t.Fatal("observed content missing")
	}
}

func TestFragmentationInvariantAcrossProtocols(t *testing.T) {
	cases := []struct {
		protocol, body string
		output         int64
	}{
		{"/v1/chat/completions", ": keepalive\n\n" + `data: {"choices":[{"index":0,"delta":{"content":"hello"}}]}` + "\n\n" + `data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"tool","function":{"name":"hello","arguments":"hel"}}]}}]}` + "\n\n" + `data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"lo"}}]}}]}` + "\n\ndata: [DONE]\n\n", 3},
		{"/v1/responses", `data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hel"}` + "\n\n" + `data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"lo"}` + "\n\n" + `data: {"type":"response.completed","response":{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":12,"output_tokens":3}}}` + "\n\n", 1},
		{"/v1/messages", `event: content_block_start` + "\n" + `data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":"hel"}}` + "\n\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"lo"}}` + "\n\n" + `data: {"type":"message_delta","usage":{"output_tokens":3}}` + "\n\n" + `data: {"type":"message_stop"}` + "\n\n", 1},
	}
	for _, tc := range cases {
		for _, sep := range []string{"\n", "\r\n", "\r"} {
			body := "\xef\xbb\xbf" + strings.ReplaceAll(tc.body, "\n", sep)
			for _, size := range []int{1, 2, 7, 32768} {
				t.Run(tc.protocol+sep+string(rune(size)), func(t *testing.T) {
					m := newTest(t, "gpt-4", tc.protocol)
					var delivered []byte
					for b := []byte(body); len(b) > 0; {
						n := min(size, len(b))
						ready, err := m.Feed(b[:n])
						if err != nil {
							t.Fatal(err)
						}
						delivered = append(delivered, ready...)
						b = b[n:]
					}
					ready, err := m.Finish()
					if err != nil {
						t.Fatal(err)
					}
					delivered = append(delivered, ready...)
					if string(delivered) != body {
						t.Fatal("wire bytes changed")
					}
					s := m.Snapshot()
					requireCount(t, s.EstimatedOutputTokens, tc.output)
				})
			}
		}
	}
}

func TestJSONFallbackAndReportedUsageAreSeparate(t *testing.T) {
	for _, protocol := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages"} {
		t.Run(protocol, func(t *testing.T) {
			m := newTest(t, "gpt-4", protocol)
			body := `{"choices":[{"message":{"content":"hello"}}],"usage":{"prompt_tokens":12,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":4,"cache_write_tokens":2}}}`
			if protocol == "/v1/responses" {
				body = `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":12,"output_tokens":3,"input_tokens_details":{"cached_tokens":4,"cache_write_tokens":2}}}`
			}
			if protocol == "/v1/messages" {
				body = `{"type":"message","content":[{"type":"text","text":"hello"}],"usage":{"input_tokens":6,"output_tokens":3,"cache_read_input_tokens":4,"cache_creation_input_tokens":2}}`
			}
			ready, err := m.Feed([]byte(body))
			if err != nil || len(ready) != 0 {
				t.Fatal("JSON released before end")
			}
			ready, err = m.Finish()
			if err != nil || string(ready) != body {
				t.Fatal("valid JSON not released", err)
			}
			s := m.Snapshot()
			requireCount(t, s.EstimatedOutputTokens, 1)
			if s.Reported == nil {
				t.Fatal("reported usage absent")
			}
			requireCount(t, s.Reported.InputTokens, 12)
			requireCount(t, s.Reported.OutputTokens, 3)
			requireCount(t, s.Reported.CachedInputTokens, 4)
			requireCount(t, s.Reported.CacheWriteTokens, 2)
			if !s.TerminalUsage {
				t.Fatal("final reported usage not marked")
			}
		})
	}
}

func TestUnknownModelAndRandomPartitions(t *testing.T) {
	body := `data: {"choices":[{"index":0,"delta":{"content":"你好👩‍💻 hello"}}]}` + "\n\ndata: [DONE]\n\n"
	var want Snapshot
	for seed := int64(0); seed < 30; seed++ {
		m := newTest(t, "my-gpt-4o-alias", "/v1/chat/completions")
		rng := rand.New(rand.NewSource(seed))
		for b := []byte(body); len(b) > 0; {
			n := min(1+rng.Intn(19), len(b))
			if _, err := m.Feed(b[:n]); err != nil {
				t.Fatal(err)
			}
			b = b[n:]
		}
		if _, err := m.Finish(); err != nil {
			t.Fatal(err)
		}
		s := m.Snapshot()
		if s.Encoding != "unknown" || s.Estimator != "reference-o200k-v1" {
			t.Fatal("alias marked exact", s)
		}
		if seed == 0 {
			want = s
		} else if !reflect.DeepEqual(s, want) {
			t.Fatal("fragmentation changed estimates")
		}
	}
}

func TestErrorAndTruncatedEventsNeverReleaseUsefulUnmeteredBytes(t *testing.T) {
	for _, body := range []string{`data: {"error":{"message":"secret"}}` + "\n\n", `data: {"choices":[{"delta":{"content":"unfinished`, "data: \xff\n\n", `error: secret` + "\n\n"} {
		m := newTest(t, "gpt-4", "/v1/chat/completions")
		ready, err := m.Feed([]byte(body))
		if err == nil {
			var rest []byte
			rest, err = m.Finish()
			ready = append(ready, rest...)
		}
		if err == nil || len(ready) > 0 || m.Snapshot().ResponseStarted {
			t.Fatal("bad event accepted", body, err)
		}
		if _, again := m.Feed([]byte("data: [DONE]\n\n")); again == nil {
			t.Fatal("failed meter reused")
		}
	}
}

func TestSnapshotCopiesAndFinishIdempotence(t *testing.T) {
	m := newTest(t, "gpt-4", "/v1/chat/completions")
	_, err := m.Feed([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Finish(); err != nil {
		t.Fatal(err)
	}
	s := m.Snapshot()
	*s.EstimatedOutputTokens = 999
	*s.Reported.InputTokens = 999
	s = m.Snapshot()
	requireCount(t, s.EstimatedOutputTokens, 1)
	requireCount(t, s.Reported.InputTokens, 12)
	ready, err := m.Finish()
	if err != nil || len(ready) != 0 {
		t.Fatal("Finish not idempotent")
	}
	if _, err = m.Feed(nil); !errors.Is(err, ErrFinished) {
		t.Fatal("feed after finish allowed", err)
	}
	raw, _ := json.Marshal(s)
	if strings.Contains(string(raw), "hello") {
		t.Fatal("snapshot contains text")
	}
}

func TestFailedTerminalRetainsReportedUsageWithoutReleasingError(t *testing.T) {
	m := newTest(t, "gpt-4", "/v1/responses")
	good := `data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"}` + "\n\n"
	bad := `event: response.failed` + "\n" + `data: {"type":"response.failed","response":{"status":"failed","error":{"message":"secret"},"usage":{"input_tokens":12,"output_tokens":3,"input_tokens_details":{"cached_tokens":4}}}}` + "\n\n"
	ready, err := m.Feed([]byte(good + bad))
	if err == nil || len(ready) != 0 {
		t.Fatal("failed batch released")
	}
	s := m.Snapshot()
	requireCount(t, s.EstimatedOutputTokens, 1)
	if s.Reported == nil {
		t.Fatal("failed terminal usage lost")
	}
	requireCount(t, s.Reported.InputTokens, 12)
	requireCount(t, s.Reported.OutputTokens, 3)
	requireCount(t, s.Reported.CachedInputTokens, 4)
	if !s.TerminalUsage {
		t.Fatal("explicit failed terminal usage lost provenance")
	}
}

func TestPartialUsageDoesNotEraseEarlierCacheCategories(t *testing.T) {
	m := newTest(t, "gpt-4", "/v1/messages")
	for _, body := range []string{`{"type":"message_start","message":{"usage":{"input_tokens":6,"cache_creation_input_tokens":2,"cache_read_input_tokens":4}}}`, `{"type":"message_delta","usage":{"output_tokens":3}}`, `{"type":"message_stop"}`} {
		if _, err := m.Feed([]byte("data: " + body + "\n\n")); err != nil {
			t.Fatal(err)
		}
	}
	s := m.Snapshot()
	requireCount(t, s.Reported.InputTokens, 12)
	requireCount(t, s.Reported.OutputTokens, 3)
	requireCount(t, s.Reported.CachedInputTokens, 4)
	requireCount(t, s.Reported.CacheWriteTokens, 2)
}

func TestBPEObservationCanDecrease(t *testing.T) {
	m := newTest(t, "gpt-4", "/v1/chat/completions")
	for _, word := range []string{"funct", "ion"} {
		raw, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"delta": map[string]string{"content": word}}}})
		if _, err := m.Feed(append(append([]byte("data: "), raw...), []byte("\n\n")...)); err != nil {
			t.Fatal(err)
		}
		s := m.Snapshot()
		want := int64(2)
		if word == "ion" {
			want = 1
		}
		requireCount(t, s.EstimatedOutputTokens, want)
	}
}

func TestResponsesSummaryDoneAndTerminalDoNotDoubleCount(t *testing.T) {
	m := newTest(t, "gpt-4", "/v1/responses")
	frames := []string{`{"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"delta":"hel"}`, `{"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":1,"delta":"lo"}`, `{"type":"response.reasoning_summary_text.done","output_index":0,"summary_index":0,"text":"hel"}`, `{"type":"response.reasoning_summary_text.done","output_index":0,"summary_index":1,"text":"lo"}`, `{"type":"response.completed","response":{"status":"completed","output":[{"type":"reasoning","summary":[{"type":"summary_text","text":"hel"},{"type":"summary_text","text":"lo"}]}]}}`}
	for _, frame := range frames {
		if _, err := m.Feed([]byte("data: " + frame + "\n\n")); err != nil {
			t.Fatal(err)
		}
	}
	requireCount(t, m.Snapshot().EstimatedOutputTokens, 2)
}

func TestLargeResponsesWithinEightMiBAndHardBounds(t *testing.T) {
	text := strings.Repeat("hello ", 60000)
	frame, _ := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]any{"status": "completed", "output": []any{map[string]any{"type": "message", "content": []any{map[string]string{"type": "output_text", "text": text}}}}}})
	for _, stream := range []bool{true, false} {
		m := newTest(t, "gpt-4o", "/v1/responses")
		wire := frame
		if stream {
			wire = append(append([]byte("data: "), frame...), []byte("\n\n")...)
		}
		ready, err := m.Feed(wire)
		if err != nil {
			t.Fatal("valid >256KiB response rejected", err)
		}
		rest, err := m.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if len(ready)+len(rest) != len(wire) {
			t.Fatal("valid response changed")
		}
	}
	m := newTest(t, "gpt-4o", "/v1/responses")
	ready, err := m.Feed([]byte("data: " + strings.Repeat("x", MaxEventBytes)))
	if !errors.Is(err, ErrLimit) || len(ready) != 0 || m.retained > MaxContentBytes {
		t.Fatal("event bound missing", err)
	}
}

func TestDecodedLaneBoundAndCountOverflow(t *testing.T) {
	m := newTest(t, "gpt-4o", "/v1/chat/completions")
	m.retained = MaxContentBytes
	if err := m.appendLane("output", "x", false); !errors.Is(err, ErrLimit) {
		t.Fatal("decoded bound not enforced", err)
	}
	for _, usage := range []string{`{"prompt_tokens":-1}`, `{"completion_tokens":1.5}`, `{"prompt_tokens":1000000001}`, `{"prompt_tokens":"12"}`} {
		m = newTest(t, "gpt-4o", "/v1/chat/completions")
		ready, err := m.Feed([]byte("data: {\"choices\":[],\"usage\":" + usage + "}\n\n"))
		if err == nil || len(ready) > 0 {
			t.Fatal("invalid usage released", usage)
		}
	}
}

func TestUnicodeJSONSurrogateIsNotSilentlyReplaced(t *testing.T) {
	m := newTest(t, "gpt-4", "/v1/chat/completions")
	ready, err := m.Feed([]byte(`data: {"choices":[{"delta":{"content":"\ud800"}}]}` + "\n\n"))
	if err == nil || len(ready) > 0 || m.Snapshot().ResponseStarted {
		t.Fatal("unpaired surrogate accepted", err)
	}
}

func TestResponseStartedDoesNotFollowUnknownOrErrorEnvelope(t *testing.T) {
	for _, body := range []string{`{"message":"secret"}`, `{"error":{"message":"secret"}}`, `{"choices":"secret"}`} {
		m := newTest(t, "gpt-4", "/v1/chat/completions")
		if _, err := m.Feed([]byte(body)); err != nil {
			t.Fatal(err)
		}
		ready, err := m.Finish()
		if err == nil || len(ready) > 0 || m.Snapshot().ResponseStarted {
			t.Fatal("error envelope executed", err)
		}
	}
}

func TestInitialMessagesUsageIsNotTerminalUsage(t *testing.T) {
	m := newTest(t, "gpt-4", "/v1/messages")
	for _, frame := range []string{`{"type":"message_start","message":{"usage":{"input_tokens":12,"output_tokens":0}}}`, `{"type":"message_stop"}`} {
		if _, err := m.Feed([]byte("data: " + frame + "\n\n")); err != nil {
			t.Fatal(err)
		}
	}
	s := m.Snapshot()
	if s.TerminalUsage {
		t.Fatal("initial output zero upgraded to terminal actual usage")
	}
	requireCount(t, s.Reported.InputTokens, 12)
	requireCount(t, s.Reported.OutputTokens, 0)
}

func TestResponsesProviderMetadataIsNotForwardedOrMetered(t *testing.T) {
	m := newTest(t, "gpt-4", "/v1/responses")
	meta := "event: response.metadata\ndata: {\"type\":\"response.metadata\",\"metadata\":{\"private\":\"not for callers\"},\"usage\":{\"input_tokens\":999,\"output_tokens\":999}}\n\n"
	before := m.Snapshot()
	for _, b := range []byte(strings.ReplaceAll(meta, "\n", "\r\n")) {
		if ready, err := m.Feed([]byte{b}); err != nil || len(ready) != 0 {
			t.Fatalf("metadata must be consumed without forwarding: %v", err)
		}
	}
	if !reflect.DeepEqual(before, m.Snapshot()) {
		t.Fatal("provider metadata changed the meter")
	}
	done := "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":12,\"output_tokens\":5}}}\n\n"
	if ready, err := m.Feed([]byte(done)); err != nil || string(ready) != done {
		t.Fatalf("actual terminal rejected: %v", err)
	}
	s := m.Snapshot()
	if !s.TerminalUsage || s.Reported == nil {
		t.Fatal("actual terminal usage lost")
	}
	requireCount(t, s.Reported.InputTokens, 12)
	requireCount(t, s.Reported.OutputTokens, 5)
}

func TestResponsesMetadataDoesNotMaskInvalidStreams(t *testing.T) {
	for _, tc := range []struct{ name, protocol, body string }{
		{"wrong protocol", "/v1/chat/completions", "data: {\"type\":\"response.metadata\"}\n\n"},
		{"error event", "/v1/responses", "event: error\ndata: {\"type\":\"response.metadata\"}\n\n"},
		{"error body", "/v1/responses", "data: {\"type\":\"response.metadata\",\"error\":{}}\n\n"},
		{"after terminal", "/v1/responses", "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\ndata: {\"type\":\"response.metadata\"}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newTest(t, "gpt-4", tc.protocol)
			if ready, err := m.Feed([]byte(tc.body)); !errors.Is(err, ErrProtocol) || len(ready) != 0 {
				t.Fatalf("invalid stream was released: %v", err)
			}
		})
	}
}

func TestModeDetectionAdvancesPastFragmentedWhitespaceAndSingleBOM(t *testing.T) {
	for _, prefix := range []string{" \t\n", "\xef\xbb\xbf \r\n", " \v\f\xef\xbb\xbf \t"} {
		m, err := New("unknown", "/v1/responses", nil)
		if err != nil {
			t.Fatal(err)
		}
		preamble := prefix + strings.Repeat(" ", 16000)
		for i := 0; i < len(preamble); i++ {
			if _, err = m.Feed([]byte{preamble[i]}); err != nil {
				t.Fatal(err)
			}
		}
		if m.mode != 0 || m.prefixStart != len(preamble) {
			t.Fatalf("cursor=%d want=%d mode=%q", m.prefixStart, len(preamble), m.mode)
		}
		if _, err = m.Feed([]byte("{")); err != nil || m.mode != 'j' {
			t.Fatalf("mode=%q err=%v", m.mode, err)
		}
	}
	m, _ := New("unknown", "/v1/responses", nil)
	_, _ = m.Feed([]byte("\xef\xbb\xbf\xef\xbb\xbf{"))
	if m.mode != 's' {
		t.Fatal("unexpected second BOM skipped")
	}
}
