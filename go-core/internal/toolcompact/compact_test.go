package toolcompact

import (
	"bytes"
	"testing"
)

func TestCompactCounterFailureCannotEmitUnmeasuredOutput(t *testing.T) {
	raw := []byte(`{ "a": 1, "b": 2 }`)
	calls := 0
	out := Compact(raw, func([]byte) int {
		calls++
		if calls == 1 {
			return 100
		}
		return -1
	})
	if !bytes.Equal(out, raw) {
		t.Fatal("failed token count emitted replacement")
	}
}
