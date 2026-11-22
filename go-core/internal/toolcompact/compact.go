package toolcompact

import (
	"bytes"
	"encoding/json"
	"reflect"
	"unicode/utf8"
)

// Compact retains every JSON value; unsupported or non-round-tripping shapes pass through.
func Compact(input []byte, count func([]byte) int) []byte {
	if !utf8.Valid(input) {
		return input
	}
	value, ok := parseJSONTOON(input)
	if !ok {
		return input
	}
	var compact bytes.Buffer
	if json.Compact(&compact, input) != nil {
		return input
	}
	best := input
	before := count(input)
	if before < 0 {
		return input
	}
	bestCount := before
	candidates := [][]byte{compact.Bytes()}
	if toon, ok := encodeTOON(value, EncodeOptions{}); ok {
		if restored, ok := DecodeTOON(toon); ok && reflect.DeepEqual(restored, toonValueToAny(value)) {
			candidates = append(candidates, append([]byte("TOON (lossless JSON; keys declared once per table):\n"), toon...))
		}
	}
	for _, out := range candidates {
		after := count(out)
		if after >= 0 && after < bestCount && before-after >= 8 && after*100 <= before*95 {
			best, bestCount = out, after
		}
	}
	return best
}
