package toolcompact

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestTOONNativeValuesRoundTripWithoutLosingScalarTypes(t *testing.T) {
	for _, value := range []any{
		nil, true, false, json.Number("-1.25e+3"), "", "true", "123", "a,b", "a:b", " leading", "a\nb", "a\"b", "[root]",
		[]any{}, map[string]any{},
		[]any{nil, false, json.Number("1"), "null", "a\\b"},
		map[string]any{"empty": map[string]any{}, "values": []any{}, "nested": map[string]any{"answer": true}},
		[]any{map[string]any{"id": json.Number("1"), "name": "a,b"}, map[string]any{"id": json.Number("2"), "name": "a\"b"}},
	} {
		encoded, ok := encodeTOON(value, EncodeOptions{})
		if !ok {
			t.Fatalf("encoding supported value failed: %#v", value)
		}
		decoded, ok := DecodeTOON(encoded)
		if !ok || !reflect.DeepEqual(decoded, value) {
			t.Fatalf("type-preserving round trip failed: %#v -> %q -> %#v (%v)", value, encoded, decoded, ok)
		}
	}
}

func TestTOONEncoderRejectsUnrepresentableValues(t *testing.T) {
	for _, value := range []any{
		1, []any{1}, map[string]any{"value": 1},
		json.Number("01"), json.Number("1 true"), json.Number("NaN"),
		map[string]any{"unsafe key": "value"},
		[]any{[]any{"nested"}},
		[]any{map[string]any{}, map[string]any{}},
		[]any{map[string]any{"id": true}, map[string]any{"name": true}},
		[]any{map[string]any{"id": true}, map[string]any{"id": true, "name": "extra"}},
		[]any{map[string]any{"id": map[string]any{"nested": true}}},
	} {
		if encoded, ok := encodeTOON(value, EncodeOptions{}); ok {
			t.Fatalf("encoded unsupported value %#v as %q", value, encoded)
		}
	}
	if _, ok := encodeTOON("value", EncodeOptions{Delimiter: ';'}); ok {
		t.Fatal("accepted an unsupported delimiter")
	}
	if encoded, ok := encodeTOON([]any{"a\tb", "c"}, EncodeOptions{Delimiter: '\t'}); !ok || string(encoded) != "[2]: \"a\\tb\"\tc" {
		t.Fatalf("tab delimiter failed to preserve a quoted embedded tab: %q (%v)", encoded, ok)
	}
}

func TestTOONDecoderRejectsMalformedStructureAndQuotedCells(t *testing.T) {
	for _, input := range []string{
		"", " \n", "  key: value", "key: value\n\nnext: value", " key: value",
		"key:", "key:\nnext: value", "key:\n    child: value", "key: value\n  child: value",
		"key: {}\nkey: value", "rows[1]: a\nrows[1]: b", "rows[1]{id}:\n  1\nrows[1]{id}:\n  2",
		"[0]: wrong", "[2]: one", "[1]: \"unclosed", "[1]: \"bad\\\"", "[1]: \"bad\\q\"",
		"rows[1]{unsafe key}:\n  1", "rows[1]{id}:\n1", "rows[1]{id}:\n  1,2", "rows[1]{id}:\n  \"bad\\q\"",
		"[999999999999999999999999999999999999]: one", "rows[999999999999999999999999999999999999]{id}:\n  1",
		"key: \"bad\\q\"", "key: leading ", "key: a\tb",
	} {
		if decoded, ok := DecodeTOON([]byte(input)); ok {
			t.Fatalf("accepted malformed TOON %q as %#v", input, decoded)
		}
	}
	for _, input := range []string{"", "{", "[", "{\"key\":", "{\"key\":true", "[true", "[true,]", "{\"key\":true} false", "{\"key\":{\"nested\":}"} {
		if value, ok := DecodeJSONForTOONGrader([]byte(input)); ok {
			t.Fatalf("accepted malformed JSON %q as %#v", input, value)
		}
	}
	if value, ok := DecodeJSONForTOONGrader([]byte(`{"list":[null,true,1.00,"text"],"object":{"value":false}}`)); !ok || !reflect.DeepEqual(value, map[string]any{
		"list": []any{nil, true, json.Number("1.00"), "text"}, "object": map[string]any{"value": false},
	}) {
		t.Fatalf("JSON grading lost scalar types: %#v (%v)", value, ok)
	}
}
