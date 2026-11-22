package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDispatchReservationHintsUseOriginalBytesAndValidatedOutput(t *testing.T) {
	for _, tc := range []struct {
		name, path, raw string
		input           bool
		output          int64
	}{
		{"chat", "/v1/chat/completions", ` {"model":"fixture","messages":[{"role":"user","content":"你好"}],"max_tokens":16,"reserveInputUnits":1,"reserveOutputTokens":1} `, true, 16},
		{"completion-limit", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":32}`, true, 32},
		{"responses", "/v1/responses", `{"model":"fixture","input":"hi","max_output_tokens":64}`, true, 64},
		{"chat-cross-protocol-cap", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"max_output_tokens":1000000}`, true, 0},
		{"responses-cross-protocol-cap", "/v1/responses", `{"model":"fixture","input":"hi","max_output_tokens":1,"max_tokens":1000000}`, true, 0},
		{"responses-completion-cap", "/v1/responses", `{"model":"fixture","input":"hi","max_output_tokens":1,"max_completion_tokens":1000000}`, true, 0},
		{"chat-null-alias", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"max_output_tokens":null}`, true, 0},
		{"chat-equal-alias", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"max_output_tokens":16}`, true, 0},
		{"chat-alias-with-empty-key", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_tokens":1,"max_output_tokens":1000000,"":1}`, false, 0},
		{"responses-history", "/v1/responses", `{"model":"fixture","input":"hi","previous_response_id":"remote","max_output_tokens":64}`, false, 64},
		{"image-input", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/image"}}]}],"max_tokens":16}`, false, 16},
		{"unknown-block", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":[{"type":"future-expensive","text":"hi"}]}],"max_tokens":16}`, false, 16},
		{"text-block", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"max_tokens":16}`, true, 16},
		{"multiple-choices", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"n":2}`, true, 0},
		{"fractional-choices", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"n":1.1}`, true, 0},
		{"overflow-choices", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_tokens":16,"n":99999999999999999999999}`, true, 0},
		{"missing-limit", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"reserveOutputTokens":1}`, true, 0},
		{"null-limit", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_tokens":null}`, true, 0},
		{"integral-float-limit", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"max_tokens":16.0}`, true, 0},
		{"remote-tools", "/v1/responses", `{"model":"fixture","input":"hi","tools":[{"type":"web_search"}],"max_output_tokens":64}`, false, 64},
		{"svg-no-thinking", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"draw svg"}],"reasoning_effort":"none","max_tokens":32768}`, true, 32768},
		{"tools-in-json", "/v1/chat/completions", `{"model":"fixture","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"test","parameters":{"type":"object","properties":{"x":{"type":"string"}}}}}],"max_tokens":16}`, true, 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dispatch map[string]json.RawMessage
			var original map[string]any
			if err := json.Unmarshal([]byte(tc.raw), &original); err != nil {
				t.Fatal(err)
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var actual map[string]any
				json.NewDecoder(r.Body).Decode(&actual)
				for _, key := range []string{"max_tokens", "max_completion_tokens", "max_output_tokens", "n", "reserveInputUnits", "reserveOutputTokens"} {
					if actual[key] != original[key] {
						t.Errorf("caller body field %s changed", key)
					}
				}
				io.WriteString(w, `{"choices":[{"message":{"content":"OK"},"finish_reason":"stop"}],"status":"completed","output":[]}`)
			}))
			defer upstream.Close()
			control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]json.RawMessage
				json.NewDecoder(r.Body).Decode(&body)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					if body["reserveInputUnits"] != nil || body["reserveOutputTokens"] != nil {
						t.Error("reservation hints sent outside dispatch")
					}
					io.WriteString(w, `{"model":"fixture"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					dispatch = body
					json.NewEncoder(w).Encode(map[string]any{"requestId": "fixture", "permit": "fixture", "endpoint": upstream.URL})
				default:
					if body["reserveInputUnits"] != nil || body["reserveOutputTokens"] != nil {
						t.Error("reservation hints sent outside dispatch")
					}
					io.WriteString(w, `{}`)
				}
			}))
			defer control.Close()
			g := newGateway(control.URL, "internal-fixture", "fixture", []string{"127.0.0.1"})
			request := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.raw))
			request.Header.Set("Authorization", "Bearer ses_fixture")
			request.Header.Set("X-Api-Key", "fixture-key")
			out := httptest.NewRecorder()
			g.ServeHTTP(out, request)
			if dispatch == nil {
				t.Fatalf("dispatch missing: HTTP %d %s", out.Code, out.Body.String())
			}
			var units, output int64
			if tc.input {
				if err := json.Unmarshal(dispatch["reserveInputUnits"], &units); err != nil || units != int64(len(tc.raw)) {
					t.Fatalf("input hint=%s want original bytes=%d", dispatch["reserveInputUnits"], len(tc.raw))
				}
			} else if dispatch["reserveInputUnits"] != nil {
				t.Fatal("expensive or unknown input received a finite input estimate")
			}
			if tc.output > 0 {
				if err := json.Unmarshal(dispatch["reserveOutputTokens"], &output); err != nil || output != tc.output {
					t.Fatalf("output hint=%s want=%d", dispatch["reserveOutputTokens"], tc.output)
				}
			} else if dispatch["reserveOutputTokens"] != nil {
				t.Fatal("unknown output received a finite estimate")
			}
		})
	}
}

func TestReservationHintsRejectUnknownNumericsAndExpensiveInputs(t *testing.T) {
	for _, field := range []string{"max_tokens", "n"} {
		for _, value := range []string{"null", "0", "-1", "1.1", "1.0", "1e3", "9223372036854775808", "1e100", `"16"`, "true", "[]", "{}"} {
			raw := `{"model":"fixture","messages":[{"content":"hi"}],"max_tokens":16,"` + field + `":` + value + `}`
			if hint := reservationHints([]byte(raw), "/v1/chat/completions")["reserveOutputTokens"]; hint != nil {
				t.Errorf("%s=%s produced finite output estimate %v", field, value, hint)
			}
		}
	}
	for _, path := range []string{"/v1/audio/transcriptions", "/v1/images/generations", "/v1/systemone"} {
		raw := []byte(`{"model":"fixture","messages":[{"content":"hi"}],"max_tokens":16,"reserveInputUnits":1,"reserveOutputTokens":1}`)
		if hints := reservationHints(raw, path); len(hints) != 0 {
			t.Errorf("expensive %s received hints: %v", path, hints)
		}
	}
	for _, extra := range []string{`,"attachments":[{"file_id":"remote"}]`, `,"unknown_input":{"url":"https://example.invalid"}`, `,"messages":[{"role":"user","audio":{"id":"remote"},"content":"hi"}]`} {
		raw := []byte(`{"model":"fixture","messages":[{"content":"hi"}],"max_tokens":16` + extra + `}`)
		if hint := reservationHints(raw, "/v1/chat/completions")["reserveInputUnits"]; hint != nil {
			t.Errorf("unknown input received finite estimate: %v", hint)
		}
	}
}
