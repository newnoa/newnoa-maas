package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestResponsesFunctionCarriersPreserveCallsAndMessages(t *testing.T) {
	const raw = `{"input":[{"type":"message","role":"user","content":"hello"},{"type":"additional_tools","role":"developer","tools":[{"type":"namespace","name":"archive","description":"Archive tools","tools":[{"type":"function","name":"read","parameters":{"type":"object"}}]}]},{"type":"function_call","namespace":"archive","name":"read","call_id":"one","arguments":"{}"},{"type":"function_call_output","call_id":"one","output":"original result"}],"tools":[{"type":"function","name":"list","parameters":{"type":"object"}}]}`
	var input map[string]any
	if err := json.Unmarshal([]byte(raw), &input); err != nil {
		t.Fatal(err)
	}
	body, aliases, err := normalizeResponsesFunctions(input)
	if err != nil || len(aliases) != 1 {
		t.Fatalf("wanted a single reversible alias: %v %v", aliases, err)
	}
	items := body["input"].([]any)
	original := input["input"].([]any)
	if len(items) != 3 || !reflect.DeepEqual(items[0], original[0]) || !reflect.DeepEqual(items[2], original[3]) {
		t.Fatal("user content or the historical tool result changed", items)
	}
	tools := body["tools"].([]any)
	if len(tools) != 2 || !reflect.DeepEqual(tools[0], input["tools"].([]any)[0]) {
		t.Fatal("tools lost", tools)
	}
	call := items[1].(map[string]any)
	if call["namespace"] != nil || call["name"] != tools[1].(map[string]any)["name"] || call["call_id"] != "one" {
		t.Fatal("historical tool call was not mapped", call)
	}
	if original[2].(map[string]any)["namespace"] != "archive" || len(original) != 4 {
		t.Fatal("input history was mutated")
	}
	result := map[string]any{"type": "response.completed", "response": map[string]any{"output": []any{call}, "usage": map[string]any{"input_tokens": 17}}}
	if _, err := restoreResponsesFunctions(result, aliases); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(result)
	if !strings.Contains(string(payload), `"name":"read","namespace":"archive"`) || !strings.Contains(string(payload), `"input_tokens":17`) {
		t.Fatal("tool identity or reported usage was not preserved", string(payload))
	}
}

func TestNamespaceDescriptionBudget(t *testing.T) {
	for _, tc := range []struct {
		bytes int
		ok    bool
	}{{(1 << 19) - 1, true}, {1 << 19, false}} {
		ns := map[string]any{
			"type": "namespace", "name": "catalog", "description": strings.Repeat("x", tc.bytes),
			"tools": []any{map[string]any{"type": "function", "name": "read"}, map[string]any{"type": "function", "name": "list"}},
		}
		_, _, err := normalizeResponsesFunctions(map[string]any{"tools": []any{ns}})
		if (err == nil) != tc.ok {
			t.Fatalf("description bytes %d: unexpected result %v", tc.bytes, err)
		}
	}
}

func TestResponsesFunctionCarrierFailsClosed(t *testing.T) {
	for _, raw := range []string{
		`{"input":[{"type":"additional_tools","role":"user","tools":[]}]}`,
		`{"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"patch"}]}]}`,
		`{"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"patch","format":{"type":"grammar","syntax":"lark","definition":"start: /.+/"}}]}]}`,
		`{"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"function","name":"f","parameters":{"type":"object"}},{"type":"function","name":"f","parameters":{"type":"string"}}]}]}`,
		`{"input":[{"type":"function_call","namespace":"missing","name":"read","arguments":"{}"}]}`,
	} {
		var body map[string]any
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			t.Fatal(err)
		}
		if _, _, err := normalizeResponsesFunctions(body); err == nil {
			t.Fatal("ambiguous input was forwarded", raw)
		}
	}
}

func TestResponsesCustomToolInputRoundTripAndStreaming(t *testing.T) {
	var body map[string]any
	if err := json.Unmarshal([]byte(`{"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"namespace","name":"functions","tools":[{"type":"custom","name":"patch","format":{"type":"text"}}]}]},{"type":"custom_tool_call","namespace":"functions","name":"patch","call_id":"call_one","input":"change A\nchange B"},{"type":"custom_tool_call_output","call_id":"call_one","name":"patch","output":"unchanged result"}]}`), &body); err != nil {
		t.Fatal(err)
	}
	request, names, err := normalizeResponsesFunctions(body)
	if err != nil {
		t.Fatal(err)
	}
	top := request["tools"].([]any)[0].(map[string]any)
	call := request["input"].([]any)[0].(map[string]any)
	if top["type"] != "function" || call["name"] != top["name"] || call["type"] != "function_call" || request["input"].([]any)[1].(map[string]any)["type"] != "function_call_output" {
		t.Fatal(request)
	}
	if got := request["input"].([]any)[1].(map[string]any)["output"]; got != "unchanged result" {
		t.Fatal("tool result was altered", got)
	}
	wire := `event: response.output_item.added` + "\r\n" + `data: {"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"call_one","arguments":"","name":"` + top["name"].(string) + `"}}` + "\r\n\r\n"
	wire += "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"item_id\":\"fc_1\",\"delta\":\"{}\"}\n\n"
	done, _ := json.Marshal(map[string]any{"type": "response.output_item.done", "output_index": 0, "item": call})
	wire += "data: " + string(done) + "\n\n"
	wire += "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":17,\"output_tokens\":3}}}\n\n"
	bridge := responsesFunctionStream{aliases: names}
	var out []byte
	for i := range wire {
		frame, err := bridge.feed([]byte(wire[i:i+1]), i == len(wire)-1)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, frame...)
	}
	if bytes := string(out); !strings.Contains(bytes, `"type":"custom_tool_call"`) || !strings.Contains(bytes, `"input":"change A\nchange B"`) || !strings.Contains(bytes, `"input_tokens":17`) || strings.Contains(bytes, "response.function_call_arguments.delta") || strings.Contains(bytes, top["name"].(string)) {
		t.Fatal(bytes)
	}
}

func TestResponsesFreeformAcceptsOnlyPinnedNativePatchGrammar(t *testing.T) {
	definition, err := os.ReadFile("testdata/codex-0.169-apply-patch.lark")
	if err != nil {
		t.Fatal(err)
	}
	format := map[string]any{"type": "grammar", "syntax": "lark", "definition": string(definition)}
	if !responsesFreeformSupported("", "apply_patch", format) || responsesFreeformSupported("", "different_tool", format) {
		t.Fatal("Codex grammar scope widened or rejected the pinned definition")
	}
	format["definition"] = string(definition) + "\n"
	if responsesFreeformSupported("", "apply_patch", format) {
		t.Fatal("unverified grammar accepted")
	}
}

func TestLlamaResponsesCompatIsBoundToAuthorizedRouteAndMetered(t *testing.T) {
	for _, tc := range []struct {
		label, authorized, dispatched string
		code, calls                   int
	}{
		{"strict backend", "llama.cpp", "llama.cpp", 200, 1},
		{"unrelated provider", "other", "other", 200, 1},
		{"route changed", "llama.cpp", "other", 502, 0},
	} {
		t.Run(tc.label, func(t *testing.T) {
			const raw = `{"model":"deployment","stream":true,"input":[{"type":"message","role":"user","content":"hello"},{"type":"additional_tools","role":"developer","tools":[{"type":"namespace","name":"archive","tools":[{"type":"function","name":"read","parameters":{"type":"object"}}]}]}]}`
			var execution string
			calls, consumed := 0, 0
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				data, _ := io.ReadAll(r.Body)
				digest := sha256.Sum256(append([]byte("POST /v1/responses\n"), data...))
				if execution != hex.EncodeToString(digest[:]) || r.Header.Get("Authorization") != "Bearer upstream-private" {
					t.Error("execution digest or credential was not bound")
				}
				var body map[string]any
				json.Unmarshal(data, &body)
				alias := "plain"
				if tc.authorized == "llama.cpp" {
					tools, _ := body["tools"].([]any)
					if len(tools) != 1 || len(body["input"].([]any)) != 1 {
						t.Error("carrier not normalized")
						w.WriteHeader(400)
						return
					}
					alias, _ = tools[0].(map[string]any)["name"].(string)
				} else if len(body["input"].([]any)) != 2 || body["tools"] != nil {
					t.Error("other provider request was rewritten")
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n")
				io.WriteString(w, `data: {"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc","call_id":"one","name":"`+alias+`","arguments":"{}"}}`+"\n\n")
				io.WriteString(w, `data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":17,"output_tokens":3}}}`+"\n\n")
			}))
			defer up.Close()
			var final map[string]any
			control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b := map[string]any{}
				json.NewDecoder(r.Body).Decode(&b)
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					json.NewEncoder(w).Encode(map[string]any{"deploymentId": "deployment", "model": "upstream-model", "deploymentMethod": tc.authorized, "revision": 1})
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					original := sha256.Sum256([]byte("POST /v1/responses\n" + raw))
					if b["bodyDigest"] != hex.EncodeToString(original[:]) {
						t.Error("original digest changed")
					}
					execution, _ = b["executionDigest"].(string)
					json.NewEncoder(w).Encode(map[string]any{"requestId": "compat", "permit": "fixture", "endpoint": up.URL, "apiKey": "upstream-private", "deploymentId": "deployment", "model": "upstream-model", "deploymentMethod": tc.dispatched, "revision": 1})
				case strings.HasSuffix(r.URL.Path, "/consume"):
					consumed++
					io.WriteString(w, `{}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					final = meterFixtureSelectedEvidence(r, b)
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer control.Close()
			req := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(raw))
			req.Header.Set("X-Api-Key", "nn_fixture")
			req.Header.Set("Authorization", "Bearer ses_client_login")
			res := httptest.NewRecorder()
			aborted := serveGatewayCapturingAbort(newGateway(control.URL, "internal", "fixture", []string{"127.0.0.1"}), res, req)
			if aborted || res.Code != tc.code || calls != tc.calls || consumed != tc.calls {
				t.Fatalf("status %d; calls %d; permits consumed %d; abort %v; %s", res.Code, calls, consumed, aborted, res.Body.String())
			}
			if tc.code == 200 && (final["status"] != "succeeded" || final["inputTokens"] != float64(17)) {
				t.Fatal(final)
			}
			if tc.authorized == "llama.cpp" && tc.code == 200 && !strings.Contains(res.Body.String(), `"name":"read","namespace":"archive"`) {
				t.Fatal(res.Body.String())
			}
		})
	}
}

func TestResponsesPinnedCodexExecCarrier(t *testing.T) {
	definition, err := os.ReadFile("testdata/codex-0.169-functions-exec.lark")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		namespace, name, definition string
		allowed                     bool
	}{
		{"functions", "exec", string(definition), true},
		{"other", "exec", string(definition), false},
		{"functions", "different", string(definition), false},
		{"functions", "exec", string(definition) + "\n", false},
	} {
		tool := map[string]any{"type": "custom", "name": test.name, "description": "fixture", "format": map[string]any{"type": "grammar", "syntax": "lark", "definition": test.definition}}
		carrier := map[string]any{"type": "additional_tools", "role": "developer", "tools": []any{map[string]any{"type": "namespace", "name": test.namespace, "tools": []any{tool}}}}
		const text = "/* inert transport fixture */"
		call := map[string]any{"type": "custom_tool_call", "namespace": test.namespace, "name": test.name, "input": text, "call_id": "fixture"}
		body, aliases, err := normalizeResponsesFunctions(map[string]any{"input": []any{carrier, call}})
		if !test.allowed {
			if err == nil {
				t.Fatal("unverified grammar or tool identity accepted")
			}
			continue
		}
		if err != nil || len(aliases) != 1 {
			t.Fatalf("pinned tool carrier rejected: %v", err)
		}
		item := body["input"].([]any)[0].(map[string]any)
		if item["type"] != "function_call" || item["namespace"] != nil || item["name"] == test.name {
			t.Fatal("tool call not normalized")
		}
		if changed, err := restoreResponsesFunctions(map[string]any{"item": item}, aliases); err != nil || !changed || !reflect.DeepEqual(call, item) {
			t.Fatalf("tool identity or input lost on round trip: %v", err)
		}
	}
}
