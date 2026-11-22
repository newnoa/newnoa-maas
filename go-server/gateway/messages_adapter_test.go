package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMessagesAdapterRequestTools(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			var body map[string]any
			json.Unmarshal([]byte(`{"model":"gpt","max_tokens":99,"stream":true,"system":[{"type":"text","text":"system","cache_control":{"type":"ephemeral"}}],"tools":[{"name":"Read","description":"read","input_schema":{"type":"object"}}],"tool_choice":{"type":"tool","name":"Read"},"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"Read","input":{"file":"a"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"ok"},{"type":"text","text":"next"}]}]}`), &body)
			out, err := adaptMessagesRequest(body, path)
			if err != nil {
				t.Fatal(err)
			}
			tool := out["tools"].([]any)[0].(map[string]any)
			if path == "/v1/chat/completions" {
				tool = tool["function"].(map[string]any)
			}
			if tool["strict"] != false {
				t.Fatal("optional Anthropic schema became strict", tool)
			}
			data, _ := json.Marshal(out)
			for _, want := range []string{"call_1", "Read", "system", "next", "ok"} {
				if !strings.Contains(string(data), want) {
					t.Fatalf("lost %s: %s", want, data)
				}
			}
			if out["max_tokens"] != nil || out["system"] != nil {
				t.Fatal("native fields leaked", out)
			}
			if path == "/v1/responses" && out["max_output_tokens"] != float64(99) {
				t.Fatal(out)
			}
			if path == "/v1/chat/completions" && out["stream_options"].(map[string]any)["include_usage"] != true {
				t.Fatal(out)
			}
		})
	}
}
func TestMessagesAdapterRejectsUnsupportedContent(t *testing.T) {
	for _, raw := range []string{
		`{"messages":[{"role":"user","content":[{"type":"document","source":{"type":"file","file_id":"x"}}]}]}`,
		`{"thinking":{"type":"enabled","budget_tokens":-1},"messages":[{"role":"user","content":"hi"}]}`,
		`{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"x","name":"Read","input":"bad"}]}]}`,
	} {
		var body map[string]any
		json.Unmarshal([]byte(raw), &body)
		if _, err := adaptMessagesRequest(body, "/v1/chat/completions"); err == nil {
			t.Fatal("silently accepted", raw)
		}
	}
}
func TestMessagesAdapterResponse(t *testing.T) {
	for _, tc := range []struct{ path, raw string }{
		{"/v1/chat/completions", `{"id":"chat_1","choices":[{"message":{"role":"assistant","content":"ok","tool_calls":[{"id":"call_1","type":"function","function":{"name":"Read","arguments":"{\"file\":\"a\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":7,"prompt_tokens_details":{"cached_tokens":3}}}`},
		{"/v1/responses", `{"id":"resp_1","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]},{"type":"function_call","call_id":"call_1","name":"Read","arguments":"{\"file\":\"a\"}"}],"usage":{"input_tokens":10,"output_tokens":7,"input_tokens_details":{"cached_tokens":3}}}`},
	} {
		ev := evidence{}
		ev.usage([]byte(tc.raw))
		data, err := adaptMessagesResponse([]byte(tc.raw), tc.path, "deployment", &ev)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		json.Unmarshal(data, &out)
		usage := out["usage"].(map[string]any)
		if out["stop_reason"] != "tool_use" || usage["input_tokens"] != float64(7) || usage["cache_read_input_tokens"] != float64(3) {
			t.Fatal(string(data))
		}
	}
}

func TestMessagesAdapterGatewayBoundExecution(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%v", path, stream), func(t *testing.T) {
				var execution string
				var finalized map[string]any
				calls := 0
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls++
					data, _ := io.ReadAll(r.Body)
					sum := sha256.Sum256(append([]byte("POST "+path+"\n"), data...))
					if r.URL.Path != path || execution != hex.EncodeToString(sum[:]) || r.Header.Get("Authorization") != "Bearer private" || r.Header.Get("X-Api-Key") != "" || r.Header.Get("Anthropic-Version") != "" {
						t.Error("execution boundary", r.URL.Path, r.Header)
					}
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						if path == "/v1/responses" {
							io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":7}}}\n\n")
						} else {
							io.WriteString(w, "data: {\"id\":\"chat_1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":7}}\n\ndata: [DONE]\n\n")
						}
						return
					}
					if path == "/v1/responses" {
						io.WriteString(w, `{"id":"resp_1","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":10,"output_tokens":7}}`)
					} else {
						io.WriteString(w, `{"id":"chat_1","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":7}}`)
					}
				}))
				defer upstream.Close()
				control := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var body map[string]any
					json.NewDecoder(r.Body).Decode(&body)
					switch {
					case strings.HasSuffix(r.URL.Path, "/authorize"):
						json.NewEncoder(w).Encode(map[string]any{"model": "upstream", "inferencePath": path})
					case strings.HasSuffix(r.URL.Path, "/dispatches"):
						if body["inferencePath"] != path {
							t.Error("wrong dispatch path", body)
						}
						execution = body["executionDigest"].(string)
						json.NewEncoder(w).Encode(map[string]any{"requestId": "adapt", "permit": "once", "endpoint": upstream.URL, "apiKey": "private"})
					case strings.HasSuffix(r.URL.Path, "/finalize"):
						finalized = meterFixtureSelectedEvidence(r, body)
						io.WriteString(w, `{}`)
					default:
						io.WriteString(w, `{}`)
					}
				}))
				defer control.Close()
				g := newGateway(control.URL, "internal", "g", []string{"127.0.0.1"})
				r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(fmt.Sprintf(`{"model":"deployment","messages":[{"role":"user","content":"hi"}],"max_tokens":20,"stream":%v}`, stream)))
				r.Header.Set("X-Api-Key", "consumer")
				r.Header.Set("Authorization", "Bearer ses_client_login")
				w := httptest.NewRecorder()
				g.ServeHTTP(w, r)
				if w.Code != 200 || calls != 1 || finalized["status"] != "succeeded" || finalized["inputTokens"] != float64(10) {
					t.Fatalf("code=%d calls=%d final=%v body=%s", w.Code, calls, finalized, w.Body.String())
				}
				if stream {
					ev := evidence{anthropic: true}
					ev.feed(w.Body.Bytes())
					if !ev.complete() {
						t.Fatal("invalid Anthropic SSE", w.Body.String())
					}
				} else {
					ev := evidence{}
					if !ev.messagesResponse(w.Body.Bytes()) {
						t.Fatal("invalid Anthropic JSON", w.Body.String())
					}
				}
			})
		}
	}
}

func TestMessagesBridgeFragmentedTools(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			events := []string{}
			if path == "/v1/chat/completions" {
				events = []string{
					`{"id":"chat_1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"Re","arguments":""}}]}}]}`,
					`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"ad","arguments":"{\"file\":"}}]}}]}`,
					`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"a\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":7}}`,
					`[DONE]`,
				}
			} else {
				events = []string{
					`{"type":"response.created","response":{"id":"resp_1"}}`,
					`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call_1","name":"Read"}}`,
					`{"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"file\":"}`,
					`{"type":"response.function_call_arguments.delta","output_index":0,"delta":"\"a\"}"}`,
					`{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":10,"output_tokens":7}}}`,
				}
			}
			bridge := messagesBridge{path: path, model: "deployment"}
			source := evidence{responses: path == "/v1/responses"}
			var output []byte
			for _, event := range events {
				frame := []byte("data: " + event + "\n\n")
				for _, c := range frame {
					piece := []byte{c}
					source.feed(piece)
					converted, err := bridge.feed(piece)
					if err != nil {
						t.Fatal(err)
					}
					output = append(output, converted...)
				}
			}
			tail, err := bridge.finish(&source)
			if err != nil {
				t.Fatal(err)
			}
			output = append(output, tail...)
			ev := evidence{anthropic: true}
			ev.feed(output)
			if !ev.complete() || !strings.Contains(string(output), `"name":"Read"`) || !strings.Contains(string(output), `"stop_reason":"tool_use"`) {
				t.Fatal(string(output))
			}
		})
	}
}
func TestMessagesBridgeRejectsInvalidToolJSON(t *testing.T) {
	b := messagesBridge{path: "/v1/chat/completions", model: "x"}
	ev := evidence{}
	raw := []byte("data: {\"id\":\"chat_1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"Read\",\"arguments\":\"{invalid\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	ev.feed(raw)
	b.feed(raw)
	if _, err := b.finish(&ev); err == nil {
		t.Fatal("invalid tool accepted")
	}
}

func TestMessagesAdapterUpstreamErrorCompatibility(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			var final map[string]any
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"gpt","inferencePath":"/v1/chat/completions"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					io.WriteString(w, `{"requestId":"error","permit":"one","endpoint":"https://example.test"}`)
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					json.NewDecoder(r.Body).Decode(&final)
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "g", nil)
			g.egress.Transport = gatewayRoundTripFunc(func(*http.Request) (*http.Response, error) {
				status := 429
				body := `{"error":{"message":"private upstream detail"}}`
				header := http.Header{"Retry-After": {"2"}}
				if stream {
					status = 200
					body = "data: {\"id\":\"chat_1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"error\":{\"message\":\"private upstream detail\"}}\n\n"
					header.Set("Content-Type", "text/event-stream")
				}
				return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			r := httptest.NewRequest("POST", "/v1/messages", strings.NewReader(fmt.Sprintf(`{"model":"deployment","messages":[{"role":"user","content":"hi"}],"max_tokens":20,"stream":%v}`, stream)))
			r.Header.Set("X-Api-Key", "consumer")
			r.Header.Set("Authorization", "Bearer ses_client_login")
			w := httptest.NewRecorder()
			aborted := serveGatewayCapturingAbort(g, w, r)
			if stream {
				if !aborted || !strings.Contains(w.Body.String(), `"type":"error"`) {
					t.Fatal("missing stream error", w.Body.String())
				}
			} else {
				if w.Code != 429 || !strings.Contains(w.Body.String(), `"type":"rate_limit_error"`) || w.Header().Get("Retry-After") != "2" {
					t.Fatal("missing rate limit", w.Code, w.Header(), w.Body.String())
				}
			}
			if strings.Contains(w.Body.String(), "private upstream detail") || final["status"] == "succeeded" {
				t.Fatal("leaked or accepted upstream error", w.Body.String(), final)
			}
		})
	}
}

func TestMessagesReasoningEffortMapping(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		for _, tc := range []struct{ thinking, config, effort string }{
			{`{"type":"enabled","budget_tokens":1024}`, `null`, "medium"},
			{`{"type":"adaptive"}`, `null`, "medium"},
			{`{"type":"adaptive","display":"omitted"}`, `{"effort":"high"}`, "high"},
			{`{"type":"adaptive"}`, `{"effort":"max"}`, "max"},
			{`{"type":"disabled"}`, `null`, ""},
		} {
			var body map[string]any
			json.Unmarshal([]byte(fmt.Sprintf(`{"model":"gpt","max_tokens":2048,"messages":[{"role":"user","content":"hi"}],"thinking":%s,"output_config":%s}`, tc.thinking, tc.config)), &body)
			if body["output_config"] == nil {
				delete(body, "output_config")
			}
			out, err := adaptMessagesRequest(body, path)
			if err != nil {
				t.Fatal(err)
			}
			effort := out["reasoning_effort"]
			if path == "/v1/responses" {
				if reasoning, ok := out["reasoning"].(map[string]any); ok {
					effort = reasoning["effort"]
				} else {
					effort = nil
				}
			}
			if tc.effort == "" {
				if effort != nil {
					t.Fatal(out)
				}
			} else if effort != tc.effort {
				t.Fatalf("want=%s out=%v", tc.effort, out)
			}
			if out["thinking"] != nil || out["output_config"] != nil {
				t.Fatal("Anthropic reasoning fields leaked", out)
			}
		}
	}
	for _, fields := range []string{`"thinking":{"type":"enabled"}`, `"thinking":{"type":"enabled","budget_tokens":-1}`, `"thinking":{"type":"adaptive","budget_tokens":1000}`, `"thinking":{"type":"unknown"}`, `"output_config":{"effort":"unknown"}`, `"output_config":{"format":{"type":"json_schema"}}`} {
		var body map[string]any
		json.Unmarshal([]byte(`{"messages":[{"role":"user","content":"hi"}],`+fields+`}`), &body)
		if _, err := adaptMessagesRequest(body, "/v1/responses"); err == nil {
			t.Fatal("invalid reasoning accepted", fields)
		}
	}
}

func TestMessagesResponsesOutputLimit(t *testing.T) {
	raw := []byte(`{"id":"resp_1","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"message","content":[{"type":"output_text","text":"partial"}]}],"usage":{"input_tokens":10,"output_tokens":20}}`)
	ev := evidence{}
	ev.usage(raw)
	data, err := adaptMessagesResponse(raw, "/v1/responses", "deployment", &ev)
	if err != nil || !strings.Contains(string(data), `"stop_reason":"max_tokens"`) {
		t.Fatal(string(data), err)
	}
}
func TestMessagesResponsesStreamOutputLimit(t *testing.T) {
	b := messagesBridge{path: "/v1/responses", model: "deployment"}
	ev := evidence{responses: true, allowOutputLimit: true}
	raw := []byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\ndata: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":10,\"output_tokens\":20}}}\n\n")
	ev.feed(raw)
	output, err := b.feed(raw)
	if err != nil {
		t.Fatal(err)
	}
	tail, err := b.finish(&ev)
	if err != nil {
		t.Fatal(err)
	}
	output = append(output, tail...)
	if !strings.Contains(string(output), `"stop_reason":"max_tokens"`) {
		t.Fatal(string(output))
	}
	strict := evidence{responses: true}
	strict.feed(raw)
	if strict.complete() {
		t.Fatal("changed native Responses integrity policy")
	}
}

func TestMessagesAdapterOptionalToolResultContent(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses"} {
		t.Run(path, func(t *testing.T) {
			for _, tc := range []struct {
				name, field string
				valid       bool
			}{
				{"omitted", "", true},
				{"empty-text", `,"content":""`, true},
				{"empty-blocks", `,"content":[]`, true},
				{"null", `,"content":null`, false},
				{"object", `,"content":{}`, false},
				{"number", `,"content":1`, false},
				{"bad-block", `,"content":[{"type":"text","text":null}]`, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var body map[string]any
					if err := json.Unmarshal([]byte(`{"model":"gpt","max_tokens":100,"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1"`+tc.field+`}]}]}`), &body); err != nil {
						t.Fatal(err)
					}
					out, err := adaptMessagesRequest(body, path)
					if !tc.valid {
						if err == nil {
							t.Fatal("malformed present content accepted", out)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if path == "/v1/chat/completions" {
						result := out["messages"].([]any)[0].(map[string]any)
						if result["role"] != "tool" || result["tool_call_id"] != "call_1" || result["content"] != "" {
							t.Fatal(result)
						}
					} else {
						result := out["input"].([]any)[0].(map[string]any)
						if result["type"] != "function_call_output" || result["call_id"] != "call_1" || result["output"] != "" {
							t.Fatal(result)
						}
					}
				})
			}
		})
	}
}
