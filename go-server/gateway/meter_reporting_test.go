package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type meteredFragments struct {
	parts        []string
	beforeSecond func()
	closed       atomic.Bool
	reads        int
}

func (b *meteredFragments) Read(p []byte) (int, error) {
	b.reads++
	if b.reads == 2 && b.beforeSecond != nil {
		b.beforeSecond()
	}
	if len(b.parts) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.parts[0])
	b.parts[0] = b.parts[0][n:]
	if b.parts[0] == "" {
		b.parts = b.parts[1:]
	}
	return n, nil
}
func (b *meteredFragments) Close() error { b.closed.Store(true); return nil }

func TestGatewayMeterPersistsBeforeExecutionAndCompleteEventDelivery(t *testing.T) {
	for _, mode := range []string{"complete", "device route", "missing terminal", "start rejected", "output rejected", "output ack mismatch"} {
		t.Run(mode, func(t *testing.T) {
			var sequence atomic.Int64
			var upstreamCalls atomic.Int32
			var final map[string]any
			out := httptest.NewRecorder()
			body := &meteredFragments{parts: []string{"data: {\"choices\":[{\"delta\":{\"content\":\"he", "llo\"}}]}\n\n"}}
			body.beforeSecond = func() {
				if out.Body.Len() != 0 {
					t.Errorf("uncheckpointed partial event delivered %q", out.Body.String())
				}
			}
			if mode == "complete" || mode == "device route" {
				body.parts = append(body.parts, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3,\"prompt_tokens_details\":{\"cached_tokens\":4,\"cache_write_tokens\":2}}}\n\ndata: [DONE]\n\n")
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"gpt-4"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					value := map[string]any{"requestId": "meter-fixture", "permit": "one", "endpoint": "http://127.0.0.1:1"}
					if mode == "device route" {
						value["nodeId"] = "device"
						value["ownerId"] = "owner"
						value["route"] = "http://127.0.0.1:18101/tunnels/device/1"
					}
					_ = json.NewEncoder(w).Encode(value)
				case strings.HasSuffix(r.URL.Path, "/meter"):
					var checkpoint map[string]any
					_ = json.NewDecoder(r.Body).Decode(&checkpoint)
					next := int64(checkpoint["sequence"].(float64))
					snapshot := checkpoint["snapshot"].(map[string]any)
					raw, _ := json.Marshal(snapshot)
					if bytes.Contains(raw, []byte("hello")) || bytes.Contains(raw, []byte("prompt text")) {
						t.Error("meter persisted plaintext")
					}
					if next != sequence.Load()+1 && next != sequence.Load() {
						t.Errorf("sequence gap next=%d current=%d", next, sequence.Load())
					}
					if next == 1 && snapshot["responseStarted"] != false {
						t.Error("start claimed provider consumption")
					}
					if mode == "start rejected" || mode == "output rejected" && next > 1 {
						http.Error(w, "fixture store down", 503)
						return
					}
					requestID := "meter-fixture"
					if mode == "output ack mismatch" && next > 1 {
						requestID = "other"
					}
					sequence.Store(next)
					_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": requestID, "sequence": next})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					_ = json.NewDecoder(r.Body).Decode(&final)
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			t.Setenv("NEWNOA_FRP_OWNER_ID", "owner")
			t.Setenv("NEWNOA_FRP_OWNER_ORIGIN", "http://127.0.0.1:18101")
			g := newGateway(api.URL, "internal", "fixture", nil)
			g.egress = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				upstreamCalls.Add(1)
				if sequence.Load() != 1 {
					t.Error("producer invoked without durable start checkpoint")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: body}, nil
			})}
			g.owner = g.egress
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"prompt text"}],"stream":true}`))
			r.Header.Set("X-Api-Key", "fixture")
			r.Header.Set("Authorization", "Bearer ses_fixture")
			serveGatewayCapturingAbort(g, out, r)
			if mode == "start rejected" {
				if upstreamCalls.Load() != 0 || out.Code != 502 {
					t.Fatalf("start fail-open calls=%d code=%d", upstreamCalls.Load(), out.Code)
				}
				return
			}
			if upstreamCalls.Load() != 1 || !body.closed.Load() {
				t.Fatal("producer replayed or not closed")
			}
			rejected := mode == "output rejected" || mode == "output ack mismatch"
			if rejected {
				if strings.Contains(out.Body.String(), "hello") || final["meterSequence"] != float64(1) {
					t.Fatalf("unpersisted content/sequence escaped out=%q final=%v", out.Body.String(), final)
				}
			} else {
				if !strings.Contains(out.Body.String(), "hello") || sequence.Load() < 2 || final["meterSequence"] != float64(sequence.Load()) {
					t.Fatalf("missing checkpoint-backed content seq=%d final=%v out=%q", sequence.Load(), final, out.Body.String())
				}
				if mode == "missing terminal" && (final["inputTokens"] != nil || final["outputTokens"] != nil) {
					t.Fatalf("estimated counts masqueraded as upstream %v", final)
				}
				for _, key := range []string{"inputTokens", "outputTokens", "cachedInputTokens", "cacheWriteTokens"} {
					if _, present := final[key]; present {
						t.Fatalf("raw counter key in metered closure: %s %v", key, final)
					}
				}
			}
		})
	}
}

func TestMeterCheckpointRejectsSequenceOverflow(t *testing.T) {
	g := newGateway("http://fixture.invalid", "internal", "fixture", nil)
	g.client = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Fatal("overflow request sent")
		return nil, errors.New("unexpected")
	})}
	reporter := meterCheckpoint{gateway: g, request: "request", permit: "permit", sequence: 9007199254740991}
	if err := reporter.persist(context.Background(), map[string]any{}); err == nil {
		t.Fatal("overflow accepted")
	}
}

func TestGatewayMeterNonstreamPersistsBeforeJSONDelivery(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "rejected"}[rejected], func(t *testing.T) {
			var seq atomic.Int64
			out := httptest.NewRecorder()
			var final map[string]any
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"gpt-4"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					io.WriteString(w, `{"requestId":"nonstream-meter","permit":"one","endpoint":"http://127.0.0.1:1"}`)
				case strings.HasSuffix(r.URL.Path, "/meter"):
					var b map[string]any
					_ = json.NewDecoder(r.Body).Decode(&b)
					next := int64(b["sequence"].(float64))
					if strings.Contains(out.Body.String(), "hello") {
						t.Error("JSON delivered before checkpoint acknowledgement")
					}
					if rejected && next == 2 {
						http.Error(w, "outage", 503)
						return
					}
					seq.Store(next)
					_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": "nonstream-meter", "sequence": next})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					_ = json.NewDecoder(r.Body).Decode(&final)
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			g := newGateway(api.URL, "internal", "fixture", nil)
			g.egress = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if seq.Load() != 1 {
					t.Error("start not persisted")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":3}}`))}, nil
			})}
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"hi"}]}`))
			r.Header.Set("X-Api-Key", "fixture")
			r.Header.Set("Authorization", "Bearer ses_fixture")
			g.ServeHTTP(out, r)
			if rejected {
				if strings.Contains(out.Body.String(), "hello") || out.Code != 502 || final["meterSequence"] != float64(1) {
					t.Fatalf("JSON fail-open %d %q %v", out.Code, out.Body.String(), final)
				}
			} else if out.Code != 200 || !strings.Contains(out.Body.String(), "hello") || seq.Load() != 2 || final["meterSequence"] != float64(2) {
				t.Fatalf("JSON metering missing %d %s %v", out.Code, out.Body.String(), final)
			}
		})
	}
}

func TestGatewayFailedResponseRetainsNonterminalReportedCheckpoint(t *testing.T) {
	var seq int64
	var last map[string]any
	var final map[string]any
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/authorize"):
			io.WriteString(w, `{"model":"gpt-4"}`)
		case strings.HasSuffix(r.URL.Path, "/dispatches"):
			io.WriteString(w, `{"requestId":"failed-meter","permit":"one","endpoint":"http://127.0.0.1:1"}`)
		case strings.HasSuffix(r.URL.Path, "/meter"):
			var b map[string]any
			_ = json.NewDecoder(r.Body).Decode(&b)
			seq = int64(b["sequence"].(float64))
			last = b["snapshot"].(map[string]any)
			_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true, "requestId": "failed-meter", "sequence": seq})
		case strings.HasSuffix(r.URL.Path, "/finalize"):
			_ = json.NewDecoder(r.Body).Decode(&final)
			io.WriteString(w, `{}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	defer api.Close()
	g := newGateway(api.URL, "internal", "fixture", nil)
	g.egress = &http.Client{Transport: gatewayRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"private-upstream-diagnostic\"},\"usage\":{\"input_tokens\":12,\"output_tokens\":3}}}\n\n"))}, nil
	})}
	r := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"fixture","input":"hello","stream":true}`))
	r.Header.Set("Authorization", "Bearer ses_fixture")
	r.Header.Set("X-Api-Key", "fixture")
	out := httptest.NewRecorder()
	serveGatewayCapturingAbort(g, out, r)
	if strings.Contains(out.Body.String(), "private-upstream-diagnostic") {
		t.Fatal("upstream diagnostic escaped")
	}
	if seq < 2 || last["terminalUsage"] != false || last["responseStarted"] != true || final["meterSequence"] != float64(seq) {
		t.Fatalf("failure checkpoint missing or falsely terminal snapshot=%v final=%v", last, final)
	}
	reported := last["reported"].(map[string]any)
	if reported["inputTokens"] != float64(12) || reported["outputTokens"] != float64(3) {
		t.Fatalf("explicit usage lost %v", last)
	}
}
