package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestMessagesBetaWirePolicy(t *testing.T) {
	valid := "claude-code-20250219,interleaved-thinking-2025-05-14,context-management-2025-06-27,prompt-caching-scope-2026-01-05,mid-conversation-system-2026-04-07,effort-2025-11-24"
	h := http.Header{}
	if v, e := messagesBeta(h); e != nil || v != "" {
		t.Fatal(v, e)
	}
	h.Set("Anthropic-Beta", valid)
	if v, e := messagesBeta(h); e != nil || v != valid {
		t.Fatal(v, e)
	}
	if messagesBinding(valid) != "anthropic-beta:"+valid+"\n" || messagesBinding("") != "" {
		t.Fatal("binding changed")
	}
	for _, v := range []string{"unknown", "", valid + ",", "effort-2025-11-24,effort-2025-11-24", strings.Repeat("a", 513)} {
		h.Set("Anthropic-Beta", v)
		if _, e := messagesBeta(h); e == nil {
			t.Fatal("accepted invalid beta", v)
		}
	}
	h.Set("Anthropic-Beta", valid)
	h.Add("Anthropic-Beta", valid)
	if _, e := messagesBeta(h); e == nil {
		t.Fatal("multiple header fields accepted")
	}
}

func TestMessagesBetaClaude2325CannotChangeConsumedPermit(t *testing.T) {
	const raw = `{"model":"approved-model","messages":[{"role":"user","content":"fixture"}],"max_tokens":16}`
	beta := claude2325Betas[0]
	wantDigest := hash([]byte("POST /v1/messages\nanthropic-beta:" + beta + "\n" + raw))
	var executions, consumptions atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		executions.Add(1)
		body, _ := io.ReadAll(r.Body)
		if string(body) != raw || r.Header.Get("Anthropic-Beta") != beta || r.Header.Get("Anthropic-Version") != "2023-06-01" {
			t.Error("bound native request changed before execution")
		}
		io.WriteString(w, `{}`)
	}))
	defer upstream.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/consume") {
			t.Errorf("unexpected authority route %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		consumptions.Add(1)
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["bodyDigest"] != wantDigest || body["epoch"] != float64(4) {
			w.WriteHeader(409)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"endpoint": upstream.URL, "model": "approved-model", "apiKey": "upstream-fixture"})
	}))
	defer api.Close()
	n := &connector{control: api.URL, purpose: "serving", credential: "node_fixture", client: api.Client(), upstream: upstream.Client()}
	device := httptest.NewServer(n)
	defer device.Close()
	parts := strings.Split(beta, ",")
	parts[0], parts[1] = parts[1], parts[0]
	for i, candidate := range []string{beta, "", strings.Join(parts, ","), strings.Replace(beta, "thinking-token-count-2026-05-13,", "", 1)} {
		r, _ := http.NewRequest("POST", device.URL+"/v1/messages", strings.NewReader(raw))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Newnoa-Request", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
		r.Header.Set("X-Newnoa-Permit", "permit_fixture")
		r.Header.Set("X-Newnoa-Gateway", "gateway-fixture")
		r.Header.Set("X-Newnoa-Epoch", "4")
		if candidate != "" {
			r.Header.Set("Anthropic-Beta", candidate)
		}
		res, err := device.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		want := 409
		if i == 0 {
			want = 200
		}
		if res.StatusCode != want {
			t.Fatalf("header variant %d: status=%d want=%d", i, res.StatusCode, want)
		}
	}
	if executions.Load() != 1 || consumptions.Load() != 4 {
		t.Fatalf("tampered beta executed or skipped digest consumption: executions=%d consumes=%d", executions.Load(), consumptions.Load())
	}
}

// Captured from the single network-isolated Claude Code 2.1.325 process.
// Later requests remove optional beta tokens; they are not identical-header retries.
var claude2325Betas = []string{
	"interleaved-thinking-2025-05-14,context-management-2025-06-27,claude-code-20250219,per-turn-control-2026-07-01,mid-conversation-tool-changes-2026-07-01,thinking-token-count-2026-05-13,effort-2025-11-24,thinking-display-updates-2026-08-18",
	"interleaved-thinking-2025-05-14,context-management-2025-06-27,claude-code-20250219,per-turn-control-2026-07-01,mid-conversation-tool-changes-2026-07-01,effort-2025-11-24,thinking-display-updates-2026-08-18",
	"interleaved-thinking-2025-05-14,context-management-2025-06-27,claude-code-20250219,per-turn-control-2026-07-01,mid-conversation-tool-changes-2026-07-01,effort-2025-11-24",
}

func TestMessagesBetaClaude2325CapturedHeaders(t *testing.T) {
	values := append([]string{}, claude2325Betas...)
	values = append(values, "per-turn-control-2026-07-01", "mid-conversation-tool-changes-2026-07-01", "thinking-token-count-2026-05-13", "thinking-display-updates-2026-08-18")
	values = append(values, " thinking-display-updates-2026-08-18 , per-turn-control-2026-07-01 ")
	values = append(values, "per-turn-control-2026-07-01"+strings.Repeat(" ", 512-len("per-turn-control-2026-07-01")))
	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			h := http.Header{"Anthropic-Beta": {value}}
			got, err := messagesBeta(h)
			if err != nil || got != value {
				t.Fatalf("captured/explicit beta was rejected or reordered: got=%q err=%v", got, err)
			}
		})
	}
	for _, value := range []string{
		"per-turn-control-2026-07-01,per-turn-control-2026-07-01",
		"per-turn-control-2026-07-01, per-turn-control-2026-07-01",
		"mid-conversation-tool-changes-2026-07-01,unknown-2026-11-17",
		"thinking-display-updates-2026-08-18,",
		"per-turn-control-2026-07-01" + strings.Repeat(" ", 513-len("per-turn-control-2026-07-01")),
	} {
		if _, err := messagesBeta(http.Header{"Anthropic-Beta": {value}}); err == nil {
			t.Fatalf("accepted invalid beta %q", value)
		}
	}
	if _, err := messagesBeta(http.Header{"Anthropic-Beta": {"per-turn-control-2026-07-01", "thinking-token-count-2026-05-13"}}); err == nil {
		t.Fatal("multiple new beta header fields accepted")
	}
}
