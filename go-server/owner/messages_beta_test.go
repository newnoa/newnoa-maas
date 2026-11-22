package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
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

func TestMessagesBetaClaude2325OwnerPreservesBoundHeader(t *testing.T) {
	const raw = `{"model":"approved-model","messages":[{"role":"user","content":"fixture"}],"max_tokens":16}`
	beta := claude2325Betas[0]
	want := sha256.Sum256([]byte("POST /v1/messages\nanthropic-beta:" + beta + "\n" + raw))
	var executions, forwarded atomic.Int32
	device := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded.Add(1)
		body, _ := io.ReadAll(r.Body)
		bound := "POST /v1/messages\n"
		if value := r.Header.Get("Anthropic-Beta"); value != "" {
			bound += "anthropic-beta:" + value + "\n"
		}
		actual := sha256.Sum256(append([]byte(bound), body...))
		if actual != want {
			w.WriteHeader(409)
			return
		}
		if r.Header.Get("X-Newnoa-Permit") != "permit_fixture" || r.Header.Get("X-Newnoa-Epoch") != "4" || r.Header.Get("Anthropic-Version") != "2023-06-01" {
			t.Error("owner changed execution metadata")
		}
		executions.Add(1)
		io.WriteString(w, `{}`)
	}))
	var listener net.Listener
	for port := 20000; port < 30000; port++ {
		var err error
		listener, err = net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err == nil {
			break
		}
	}
	if listener == nil {
		t.Fatal("no owner device fixture port available")
	}
	device.Listener.Close()
	device.Listener = listener
	device.Start()
	defer device.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int{"port": port})
	}))
	defer api.Close()
	o := newOwner("owner-fixture", api.URL, "internal-fixture")
	bridge := httptest.NewServer(o)
	defer bridge.Close()
	parts := strings.Split(beta, ",")
	parts[0], parts[1] = parts[1], parts[0]
	for i, candidate := range []string{beta, "", strings.Join(parts, ","), strings.Replace(beta, "thinking-token-count-2026-05-13,", "", 1)} {
		r, _ := http.NewRequest("POST", bridge.URL+"/tunnels/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/4/v1/messages", strings.NewReader(raw))
		r.Header.Set("Authorization", "Bearer internal-fixture")
		r.Header.Set("X-Newnoa-Permit", "permit_fixture")
		r.Header.Set("X-Newnoa-Request", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
		r.Header.Set("X-Newnoa-Gateway", "gateway-fixture")
		if candidate != "" {
			r.Header.Set("Anthropic-Beta", candidate)
		}
		res, err := bridge.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		wantStatus := 409
		if i == 0 {
			wantStatus = 200
		}
		if res.StatusCode != wantStatus {
			t.Fatalf("header variant %d: status=%d want=%d digest=%s", i, res.StatusCode, wantStatus, hex.EncodeToString(want[:]))
		}
	}
	if forwarded.Load() != 4 || executions.Load() != 1 {
		t.Fatalf("owner dropped binding or replayed request: forwarded=%d executions=%d", forwarded.Load(), executions.Load())
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
