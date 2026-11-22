package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// remoteProtocolExecutable puts an offline, version-pinned CLI on this test's PATH.
// Its protocol runs in a test subprocess, never in an installed native agent.
func remoteProtocolExecutable(t *testing.T, agent, mode string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("offline executable shim requires a POSIX shell")
	}
	if agent == "claude-cli" {
		agent = "claude"
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, agent)
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	script := "#!/bin/sh\nNEWNOA_REMOTE_PROTOCOL_AGENT=" + quote(agent) + " NEWNOA_REMOTE_PROTOCOL_MODE=" + quote(mode) + " exec " + quote(os.Args[0]) + " -test.run='^TestRemoteProtocolCLI$' -- \"$@\"\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return binary
}

func TestRemoteProtocolCLI(t *testing.T) {
	agent := os.Getenv("NEWNOA_REMOTE_PROTOCOL_AGENT")
	if agent == "" {
		return
	}
	mode := os.Getenv("NEWNOA_REMOTE_PROTOCOL_MODE")
	for _, arg := range os.Args {
		if arg == "--version" {
			switch mode {
			case "version-failure":
				os.Exit(3)
			case "version-long":
				fmt.Print(strings.Repeat("x", 121))
			case "version-new":
				fmt.Print("unverified-next-version")
			default:
				if agent == "codex" {
					fmt.Print("codex-cli 0.169.0")
				} else if agent == "pi" {
					fmt.Print("1.5.2")
				} else {
					fmt.Print("2.1.325 (Claude Code)")
				}
			}
			os.Exit(0)
		}
	}
	session := "protocol-thread"
	for i, arg := range os.Args {
		if (arg == "--session-id" || arg == "--resume") && i+1 < len(os.Args) {
			session = os.Args[i+1]
		}
	}
	d, e := json.NewDecoder(os.Stdin), json.NewEncoder(os.Stdout)
	for {
		var m map[string]json.RawMessage
		if d.Decode(&m) != nil {
			os.Exit(0)
		}
		if log := os.Getenv("NEWNOA_REMOTE_PROTOCOL_LOG"); log != "" {
			f, err := os.OpenFile(log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				os.Exit(8)
			}
			_ = json.NewEncoder(f).Encode(m)
			_ = f.Close()
		}
		var method, typ string
		_ = json.Unmarshal(m["method"], &method)
		_ = json.Unmarshal(m["type"], &typ)
		if agent == "hermes" {
			var result any
			if method == "initialize" {
				result = map[string]any{"protocolVersion": 1}
			}
			if method == "session/load" {
				var params struct {
					ID string `json:"sessionId"`
				}
				_ = json.Unmarshal(m["params"], &params)
				if params.ID == "original" {
					result = map[string]any{"models": map[string]string{"currentModelId": "fixture"}}
				}
			}
			_ = e.Encode(map[string]any{"jsonrpc": "2.0", "id": m["id"], "result": result})
			continue
		}
		if agent == "openclaw" {
			var result any
			switch method {
			case "initialize":
				result = map[string]any{"protocolVersion": 1, "agentCapabilities": map[string]bool{"loadSession": true}}
			case "session/resume":
				var params struct {
					ID   string `json:"sessionId"`
					Meta struct {
						Key      string `json:"sessionKey"`
						Required bool   `json:"requireExistingSession"`
					} `json:"_meta"`
				}
				_ = json.Unmarshal(m["params"], &params)
				if mode == "missing" || params.ID != "original" || params.Meta.Key != "acp-bridge:original" || !params.Meta.Required {
					_ = e.Encode(map[string]any{"jsonrpc": "2.0", "id": m["id"], "error": map[string]any{"code": -32602}})
					continue
				}
				// The native resume result contains presentation metadata, not another ID.
				result = map[string]any{}
			case "session/prompt":
				_ = e.Encode(map[string]any{"jsonrpc": "2.0", "method": "session/update", "params": map[string]any{"sessionId": "original", "update": map[string]any{"sessionUpdate": "agent_message_chunk", "content": map[string]string{"type": "text", "text": "fixture reply"}}}})
				result = map[string]string{"stopReason": "end_turn"}
			}
			_ = e.Encode(map[string]any{"jsonrpc": "2.0", "id": m["id"], "result": result})
			continue
		}
		if agent == "pi" {
			var data any
			switch typ {
			case "get_state":
				model := ""
				for i, arg := range os.Args[:len(os.Args)-1] {
					if arg == "--model" {
						model = os.Args[i+1]
					}
				}
				if session == "protocol-thread" {
					session = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
				}
				data = map[string]any{"sessionId": session, "model": map[string]any{"provider": "newnoa", "id": model}}
			case "prompt":
				data = map[string]string{"disposition": "started"}
			}
			_ = e.Encode(map[string]any{"id": m["id"], "type": "response", "command": typ, "success": true, "data": data})
			if typ == "prompt" {
				_ = e.Encode(map[string]any{"type": "message_update", "assistantMessageEvent": map[string]string{"type": "text_delta", "delta": "NEWNOA_REMOTE_OK"}})
				_ = e.Encode(map[string]any{"type": "agent_settled", "aborted": false})
			}
			continue
		}
		if agent == "codex" {
			switch method {
			case "initialize":
				if mode == "malformed" {
					fmt.Println("not-json")
					continue
				}
				if mode == "init-failure" {
					_ = e.Encode(map[string]any{"id": m["id"], "error": map[string]any{"code": -1}})
				} else {
					_ = e.Encode(map[string]any{"id": m["id"], "result": map[string]any{}})
				}
			case "thread/start", "thread/resume":
				var params map[string]any
				_ = json.Unmarshal(m["params"], &params)
				if method == "thread/resume" {
					session, _ = params["threadId"].(string)
				}
				if mode == "wrong-resume" {
					session = "different-thread"
				}
				if mode == "missing-session" {
					session = ""
				}
				_ = e.Encode(map[string]any{"id": m["id"], "result": map[string]any{"thread": map[string]any{"id": session}}})
			case "turn/start":
				if mode == "missing-run" {
					_ = e.Encode(map[string]any{"id": m["id"], "result": map[string]any{}})
					continue
				}
				_ = e.Encode(map[string]any{"id": m["id"], "result": map[string]any{"turn": map[string]any{"id": "protocol-run"}}})
				switch mode {
				case "probe-native-error":
					fmt.Fprintln(os.Stderr, "PRIVATE_NATIVE_PROBE_SENTINEL")
					_ = e.Encode(map[string]any{"method": "error", "params": map[string]any{"threadId": session, "turnId": "protocol-run", "message": "PRIVATE_NATIVE_PROBE_SENTINEL"}})
					continue
				case "probe-http-402", "probe-http-other", "probe-http-terminal-only", "probe-http-retry":
					status := 402
					if mode == "probe-http-other" {
						status = 403
					}
					nativeError := map[string]any{"message": "PRIVATE_NATIVE_PROBE_SENTINEL", "codexErrorInfo": map[string]any{"httpConnectionFailed": map[string]any{"httpStatusCode": status}}}
					if mode == "probe-http-terminal-only" {
						_ = e.Encode(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": session, "turn": map[string]any{"id": "protocol-run", "status": "failed", "error": nativeError}}})
						continue
					}
					_ = e.Encode(map[string]any{"method": "error", "params": map[string]any{"threadId": session, "turnId": "protocol-run", "willRetry": mode == "probe-http-retry", "error": nativeError}})
					if mode != "probe-http-retry" {
						continue
					}
				case "probe-other-error":
					_ = e.Encode(map[string]any{"id": "unsupported", "method": "PRIVATE_NATIVE_PROBE_SENTINEL", "params": map[string]any{"threadId": session, "turnId": "protocol-run"}})
					continue
				case "probe-malformed":
					fmt.Println("PRIVATE_NATIVE_PROBE_SENTINEL not-json")
					continue
				case "probe-transport-large":
					fmt.Println(strings.Repeat("x", (4<<20)+1) + "PRIVATE_NATIVE_PROBE_SENTINEL")
					continue
				}
				if mode != "interactive" {
					if mode == "probe-tool" {
						_ = e.Encode(map[string]any{"method": "item/started", "params": map[string]any{"threadId": session, "turnId": "protocol-run", "item": map[string]any{"type": "commandExecution"}}})
					}
					if mode == "probe-approval" {
						_ = e.Encode(map[string]any{"id": "probe-approval", "method": "item/fileChange/requestApproval", "params": map[string]any{"threadId": session, "turnId": "protocol-run"}})
					}
					output := "NEWNOA_REMOTE_OK"
					if mode == "probe-large" {
						output = strings.Repeat("x", 4097) + "PRIVATE_NATIVE_PROBE_SENTINEL"
					}
					_ = e.Encode(map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{"threadId": session, "turnId": "protocol-run", "delta": output}})
					status := "completed"
					if mode == "probe-failed" {
						status = "failed"
					}
					if mode == "probe-stopped" {
						status = "interrupted"
					}
					_ = e.Encode(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": session, "turn": map[string]any{"id": "protocol-run", "status": status}}})
				}
			case "turn/steer", "turn/interrupt":
				_ = e.Encode(map[string]any{"id": m["id"], "result": map[string]any{}})
			}
		} else {
			switch typ {
			case "control_request":
				subtype := "success"
				if mode == "init-failure" {
					subtype = "error"
				}
				_ = e.Encode(map[string]any{"type": "control_response", "response": map[string]any{"request_id": m["request_id"], "subtype": subtype, "response": map[string]any{}}})
			case "user":
				_ = e.Encode(m)
				if mode == "probe-http-402" || mode == "probe-http-other" {
					result := "API Error: 402 PRIVATE_NATIVE_PROBE_SENTINEL"
					if mode == "probe-http-other" {
						result = "API Error: 403 PRIVATE_NATIVE_PROBE_SENTINEL with 402 in the text"
					}
					_ = e.Encode(map[string]any{"type": "result", "session_id": session, "subtype": "success", "is_error": true, "result": result})
					continue
				}
				if mode != "interactive" {
					_ = e.Encode(map[string]any{"type": "stream_event", "session_id": session, "event": map[string]any{"delta": map[string]any{"text": "NEWNOA_REMOTE_OK"}}})
					_ = e.Encode(map[string]any{"type": "result", "session_id": session, "is_error": mode == "probe-failed"})
				}
			}
		}
	}
}

func TestRemoteCodexClaudeVersionPinsArePlatformScoped(t *testing.T) {
	for _, row := range []struct {
		platform, agent, version string
		want                     bool
	}{
		{"darwin", "codex", "codex-cli 0.170.0", true},
		{"darwin", "claude", "2.1.328 (Claude Code)", true},
		{"darwin", "codex", "codex-cli 0.169.0", true},
		{"darwin", "claude", "2.1.325 (Claude Code)", true},
		{"darwin", "codex", "codex-cli 0.170.1", false},
		{"darwin", "claude", "2.1.328 (Other)", false},
		{"windows", "codex", "codex-cli 0.170.0", true},
		{"windows", "codex", "codex-cli 0.170.1", false},
		{"windows", "claude", "2.1.328 (Claude Code)", true},
		{"windows", "claude", "2.1.328 (Other)", false},
		{"linux", "codex", "codex-cli 0.170.0", false},
		{"linux", "claude", "2.1.328 (Claude Code)", false},
	} {
		if got := remoteCodexClaudeVersionAllowed(row.platform, row.agent, row.version); got != row.want {
			t.Errorf("%s %s %q: expected %t", row.platform, row.agent, row.version, row.want)
		}
	}
}

func TestRemoteProtocolMetadataAndVersionRejection(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli"} {
		t.Run(agent, func(t *testing.T) {
			remoteProtocolExecutable(t, agent, "ok")
			info, err := remoteProviderDetails(agent)
			if err != nil || info.Agent != agent || !info.Capabilities["image"] || !info.Capabilities["resume"] || info.Capabilities["steer"] != (agent == "codex") || info.Capabilities["actualModel"] != (agent == "claude-cli") {
				t.Fatalf("metadata: %+v %v", info, err)
			}
		})
	}
	for _, mode := range []string{"version-failure", "version-new", "version-long"} {
		t.Run(mode, func(t *testing.T) {
			remoteProtocolExecutable(t, "codex", mode)
			if _, err := remoteProviderDetails("codex"); err == nil {
				t.Fatal("unvalidated native version accepted")
			}
		})
	}
	t.Run("missing", func(t *testing.T) {
		t.Setenv("PATH", t.TempDir())
		if _, err := remoteProviderDetails("codex"); err == nil {
			t.Fatal("missing executable accepted")
		}
	})
}

func TestRemoteProtocolStandardLauncherAndNativeInputs(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli"} {
		t.Run(agent, func(t *testing.T) {
			remoteProtocolExecutable(t, agent, "interactive")
			log := filepath.Join(t.TempDir(), "wire.jsonl")
			t.Setenv("NEWNOA_REMOTE_PROTOCOL_LOG", log)
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			nativeID := "previous-thread"
			if agent == "claude-cli" {
				nativeID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
			}
			p, err := startRemoteProvider(ctx, agent, t.TempDir(), nativeID, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Close() })
			input := remoteProviderInput{Text: "describe", Images: []remoteProviderImage{{URL: "data:image/png;base64,aGVsbG8="}}}
			if err = p.StartTurn(ctx, input); err != nil {
				t.Fatal(err)
			}
			if err = p.Steer(ctx, remoteProviderInput{Text: "adjust"}); (err == nil) != (agent == "codex") {
				t.Fatalf("native steer capability: %v", err)
			}
			if err = p.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			wire, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			if agent == "codex" {
				for _, wanted := range []string{`"method":"thread/resume"`, `"threadId":"previous-thread"`, `"expectedTurnId":"protocol-run"`, `"type":"image"`, `"url":"data:image/png;base64,aGVsbG8="`, `"method":"turn/interrupt"`} {
					if !bytes.Contains(wire, []byte(wanted)) {
						t.Errorf("native wire missing %s: %s", wanted, wire)
					}
				}
			} else {
				for _, wanted := range []string{`"type":"user"`, `"session_id":"` + nativeID + `"`, `"media_type":"image/png"`, `"data":"aGVsbG8="`, `"subtype":"interrupt"`} {
					if !bytes.Contains(wire, []byte(wanted)) {
						t.Errorf("native wire missing %s: %s", wanted, wire)
					}
				}
			}
		})
	}
}

func TestRemoteProtocolProbeAndHandshakeBoundaries(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli"} {
		t.Run("probe/"+agent, func(t *testing.T) {
			remoteProtocolExecutable(t, agent, "ok")
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			if info, err := probeRemoteProvider(ctx, agent, t.TempDir()); err != nil || info.Agent != agent {
				t.Fatalf("offline conversation probe: %+v %v", info, err)
			}
		})
	}
	for _, mode := range []string{"probe-tool", "probe-approval", "probe-large", "probe-failed"} {
		t.Run(mode, func(t *testing.T) {
			remoteProtocolExecutable(t, "codex", mode)
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			if _, err := probeRemoteProvider(ctx, "codex", t.TempDir()); err == nil {
				t.Fatal("unsafe or failed conversation authorized")
			}
		})
	}
	for _, row := range []struct{ agent, mode, native string }{{"codex", "init-failure", ""}, {"codex", "malformed", ""}, {"codex", "missing-session", ""}, {"codex", "wrong-resume", "prior"}, {"claude-cli", "init-failure", ""}, {"claude-cli", "ok", "invalid-session"}} {
		t.Run(row.agent+"/"+row.mode+"/"+row.native, func(t *testing.T) {
			remoteProtocolExecutable(t, row.agent, row.mode)
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			if p, err := startRemoteProvider(ctx, row.agent, t.TempDir(), row.native, nil); err == nil {
				_ = p.Close()
				t.Fatal("failed handshake or mismatched session accepted")
			}
		})
	}
}

func TestRemoteProtocolCodexProbeUsesIsolatedInstructions(t *testing.T) {
	remoteProtocolExecutable(t, "codex", "ok")
	log := filepath.Join(t.TempDir(), "wire.jsonl")
	t.Setenv("NEWNOA_REMOTE_PROTOCOL_LOG", log)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if _, err := probeRemoteProvider(ctx, "codex", t.TempDir()); err != nil {
		t.Fatal(err)
	}
	wire, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	for _, wanted := range []string{`"ephemeral":true`, `"baseInstructions":"Reply exactly as requested without using tools."`, `"approvalPolicy":"never"`, `"effort":"low"`} {
		if !bytes.Contains(wire, []byte(wanted)) {
			t.Errorf("probe did not isolate its thread configuration: missing %s", wanted)
		}
	}
}

func TestRemoteProtocolProbePaymentRejection(t *testing.T) {
	const payment = "Model request rejected (HTTP 402). Check your available points and API key points limit, then retry."
	for _, row := range []struct{ agent, mode, want string }{
		{"codex", "probe-http-402", payment},
		{"codex", "probe-http-terminal-only", payment},
		{"codex", "probe-http-retry", ""},
		{"codex", "probe-http-other", "native conversation probe failed"},
		{"claude-cli", "probe-http-402", payment},
		{"claude-cli", "probe-http-other", "native conversation probe failed"},
	} {
		t.Run(row.agent+"/"+row.mode, func(t *testing.T) {
			remoteProtocolExecutable(t, row.agent, row.mode)
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			_, err := probeRemoteProvider(ctx, row.agent, t.TempDir())
			if got := remoteSafeError(err); got != row.want {
				t.Fatalf("probe error = %q, want %q", got, row.want)
			}
		})
	}
}

func TestRemoteProtocolProbeFailureDiagnosticsAreDistinctAndPrivate(t *testing.T) {
	for _, row := range []struct{ mode, kind string }{
		{"probe-tool", "tool-attempt"},
		{"probe-approval", "tool-attempt"},
		{"probe-large", "output-limit"},
		{"probe-failed", "failed-state"},
		{"probe-stopped", "stopped-state"},
		{"probe-native-error", "native-codex-error"},
		{"probe-other-error", "other"},
		{"probe-malformed", "protocol-invalid"},
		{"probe-transport-large", "output-limit"},
		{"interactive", "timeout"},
		{"version-failure", "setup"},
	} {
		t.Run(row.mode, func(t *testing.T) {
			remoteProtocolExecutable(t, "codex", row.mode)
			var output bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			_, err := probeRemoteProvider(ctx, "codex", t.TempDir())
			if err == nil || strings.Contains(err.Error(), "PRIVATE_NATIVE_PROBE_SENTINEL") {
				t.Fatal("failed probe must retain its safe caller error")
			}
			if strings.Contains(output.String(), "PRIVATE_NATIVE_PROBE_SENTINEL") {
				t.Fatal("native stdout/stderr/error reached diagnostic log")
			}
			var record map[string]any
			decoder := json.NewDecoder(&output)
			if err := decoder.Decode(&record); err != nil {
				t.Fatalf("failed probe has no structured diagnostic: %v", err)
			}
			if record["msg"] != "remote.authorization.probe.failed" || record["agent"] != "codex" || record["kind"] != row.kind {
				t.Fatalf("probe branch diagnostic mismatch: %v", record)
			}
			for key := range record {
				if key != "time" && key != "level" && key != "msg" && key != "agent" && key != "kind" {
					t.Fatal("probe log has a field beyond fixed diagnostic attributes")
				}
			}
			if err := decoder.Decode(&record); err != io.EOF {
				t.Fatal("probe failure produced duplicate diagnostics")
			}
		})
	}
}

func TestRemoteProtocolProbeDiagnosticAgentBoundaryAndSuccess(t *testing.T) {
	for _, agent := range []string{"codex", "claude-cli", "PRIVATE_NATIVE_PROBE_SENTINEL"} {
		t.Run(agent, func(t *testing.T) {
			if agent != "PRIVATE_NATIVE_PROBE_SENTINEL" {
				remoteProtocolExecutable(t, agent, "ok")
			}
			var output bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			_, err := probeRemoteProvider(ctx, agent, t.TempDir())
			if agent != "PRIVATE_NATIVE_PROBE_SENTINEL" {
				if err != nil || output.Len() != 0 {
					t.Fatal("successful authorization probe logged a failure")
				}
				return
			}
			var record map[string]any
			if err == nil || json.Unmarshal(output.Bytes(), &record) != nil || record["agent"] != "other" || strings.Contains(output.String(), agent) {
				t.Fatal("untrusted agent label reached probe diagnostic")
			}
		})
	}
}

func TestRemoteProtocolProbeDiagnosticDistinguishesExternalCancel(t *testing.T) {
	remoteProtocolExecutable(t, "codex", "interactive")
	protocolLog := filepath.Join(t.TempDir(), "protocol.jsonl")
	t.Setenv("NEWNOA_REMOTE_PROTOCOL_LOG", protocolLog)
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cwd := t.TempDir()
	finished := make(chan error, 1)
	go func() {
		_, err := probeRemoteProvider(ctx, "codex", cwd)
		finished <- err
	}()
	deadline := time.After(6 * time.Second)
	for {
		data, _ := os.ReadFile(protocolLog)
		if bytes.Contains(data, []byte(`"method":"turn/start"`)) {
			cancel()
			break
		}
		select {
		case err := <-finished:
			t.Fatalf("probe ended before cancellation: %v", err)
		case <-deadline:
			t.Fatal("offline probe never started its turn")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("externally canceled probe succeeded")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("externally canceled probe did not finish")
	}
	var record map[string]any
	if json.Unmarshal(output.Bytes(), &record) != nil || record["kind"] != "canceled" {
		t.Fatalf("external cancellation was mislabeled: %s", output.String())
	}
}

type remoteProtocolSink struct{ bytes.Buffer }

func (*remoteProtocolSink) Close() error { return nil }

func remoteProtocolParser(agent string) (*remoteNativeProvider, *remoteProtocolSink) {
	sink := &remoteProtocolSink{}
	return &remoteNativeProvider{agent: agent, nativeID: "session", runID: "run", stdin: sink, cancel: func() {}, done: make(chan struct{}), events: make(chan remoteProviderEvent, 128), approvals: map[string]remoteNativeMessage{}, pending: map[string]chan remoteNativeMessage{}, completedRuns: map[string]bool{}}, sink
}

func remoteProtocolCodexMessage(p *remoteNativeProvider, id, method, params string) {
	var raw json.RawMessage
	if id != "" {
		raw, _ = json.Marshal(id)
	}
	p.codexMessage(remoteNativeMessage{ID: raw, Method: method, Params: json.RawMessage(params)})
}

func TestRemoteProtocolCodexApprovalAndCompletionBoundaries(t *testing.T) {
	for _, row := range []struct{ method, decision, result string }{
		{"item/fileChange/requestApproval", "decline", `"decision":"decline"`},
		{"item/tool/requestUserInput", "answer", `"answers":{"Choice":{"answers":["A","B"]}}`},
		{"item/permissions/requestApproval", "deny", `"scope":"turn"`},
	} {
		t.Run(row.method, func(t *testing.T) {
			p, sink := remoteProtocolParser("codex")
			remoteProtocolCodexMessage(p, "request", row.method, `{"threadId":"session","turnId":"run","availableDecisions":["decline",{"acceptForSession":{}}],"command":"echo safe","api_key":"hidden"}`)
			e := <-p.events
			if e.Approval == nil || bytes.Contains(e.Approval.Data, []byte("hidden")) || !bytes.Contains(e.Approval.Data, []byte("echo safe")) {
				t.Fatalf("unsafe or absent approval: %+v", e)
			}
			if err := p.Respond(context.Background(), remoteProviderReply{RequestID: "request", Decision: row.decision, Answers: map[string][]string{"Choice": {"A", "B"}}}); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(sink.String(), row.result) {
				t.Fatalf("incorrect native response: %s", sink.String())
			}
			if e = <-p.events; e.Type != "approvalResolved" || e.Text != "request" {
				t.Fatalf("missing resolution: %+v", e)
			}
			if err := p.Respond(context.Background(), remoteProviderReply{RequestID: "request", Decision: row.decision}); err == nil {
				t.Fatal("approval replay accepted")
			}
		})
	}
	for _, status := range []string{"completed", "interrupted", "failed"} {
		t.Run(status, func(t *testing.T) {
			p, _ := remoteProtocolParser("codex")
			p.approvals["stale"] = remoteNativeMessage{}
			remoteProtocolCodexMessage(p, "", "turn/completed", `{"threadId":"session","turn":{"id":"run","status":"`+status+`"}}`)
			want := map[string]string{"completed": "idle", "interrupted": "stopped", "failed": "failed"}[status]
			if e := <-p.events; e.State != want || e.RunID != "run" {
				t.Fatalf("completion projection: %+v", e)
			}
			remoteProtocolCodexMessage(p, "", "item/agentMessage/delta", `{"threadId":"session","turnId":"run","delta":"late"}`)
			if len(p.events) != 0 || len(p.approvals) != 0 || !p.completedRuns["run"] || p.runID != "" {
				t.Fatal("completed run accepted late output or retained approvals")
			}
		})
	}
	p, sink := remoteProtocolParser("codex")
	remoteProtocolCodexMessage(p, "unsupported", "item/unknown/requestApproval", `{}`)
	if e := <-p.events; e.Type != "error" || !strings.Contains(sink.String(), "-32601") || len(p.approvals) != 0 {
		t.Fatal("unsupported native request was exposed as actionable")
	}
}

func TestRemoteProtocolClaudeEventsAndApprovalInvalidation(t *testing.T) {
	p, sink := remoteProtocolParser("claude")
	send := func(raw string) {
		var m remoteNativeMessage
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatal(err)
		}
		m.Raw = json.RawMessage(raw)
		p.claudeMessage(m)
	}
	send(`{"type":"assistant","session_id":"session","message":{"model":"confirmed-model"}}`)
	if e := <-p.events; e.Type != "model" || e.Model != "confirmed-model" {
		t.Fatalf("native actual model lost: %+v", e)
	}
	send(`{"type":"stream_event","session_id":"session","event":{"message":{"model":"stream-model"},"delta":{"text":"hello"}}}`)
	if e := <-p.events; e.Type != "output" || e.Text != "hello" {
		t.Fatalf("stream output lost: %+v", e)
	}
	if e := <-p.events; e.Model != "stream-model" {
		t.Fatalf("stream model lost: %+v", e)
	}
	send(`{"type":"control_request","request_id":"edit","request":{"subtype":"can_use_tool","tool_name":"Edit","input":{"file_path":"a.txt","old_string":"old","new_string":"new","api_key":"hidden"}}}`)
	e := <-p.events
	if e.Approval == nil || e.Approval.Kind != "Edit" || bytes.Contains(e.Approval.Data, []byte("hidden")) {
		t.Fatalf("bad native tool approval: %+v", e)
	}
	if err := p.Respond(context.Background(), remoteProviderReply{RequestID: "edit", Decision: "allow"}); err != nil || !strings.Contains(sink.String(), `"behavior":"allow"`) || !strings.Contains(sink.String(), `"file_path":"a.txt"`) {
		t.Fatalf("tool response did not preserve native input: %s %v", sink.String(), err)
	}
	<-p.events
	send(`{"type":"control_request","request_id":"question","request":{"subtype":"can_use_tool","tool_name":"AskUserQuestion","input":{"questions":[]}}}`)
	<-p.events
	if err := p.Respond(context.Background(), remoteProviderReply{RequestID: "question", Decision: "allow"}); err == nil {
		t.Fatal("question accepted without answers")
	}
	send(`{"type":"control_cancel_request","request_id":"question"}`)
	if e = <-p.events; e.Type != "approvalInvalidated" || e.Text != "question" {
		t.Fatalf("missing native cancellation: %+v", e)
	}
	if err := p.Respond(context.Background(), remoteProviderReply{RequestID: "question", Decision: "deny"}); err == nil {
		t.Fatal("late answer accepted after native cancellation")
	}
	send(`{"type":"control_request","request_id":"unsupported","request":{"subtype":"unknown"}}`)
	if e = <-p.events; e.Type != "error" || !strings.Contains(sink.String(), "unsupported host control request") {
		t.Fatal("unsupported host request accepted")
	}
	send(`{"type":"result","session_id":"session","is_error":true}`)
	if e = <-p.events; e.State != "failed" || p.runID != "" || len(p.approvals) != 0 {
		t.Fatal("native failed result retained live state")
	}
	canceled := false
	p.cancel = func() { canceled = true }
	send(`{"type":"assistant","session_id":"other","message":{"model":"foreign-model"}}`)
	if e = <-p.events; e.Type != "error" || !canceled {
		t.Fatal("foreign native session was accepted")
	}
}

func TestRemoteProtocolImageAndTransportRejections(t *testing.T) {
	for _, url := range []string{"https://example.test/image", "data:image/png,raw", "data:image/svg+xml;base64,YQ==", "data:image/png;base64,%%%"} {
		if _, err := remoteClaudeInput(remoteProviderInput{Images: []remoteProviderImage{{URL: url}}}); err == nil {
			t.Errorf("untrusted native image accepted: %s", url)
		}
	}
	for _, convert := range []func(remoteProviderInput) ([]map[string]any, error){remoteClaudeInput, remoteCodexInput} {
		if _, err := convert(remoteProviderInput{}); err == nil {
			t.Fatal("empty native message accepted")
		}
	}
	p, _ := remoteProtocolParser("codex")
	if err := p.Steer(context.Background(), remoteProviderInput{}); err == nil {
		t.Fatal("empty steer accepted")
	}
	p.runID = ""
	if err := p.Steer(context.Background(), remoteProviderInput{Text: "late"}); err == nil {
		t.Fatal("steer without active run accepted")
	}
	if err := p.Stop(context.Background()); err == nil {
		t.Fatal("stop without active run accepted")
	}
	if err := p.write(make(chan int)); err == nil {
		t.Fatal("unencodable native message accepted")
	}
	if err := p.write(strings.Repeat("x", 8<<20)); err == nil {
		t.Fatal("oversized native message accepted")
	}
}

func TestRemoteProtocolStartupAuthorityAndMissingTurn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := startRemoteProvider(ctx, "codex", t.TempDir(), "", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expired launch authority lost: %v", err)
	}
	for _, row := range []struct{ agent, cwd, binary string }{{"unsupported", "/", "missing"}, {"codex", "relative", "missing"}, {"codex", t.TempDir(), filepath.Join(t.TempDir(), "absent")}} {
		if p, err := startRemoteProviderCommand(context.Background(), row.agent, row.cwd, "", nil, row.binary, nil, nil); err == nil {
			_ = p.Close()
			t.Fatal("invalid native launch accepted")
		}
	}
	remoteProtocolExecutable(t, "codex", "missing-run")
	active, stop := context.WithTimeout(context.Background(), 6*time.Second)
	defer stop()
	p, err := startRemoteProvider(active, "codex", t.TempDir(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.StartTurn(active, remoteProviderInput{Text: "start"}); err == nil || !strings.Contains(err.Error(), "no run ID") {
		t.Fatalf("missing native turn identity accepted: %v", err)
	}
	if _, err = remoteProviderDetailsContext(ctx, "codex"); err == nil {
		t.Fatal("canceled native version lookup accepted")
	}
	if _, err = probeRemoteProvider(active, "unsupported", t.TempDir()); err == nil {
		t.Fatal("unsupported executor authorized")
	}
}

func TestRemoteProtocolCodexEventIdentityAndProbeTools(t *testing.T) {
	p, _ := remoteProtocolParser("codex")
	remoteProtocolCodexMessage(p, "", "thread/started", `{"thread":{"id":"new-session"}}`)
	remoteProtocolCodexMessage(p, "", "turn/started", `{"threadId":"new-session","turn":{"id":"run"}}`)
	if e := <-p.events; e.State != "running" || e.NativeID != "new-session" || e.RunID != "run" {
		t.Fatalf("native start identity lost: %+v", e)
	}
	remoteProtocolCodexMessage(p, "", "item/agentMessage/delta", `{"threadId":"foreign","turnId":"run","delta":"foreign"}`)
	if len(p.events) != 0 {
		t.Fatal("foreign thread output crossed ownership")
	}
	remoteProtocolCodexMessage(p, "", "error", `{"threadId":"new-session","turnId":"run","willRetry":true}`)
	if len(p.events) != 0 {
		t.Fatal("native retry was reported as terminal failure")
	}
	remoteProtocolCodexMessage(p, "", "error", `{"threadId":"new-session"}`)
	if e := <-p.events; e.Type != "error" || e.State != "failed" {
		t.Fatalf("native execution error lost: %+v", e)
	}
	p.probe = true
	canceled := false
	p.cancel = func() { canceled = true }
	for _, kind := range []string{"agentMessage", "reasoning", "userMessage"} {
		remoteProtocolCodexMessage(p, "", "item/started", `{"item":{"type":"`+kind+`"}}`)
	}
	if canceled || len(p.events) != 0 {
		t.Fatal("non-tool probe events rejected")
	}
	remoteProtocolCodexMessage(p, "", "item/started", `{"item":{"type":"mcpToolCall"}}`)
	if e := <-p.events; e.Type != "error" || !canceled {
		t.Fatal("probe permitted a native tool")
	}
}

func TestRemoteProtocolMalformedAndOversizedNativeOutput(t *testing.T) {
	for _, raw := range []string{"not-json\n", strings.Repeat("x", (4<<20)+1) + "\n"} {
		p, _ := remoteProtocolParser("codex")
		canceled := false
		p.cancel = func() { canceled = true }
		p.read(strings.NewReader(raw))
		if e := <-p.events; e.Type != "error" || e.State != "failed" || !canceled {
			t.Fatalf("invalid native transport did not terminate: %+v", e)
		}
	}
}

func TestRemoteProtocolClaudeQuestionAndDenyWire(t *testing.T) {
	for _, decision := range []string{"deny", "answer"} {
		t.Run(decision, func(t *testing.T) {
			p, sink := remoteProtocolParser("claude")
			p.approvals["question"] = remoteNativeMessage{Request: json.RawMessage(`{"tool_name":"AskUserQuestion","input":{"questions":[{"question":"Choice"}]}}`)}
			if err := p.Respond(context.Background(), remoteProviderReply{RequestID: "question", Decision: decision, Answers: map[string][]string{"Choice": {"A", "B"}}}); err != nil {
				t.Fatal(err)
			}
			var wire struct {
				Response struct {
					RequestID string `json:"request_id"`
					Response  struct {
						Behavior string `json:"behavior"`
						Input    struct {
							Answers map[string]string `json:"answers"`
						} `json:"updatedInput"`
					} `json:"response"`
				} `json:"response"`
			}
			if err := json.Unmarshal(sink.Bytes(), &wire); err != nil {
				t.Fatal(err)
			}
			if wire.Response.RequestID != "question" || decision == "deny" && wire.Response.Response.Behavior != "deny" || decision == "answer" && (wire.Response.Response.Behavior != "allow" || wire.Response.Response.Input.Answers["Choice"] != "A, B") {
				t.Fatalf("native question wire: %s", sink.String())
			}
		})
	}
}
