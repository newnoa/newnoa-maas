package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRemoteProviderUnsupportedAgent(t *testing.T) {
	if _, err := remoteProviderDetails("claude-desktop"); err == nil {
		t.Fatal("desktop must not be a remote executor")
	}
}

func TestRemoteProviderImageInput(t *testing.T) {
	in := remoteProviderInput{Text: "describe", Images: []remoteProviderImage{{URL: "data:image/png;base64,aGVsbG8="}}}
	items, err := remoteCodexInput(in)
	if err != nil || len(items) != 2 {
		t.Fatalf("%v %v", items, err)
	}
	if _, err = remoteClaudeInput(remoteProviderInput{Images: []remoteProviderImage{{URL: "https://untrusted.example/image"}}}); err == nil {
		t.Fatal("remote images must not fetch arbitrary URLs")
	}
}

func TestRemoteProviderFakeCodex(t *testing.T) {
	if os.Getenv("NEWNOA_REMOTE_FAKE_CLI") == "1" {
		remoteProviderFakeCodex()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events := make(chan remoteProviderEvent, 32)
	p, err := startRemoteProviderCommand(ctx, "codex", t.TempDir(), "", func(e remoteProviderEvent) { events <- e }, os.Args[0], []string{"-test.run=^TestRemoteProviderFakeCodex$"}, []string{"NEWNOA_REMOTE_FAKE_CLI=1"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.StartTurn(ctx, remoteProviderInput{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	if err = p.StartTurn(ctx, remoteProviderInput{Text: "must not queue"}); err == nil {
		t.Fatal("second native turn was accepted while running")
	}
	if err = p.Steer(ctx, remoteProviderInput{Text: "more"}); err != nil {
		t.Fatal(err)
	}
	if err = p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	var approval *remoteProviderApproval
	for approval == nil {
		select {
		case e := <-events:
			approval = e.Approval
		case <-ctx.Done():
			t.Fatal("approval missing")
		}
	}
	if err = p.Respond(ctx, remoteProviderReply{RequestID: approval.ID, Decision: "cancel"}); err == nil {
		t.Fatal("approval decision absent from native options was accepted")
	}
	if err = p.Respond(ctx, remoteProviderReply{RequestID: approval.ID, Decision: "accept"}); err != nil {
		t.Fatal(err)
	}
	if err = p.Respond(ctx, remoteProviderReply{RequestID: approval.ID, Decision: "accept"}); err == nil {
		t.Fatal("approval replay accepted")
	}
}

func remoteProviderFakeCodex() {
	d := json.NewDecoder(os.Stdin)
	e := json.NewEncoder(os.Stdout)
	for {
		var m map[string]json.RawMessage
		if d.Decode(&m) != nil {
			return
		}
		var method string
		_ = json.Unmarshal(m["method"], &method)
		id := m["id"]
		switch method {
		case "initialize":
			e.Encode(map[string]any{"id": id, "result": map[string]any{"userAgent": "codex/0.169.0"}})
		case "thread/start", "thread/resume":
			e.Encode(map[string]any{"id": id, "result": map[string]any{"thread": map[string]any{"id": "native-test"}, "model": "configured-model"}})
		case "turn/start":
			e.Encode(map[string]any{"id": id, "result": map[string]any{"turn": map[string]any{"id": "run-test"}}})
			e.Encode(map[string]any{"method": "item/commandExecution/requestApproval", "id": "approval-test", "params": map[string]any{"command": "echo test", "availableDecisions": []string{"accept", "decline"}}})
		case "turn/steer":
			var p map[string]any
			json.Unmarshal(m["params"], &p)
			if p["expectedTurnId"] != "run-test" {
				os.Exit(3)
			}
			e.Encode(map[string]any{"id": id, "result": map[string]any{"turnId": "run-test"}})
		case "turn/interrupt":
			e.Encode(map[string]any{"id": id, "result": map[string]any{}})
		default:
			if len(id) > 0 && strings.Contains(string(m["result"]), "accept") {
			}
		}
	}
}

func TestRemoteProviderLiveProbe(t *testing.T) {
	agent := os.Getenv("NEWNOA_REMOTE_LIVE_AGENT")
	if agent == "" {
		t.Skip("explicit opt-in native model probe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	cwd := t.TempDir()
	t.Cleanup(func() { cleanupRemoteProviderTestProject(t, cwd) })
	info, err := probeRemoteProvider(ctx, agent, cwd)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s %s live no-tool conversation verified", info.Agent, info.Version)
}

func TestRemoteProviderInvalidAnswerKeepsApprovalPending(t *testing.T) {
	p := &remoteNativeProvider{agent: "codex", approvals: map[string]remoteNativeMessage{"question": {Method: "item/tool/requestUserInput", ID: json.RawMessage(`"question"`)}}, events: make(chan remoteProviderEvent, 2)}
	if err := p.Respond(context.Background(), remoteProviderReply{RequestID: "question", Decision: "accept"}); err == nil {
		t.Fatal("wrong question action accepted")
	}
	p.mu.Lock()
	_, ok := p.approvals["question"]
	p.mu.Unlock()
	if !ok {
		t.Fatal("invalid input consumed pending approval")
	}
}

func TestRemoteProviderLateCodexOutputIsDiscarded(t *testing.T) {
	p := &remoteNativeProvider{agent: "codex", nativeID: "thread", runID: "new-run", events: make(chan remoteProviderEvent, 8)}
	p.codexMessage(remoteNativeMessage{Method: "item/agentMessage/delta", Params: json.RawMessage(`{"threadId":"thread","turnId":"old-run","delta":"stale"}`)})
	select {
	case e := <-p.events:
		t.Fatalf("stale event forwarded: %+v", e)
	default:
	}
}

func TestRemoteProviderFakeClaude(t *testing.T) {
	if os.Getenv("NEWNOA_REMOTE_FAKE_CLAUDE") == "1" {
		remoteProviderFakeClaude()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events := make(chan remoteProviderEvent, 32)
	p, err := startRemoteProviderCommand(ctx, "claude", t.TempDir(), "fake-session", func(e remoteProviderEvent) { events <- e }, os.Args[0], []string{"-test.run=^TestRemoteProviderFakeClaude$"}, []string{"NEWNOA_REMOTE_FAKE_CLAUDE=1"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.StartTurn(ctx, remoteProviderInput{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	if err = p.Steer(ctx, remoteProviderInput{Text: "do not queue"}); err == nil {
		t.Fatal("unverified Claude steer accepted")
	}
	var approval *remoteProviderApproval
	for approval == nil {
		select {
		case e := <-events:
			approval = e.Approval
		case <-ctx.Done():
			t.Fatal("question missing")
		}
	}
	if err = p.Respond(ctx, remoteProviderReply{RequestID: approval.ID, Decision: "answer", Answers: map[string][]string{"Choice?": {"A"}}}); err != nil {
		t.Fatal(err)
	}
	if err = p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}
func remoteProviderFakeClaude() {
	d := json.NewDecoder(os.Stdin)
	e := json.NewEncoder(os.Stdout)
	for {
		var m map[string]json.RawMessage
		if d.Decode(&m) != nil {
			return
		}
		var typ string
		json.Unmarshal(m["type"], &typ)
		switch typ {
		case "control_request":
			var request map[string]any
			json.Unmarshal(m["request"], &request)
			e.Encode(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": m["request_id"], "response": map[string]any{}}})
		case "user":
			e.Encode(m)
			e.Encode(map[string]any{"type": "control_request", "request_id": "ask-1", "request": map[string]any{"subtype": "can_use_tool", "tool_name": "AskUserQuestion", "input": map[string]any{"questions": []any{map[string]any{"question": "Choice?", "header": "Choice", "options": []any{map[string]any{"label": "A", "description": "A choice"}}}}}}})
		case "control_response":
			if !strings.Contains(string(m["response"]), `"Choice?":"A"`) {
				os.Exit(4)
			}
		}
	}
}

func TestRemoteProviderLiveResume(t *testing.T) {
	agent := os.Getenv("NEWNOA_REMOTE_LIVE_AGENT")
	if agent == "" {
		t.Skip("explicit opt-in native model resume probe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cwd := t.TempDir()
	t.Cleanup(func() { cleanupRemoteProviderTestProject(t, cwd) })
	events := make(chan remoteProviderEvent, 128)
	emit := func(e remoteProviderEvent) { events <- e }
	p, err := startRemoteProvider(ctx, agent, cwd, "", emit)
	if err != nil {
		t.Fatal(err)
	}
	native := p.(*remoteNativeProvider)
	nativeID := native.nativeID
	t.Logf("native test session %s", nativeID)
	t.Cleanup(func() {
		p.Close()
		if agent == "codex" {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			q, e := startRemoteProvider(cleanupCtx, agent, cwd, nativeID, func(remoteProviderEvent) {})
			if e == nil {
				defer q.Close()
				_, e = q.(*remoteNativeProvider).rpc(cleanupCtx, "thread/archive", map[string]any{"threadId": nativeID})
			}
			if e != nil {
				t.Logf("native test archive incomplete: %v", e)
			}
		}
	})
	wait := func() string {
		var out strings.Builder
		for {
			select {
			case <-ctx.Done():
				t.Fatal("native turn timeout")
				return ""
			case e := <-events:
				if e.Type == "output" {
					out.WriteString(e.Text)
				}
				if e.Model != "" {
					t.Logf("confirmed native model %s", e.Model)
				}
				if e.Type == "error" || e.Type == "approval" {
					t.Fatalf("unexpected native probe event: %+v", e)
				}
				if e.Type == "state" && e.State == "idle" {
					return out.String()
				}
			}
		}
	}
	if err = p.StartTurn(ctx, remoteProviderInput{Text: "Do not use tools. Remember the marker NEWNOA_REMOTE_RESUME_ORCHID. Reply with exactly NEWNOA_REMOTE_RESUME_ORCHID."}); err != nil {
		t.Fatal(err)
	}
	if out := wait(); !strings.Contains(out, "NEWNOA_REMOTE_RESUME_ORCHID") {
		t.Fatalf("first native turn did not confirm marker")
	}
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = startRemoteProvider(ctx, agent, cwd, nativeID, emit)
	if err != nil {
		t.Fatal(err)
	}
	if p.(*remoteNativeProvider).nativeID != nativeID {
		t.Fatal("resume changed native ID")
	}
	if err = p.StartTurn(ctx, remoteProviderInput{Text: "Do not use tools. What marker did I ask you to remember? Reply only with that marker."}); err != nil {
		t.Fatal(err)
	}
	if out := wait(); !strings.Contains(out, "NEWNOA_REMOTE_RESUME_ORCHID") {
		t.Fatalf("resumed session did not retain marker")
	}
}

func TestRemoteProviderCodexUsesNativeDecisionSet(t *testing.T) {
	p := &remoteNativeProvider{agent: "codex", nativeID: "thread", runID: "run", events: make(chan remoteProviderEvent, 8), approvals: map[string]remoteNativeMessage{}}
	p.codexMessage(remoteNativeMessage{ID: json.RawMessage(`"approval"`), Method: "item/commandExecution/requestApproval", Params: json.RawMessage(`{"threadId":"thread","turnId":"run","availableDecisions":["decline"]}`)})
	e := <-p.events
	if e.Approval == nil || len(e.Approval.Options) != 1 || e.Approval.Options[0] != "decline" {
		t.Fatalf("invented native approval options: %+v", e.Approval)
	}
}

func TestRemoteProviderClaudeIgnoresSubagentOutput(t *testing.T) {
	p := &remoteNativeProvider{agent: "claude", runID: "run", nativeID: "session", events: make(chan remoteProviderEvent, 8)}
	p.claudeMessage(remoteNativeMessage{Type: "stream_event", Raw: json.RawMessage(`{"type":"stream_event","session_id":"session","parent_tool_use_id":"tool-child","event":{"delta":{"text":"old child"}}}`)})
	select {
	case e := <-p.events:
		t.Fatalf("child output crossed turn boundary: %+v", e)
	default:
	}
}

func TestRemoteProviderLiveInterrupt(t *testing.T) {
	agent := os.Getenv("NEWNOA_REMOTE_LIVE_AGENT")
	if agent == "" {
		t.Skip("explicit opt-in native model interrupt probe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	events := make(chan remoteProviderEvent, 128)
	binary, err := exec.LookPath(agent)
	if err != nil {
		t.Fatal(err)
	}
	cwd := t.TempDir()
	t.Cleanup(func() { cleanupRemoteProviderTestProject(t, cwd) })
	p, err := startRemoteProviderCommand(ctx, agent, cwd, "", func(e remoteProviderEvent) { events <- e }, binary, nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.StartTurn(ctx, remoteProviderInput{Text: "Do not use tools. Write a detailed 300-word explanation of the numbers one through ten."}); err != nil {
		t.Fatal(err)
	}
	if err = p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case <-ctx.Done():
			t.Fatal("native interrupt never completed")
		case e := <-events:
			if e.Type == "state" && (e.State == "stopped" || e.State == "idle" || e.State == "failed") {
				t.Logf("native interrupt terminal state %s", e.State)
				return
			}
		}
	}
}

func TestRemoteProviderLiveImage(t *testing.T) {
	agent := os.Getenv("NEWNOA_REMOTE_LIVE_AGENT")
	if agent == "" {
		t.Skip("explicit opt-in native model image probe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	events := make(chan remoteProviderEvent, 128)
	binary, err := exec.LookPath(agent)
	if err != nil {
		t.Fatal(err)
	}
	cwd := t.TempDir()
	t.Cleanup(func() { cleanupRemoteProviderTestProject(t, cwd) })
	p, err := startRemoteProviderCommand(ctx, agent, cwd, "", func(e remoteProviderEvent) { events <- e }, binary, nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var encoded bytes.Buffer
	im := image.NewRGBA(image.Rect(0, 0, 32, 32))
	for y := 0; y < 32; y++ {
		for x := 0; x < 32; x++ {
			im.Set(x, y, color.RGBA{R: 255, A: 255})
		}
	}
	if err = png.Encode(&encoded, im); err != nil {
		t.Fatal(err)
	}
	if err = p.StartTurn(ctx, remoteProviderInput{Text: "Do not use tools. Name the solid color of this attached image using one English word.", Images: []remoteProviderImage{{URL: "data:image/png;base64," + base64.StdEncoding.EncodeToString(encoded.Bytes())}}}); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	for {
		select {
		case <-ctx.Done():
			t.Fatal("native image inference timed out")
		case e := <-events:
			if e.Type == "output" {
				output.WriteString(e.Text)
			}
			if e.Type == "error" || e.Type == "approval" {
				t.Fatalf("unexpected native image event: %+v", e)
			}
			if e.Type == "state" && e.State == "idle" {
				if !strings.Contains(strings.ToLower(output.String()), "red") {
					t.Fatal("native image inference did not identify the red image")
				}
				return
			}
		}
	}
}

func cleanupRemoteProviderTestProject(t *testing.T, cwd string) {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	name := strings.ReplaceAll(cwd, string(filepath.Separator), "-")
	dir := filepath.Join(home, ".claude", "projects", name)
	if !strings.Contains(name, "TestRemoteProvider") {
		t.Fatal("refusing non-test project cleanup")
	}
	if info, err := os.Lstat(dir); err == nil && !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
		if err = os.RemoveAll(dir); err != nil {
			t.Logf("native test project cleanup incomplete: %v", err)
		}
	}
}

func TestRemoteProviderExpiredStartupDoesNotLaunch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := startRemoteProviderCommand(ctx, "codex", t.TempDir(), "", func(remoteProviderEvent) {}, "/definitely/missing/native-executable", []string{"ignored"}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("startup did not check operation before launch: %v", err)
	}
}

func TestRemoteProviderLifetimeOutlivesStartupOperation(t *testing.T) {
	if os.Getenv("NEWNOA_REMOTE_LIFETIME_FAKE") == "1" {
		remoteProviderFakeCodex()
		return
	}
	lifetime, stop := context.WithCancel(context.Background())
	defer stop()
	operation, cancel := context.WithCancel(context.Background())
	p, err := startRemoteProviderCommand(remoteProviderContext(lifetime, operation), "codex", t.TempDir(), "", func(remoteProviderEvent) {}, os.Args[0], []string{"-test.run=^TestRemoteProviderLifetimeOutlivesStartupOperation$"}, []string{"NEWNOA_REMOTE_LIFETIME_FAKE=1"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	cancel()
	if err = p.StartTurn(context.Background(), remoteProviderInput{Text: "next operation"}); err != nil {
		t.Fatalf("startup operation cancellation killed owned lifetime: %v", err)
	}
	stop()
	select {
	case <-p.(*remoteNativeProvider).done:
	case <-time.After(3 * time.Second):
		t.Fatal("owned CLI survived lifetime cancellation")
	}
}

func TestRemoteProviderProcessOwnerRecordedBeforeHandshake(t *testing.T) {
	if os.Getenv("NEWNOA_REMOTE_BLOCK_HANDSHAKE") == "1" {
		for {
			time.Sleep(time.Second)
		}
	}
	lifetime, stop := context.WithCancel(context.Background())
	defer stop()
	operation, cancel := context.WithCancel(context.Background())
	defer cancel()
	recorded := make(chan remoteOwnedProcess, 1)
	ctx := context.WithValue(remoteProviderContext(lifetime, operation), remoteProcessOwnerKey{}, func(p remoteOwnedProcess) error { recorded <- p; return nil })
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		p, err := startRemoteProviderCommand(ctx, "codex", t.TempDir(), "", func(remoteProviderEvent) {}, os.Args[0], []string{"-test.run=^TestRemoteProviderProcessOwnerRecordedBeforeHandshake$"}, []string{"NEWNOA_REMOTE_BLOCK_HANDSHAKE=1"})
		if p != nil {
			p.Close()
		}
		result <- err
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		stop()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("blocked native test process cleanup timed out")
		}
	})
	var owned remoteOwnedProcess
	select {
	case owned = <-recorded:
	case <-time.After(time.Second):
		t.Fatal("owned native process was not recorded before handshake completed")
	}
	if owned.PID <= 1 {
		t.Fatal("invalid owned process record")
	}
	cancel()
	if err := <-result; err == nil {
		t.Fatal("cancelled native handshake returned success")
	}
	if alive, err := remoteOwnedProcessAlive(owned); err != nil || alive {
		t.Fatalf("cancelled startup child remained alive: %v %v", alive, err)
	}
}
func TestRemoteProviderOwnerRecordFailureStopsChild(t *testing.T) {
	expected := errors.New("cannot persist owned process intent")
	var owned remoteOwnedProcess
	ctx := context.WithValue(context.Background(), remoteProcessOwnerKey{}, func(p remoteOwnedProcess) error { owned = p; return expected })
	p, err := startRemoteProviderCommand(ctx, "codex", t.TempDir(), "", func(remoteProviderEvent) {}, os.Args[0], []string{"-test.run=^TestRemoteProviderProcessOwnerRecordedBeforeHandshake$"}, []string{"NEWNOA_REMOTE_BLOCK_HANDSHAKE=1"})
	if p != nil {
		defer p.Close()
	}
	if !errors.Is(err, expected) {
		t.Fatalf("record persistence failure did not abort native startup: %v", err)
	}
	if alive, e := remoteOwnedProcessAlive(owned); e != nil || alive {
		t.Fatalf("record failure left native child alive: %v %v", alive, e)
	}
}

func TestRemoteProviderApprovalRetainsSafeNativeToolFields(t *testing.T) {
	data := map[string]any{"file_path": "a.txt", "path": "b.txt", "url": "https://example.test/", "old_string": "before", "new_string": "after", "content": "text", "query": "search", "pattern": "*.go", "api_key": "do-not-forward", "arbitrary": "excluded"}
	raw := remoteApprovalData("Edit", data)
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"file_path", "path", "url", "old_string", "new_string", "content", "query", "pattern"} {
		if got[key] != data[key] {
			t.Fatalf("approval omitted safe native input %s", key)
		}
	}
	if got["api_key"] != nil || got["arbitrary"] != nil {
		t.Fatal("approval forwarded arbitrary tool parameters")
	}
}
