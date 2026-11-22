//go:build noa_pibolt

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

//go:embed skills/noa-client/SKILL.md third_party/pi-bolt/pi-linux-amd64.gz third_party/pi-bolt/LICENSE third_party/pi-bolt/LICENSE.pi third_party/pi-bolt/THIRD_PARTY_NOTICES.md
var noaBundle embed.FS

func noaAgentCLI(action string, out io.Writer) error {
	const base = "third_party/pi-bolt/"
	if action == "--noa-agent-notices" {
		for _, name := range []string{"LICENSE", "LICENSE.pi", "THIRD_PARTY_NOTICES.md"} {
			data, err := noaBundle.ReadFile(base + name)
			if err != nil {
				return err
			}
			if _, err = fmt.Fprintf(out, "%s\n%s\n", name, data); err != nil {
				return err
			}
		}
		return nil
	}
	// Use an isolated directory for this offline smoke check. Never load the user's agent config.
	dir, err := os.MkdirTemp("", "newnoa-noa-agent-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err = stageNoa(dir, "http://127.0.0.1:18222/v1"); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	executable := filepath.Join(dir, "pi")
	skillFile := filepath.Join(dir, "noa-client", "SKILL.md")
	agentDir := filepath.Join(dir, "agent")
	cmd := exec.CommandContext(ctx, executable, "--offline", "--no-tools", "--no-extensions", "--no-mcp", "--no-skills", "--no-context-files", "--no-session", "--skill", skillFile, "--provider", "newnoa", "--model", "spark-2.6-4b", "--thinking", "low", "--mode", "rpc")
	cmd.Dir = dir
	cmd.Env = []string{"HOME=" + dir, "PATH=/usr/bin:/bin", "PI_OFFLINE=1", "PI_CODING_AGENT_DIR=" + agentDir}
	cmd.Stdin = strings.NewReader("{\"id\":\"state\",\"type\":\"get_state\"}\n{\"id\":\"skills\",\"type\":\"get_commands\"}\n")
	raw, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("Noa Agent offline RPC: %w", err)
	}
	var modelOK, skillOK bool
	decoder := json.NewDecoder(bytes.NewReader(raw))
	for {
		var reply struct {
			ID, Type, Command string
			Success           bool
			Data              struct {
				ThinkingLevel string
				Model         struct{ ID, Provider string }
				Commands      []struct{ Name, Source string }
			}
		}
		err = decoder.Decode(&reply)
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.New("invalid Noa Agent reply")
		}
		if !reply.Success || reply.Type != "response" {
			continue
		}
		if reply.ID == "state" && reply.Command == "get_state" {
			modelOK = reply.Data.ThinkingLevel == "low" && reply.Data.Model.ID == "spark-2.6-4b" && reply.Data.Model.Provider == "newnoa"
		}
		if reply.ID == "skills" && reply.Command == "get_commands" {
			for _, item := range reply.Data.Commands {
				if item.Name == "skill:noa-client" && item.Source == "skill" {
					skillOK = true
				}
			}
		}
	}
	if !modelOK || !skillOK {
		return fmt.Errorf("Noa Agent self-check failed (model=%t, skill=%t)", modelOK, skillOK)
	}
	_, err = fmt.Fprintln(out, "Noa Agent | skill:noa-client | newnoa/spark-2.6-4b | low | ephemeral")
	return err
}

const noaBundled = true

func stageNoa(dir, endpoint string) error {
	f, err := noaBundle.Open("third_party/pi-bolt/pi-linux-amd64.gz")
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	executable := filepath.Join(dir, "pi")
	dst, err := os.OpenFile(executable, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0700)
	if err != nil {
		return err
	}
	_, err = io.Copy(dst, gz)
	closeErr := dst.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	skill, err := noaBundle.ReadFile("skills/noa-client/SKILL.md")
	if err != nil {
		return err
	}
	skillDir := filepath.Join(dir, "noa-client")
	if err = os.Mkdir(skillDir, 0700); err != nil {
		return err
	}
	skillFile := filepath.Join(skillDir, "SKILL.md")
	if err = os.WriteFile(skillFile, skill, 0600); err != nil {
		return err
	}
	agentDir := filepath.Join(dir, "agent")
	if err = os.Mkdir(agentDir, 0700); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(agentDir, "settings.json"), []byte(`{"enableInstallTelemetry":false,"retry":{"enabled":false},"compaction":{"enabled":false}}`), 0600); err != nil {
		return err
	}
	profile := fmt.Sprintf(`{"providers":{"newnoa":{"baseUrl":"%s","api":"openai-completions","apiKey":"$NEWNOA_NOA_API_KEY","models":[{"id":"spark-2.6-4b","name":"spark-2.6-4b","reasoning":true}]}}}`, endpoint)
	if err = os.WriteFile(filepath.Join(agentDir, "models.json"), []byte(profile), 0600); err != nil {
		return err
	}
	return nil
}

// Only this turn's private Pi process can submit chat completions. Core attaches the
// account credential itself and routes through normal gateway authorization and metering.
func (c *core) runNoa(ctx context.Context, session string, binding noaBinding, messages []noaMessage) (string, error) {
	inference, err := privateInferenceWithSelector(c.control, c.gateway, c.gatewaySelector)
	if err != nil {
		return "", err
	}
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	token := uuid.NewString()
	var gatewayStatus atomic.Int32
	var executionPhase atomic.Bool
	server := &http.Server{ReadHeaderTimeout: 3 * time.Second, MaxHeaderBytes: 8192, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/chat/completions" || r.URL.RawQuery != "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 || c.historySessionRevoked(session) {
			http.Error(w, "unauthorized", 403)
			return
		}
		var body map[string]json.RawMessage
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body) != nil || string(body["model"]) != "\"spark-2.6-4b\"" {
			http.Error(w, "invalid model request", 400)
			return
		}
		// Spark's chat template defaults to high even when the agent selected low.
		body["chat_template_kwargs"] = json.RawMessage(`{"reasoning_effort":"low"}`)
		body["reasoning_effort"] = json.RawMessage(`"low"`)
		// Constrain the wire format while leaving the semantic choice to Spark.
		if executionPhase.Load() {
			body["response_format"] = json.RawMessage(`{"type":"json_schema","json_schema":{"name":"client_action","schema":{"type":"object","properties":{"action":{"type":"string","enum":["decline","reply","charityKey","points","records","balance","news","tickets","usage","proxy","catalog","favorites","myModels","publish","open"]},"text":{"type":"string"},"endpoint":{"type":"string"},"page":{"type":"string"},"tab":{"type":"string"},"days":{"type":"integer","minimum":1,"maximum":365},"modelOnly":{"type":"boolean"},"direction":{"enum":["supply","consumption"]},"category":{"enum":["text","image"]}},"required":["action"],"additionalProperties":false}}}`)
		} else {
			body["response_format"] = json.RawMessage(`{"type":"json_schema","json_schema":{"name":"client_scope","schema":{"type":"object","properties":{"scope":{"enum":["client","outside","internal"]},"relation":{"enum":["new","continue"]}},"required":["scope","relation"],"additionalProperties":false}}}`)
		}
		body["model"], _ = json.Marshal(binding.deploymentID)
		raw, _ := json.Marshal(body)
		req, err := http.NewRequestWithContext(ctx, "POST", "http://localhost/v1/chat/completions", bytes.NewReader(raw))
		if err != nil {
			http.Error(w, "request unavailable", 500)
			return
		}
		req.RemoteAddr = r.RemoteAddr
		req.GetBody = nil
		req.Header.Set("Idempotency-Key", uuid.NewString())
		req.Header.Set("X-Newnoa-Agent", "Noa Agent")
		req.Header.Set("Authorization", "Bearer "+session)
		req.Header.Set("X-Api-Key", binding.secret)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "text/event-stream")
		writer := &coreStatusWriter{ResponseWriter: w, status: http.StatusOK}
		inference.ServeHTTP(writer, req)
		gatewayStatus.Store(int32(writer.status))
	})}
	go func() { _ = server.Serve(l) }()
	defer server.Close()
	dir, err := os.MkdirTemp("", "newnoa-noa-agent-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	if err = stageNoa(dir, "http://"+l.Addr().String()+"/v1"); err != nil {
		return "", err
	}
	skill := filepath.Join(dir, "noa-client", "SKILL.md")
	rules, err := os.ReadFile(skill)
	if err != nil {
		return "", err
	}
	boundary, execution, ok := strings.Cut(string(rules), "## 返回宿主决策")
	if !ok {
		return "", errors.New("incomplete Noa Agent skill")
	}
	ask := func(instruction string, history []noaMessage) (string, error) {
		cmd := exec.CommandContext(ctx, filepath.Join(dir, "pi"), "--offline", "--no-tools", "--no-extensions", "--no-mcp", "--no-context-files", "--no-skills", "--no-session", "--no-prompt-templates", "--skill", skill, "--system-prompt", instruction, "--provider", "newnoa", "--model", noaModel, "--thinking", "low", "--print", "--mode", "text")
		cmd.Env = []string{"HOME=" + dir, "PATH=/usr/bin:/bin", "PI_OFFLINE=1", "PI_CODING_AGENT_DIR=" + filepath.Join(dir, "agent"), "NEWNOA_NOA_API_KEY=" + token}
		cmd.Dir = dir
		prompt, _ := json.Marshal(history)
		cmd.Stdin = bytes.NewReader(append([]byte("以下是对话数据。处理最后一条消息。只输出所要求的 JSON。\n"), prompt...))
		raw, err := cmd.Output()
		if err != nil {
			return "", fmt.Errorf("Noa Agent failed (gateway HTTP %d)", gatewayStatus.Load())
		}
		answer := strings.TrimSpace(string(raw))
		if answer == "" || len(answer) > 16000 {
			return "", errors.New("invalid Noa Agent answer")
		}
		answer = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(answer, "```json"), "```"))
		return answer, nil
	}
	classification, err := ask(boundary, messages)
	if err != nil {
		return "", err
	}
	var scope struct {
		Scope    string
		Relation string
	}
	if json.Unmarshal([]byte(classification), &scope) != nil || (scope.Relation != "new" && scope.Relation != "continue") {
		return "", errors.New("invalid Noa Agent scope")
	}
	fresh := len(messages) == 1 || scope.Relation == "new"
	if scope.Scope == "outside" || scope.Scope == "internal" {
		return fmt.Sprintf(`{"action":"decline","fresh":%t}`, fresh), nil
	}
	if scope.Scope != "client" {
		return "", errors.New("unknown Noa Agent scope")
	}
	if fresh {
		messages = messages[len(messages)-1:]
	}
	identity, _, _ := strings.Cut(boundary, "## 职责预判")
	executionPhase.Store(true)
	result, err := ask(identity+"\n## 返回宿主决策"+execution, messages)
	if err != nil {
		return "", err
	}
	var decision map[string]json.RawMessage
	if json.Unmarshal([]byte(result), &decision) != nil || decision == nil {
		return "", errors.New("invalid Noa Agent decision")
	}
	decision["fresh"], _ = json.Marshal(fresh)
	raw, err := json.Marshal(decision)
	return string(raw), err
}
