package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const svgContentLimit = 1024 * 1024
const svgStreamLimit = 64 * 1024 * 1024

type svgModelTestContext struct{}

type svgTest struct {
	ID            string `json:"id"`
	QuestionID    int    `json:"questionId"`
	QuestionName  string `json:"questionName"`
	Prompt        string `json:"prompt"`
	KeyID         string `json:"keyId"`
	Model         string `json:"model"`
	ModelName     string `json:"modelName"`
	SubmissionID  string `json:"submissionId"`
	Thinking      bool   `json:"thinking"`
	Status        string `json:"status"`
	CreatedAt     string `json:"createdAt"`
	StartedAt     string `json:"startedAt,omitempty"`
	CompletedAt   string `json:"completedAt,omitempty"`
	Content       string `json:"content,omitempty"`
	FinishReason  string `json:"finishReason,omitempty"`
	Error         string `json:"error,omitempty"`
	InputTokens   *int64 `json:"inputTokens,omitempty"`
	OutputTokens  *int64 `json:"outputTokens,omitempty"`
	Points        any    `json:"points,omitempty"`
	cancel        context.CancelFunc
	callerIP      string
	contentOnDisk bool
	unsaved       bool
}
type svgTestStore struct {
	changed   func()
	directory string
	jobs      map[string]*svgTest
	workers   int
}

func svgNow() string               { return time.Now().UTC().Format(time.RFC3339Nano) }
func svgPending(job *svgTest) bool { return job.Status == "queued" || job.Status == "running" }
func svgPublic(job *svgTest, detail bool) map[string]any {
	row := map[string]any{"id": job.ID, "questionId": job.QuestionID, "questionName": job.QuestionName, "keyId": job.KeyID, "model": job.Model, "modelName": job.ModelName, "thinking": job.Thinking, "status": job.Status, "createdAt": job.CreatedAt, "startedAt": job.StartedAt, "completedAt": job.CompletedAt, "finishReason": job.FinishReason, "error": publicStatusError("svg.tests.get", job.Error), "inputTokens": job.InputTokens, "outputTokens": job.OutputTokens, "points": job.Points}
	if detail {
		row["prompt"] = job.Prompt
		row["content"] = job.Content
	}
	return row
}
func (c *core) svgStore(account string) (*svgTestStore, error) {
	scope := hash([]byte(strings.TrimRight(c.control, "/") + "\n" + account))
	if c.svgStores == nil {
		c.svgStores = map[string]*svgTestStore{}
	}
	if c.publicStateless {
		// Keep active job pointers stable; completed account stores can be reloaded from disk.
		for key, store := range c.svgStores {
			pending := store.workers > 0
			for _, job := range store.jobs {
				pending = pending || svgPending(job) || job.unsaved
			}
			if !pending {
				delete(c.svgStores, key)
			}
		}
	}
	if value := c.svgStores[scope]; value != nil {
		return value, nil
	}
	if c.publicStateless && len(c.svgStores) >= 32 {
		return nil, errors.New("SVG test active or storage limit reached")
	}
	kind := "svg-tests"
	if c.publicStateless {
		kind = "www-svg-tests"
	}
	directory := filepath.Join(c.home, ".newnoa", kind, scope)
	// Check every parent before reading or creating private local task state.
	if err := c.safeConfigFile(filepath.Join(directory, "index")); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(directory)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	store := &svgTestStore{directory: directory, jobs: map[string]*svgTest{}}
	if !c.publicStateless {
		store.changed = func() { c.historyEvents.publish(account, historySVGChanged) }
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".newnoa-") {
			continue
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || uuid.Validate(strings.TrimSuffix(entry.Name(), ".json")) != nil {
			return nil, errors.New("Invalid SVG test storage")
		}
		file := filepath.Join(directory, entry.Name())
		info, err := os.Lstat(file)
		if err != nil || !info.Mode().IsRegular() || info.Size() > 8*svgContentLimit {
			return nil, errors.New("Invalid SVG test storage")
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var job svgTest
		if json.Unmarshal(raw, &job) != nil || job.ID+".json" != entry.Name() || len(job.Content) > svgContentLimit || job.QuestionID < 1 || job.QuestionID > 10 {
			return nil, errors.New("Invalid SVG test storage")
		}
		if len(store.jobs) >= 100 {
			return nil, errors.New("SVG test storage limit reached")
		}
		if svgPending(&job) {
			job.Status = "failed"
			job.Error = "SVG test interrupted; start a new test to retry"
			job.CompletedAt = svgNow()
			if err := store.save(&job); err != nil {
				return nil, err
			}
		}
		if c.publicStateless && job.Content != "" {
			job.Content, job.contentOnDisk = "", true
		}
		store.jobs[job.ID] = &job
	}
	c.svgStores[scope] = store
	return store, nil
}
func (s *svgTestStore) save(job *svgTest) error {
	raw, err := json.Marshal(job)
	if err != nil {
		job.unsaved = true
		return err
	}
	err = atomicFile(filepath.Join(s.directory, job.ID+".json"), raw)
	job.unsaved = err != nil
	if err == nil && s.changed != nil {
		s.changed()
	}
	return err
}

func (s *svgTestStore) detail(job *svgTest) (map[string]any, error) {
	if !job.contentOnDisk {
		return svgPublic(job, true), nil
	}
	file := filepath.Join(s.directory, job.ID+".json")
	info, err := os.Lstat(file)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 8*svgContentLimit {
		return nil, errors.New("Invalid SVG test storage")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var saved svgTest
	if json.Unmarshal(raw, &saved) != nil || saved.ID != job.ID || len(saved.Content) > svgContentLimit {
		return nil, errors.New("Invalid SVG test storage")
	}
	copy := *job
	copy.Content = saved.Content
	return svgPublic(&copy, true), nil
}

func (c *core) cancelSVGTests() {
	c.svgMu.Lock()
	defer c.svgMu.Unlock()
	saved := true
	for _, store := range c.svgStores {
		for _, job := range store.jobs {
			if svgPending(job) {
				if job.cancel != nil {
					job.cancel()
				}
				job.Status = "cancelled"
				job.Error = "SVG test stopped because the proxy or account changed"
				job.CompletedAt = svgNow()
				if err := store.save(job); err != nil {
					job.Error = "SVG test stopped; result could not be saved"
					saved = false
				}
			}
		}
	}
	if saved {
		c.svgStores = nil
	}
}

func (c *core) svgCommand(method string, raw json.RawMessage) (any, error) {
	return c.svgCommandWithCaller(method, raw, "")
}

func (c *core) svgCommandWithCaller(method string, raw json.RawMessage, callerIP string) (any, error) {
	var input struct {
		Session      string `json:"session"`
		ID           string `json:"id"`
		QuestionID   int    `json:"questionId"`
		KeyID        string `json:"keyId"`
		Model        string `json:"model"`
		SubmissionID string `json:"submissionId"`
		Thinking     bool   `json:"thinking"`
		Page         int    `json:"page"`
		Status       string `json:"status"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("Invalid SVG test command")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || bytes.Equal(bytes.TrimSpace(fields["thinking"]), []byte("null")) {
		return nil, errors.New("Invalid SVG test command")
	}
	allowed := map[string]bool{"svg.tests.questions": true, "svg.tests.create": true, "svg.tests.list": true, "svg.tests.get": true, "svg.tests.cancel": true, "svg.tests.delete": true}
	if !allowed[method] {
		return nil, errors.New("Invalid SVG test command")
	}
	if method == "svg.tests.create" {
		c.mu.Lock()
		running := c.publicStateless || c.server != nil
		c.mu.Unlock()
		if !running {
			return nil, errors.New("Enable the proxy before model testing")
		}
	}
	var account string
	var err error
	if c.publicStateless {
		account, err = c.publicSVGAccount(input.Session)
	} else {
		account, err = c.historyIdentity(input.Session)
	}
	if err != nil {
		return nil, err
	}
	if method == "svg.tests.questions" {
		return svgQuestions, nil
	}
	if method == "svg.tests.create" {
		if input.QuestionID < 1 || input.QuestionID > len(svgQuestions) || uuid.Validate(input.KeyID) != nil || uuid.Validate(input.Model) != nil || uuid.Validate(input.SubmissionID) != nil {
			return nil, errors.New("Invalid SVG test selection")
		}
	}
	c.svgMu.Lock()
	store, err := c.svgStore(account)
	if err != nil {
		c.svgMu.Unlock()
		return nil, err
	}
	if method == "svg.tests.create" {
		for _, job := range store.jobs {
			if job.SubmissionID == input.SubmissionID {
				if job.QuestionID != input.QuestionID || job.KeyID != input.KeyID || job.Model != input.Model || job.Thinking != input.Thinking {
					c.svgMu.Unlock()
					return nil, errors.New("SVG test submission identity changed")
				}
				row, err := store.detail(job)
				c.svgMu.Unlock()
				return row, err
			}
		}
		if len(store.jobs) >= 100 {
			c.svgMu.Unlock()
			return nil, errors.New("SVG test storage limit reached; delete old results")
		}
		active := 0
		for _, job := range store.jobs {
			if svgPending(job) {
				active++
			}
		}
		if active >= 3 {
			c.svgMu.Unlock()
			return nil, errors.New("SVG test active task limit reached")
		}
		// Revalidate selection with the existing user/key model catalogue; no task-specific server credential exists.
		c.svgMu.Unlock()
		var keys []struct {
			ID        string `json:"id"`
			Paused    bool   `json:"paused"`
			RevokedAt any    `json:"revoked_at"`
			Expired   bool   `json:"expired"`
			Models    []struct {
				ID        string `json:"id"`
				Name      string `json:"name"`
				ModelType string `json:"modelType"`
				Online    bool   `json:"online"`
			} `json:"models"`
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		err = c.controlRemote(ctx, "/me/model-tests/keys", "Bearer "+input.Session, nil, &keys)
		cancel()
		if err != nil {
			return nil, err
		}
		name := ""
		for _, key := range keys {
			if key.ID == input.KeyID && !key.Paused && key.RevokedAt == nil && !key.Expired {
				for _, model := range key.Models {
					if model.ID == input.Model && model.ModelType == "llm" && model.Online {
						name = model.Name
					}
				}
			}
		}
		if name == "" {
			return nil, errors.New("Selected SVG model is unavailable or not authorized")
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.proxySessionMu.RLock()
		session, sessionCtx := c.proxySession, c.proxySessionContext
		c.proxySessionMu.RUnlock()
		if c.publicStateless {
			session, sessionCtx = input.Session, c.publicSVGContext
		}
		if (!c.publicStateless && c.server == nil) || session != input.Session || sessionCtx == nil || sessionCtx.Err() != nil {
			return nil, errors.New("Enable the proxy before model testing")
		}
		c.svgMu.Lock()
		defer c.svgMu.Unlock()
		store, err = c.svgStore(account)
		if err != nil {
			return nil, err
		}
		// The catalogue round trip may race another submission; check identity and limits again.
		for _, job := range store.jobs {
			if job.SubmissionID == input.SubmissionID {
				if job.QuestionID != input.QuestionID || job.KeyID != input.KeyID || job.Model != input.Model || job.Thinking != input.Thinking {
					return nil, errors.New("SVG test submission identity changed")
				}
				return store.detail(job)
			}
		}
		active = 0
		for _, job := range store.jobs {
			if svgPending(job) {
				active++
			}
		}
		if len(store.jobs) >= 100 || active >= 3 {
			return nil, errors.New("SVG test active or storage limit reached")
		}
		if c.publicStateless {
			reserved := 0
			for _, cached := range c.svgStores {
				for _, running := range cached.jobs {
					if svgPending(running) {
						reserved += svgContentLimit
					}
					if running.unsaved {
						reserved += len(running.Content)
					}
				}
			}
			if reserved+svgContentLimit > 32*svgContentLimit {
				return nil, errors.New("SVG test active task limit reached")
			}
		}
		question := svgQuestions[input.QuestionID-1]
		if !c.updateGate.begin() {
			return nil, errors.New("Client update installation is starting")
		}
		if c.publicStateless {
			if c.publicSVGSlots == nil {
				c.publicSVGSlots = make(chan struct{}, 16)
			}
			select {
			case c.publicSVGSlots <- struct{}{}:
			default:
				c.updateGate.end()
				return nil, errors.New("SVG test active task limit reached")
			}
		}
		jobCtx, stop := context.WithTimeout(context.WithValue(sessionCtx, svgModelTestContext{}, true), inferenceWrapperTimeout)
		job := &svgTest{ID: uuid.NewString(), QuestionID: input.QuestionID, QuestionName: question.Name, Prompt: question.Prompt, KeyID: input.KeyID, Model: input.Model, ModelName: name, SubmissionID: input.SubmissionID, Thinking: input.Thinking, Status: "running", CreatedAt: svgNow(), StartedAt: svgNow(), cancel: stop}
		job.callerIP = callerIP
		if err = store.save(job); err != nil {
			stop()
			c.updateGate.end()
			if c.publicStateless {
				<-c.publicSVGSlots
			}
			return nil, err
		}
		store.jobs[job.ID] = job
		store.workers++
		c.svgWorkers.Add(1)
		go c.runSVGTest(jobCtx, store, job, input.Session)
		return store.detail(job)
	}
	defer c.svgMu.Unlock()
	if method == "svg.tests.list" {
		if input.Page == 0 {
			input.Page = 1
		}
		if input.Page < 1 || input.Page > 100000 {
			return nil, errors.New("Invalid SVG test page")
		}
		if input.Status != "" && input.Status != "queued" && input.Status != "running" && input.Status != "succeeded" && input.Status != "failed" && input.Status != "cancelled" {
			return nil, errors.New("Invalid SVG test status")
		}
		jobs := make([]*svgTest, 0, len(store.jobs))
		for _, job := range store.jobs {
			if input.Status == "" || job.Status == input.Status {
				jobs = append(jobs, job)
			}
		}
		sort.Slice(jobs, func(i, j int) bool { return jobs[i].CreatedAt > jobs[j].CreatedAt })
		rows := []map[string]any{}
		start := (input.Page - 1) * 20
		for i := start; i < len(jobs) && i < start+20; i++ {
			rows = append(rows, svgPublic(jobs[i], false))
		}
		return map[string]any{"items": rows, "total": len(jobs), "page": input.Page, "pageSize": 20}, nil
	}
	job := store.jobs[input.ID]
	if job == nil {
		return nil, errors.New("SVG test not found")
	}
	switch method {
	case "svg.tests.get":
		return store.detail(job)
	case "svg.tests.cancel":
		if svgPending(job) {
			if job.cancel != nil {
				job.cancel()
			}
			job.Status = "cancelled"
			job.CompletedAt = svgNow()
			if err = store.save(job); err != nil {
				return nil, err
			}
		}
		return store.detail(job)
	case "svg.tests.delete":
		if svgPending(job) {
			return nil, errors.New("Cancel the SVG test before deleting it")
		}
		if err = os.Remove(filepath.Join(store.directory, job.ID+".json")); err != nil {
			return nil, err
		}
		delete(store.jobs, job.ID)
		if store.changed != nil {
			store.changed()
		}
		return map[string]bool{"deleted": true}, nil
	}
	return nil, errors.New("Invalid SVG test command")
}

type svgResponse struct {
	header          http.Header
	status          int
	body            bytes.Buffer
	event           bytes.Buffer
	content         strings.Builder
	result          svgTest
	size            int
	done, reasoning bool
	err             error
	cancel          context.CancelFunc
}

func (w *svgResponse) Header() http.Header { return w.header }
func (w *svgResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *svgResponse) Flush() {}
func (w *svgResponse) fail(message string) error {
	if w.err == nil {
		w.err = errors.New(message)
		if w.cancel != nil {
			w.cancel()
		}
	}
	return w.err
}
func (w *svgResponse) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.size+len(value) > svgStreamLimit {
		return 0, w.fail("SVG response exceeds limit")
	}
	w.size += len(value)
	size := len(value)
	for len(value) > 0 {
		i := bytes.IndexByte(value, '\n')
		n := len(value)
		if i >= 0 {
			n = i + 1
		}
		if w.body.Len()+w.event.Len()+n > modelTestResponseLimit {
			return 0, w.fail("SVG response exceeds limit")
		}
		w.body.Write(value[:n])
		value = value[n:]
		if i < 0 {
			break
		}
		line := bytes.TrimSuffix(w.body.Bytes()[:w.body.Len()-1], []byte("\r"))
		if len(line) == 0 {
			if w.event.Len() > 0 {
				w.parseEvent(bytes.TrimSuffix(w.event.Bytes(), []byte("\n")))
				w.event.Reset()
			}
		} else if bytes.HasPrefix(line, []byte("data:")) {
			w.event.Write(bytes.TrimPrefix(line[5:], []byte(" ")))
			w.event.WriteByte('\n')
		}
		w.body.Reset()
		if w.err != nil {
			return 0, w.err
		}
	}
	return size, nil
}
func (w *svgResponse) parseEvent(raw []byte) {
	if w.done {
		w.fail("Invalid SVG model stream")
		return
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("[DONE]")) {
		w.done = true
		return
	}
	var chunk struct {
		Error   json.RawMessage `json:"error"`
		Choices []struct {
			Delta struct {
				Content   string `json:"content"`
				Reasoning string `json:"reasoning_content"`
			} `json:"delta"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			Input  *int64 `json:"prompt_tokens"`
			Output *int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if !utf8.Valid(raw) || !bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{")) || json.Unmarshal(raw, &chunk) != nil || len(chunk.Error) > 0 && string(chunk.Error) != "null" {
		w.fail("Invalid SVG model stream")
		return
	}
	for _, choice := range chunk.Choices {
		w.reasoning = w.reasoning || choice.Delta.Reasoning != ""
		if w.content.Len()+len(choice.Delta.Content) > svgContentLimit {
			w.fail("SVG content exceeds limit")
			return
		}
		w.content.WriteString(choice.Delta.Content)
		if choice.FinishReason != nil {
			w.result.FinishReason = *choice.FinishReason
		}
	}
	if chunk.Usage != nil {
		if chunk.Usage.Input != nil && *chunk.Usage.Input < 0 || chunk.Usage.Output != nil && *chunk.Usage.Output < 0 {
			w.fail("Invalid SVG model usage")
			return
		}
		w.result.InputTokens, w.result.OutputTokens = chunk.Usage.Input, chunk.Usage.Output
	}
}
func (c *core) runSVGTest(ctx context.Context, store *svgTestStore, job *svgTest, session string) {
	defer c.svgWorkers.Done()
	defer func() {
		c.svgMu.Lock()
		store.workers--
		c.svgMu.Unlock()
		if c.publicStateless {
			<-c.publicSVGSlots
		}
	}()
	defer c.updateGate.end()
	defer job.cancel()
	if c.publicStateless {
		go c.guardPublicSVGSession(ctx, session, job.cancel)
	}
	payload, _ := json.Marshal(map[string]any{"keyId": job.KeyID, "model": job.Model, "conversationId": job.ID, "requestId": job.ID, "maxTokens": 32768, "thinking": job.Thinking, "messages": []map[string]string{{"role": "user", "content": job.Prompt}}})
	writer := &svgResponse{header: make(http.Header), cancel: job.cancel}
	var handler http.Handler
	var err error
	if c.publicStateless {
		handler = c.publicSVGModelTestHandler()
	} else {
		handler, err = c.modelTestHandler()
	}
	if err == nil {
		request, _ := http.NewRequestWithContext(ctx, "POST", "http://localhost/api/local/model-test", bytes.NewReader(payload))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+session)
		if job.callerIP != "" {
			request.Header.Set("X-Newnoa-Caller-IP", job.callerIP)
		}
		handler.ServeHTTP(writer, request)
	}
	result, parseErr := parseSVGResponse(writer)
	if err == nil {
		err = parseErr
	}
	if ctx.Err() != nil && writer.err == nil {
		err = ctx.Err()
	}
	// The response headers carry the ordinary inference request ID; hydrate only trusted consumption metadata.
	var consumption map[string]any
	requestID := writer.header.Get("X-Newnoa-Request")
	if requestID == "" {
		requestID = writer.header.Get("X-Request-Id")
	}
	if err == nil && uuid.Validate(requestID) == nil {
		lookup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		_ = c.controlRemote(lookup, "/client/requests/"+requestID+"/consumption", "Bearer "+session, nil, &consumption)
		stop()
	}
	c.svgMu.Lock()
	defer c.svgMu.Unlock()
	if !svgPending(job) {
		return
	}
	job.CompletedAt = svgNow()
	job.Content = result.Content
	job.FinishReason = result.FinishReason
	job.InputTokens = result.InputTokens
	job.OutputTokens = result.OutputTokens
	if err != nil {
		job.Status = "failed"
		job.Error = "SVG test failed; verify the proxy and model before retrying"
		if parseErr != nil {
			job.Error = parseErr.Error()
		}
		if ctx.Err() != nil && writer.err == nil {
			job.Status = "cancelled"
			job.Error = "SVG test stopped because the proxy or account changed"
		}
	} else {
		job.Status = "succeeded"
		if consumption["pointsStatus"] == "known" {
			job.Points = consumption["pointsConsumed"]
		}
	}
	if saveErr := store.save(job); saveErr != nil {
		job.Error = "SVG result could not be saved; keep this page open"
	} else if c.publicStateless && job.Content != "" {
		job.Content, job.contentOnDisk = "", true
	}
}
func parseSVGResponse(w *svgResponse) (svgTest, error) {
	result := w.result
	result.Content = w.content.String()
	if w.status != 200 || !strings.HasPrefix(w.header.Get("Content-Type"), "text/event-stream") {
		return result, errors.New("SVG model request failed")
	}
	if w.err != nil {
		return result, w.err
	}
	if result.FinishReason == "length" {
		if result.Content == "" && w.reasoning {
			return result, errors.New("SVG thinking used the output limit before drawing; disable thinking and retry")
		}
		return result, errors.New("SVG output reached the token limit; saved content is incomplete")
	}
	if !w.done || w.body.Len() > 0 || w.event.Len() > 0 {
		return result, errors.New("SVG model stream ended early; saved content may be incomplete")
	}
	if result.Content == "" && w.reasoning {
		return result, errors.New("SVG model returned thinking without drawing; disable thinking and retry")
	}
	if result.FinishReason != "stop" || !strings.Contains(strings.ToLower(result.Content), "<svg") || !strings.Contains(strings.ToLower(result.Content), "</svg>") {
		return result, errors.New("SVG model did not return a complete SVG; check the returned content")
	}
	return result, nil
}
