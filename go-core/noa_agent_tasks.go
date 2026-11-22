package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type noaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type noaTask struct {
	owner   [32]byte
	cancel  context.CancelFunc
	result  string
	phase   string
	expires time.Time
}
type noaTasks struct {
	sync.Mutex
	items map[string]*noaTask
}

func (c *core) noaTaskCommand(method string, raw json.RawMessage) (any, error) {
	var p struct {
		Session, ID string
		Messages    []noaMessage
	}
	if len(raw) > 60000 || json.Unmarshal(raw, &p) != nil || p.Session == "" || c.historySessionRevoked(p.Session) {
		return nil, errors.New("invalid Noa Agent session")
	}
	owner := sha256.Sum256([]byte(p.Session))
	jobs := &c.noaTasks
	jobs.Lock()
	defer jobs.Unlock()
	for id, t := range jobs.items {
		if time.Now().After(t.expires) {
			t.cancel()
			delete(jobs.items, id)
		}
	}
	if method != "noa.start" {
		t := jobs.items[p.ID]
		if t == nil || t.owner != owner {
			return nil, errors.New("Noa Agent task unavailable")
		}
		if method == "noa.cancel" {
			t.cancel()
			delete(jobs.items, p.ID)
			return map[string]bool{"stopped": true}, nil
		}
		if method != "noa.poll" {
			return nil, errors.New("invalid Noa Agent method")
		}
		result := map[string]string{"phase": t.phase, "answer": t.result}
		if t.phase == "done" || t.phase == "failed" {
			t.cancel()
			delete(jobs.items, p.ID)
		}
		return result, nil
	}
	if len(p.Messages) == 0 || len(p.Messages) > 29 || len(p.Messages)%2 == 0 {
		return nil, errors.New("invalid Noa Agent context")
	}
	for i, m := range p.Messages {
		if len(m.Content) == 0 || len(m.Content) > 16000 || m.Role != []string{"user", "assistant"}[i%2] {
			return nil, errors.New("invalid Noa Agent message")
		}
	}
	if !noaBundled {
		return nil, errors.New("Noa Agent is unavailable in this Core build")
	}
	if jobs.items == nil {
		jobs.items = make(map[string]*noaTask)
	}
	if len(jobs.items) >= 8 {
		return nil, errors.New("Noa Agent busy")
	}
	for _, t := range jobs.items {
		if t.owner == owner && t.phase != "done" && t.phase != "failed" {
			return nil, errors.New("Noa Agent already working")
		}
	}
	id := uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	task := &noaTask{owner: owner, cancel: cancel, phase: "authorizing", expires: time.Now().Add(2 * time.Minute)}
	jobs.items[id] = task
	go func() {
		defer cancel()
		binding, err := c.resolveNoaBinding(ctx, p.Session)
		var answer string
		if err == nil {
			jobs.Lock()
			task.phase = "running"
			jobs.Unlock()
			answer, err = c.runNoa(ctx, p.Session, binding, p.Messages)
		}
		jobs.Lock()
		defer jobs.Unlock()
		if jobs.items[id] != task {
			return
		}
		task.phase = "done"
		task.result = answer
		if err != nil {
			task.phase = "failed"
			task.result = "runtime"
			var status int
			if _, e := fmt.Sscanf(err.Error(), "Noa Agent failed (gateway HTTP %d)", &status); e == nil && status >= 100 && status <= 599 {
				task.result = fmt.Sprintf("gateway_http_%d", status)
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				task.result = "timeout"
			} else if binding.secret == "" {
				task.result = "authorization"
			}
		}
		// Clear abandoned results even if the client never polls again.
		time.AfterFunc(time.Until(task.expires), func() {
			jobs.Lock()
			defer jobs.Unlock()
			if jobs.items[id] == task {
				delete(jobs.items, id)
			}
		})
	}()
	return map[string]string{"id": id, "phase": "authorizing"}, nil
}

func (c *core) cancelNoaTasks(session string) {
	owner := sha256.Sum256([]byte(session))
	jobs := &c.noaTasks
	jobs.Lock()
	defer jobs.Unlock()
	for id, task := range jobs.items {
		if session == "" || task.owner == owner {
			task.cancel()
			delete(jobs.items, id)
		}
	}
}
