package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const remoteHistoryLimit = 24

type remoteHistorySession struct {
	DetailAvailable bool   `json:"detailAvailable,omitempty"`
	MessageCount    int    `json:"messageCount"`
	ID              string `json:"id"`
	Cwd             string `json:"cwd"`
	Agent           string `json:"agent"`
	State           string `json:"state"`
	Title           string `json:"title"`
	UpdatedAt       string `json:"updatedAt"`
}

func historySessionSummary(s *remoteSession) remoteHistorySession {
	title := filepath.Base(s.Cwd)
	for _, msg := range s.Messages {
		if msg.Role == "user" && strings.TrimSpace(msg.Text) != "" {
			firstLine, _, _ := strings.Cut(strings.TrimSpace(msg.Text), "\n")
			title = historyPrefix(firstLine, 160)
			break
		}
	}
	return remoteHistorySession{
		ID: s.ID, Cwd: s.Cwd, Agent: s.Agent, State: s.State, Title: title, UpdatedAt: s.UpdatedAt,
		MessageCount: len(s.Messages), DetailAvailable: len(s.Messages) > 0,
	}
}

func historySessionTitle(s *remoteSession) string {
	return historySessionSummary(s).Title
}

func historyPrefix(text string, limit int) string {
	if len(text) <= limit {
		return text
	}
	for limit > 0 && !utf8.RuneStart(text[limit]) {
		limit--
	}
	return text[:limit]
}

func historyOlder(aTime, aID, bTime, bID string) bool {
	a, _ := time.Parse(time.RFC3339Nano, aTime)
	b, _ := time.Parse(time.RFC3339Nano, bTime)
	if a.Equal(b) {
		return aID < bID
	}
	return a.Before(b)
}

func (m *remoteManager) archiveSessionLocked(s *remoteSession) {
	m.history = append(m.history, s)
	sort.Slice(m.history, func(i, j int) bool {
		a, b := m.history[i], m.history[j]
		return historyOlder(b.UpdatedAt, b.ID, a.UpdatedAt, a.ID)
	})
	if len(m.history) > remoteHistoryLimit {
		m.history = slices.Delete(m.history, remoteHistoryLimit, len(m.history))
	}
}

func (m *remoteManager) archiveCandidateLocked() *remoteSession {
	var oldest *remoteSession
	for _, s := range m.sessions {
		// A legacy row without workspace identity cannot be safely listed.
		if s.Agent == "" || s.Cwd == "" {
			continue
		}
		if s.State != "idle" && s.State != "stopped" && s.State != "failed" {
			continue
		}
		if s.pendingJobs != 0 || m.switching[s.Agent] || (m.providers[s.ID] == nil && (s.OwnedProcess.PID > 1 || s.OwnedGateway != nil)) {
			continue
		}
		if slices.ContainsFunc(s.Approvals, func(a *remoteApproval) bool { return a.State == "open" }) {
			continue
		}
		if oldest == nil || historyOlder(s.UpdatedAt, s.ID, oldest.UpdatedAt, oldest.ID) {
			oldest = s
		}
	}
	return oldest
}

// Holds shutdownMu and m.mu on entry and return. Closing may synchronously emit provider events.
func (m *remoteManager) detachForArchiveLocked(s *remoteSession, validate func(context.Context) error) error {
	if validate != nil {
		// Grant verification can block. Let native callbacks update the session and recheck it.
		revision := s.Revision
		m.mu.Unlock()
		err := validate(m.ctx)
		m.mu.Lock()
		if err != nil {
			return err
		}
		if s.Revision != revision || m.archiveCandidateLocked() != s {
			return errors.New(remoteSessionLimitError)
		}
	}
	p := m.providers[s.ID]
	if p == nil {
		return nil
	}
	s.State = "stopping"
	delete(m.generations, s.ID)
	m.changedLocked(s, "", "session.detaching", nil)
	m.mu.Unlock()
	err := p.Close()
	m.mu.Lock()
	if err != nil {
		s.State = "unknown"
		s.Error = remoteSafeError(err)
		m.changedLocked(s, "", "session.detachFailed", nil)
		return errors.Join(err, m.saveLocked())
	}
	delete(m.providers, s.ID)
	s.OwnedProcess = remoteOwnedProcess{}
	s.OwnedGateway = nil
	s.State = "stopped"
	m.changedLocked(s, "", "session.detached", nil)
	return nil
}

const remoteSnapshotLimit = 512 * 1024

func (m *remoteManager) addHistoryProjectionLocked(snap map[string]any) {
	snap["historySessions"] = []any{}
	if len(m.history) == 0 {
		return
	}
	for _, s := range m.history {
		row := historySessionSummary(s)
		raw, _ := json.Marshal(row)
		var value map[string]any
		_ = json.Unmarshal(raw, &value)
		snap["historySessions"] = append(snap["historySessions"].([]any), value)
	}
}

const remoteHistoryPageExpired = "History page expired; read the page again"

type remoteHistoryPageRequest struct {
	Before *int    `json:"before,omitempty"`
	Limit  *int    `json:"limit,omitempty"`
	Offset *int    `json:"offset,omitempty"`
	PageID *string `json:"pageId,omitempty"`
}

// Frozen bytes keep a streaming assistant message consistent between reads.
type remoteHistoryPage struct {
	sessionID                string
	start, end, total, limit int
	data                     []byte
	lastRead                 time.Time
}

func (p *remoteHistoryPage) chunk(id string, offset int) map[string]any {
	next := min(offset+4096, len(p.data))
	for next < len(p.data) && !utf8.RuneStart(p.data[next]) {
		next--
	}
	return map[string]any{
		"sessionId": p.sessionID, "start": p.start, "end": p.end, "total": p.total, "hasOlder": p.start > 0,
		"pageId": id, "offset": offset, "nextOffset": next, "totalBytes": len(p.data), "content": string(p.data[offset:next]),
	}
}

func (m *remoteManager) historyPageReceiptLocked(c remoteCommandEnvelope, digest string, req remoteHistoryPageRequest) (remoteReceipt, error) {
	limit := *req.Limit
	if limit < 1 || limit > 20 || (req.Before != nil && (*req.Before < 0 || *req.Before > 1000000)) || c.ExpectedHostEpoch != m.epoch {
		return remoteReceipt{}, errors.New("Invalid history page")
	}
	now := time.Now()
	r := &remoteReceipt{OperationID: c.OperationID, SessionID: c.SessionID, Status: "providerAccepted", Digest: digest, Transient: true, created: now}
	if req.PageID != nil || req.Offset != nil {
		if req.PageID == nil || req.Offset == nil || req.Before == nil {
			return remoteReceipt{}, errors.New("Invalid history continuation")
		}
		p := m.historyPages[*req.PageID]
		if p == nil || p.sessionID != c.SessionID || p.end != *req.Before || p.limit != limit {
			return remoteReceipt{}, errors.New(remoteHistoryPageExpired)
		}
		offset := *req.Offset
		if offset <= 0 || offset >= len(p.data) || !utf8.RuneStart(p.data[offset]) {
			return remoteReceipt{}, errors.New("Invalid history page offset")
		}
		if err := m.reserveTransientLocked(); err != nil {
			return remoteReceipt{}, err
		}
		p.lastRead = now
		r.Result = p.chunk(*req.PageID, offset)
		m.operations[c.OperationID] = r
		return *r, nil
	}
	session := m.sessions[c.SessionID]
	if session == nil {
		for _, archived := range m.history {
			if archived.ID == c.SessionID {
				session = archived
				break
			}
		}
	}
	if session == nil {
		return remoteReceipt{}, errors.New("Remote session not found")
	}
	end := len(session.Messages)
	if req.Before != nil {
		end = *req.Before
	}
	if end > len(session.Messages) {
		return remoteReceipt{}, errors.New("Invalid history page offset")
	}
	if err := m.reserveTransientLocked(); err != nil {
		return remoteReceipt{}, err
	}
	start := max(0, end-limit)
	messages := append([]remoteMessage{}, session.Messages[start:end]...)
	r.Result = map[string]any{"sessionId": c.SessionID, "start": start, "end": end, "total": len(session.Messages), "hasOlder": start > 0, "messages": messages}
	encoded, err := json.Marshal(r)
	if err != nil {
		return remoteReceipt{}, err
	}
	if len(encoded) > 32*1024 {
		data, err := json.Marshal(messages)
		if err != nil {
			return remoteReceipt{}, err
		}
		if len(data) > 8*1024*1024 {
			return remoteReceipt{}, errors.New("History page too large; request fewer messages")
		}
		if m.historyPages == nil {
			m.historyPages = make(map[string]*remoteHistoryPage)
		}
		if len(m.historyPages) >= 8 {
			var oldestID string
			for id, other := range m.historyPages {
				if oldestID == "" || other.lastRead.Before(m.historyPages[oldestID].lastRead) {
					oldestID = id
				}
			}
			delete(m.historyPages, oldestID)
		}
		page := &remoteHistoryPage{sessionID: c.SessionID, start: start, end: end, total: len(session.Messages), limit: limit, data: data, lastRead: now}
		id := uuid.NewString()
		m.historyPages[id] = page
		r.Result = page.chunk(id, 0)
	}
	m.operations[c.OperationID] = r
	return *r, nil
}

// History reads never enter the native worker, advance revisions, or write their payload to the journal.
func (m *remoteManager) historyReceiptLocked(c remoteCommandEnvelope, digest string) (remoteReceipt, error) {
	var page remoteHistoryPageRequest
	if strictRemoteJSON(c.Payload, &page) == nil && page.Limit != nil {
		return m.historyPageReceiptLocked(c, digest, page)
	}
	var p struct {
		Offset *int `json:"offset"`
	}
	if strictRemoteJSON(c.Payload, &p) != nil || p.Offset == nil || *p.Offset < 0 || c.ExpectedHostEpoch != m.epoch {
		return remoteReceipt{}, errors.New("Invalid archived history read")
	}
	for _, s := range m.history {
		if s.ID != c.SessionID {
			continue
		}
		if err := m.reserveTransientLocked(); err != nil {
			return remoteReceipt{}, err
		}
		messages := s.Messages
		if messages == nil {
			messages = []remoteMessage{}
		}
		if m.historyReadID != s.ID {
			data, err := json.Marshal(messages)
			if err != nil {
				return remoteReceipt{}, err
			}
			m.historyReadID, m.historyReadData = s.ID, data
		}
		data := m.historyReadData
		offset := *p.Offset
		if offset > len(data) || (offset < len(data) && !utf8.RuneStart(data[offset])) {
			return remoteReceipt{}, errors.New("Invalid archived history offset")
		}
		end := min(offset+4096, len(data))
		for end < len(data) && !utf8.RuneStart(data[end]) {
			end--
		}
		r := &remoteReceipt{OperationID: c.OperationID, SessionID: s.ID, Status: "providerAccepted", Digest: digest, Transient: true, created: time.Now().UTC(), Result: map[string]any{"sessionId": s.ID, "offset": offset, "nextOffset": end, "total": len(data), "content": string(data[offset:end])}}
		m.operations[c.OperationID] = r
		return *r, nil
	}
	return remoteReceipt{}, errors.New("Archived remote session not found")
}
