package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const remoteMaxSessions = 8
const remoteSessionLimitError = "remote session limit reached (8)"

const remoteMaxImageBytes = 4 << 20
const remoteMaxJournalBytes = 40 << 20

type remoteCommandEnvelope struct {
	ProtocolVersion   int             `json:"protocolVersion"`
	ExpectedHostEpoch string          `json:"expectedHostEpoch,omitempty"`
	OperationID       string          `json:"operationId"`
	SessionID         string          `json:"sessionId,omitempty"`
	RunID             string          `json:"runId,omitempty"`
	AttemptID         string          `json:"attemptId,omitempty"`
	ExpectedRevision  *uint64         `json:"expectedRevision,omitempty"`
	Type              string          `json:"type"`
	Payload           json.RawMessage `json:"payload"`
}
type remoteReceipt struct {
	OperationID string         `json:"operationId"`
	Status      string         `json:"status"`
	SessionID   string         `json:"sessionId,omitempty"`
	Result      map[string]any `json:"result,omitempty"`
	Error       string         `json:"error,omitempty"`
	Digest      string         `json:"-"`
	created     time.Time
	Transient   bool `json:"-"`
}
type remoteMessage struct {
	ID          string   `json:"id"`
	Role        string   `json:"role"`
	Text        string   `json:"text"`
	CreatedAt   string   `json:"createdAt"`
	Attachments []string `json:"attachments,omitempty"`
	Truncated   bool     `json:"truncated,omitempty"`
	TextBytes   int      `json:"textBytes,omitempty"`
}
type remoteApproval struct {
	nativeInstance uint64
	NativeID       string          `json:"nativeId"`
	Generation     string          `json:"generation"`
	ID             string          `json:"id"`
	Kind           string          `json:"kind"`
	Title          string          `json:"title"`
	Data           json.RawMessage `json:"data,omitempty"`
	Options        []string        `json:"options"`
	State          string          `json:"state"`
}
type remoteSession struct {
	outputTail        string
	outputTruncated   bool
	pendingJobs       int
	SiteIdentity      string              `json:"-"`
	OwnedProcess      remoteOwnedProcess  `json:"ownedProcess"`
	OwnedGateway      *remoteOwnedProcess `json:"ownedGateway,omitempty"`
	ID                string              `json:"id"`
	Agent             string              `json:"agent"`
	Cwd               string              `json:"cwd"`
	NativeID          string              `json:"nativeId"`
	State             string              `json:"state"`
	Revision          uint64              `json:"revision"`
	RunID             string              `json:"runId"`
	AttemptID         string              `json:"attemptId"`
	Model             string              `json:"model"`
	ConfiguredModel   string              `json:"configuredModel,omitempty"`
	ConfiguredModelID string              `json:"configuredModelId,omitempty"`
	ConfiguredKeyID   string              `json:"configuredKeyId,omitempty"`
	ModelRevision     uint64              `json:"modelRevision,omitempty"`
	Source            string              `json:"source"`
	Messages          []remoteMessage     `json:"messages"`
	Approvals         []*remoteApproval   `json:"approvals"`
	Capabilities      map[string]bool     `json:"capabilities"`
	CreatedAt         string              `json:"createdAt"`
	UpdatedAt         string              `json:"updatedAt"`
	MessageCount      int                 `json:"messageCount,omitempty"`
	MessageStart      int                 `json:"messageStart,omitempty"`
	Error             string              `json:"error,omitempty"`
}
type remoteExecutor struct {
	SiteConfigured     bool            `json:"siteConfigured"`
	ConfigurationError string          `json:"configurationError,omitempty"`
	ConfiguredModel    string          `json:"configuredModel,omitempty"`
	ConfiguredKeyID    string          `json:"configuredKeyId,omitempty"`
	Agent              string          `json:"agent"`
	Supported          bool            `json:"supported"`
	Authorized         bool            `json:"authorized"`
	State              string          `json:"state"`
	Version            string          `json:"version"`
	Capabilities       map[string]bool `json:"capabilities"`
	Error              string          `json:"error,omitempty"`
	Identity           string          `json:"-"`
}
type remoteEvent struct {
	EventID       string         `json:"eventId"`
	HostEpoch     string         `json:"hostEpoch"`
	SessionID     string         `json:"sessionId"`
	Sequence      uint64         `json:"sequence"`
	StateRevision uint64         `json:"stateRevision"`
	OperationID   string         `json:"operationId,omitempty"`
	RunID         string         `json:"runId,omitempty"`
	AttemptID     string         `json:"attemptId,omitempty"`
	Type          string         `json:"type"`
	Payload       map[string]any `json:"payload"`
}
type remoteAttachment struct {
	ID, SessionID, Name, Mime, Path string
	Size, Received                  int
	Ready                           bool
	CreatedAt                       time.Time
}
type remoteJob struct {
	intent, expectedRun, expectedAttempt, expectedGeneration, previousState string
	validate                                                                func(context.Context) error
	command                                                                 remoteCommandEnvelope
	receipt                                                                 *remoteReceipt
	payload                                                                 remotePayload
}
type remotePayload struct {
	Agent         string              `json:"agent,omitempty"`
	Cwd           string              `json:"cwd,omitempty"`
	Text          string              `json:"text,omitempty"`
	Attachments   []string            `json:"attachments,omitempty"`
	ApprovalID    string              `json:"approvalId,omitempty"`
	Decision      string              `json:"decision,omitempty"`
	Answers       map[string][]string `json:"answers,omitempty"`
	KeyID         string              `json:"keyId,omitempty"`
	ModelID       string              `json:"modelId,omitempty"`
	ModelRevision *uint64             `json:"modelRevision,omitempty"`
	Name          string              `json:"name,omitempty"`
	Mime          string              `json:"mime,omitempty"`
	Size          int                 `json:"size,omitempty"`
	AttachmentID  string              `json:"attachmentId,omitempty"`
	Before        *int                `json:"before,omitempty"`
	Limit         *int                `json:"limit,omitempty"`
	Content       string              `json:"content,omitempty"`
	Offset        int                 `json:"offset,omitempty"`
	Final         bool                `json:"final,omitempty"`
}
type remoteProviderFactory func(context.Context, string, string, string, func(remoteProviderEvent)) (remoteProvider, error)
type remoteManager struct {
	live                                          remoteLiveState
	historyReadID                                 string
	historyReadData                               []byte
	historyPages                                  map[string]*remoteHistoryPage
	history                                       []*remoteSession
	mu                                            sync.Mutex
	shutdownMu                                    sync.Mutex
	lock                                          *os.File
	switching                                     map[string]bool
	owner, home, path, uid, hostID, epoch, secret string
	session, osHome                               string
	revision                                      uint64
	enabled, closed                               bool
	revoked                                       bool
	failure                                       string
	sessions                                      map[string]*remoteSession
	operations                                    map[string]*remoteReceipt
	executors                                     map[string]*remoteExecutor
	providers                                     map[string]remoteProvider
	generations                                   map[string]string
	attachments                                   map[string]*remoteAttachment
	events                                        []remoteEvent
	jobs                                          chan remoteJob
	ctx                                           context.Context
	cancel                                        context.CancelFunc
	relayCancel                                   context.CancelFunc
	start                                         remoteProviderFactory
	modelChange                                   func(context.Context, *remoteSession, remotePayload) error
	configurationGate                             *sync.Mutex
	relayOperations                               map[string]bool
	relayPublished                                map[string]string
	siteCredentials                               map[string]*remoteAgentCredential
	checkAuthorization                            func(string, string) error
	checkSite                                     func(context.Context, string) (remoteSiteLaunch, error)
	mintSiteCredential                            func(context.Context, string, string, remoteSiteLaunch) (remoteSiteLaunch, error)
}

func strictRemoteJSON(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid remote command fields")
	}
	return nil
}
func parseRemoteCommand(raw json.RawMessage) (remoteCommandEnvelope, error) {
	var c remoteCommandEnvelope
	if len(raw) > 32768 || strictRemoteJSON(raw, &c) != nil {
		return c, errors.New("invalid remote command")
	}
	if c.ProtocolVersion != 1 || len(c.OperationID) == 0 || len(c.OperationID) > 100 || !remoteOperationType(c.Type) {
		return c, errors.New("unsupported remote protocol or operation")
	}
	return c, nil
}
func remoteOperationType(s string) bool {
	switch s {
	case "session.history.read", "session.create", "session.resume", "message.send", "approval.respond", "run.stop", "model.change", "attachment.prepare", "attachment.upload", "attachment.discard", "attachment.read":
		return true
	}
	return false
}
func remoteNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }
func newRemoteManager(home, owner string) (*remoteManager, error) {
	if !nodePattern.MatchString(owner) {
		return nil, errors.New("invalid remote account")
	}
	current, err := user.Current()
	if err != nil || current.Uid == "" {
		return nil, errors.New("current operating system identity unavailable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &remoteManager{owner: owner, home: home, uid: current.Uid, hostID: uuid.NewString(), epoch: uuid.NewString(), secret: strings.ReplaceAll(uuid.NewString()+uuid.NewString(), "-", ""), sessions: map[string]*remoteSession{}, operations: map[string]*remoteReceipt{}, executors: map[string]*remoteExecutor{}, providers: map[string]remoteProvider{}, generations: map[string]string{}, attachments: map[string]*remoteAttachment{}, siteCredentials: map[string]*remoteAgentCredential{}, ctx: ctx, cancel: cancel, jobs: make(chan remoteJob, 32), start: startRemoteProvider}
	m.osHome = current.HomeDir
	sum := sha256.Sum256([]byte(owner))
	m.path = filepath.Join(home, ".newnoa", "remote-agents", hex.EncodeToString(sum[:16]), "state.json")
	if err = m.safeJournal(); err != nil {
		cancel()
		return nil, err
	}
	if err = os.MkdirAll(filepath.Dir(m.path), 0700); err != nil {
		cancel()
		return nil, err
	}
	lockPath := filepath.Join(filepath.Dir(m.path), "writer.lock")
	if err = (&core{home: home}).safeConfigFile(lockPath); err != nil {
		cancel()
		return nil, err
	}
	m.lock, err = os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		cancel()
		return nil, err
	}
	if err = lockRemoteWriter(m.lock); err != nil {
		m.lock.Close()
		cancel()
		return nil, errors.New("Another Core owns this account remote runtime")
	}
	if err = m.load(); err != nil {
		m.lock.Close()
		cancel()
		return nil, err
	}
	for _, agent := range []string{"codex", "claude-cli", "pi-agent", "openclaw", "hermes-agent", "opencode"} {
		m.executors[agent] = &remoteExecutor{Agent: agent, State: "unsupported", Capabilities: map[string]bool{}, Error: "Remote adapter has not been verified"}
		if agent == "codex" || agent == "claude-cli" || agent == "pi-agent" || agent == "opencode" && remoteOpenCodePlatform() || agent == "hermes-agent" && remoteHermesPinnedVersion(runtime.GOOS) != "" || agent == "openclaw" && remoteOpenClawPlatform() {
			m.executors[agent].Supported = true
			m.executors[agent].State = "unavailable"
			m.executors[agent].Error = "Authorize this executor with a real conversation probe"
		}
	}
	go m.worker()
	go m.cleanTransient()
	return m, nil
}
func (m *remoteManager) providerBusyLocked(agent string) bool {
	for _, s := range m.sessions {
		if s.Agent == agent {
			switch s.State {
			case "starting", "running", "awaitingApproval", "stopping", "recovering", "unknown":
				return true
			}
		}
	}
	return false
}
func remoteDigest(c remoteCommandEnvelope) string {
	var v any
	b, _ := json.Marshal(c)
	_ = json.Unmarshal(b, &v)
	b, _ = json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (m *remoteManager) command(c remoteCommandEnvelope) (remoteReceipt, error) {
	return m.commandWithAuthorization(c, nil)
}
func (m *remoteManager) commandWithAuthorization(c remoteCommandEnvelope, validate func(context.Context) error) (remoteReceipt, error) {
	if m.configurationGate != nil {
		m.configurationGate.Lock()
		defer m.configurationGate.Unlock()
	}
	raw, _ := json.Marshal(c)
	parsed, err := parseRemoteCommand(raw)
	if err != nil {
		return remoteReceipt{}, err
	}
	c = parsed
	var p remotePayload
	if c.Type != "session.history.read" && strictRemoteJSON(c.Payload, &p) != nil {
		return remoteReceipt{}, errors.New("invalid remote payload")
	}
	if m.isTransientType(c.Type) && validate != nil {
		if err := validate(m.ctx); err != nil {
			return remoteReceipt{}, err
		}
	}
	m.shutdownMu.Lock()
	defer m.shutdownMu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return remoteReceipt{}, errors.New("remote runtime is closed")
	}
	if c.ExpectedHostEpoch != "" && c.ExpectedHostEpoch != m.epoch {
		return remoteReceipt{}, errors.New("Remote host epoch changed; reconnect before sending")
	}
	m.pruneTransientLocked()
	digest := remoteDigest(c)
	if old := m.operations[c.OperationID]; old != nil {
		if old.Digest != digest {
			return remoteReceipt{}, errors.New("operation identifier already used for different input")
		}
		return publicRemoteReceipt(*old), nil
	}
	if c.Type == "session.history.read" {
		return m.historyReceiptLocked(c, digest)
	}
	if !m.isTransientType(c.Type) && m.retainedOperationsLocked() >= 16384 {
		return remoteReceipt{}, errors.New("remote operation quota reached; preserve current sessions and restart after retention cleanup")
	}
	if len(m.jobs) == cap(m.jobs) {
		return remoteReceipt{}, errors.New("remote host is busy; retry with the same operation identifier")
	}
	s := m.sessions[c.SessionID]
	beforeRevision := m.revision
	beforeEvents := append([]remoteEvent(nil), m.events...)
	var before *remoteSession
	intent := ""
	if s != nil {
		raw, _ := json.Marshal(s)
		before = &remoteSession{}
		_ = json.Unmarshal(raw, before)
		before.SiteIdentity = s.SiteIdentity
		before.pendingJobs = s.pendingJobs
		before.outputTail, before.outputTruncated = s.outputTail, s.outputTruncated
	}
	if m.switching == nil {
		m.switching = map[string]bool{}
	}
	agent := p.Agent
	if s != nil {
		agent = s.Agent
	}
	beforeSwitching := m.switching[agent]
	if m.switching[agent] {
		return remoteReceipt{}, errors.New("Executor configuration is changing")
	}
	var retired *remoteSession
	var oldHistory []*remoteSession
	if c.Type == "session.create" {
		executor := m.executors[p.Agent]
		if executor == nil || !executor.Supported || !executor.Authorized {
			if executor != nil && executor.ConfigurationError != "" {
				return remoteReceipt{}, errors.New(executor.ConfigurationError)
			}
			return remoteReceipt{}, errors.New("executor has not passed remote authorization")
		}
		if m.checkAuthorization != nil {
			if err := m.checkAuthorization(p.Agent, executor.Identity); err != nil {
				executor.Authorized = false
				executor.State = "failed"
				executor.Error = "Executor or configuration changed; authorize again"
				m.changedLocked(nil, "", "executor.authorization", nil)
				return remoteReceipt{}, err
			}
		}
		if len(m.sessions) >= remoteMaxSessions {
			retired = m.archiveCandidateLocked()
			if retired == nil {
				return remoteReceipt{}, errors.New(remoteSessionLimitError)
			}
		}
		if p.Agent == "" || p.Cwd == "" || p.Text != "" {
			return remoteReceipt{}, errors.New("executor and project directory required")
		}
		cwd, e := filepath.Abs(p.Cwd)
		if e != nil {
			return remoteReceipt{}, errors.New("invalid project directory")
		}
		cwd, e = filepath.EvalSymlinks(cwd)
		if e != nil {
			return remoteReceipt{}, errors.New("project directory does not exist or is inaccessible")
		}
		info, e := os.Stat(cwd)
		if e != nil || !info.IsDir() {
			return remoteReceipt{}, errors.New("project path is not an accessible directory")
		}
		if f, e := os.Open(cwd); e != nil {
			return remoteReceipt{}, errors.New("project directory is inaccessible")
		} else {
			f.Close()
		}
		if retired != nil {
			updated := retired.UpdatedAt
			if err := m.detachForArchiveLocked(retired, validate); err != nil {
				return remoteReceipt{}, err
			}
			// Detaching advances host state but does not make an old conversation newer.
			retired.UpdatedAt = updated
			beforeRevision = m.revision
			beforeEvents = append([]remoteEvent(nil), m.events...)
			oldHistory = slices.Clone(m.history)
			m.archiveSessionLocked(retired)
			delete(m.sessions, retired.ID)
		}
		c.SessionID = uuid.NewString()
		s = &remoteSession{ID: c.SessionID, Agent: p.Agent, Cwd: cwd, State: "starting", Source: "unknown", Messages: []remoteMessage{}, Approvals: []*remoteApproval{}, Capabilities: m.executors[p.Agent].Capabilities, CreatedAt: remoteNow(), UpdatedAt: remoteNow()}
		m.sessions[s.ID] = s
	} else {
		if s == nil {
			return remoteReceipt{}, errors.New("remote session not found")
		}
		if (c.Type == "model.change" || c.Type == "session.resume" || c.Type == "message.send") && c.ExpectedRevision != nil && *c.ExpectedRevision != s.Revision {
			return remoteReceipt{}, errors.New("remote session revision changed")
		}
		if c.RunID != "" && c.RunID != s.RunID {
			return remoteReceipt{}, errors.New("active run changed")
		}
		if c.AttemptID != "" && c.AttemptID != s.AttemptID {
			return remoteReceipt{}, errors.New("active attempt changed")
		}
		if c.Type == "attachment.read" {
			if err := m.reserveTransientLocked(); err != nil {
				return remoteReceipt{}, err
			}
			data, err := m.readAttachmentLocked(c.SessionID, p.AttachmentID, p.Offset)
			if err != nil {
				return remoteReceipt{}, err
			}
			r := &remoteReceipt{OperationID: c.OperationID, SessionID: c.SessionID, Status: "providerAccepted", Digest: digest, Transient: true, created: time.Now().UTC(), Result: data}
			m.operations[c.OperationID] = r
			return publicRemoteReceipt(*r), nil
		}
		switch c.Type {
		case "session.resume":
			if s.State == "recovering" || s.State == "starting" {
				return remoteReceipt{}, errors.New("Native session recovery is already pending")
			}
			if s.OwnedProcess.PID > 1 || s.State == "unknown" {
				alive, err := remoteOwnedProcessAlive(s.OwnedProcess)
				if err != nil {
					return remoteReceipt{}, err
				}
				if alive {
					return remoteReceipt{}, errors.New("Previous owned native process still exists; stop it before resuming")
				}
			}
			if s.OwnedGateway != nil {
				alive, err := remoteOwnedProcessAlive(*s.OwnedGateway)
				if err != nil || alive {
					return remoteReceipt{}, errors.New("Previous owned native Gateway must be stopped before resuming")
				}
			}
			executor := m.executors[s.Agent]
			if executor == nil || !executor.Authorized {
				return remoteReceipt{}, errors.New("Authorize the executor again before resuming")
			}
			if m.checkAuthorization != nil {
				if err := m.checkAuthorization(s.Agent, executor.Identity); err != nil {
					return remoteReceipt{}, err
				}
			}
			if s.NativeID == "" || !s.Capabilities["resume"] {
				return remoteReceipt{}, errors.New("native session cannot be resumed")
			}
			if m.providers[s.ID] != nil {
				return remoteReceipt{}, errors.New("native session is already attached")
			}
			s.State = "recovering"
		case "message.send":
			if p.ModelRevision != nil && (*p.ModelRevision == 0 || *p.ModelRevision != s.ModelRevision || (s.State != "idle" && s.State != "stopped")) {
				return remoteReceipt{}, errors.New("Remote model or run changed before sending")
			}
			if strings.TrimSpace(p.Text) == "" && len(p.Attachments) == 0 {
				return remoteReceipt{}, errors.New("message is empty")
			}
			if len(p.Text) > 16384 || len(p.Attachments) > 4 {
				return remoteReceipt{}, errors.New("message exceeds limits")
			}
			if s.State != "idle" && s.State != "stopped" && s.State != "running" && s.State != "starting" {
				return remoteReceipt{}, errors.New("session is not ready for input")
			}
			if (s.State == "running" || s.State == "starting") && !s.Capabilities["steer"] {
				return remoteReceipt{}, errors.New("native immediate input is unsupported; draft retained")
			}
			if m.providers[s.ID] == nil {
				return remoteReceipt{}, errors.New("native session must be explicitly resumed")
			}
			for _, id := range p.Attachments {
				a := m.attachments[id]
				if a == nil || a.SessionID != s.ID || !a.Ready {
					return remoteReceipt{}, errors.New("attachment is not ready for this session")
				}
			}
			if s.State == "idle" || s.State == "stopped" {
				intent = "start"
				s.State = "starting"
				s.RunID = uuid.NewString()
				s.AttemptID = uuid.NewString()
			} else {
				intent = "steer"
			}
		case "run.stop":
			if s.State == "idle" || s.State == "stopped" {
				break
			}
			if m.providers[s.ID] == nil && s.OwnedProcess.PID <= 1 && s.OwnedGateway == nil {
				return remoteReceipt{}, errors.New("Native run ownership cannot be verified")
			}
			s.State = "stopping"
		case "approval.respond":
			var approval *remoteApproval
			for _, a := range s.Approvals {
				if a.ID == p.ApprovalID {
					approval = a
					break
				}
			}
			if approval == nil || approval.State != "open" {
				return remoteReceipt{}, errors.New("approval already processed or invalidated")
			}
			valid := false
			for _, v := range approval.Options {
				if v == p.Decision {
					valid = true
				}
			}
			if !valid {
				return remoteReceipt{}, errors.New("invalid native approval choice")
			}
			approval.State = "resolved"
		case "model.change":
			if m.providerBusyLocked(s.Agent) {
				return remoteReceipt{}, errors.New("another session for this executor is active")
			}
			if p.KeyID == "" || p.ModelID == "" || m.modelChange == nil {
				return remoteReceipt{}, errors.New("authorized key and model required")
			}
			s.State = "recovering"
			m.switching[s.Agent] = true
		case "attachment.prepare":
			if !s.Capabilities["image"] {
				return remoteReceipt{}, errors.New("native image input is unsupported")
			}
			if p.Size <= 0 || p.Size > remoteMaxImageBytes || len(p.Name) > 120 || p.Name == "" || filepath.Base(p.Name) != p.Name || strings.ContainsAny(p.Name, "/\\") || !strings.HasPrefix(p.Mime, "image/") || p.Mime != "image/png" && p.Mime != "image/jpeg" && p.Mime != "image/gif" && p.Mime != "image/webp" {
				return remoteReceipt{}, errors.New("invalid image name, media type or size")
			}
			if len(m.attachments) >= 32 {
				return remoteReceipt{}, errors.New("attachment quota reached")
			}
		case "attachment.upload", "attachment.discard":
			a := m.attachments[p.AttachmentID]
			if a == nil || a.SessionID != s.ID {
				return remoteReceipt{}, errors.New("attachment not found for this session")
			}
			if c.Type == "attachment.upload" && (a.Ready || p.Offset != a.Received || len(p.Content) > 24000) {
				return remoteReceipt{}, errors.New("invalid image chunk offset or size")
			}
		}
	}
	r := &remoteReceipt{OperationID: c.OperationID, Status: "accepted", SessionID: c.SessionID, Digest: digest, created: time.Now().UTC()}
	m.operations[c.OperationID] = r
	m.changedLocked(s, c.OperationID, "operation.accepted", nil)
	if err = m.saveLocked(); err != nil {
		delete(m.operations, c.OperationID)
		if retired != nil {
			m.history = oldHistory
			m.sessions[retired.ID] = retired
		}
		if before == nil {
			delete(m.sessions, s.ID)
		} else {
			*s = *before
		}
		m.revision = beforeRevision
		m.events = beforeEvents
		if beforeSwitching {
			m.switching[agent] = true
		} else {
			delete(m.switching, agent)
		}
		m.setFailureLocked("Remote intent could not be persisted")
		return remoteReceipt{}, err
	}
	if retired != nil {
		delete(m.generations, retired.ID)
		for id, a := range m.attachments {
			if a.SessionID == retired.ID {
				if err := os.Remove(a.Path); err == nil || os.IsNotExist(err) {
					delete(m.attachments, id)
				}
			}
		}
	}
	s.pendingJobs++
	previousState := ""
	if before != nil {
		previousState = before.State
	}
	m.jobs <- remoteJob{command: c, receipt: r, payload: p, validate: validate, intent: intent, expectedRun: s.RunID, expectedAttempt: s.AttemptID, expectedGeneration: m.generations[s.ID], previousState: previousState}
	return publicRemoteReceipt(*r), nil
}
func (m *remoteManager) worker() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case j := <-m.jobs:
			m.perform(j)
		}
	}
}

// These CLIs persist native history only after a first turn. Keep the mobile ID for an unused draft.
func remoteNativeResumeID(s *remoteSession) string {
	if (s.Agent == "codex" || s.Agent == "claude-cli" || s.Agent == "pi-agent") &&
		len(s.Messages) == 0 && s.RunID == "" && s.AttemptID == "" {
		return ""
	}
	return s.NativeID
}

func (m *remoteManager) perform(j remoteJob) {
	c, p := j.command, j.payload
	m.mu.Lock()
	m.pruneTransientLocked()
	if m.closed || m.operations[c.OperationID] != j.receipt {
		if s := m.sessions[c.SessionID]; s != nil && s.pendingJobs > 0 {
			s.pendingJobs--
		}
		m.mu.Unlock()
		return
	}
	s := m.sessions[c.SessionID]
	provider := m.providers[s.ID]
	agent, cwd, nativeID := s.Agent, s.Cwd, s.NativeID
	if c.Type == "session.resume" {
		nativeID = remoteNativeResumeID(s)
	}
	generation := m.generations[s.ID]
	m.mu.Unlock()
	detachOnStop := agent == "claude-cli" || agent == "hermes-agent"
	deadline := 30 * time.Second
	if remoteACPAgent(remoteProviderBinary(agent)) && (c.Type == "session.create" || c.Type == "session.resume") {
		deadline = 70 * time.Second
	}
	ctx, cancel := context.WithTimeout(m.ctx, deadline)
	defer cancel()
	ctx = context.WithValue(ctx, remoteCommandAuthorityKey{}, j.validate)
	var err error
	var result map[string]any
	err = remoteCheckAuthority(ctx)
	if err == nil && m.checkSite != nil && (c.Type == "message.send" || c.Type == "approval.respond") {
		m.mu.Lock()
		adopted := s.Source == "newnoa" && s.SiteIdentity != "" && m.executors[agent] != nil && s.SiteIdentity == m.executors[agent].Identity
		m.mu.Unlock()
		if !adopted {
			err = errors.New("Switch this remote session to a site model before sending")
		}
	}
	if err == nil && m.checkSite != nil && (c.Type == "session.create" || c.Type == "session.resume" || c.Type == "message.send" || c.Type == "approval.respond") {
		var selection remoteSiteLaunch
		selection, err = m.checkSite(ctx, agent)
		if err == nil {
			ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, selection)
		}
	}
	effectStarted := false
	if err == nil {
		m.mu.Lock()
		if m.closed {
			err = errors.New("Remote runtime closed")
		}
		if c.Type != "session.create" && (s.RunID != j.expectedRun || s.AttemptID != j.expectedAttempt || m.generations[s.ID] != j.expectedGeneration) {
			err = errors.New("Native run or owner changed before execution")
		}
		if c.Type == "message.send" && j.intent == "steer" && s.State != "running" {
			err = errors.New("Native run ended before immediate input; draft retained")
		}
		if c.Type == "session.resume" && m.providers[s.ID] != nil {
			err = errors.New("Native session is already attached")
		}
		m.mu.Unlock()
	}
	if err == nil {
		effectStarted = true
		switch c.Type {
		case "session.create", "session.resume":
			if site, managed := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch); managed {
				if m.mintSiteCredential == nil {
					err = errors.New("Remote agent session credential issuance is unavailable")
					break
				}
				site, err = m.mintSiteCredential(ctx, s.ID, agent, site)
				if err != nil {
					break
				}
				ctx = context.WithValue(ctx, remoteSiteLaunchKey{}, site)
			}
			generation = uuid.NewString()
			m.mu.Lock()
			m.generations[s.ID] = generation
			m.mu.Unlock()
			provider, err = m.start(m.providerContext(ctx, s.ID, generation), agent, cwd, nativeID, func(e remoteProviderEvent) { m.providerEvent(s.ID, generation, e) })
			if err != nil {
				if site, ok := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch); ok && site.Revoke != nil {
					revokeCtx, revokeCancel := context.WithTimeout(context.Background(), 8*time.Second)
					if revokeErr := site.Revoke(revokeCtx); revokeErr != nil {
						err = errors.Join(err, revokeErr)
					}
					revokeCancel()
				}
			}
			m.mu.Lock()
			if !m.closed && m.generations[s.ID] == generation && err == nil && s.State != "failed" && s.State != "unknown" {
				m.providers[s.ID] = provider
				if owned, ok := provider.(interface{ OwnedProcess() remoteOwnedProcess }); ok {
					s.OwnedProcess = owned.OwnedProcess()
				}
				s.State = "idle"
				s.Error = ""
				if site, ok := ctx.Value(remoteSiteLaunchKey{}).(remoteSiteLaunch); ok {
					s.ConfiguredModel = site.Alias
					s.ConfiguredModelID = site.Model
					s.ConfiguredKeyID = site.KeyID
					s.Source = "newnoa"
					s.SiteIdentity = m.executors[agent].Identity
				}
			} else if provider != nil {
				if err == nil {
					err = errors.New("Native process exited before session startup completed")
				}
				m.mu.Unlock()
				_ = provider.Close()
				m.mu.Lock()
			}
			m.mu.Unlock()
		case "message.send":
			effectStarted = false
			var input remoteProviderInput
			input.OperationID = c.OperationID
			input.Text = p.Text
			for _, id := range p.Attachments {
				var image remoteProviderImage
				image, err = m.attachmentInput(s.ID, id)
				if err != nil {
					break
				}
				input.Images = append(input.Images, image)
			}
			m.mu.Lock()
			running := j.intent == "steer"
			if err == nil && !running {
				s.State = "running"
				m.changedLocked(s, c.OperationID, "run.state", map[string]any{"state": "running"})
				err = m.saveLocked()
			}
			if err == nil {
				s.Messages = append(s.Messages, remoteMessage{ID: c.OperationID, Role: "user", Text: p.Text, Attachments: p.Attachments, CreatedAt: remoteNow()})
				if len(s.Messages) > 256 {
					s.Messages = s.Messages[len(s.Messages)-256:]
				}
				err = m.saveLocked()
			}
			m.mu.Unlock()
			if err == nil {
				err = remoteCheckAuthority(ctx)
			}
			if err == nil {
				effectStarted = true
				if running {
					err = provider.Steer(ctx, input)
				} else {
					err = provider.StartTurn(ctx, input)
				}
			}

		case "run.stop":
			m.mu.Lock()
			finished := provider != nil && (s.State == "idle" || s.State == "stopped")
			m.mu.Unlock()
			if finished {
				break
			}
			if provider != nil && detachOnStop {
				// Native cancellation can report a generic error or end_turn before the process stops.
				err = provider.Stop(ctx)
				if err == nil {
					err = provider.Close()
				}
				if err == nil {
					m.mu.Lock()
					if m.generations[s.ID] == generation && m.providers[s.ID] == provider {
						delete(m.providers, s.ID)
						delete(m.generations, s.ID)
						s.OwnedProcess = remoteOwnedProcess{}
						s.OwnedGateway = nil
						s.State = "stopped"
						s.Error = ""
					} else {
						err = errors.New("Native owner changed during stop")
					}
					m.mu.Unlock()
				}
			} else if provider != nil {
				err = provider.Stop(ctx)
			} else {
				err = stopRemoteSessionProcesses(s.OwnedProcess, s.OwnedGateway)
				if err == nil {
					m.mu.Lock()
					s.State = "stopped"
					m.mu.Unlock()
				}
			}
		case "approval.respond":
			m.mu.Lock()
			nativeRequest := ""
			var nativeInstance uint64
			for _, a := range s.Approvals {
				if a.ID == p.ApprovalID && a.Generation == generation && a.State == "resolved" {
					nativeRequest = a.NativeID
					nativeInstance = a.nativeInstance
					break
				}
			}
			m.mu.Unlock()
			if nativeRequest == "" {
				err = errors.New("Native approval is no longer live")
			} else {
				err = provider.Respond(ctx, remoteProviderReply{RequestID: nativeRequest, Instance: nativeInstance, Decision: p.Decision, Answers: p.Answers})
			}
		case "model.change":
			err = m.modelChange(ctx, s, p)
		case "attachment.prepare":
			m.mu.Lock()
			full := len(m.attachments) >= 32
			m.mu.Unlock()
			if full {
				err = errors.New("Attachment quota reached before preparation")
				break
			}
			id := uuid.NewString()
			path := filepath.Join(filepath.Dir(m.path), "attachments", id)
			m.mu.Lock()
			m.attachments[id] = &remoteAttachment{ID: id, SessionID: s.ID, Name: p.Name, Mime: p.Mime, Size: p.Size, Path: path, CreatedAt: time.Now()}
			m.mu.Unlock()
			result = map[string]any{"attachmentId": id, "chunkBytes": 12288}
		case "attachment.upload":
			err = m.upload(p)
		case "attachment.discard":
			m.mu.Lock()
			a := m.attachments[p.AttachmentID]
			delete(m.attachments, p.AttachmentID)
			m.mu.Unlock()
			if a != nil {
				err = os.Remove(a.Path)
				if os.IsNotExist(err) {
					err = nil
				}
			}
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if s.pendingJobs > 0 {
		s.pendingJobs--
	}
	if m.closed {
		return
	}
	r := m.operations[c.OperationID]
	if r != j.receipt {
		return
	}
	if c.Type == "model.change" {
		delete(m.switching, s.Agent)
	}
	// The exact provider receipt can arrive on stdout before this call unwinds.
	if r.Status == "providerAccepted" {
		err = nil
	}
	if r.Status == "rejected" {
		err = errors.New("Native CLI rejected dispatch")
	}
	if err != nil {
		r.Status = "rejected"
		if isRemoteDispatchUnknown(err) {
			r.Status = "unknown"
		} else if c.Type == "message.send" {
			effectStarted = false
		}
		r.Error = remoteSafeError(err)
		if c.Type == "session.create" || c.Type == "session.resume" || c.Type == "model.change" {
			s.State = "failed"
			if c.Type == "model.change" && m.generations[s.ID] == j.expectedGeneration {
				s.State = j.previousState
			}
			s.Error = r.Error
		}
		if c.Type == "message.send" && !effectStarted && j.intent == "start" && (s.State == "starting" || s.State == "running") {
			s.State = j.previousState
			if s.State == "starting" {
				s.State = "idle"
			}
		}
		if c.Type == "message.send" && effectStarted && s.State == "running" {
			s.State = "unknown"
			s.Error = r.Error
		}
		if c.Type == "run.stop" && (s.State == "stopping" || detachOnStop && effectStarted) && s.RunID == j.expectedRun && s.AttemptID == j.expectedAttempt && m.generations[s.ID] == j.expectedGeneration {
			s.State = "unknown"
			s.Error = r.Error
		}
		if c.Type == "approval.respond" {
			for _, a := range s.Approvals {
				if a.ID == p.ApprovalID {
					a.State = "invalidated"
				}
			}
		}
	} else {
		r.Status = "providerAccepted"
		r.Result = result
	}
	m.changedLocked(s, c.OperationID, "operation."+r.Status, map[string]any{"status": r.Status, "error": r.Error})
	if c.Type == "model.change" && r.Status == "providerAccepted" {
		s.ModelRevision = s.Revision
		r.Result = map[string]any{"modelRevision": s.ModelRevision}
	}
	if e := m.saveLocked(); e != nil {
		r.Status = "unknown"
		r.Error = "Operation executed but receipt persistence failed"
		s.State = "unknown"
		m.setFailureLocked(r.Error)
	}
}
func (m *remoteManager) changedLocked(s *remoteSession, op, typ string, payload map[string]any) {
	m.revision++
	m.signalChangesLocked()
	if s != nil {
		s.Revision++
		s.UpdatedAt = remoteNow()
	}
	event := remoteEvent{EventID: uuid.NewString(), HostEpoch: m.epoch, Sequence: m.revision, OperationID: op, Type: typ, Payload: payload}
	if s != nil {
		event.SessionID = s.ID
		event.StateRevision = s.Revision
		event.RunID = s.RunID
		event.AttemptID = s.AttemptID
	}
	m.events = append(m.events, event)
	if len(m.events) > 64 {
		m.events = m.events[len(m.events)-64:]
	}
}

func (m *remoteManager) setFailureLocked(message string) {
	if m.failure != message {
		m.failure = message
		m.changedLocked(nil, "", "host.error", nil)
	}
}
func (m *remoteManager) providerEvent(id, generation string, e remoteProviderEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.generations[id] != generation {
		return
	}
	s := m.sessions[id]
	if s == nil {
		return
	}
	if e.Type == "exited" || e.Type == "error" || e.Error != "" ||
		e.Type == "state" && (e.State == "idle" || e.State == "stopped" || e.State == "failed") {
		s.appendNativeOutput("", true)
	}
	switch e.Type {
	case "dispatch":
		r := m.operations[e.OperationID]
		if r == nil || r.SessionID != id || (r.Status != "accepted" && r.Status != "unknown") {
			return
		}
		if e.Delivery != "providerAccepted" && e.Delivery != "rejected" {
			return
		}
		previousError := r.Error
		r.Status = e.Delivery
		r.Error = ""
		if s.Error == previousError && previousError != "" {
			s.Error = ""
		}
		m.changedLocked(s, e.OperationID, "operation."+r.Status, map[string]any{"status": r.Status})
		if err := m.saveLocked(); err != nil {
			r.Status = "unknown"
			r.Error = "Native receipt persistence failed"
			m.setFailureLocked(r.Error)
		}
		return
	case "exited":
		m.providerExitedLocked(s, generation)
		return
	case "session":
		if e.NativeID != "" {
			s.NativeID = e.NativeID
		}
	case "output":
		s.appendNativeOutput(e.Text, false)
	case "approval":
		if e.Approval != nil {
			for _, a := range s.Approvals {
				if a.NativeID == e.Approval.ID && a.Generation == generation && a.nativeInstance != e.Approval.Instance && a.State == "open" {
					a.State = "invalidated"
				}
			}
			closed := 0
			for _, a := range s.Approvals {
				if a.State != "open" {
					closed++
				}
			}
			filtered := s.Approvals[:0]
			for _, a := range s.Approvals {
				if a.State != "open" && closed > 16 {
					closed--
					continue
				}
				filtered = append(filtered, a)
			}
			s.Approvals = filtered
			live := 0
			for _, a := range s.Approvals {
				if a.State == "open" {
					live++
				}
			}
			if live >= 64 {
				s.Error = "Native approval quota exceeded; run is stopping"
				s.State = "stopping"
				if provider := m.providers[id]; provider != nil {
					go func() {
						ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
						defer cancel()
						_ = provider.Stop(ctx)
					}()
				}
				return
			}
			a := e.Approval
			s.Approvals = append(s.Approvals, &remoteApproval{ID: uuid.NewString(), NativeID: a.ID, nativeInstance: a.Instance, Generation: generation, Kind: a.Kind, Title: remoteRedact(a.Title), Data: remotePublicData(a.Data), Options: a.Options, State: "open"})
			s.State = "awaitingApproval"
		}
	case "state":
		switch e.State {
		case "idle", "running", "awaitingApproval", "stopping", "stopped", "failed", "unknown":
			s.State = e.State
		}
		if e.State == "idle" || e.State == "stopped" || e.State == "failed" {
			for _, a := range s.Approvals {
				if a.State == "open" {
					a.State = "invalidated"
				}
			}
		}
	}
	if e.Type == "approvalResolved" || e.Type == "approvalInvalidated" {
		for _, a := range s.Approvals {
			if a.NativeID == e.Text && a.Generation == generation && a.nativeInstance == e.ApprovalInstance && (a.State == "open" || a.State == "resolved") {
				if e.Type == "approvalResolved" {
					a.State = "resolved"
				} else {
					a.State = "invalidated"
				}
			}
		}
		if s.State == "awaitingApproval" {
			s.State = "running"
			for _, a := range s.Approvals {
				if a.State == "open" {
					s.State = "awaitingApproval"
					break
				}
			}
		}
	}
	if e.Type == "error" {
		s.State = "failed"
	}
	if e.Model != "" {
		s.Model = e.Model
	}
	if e.Error != "" {
		s.Error = remoteSafeError(errors.New(e.Error))
		s.State = "failed"
	}
	m.changedLocked(s, "", "run."+e.Type, nil)
	if err := m.saveLocked(); err != nil {
		m.setFailureLocked("Remote event persistence failed")
		s.State = "unknown"
	}
}
func (m *remoteManager) upload(p remotePayload) error {
	data, err := base64.StdEncoding.DecodeString(p.Content)
	if err != nil || len(data) > 16384 {
		return errors.New("invalid image chunk")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	a := m.attachments[p.AttachmentID]
	if a == nil || a.Received != p.Offset || a.Ready || a.Received+len(data) > a.Size {
		return errors.New("image chunk conflicts with current upload")
	}
	if err = (&core{home: m.home}).safeConfigFile(a.Path); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(a.Path), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(a.Path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	a.Received += len(data)
	if p.Final {
		if a.Received != a.Size {
			return errors.New("image upload is incomplete")
		}
		raw, err := os.ReadFile(a.Path)
		if err != nil {
			return err
		}
		detected := http.DetectContentType(raw)
		if detected != a.Mime {
			return errors.New("image content does not match declared media type")
		}
		if a.Mime != "image/webp" {
			cfg, _, err := image.DecodeConfig(bytes.NewReader(raw))
			if err != nil || cfg.Width <= 0 || cfg.Height <= 0 || int64(cfg.Width)*int64(cfg.Height) > 40000000 {
				return errors.New("invalid image dimensions")
			}
		}
		a.Ready = true
	}
	return nil
}
func (m *remoteManager) snapshot() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked()
}

func (m *remoteManager) snapshotLocked() map[string]any {
	sessions := make([]*remoteSession, 0, len(m.sessions))
	for _, s := range m.sessions {
		session := *s
		session.Error = remotePublicMessage(s.Error)
		session.MessageCount = len(s.Messages)
		if s.State == "running" {
			session.MessageStart = max(0, len(s.Messages)-20)
			session.Messages = append([]remoteMessage(nil), s.Messages[session.MessageStart:]...)
			for i := range session.Messages {
				if len(session.Messages[i].Text) > 2048 {
					session.Messages[i].TextBytes = len(session.Messages[i].Text)
					session.Messages[i].Text = historyPrefix(session.Messages[i].Text, 2048)
					session.Messages[i].Truncated = true
				}
			}
		} else {
			session.Messages = nil
		}
		sessions = append(sessions, &session)
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].CreatedAt < sessions[j].CreatedAt })
	executors := make([]*remoteExecutor, 0, len(m.executors))
	for _, id := range []string{"codex", "claude-cli", "pi-agent", "openclaw", "hermes-agent", "opencode"} {
		if e := m.executors[id]; e != nil {
			executor := publicRemoteExecutor(*e)
			executors = append(executors, &executor)
		}
	}
	hostname, _ := os.Hostname()
	if m.events == nil {
		m.events = []remoteEvent{}
	}
	operations := []remoteReceipt{}
	for _, r := range m.operations {
		if !r.Transient {
			operations = append(operations, publicRemoteReceipt(*r))
		}
	}
	sort.Slice(operations, func(i, j int) bool { return operations[i].created.After(operations[j].created) })
	if len(operations) > 64 {
		operations = operations[:64]
	}
	value := map[string]any{"hostId": m.hostID, "hostEpoch": m.epoch, "enabled": m.enabled, "home": m.osHome, "platform": runtime.GOOS, "distribution": remoteDistribution(), "hostname": hostname, "revision": m.revision, "cursor": m.revision, "sessions": sessions, "sessionLimit": remoteMaxSessions, "events": m.events, "operations": operations, "executors": executors, "error": remotePublicMessage(m.failure), "attachments": m.attachmentProjectionLocked()}
	value["canCreateSession"] = !m.closed && (len(m.sessions) < remoteMaxSessions || m.archiveCandidateLocked() != nil)
	raw, _ := json.Marshal(value)
	var copied map[string]any
	_ = json.Unmarshal(raw, &copied)
	// Historical operation events can contain a diagnostic recorded by an older Core.
	for _, entry := range copied["events"].([]any) {
		event, _ := entry.(map[string]any)
		payload, _ := event["payload"].(map[string]any)
		if _, exists := payload["error"]; exists {
			msg, _ := payload["error"].(string)
			payload["error"] = remotePublicMessage(msg)
		}
	}
	for _, entry := range copied["sessions"].([]any) {
		row := entry.(map[string]any)
		delete(row, "ownedProcess")
		delete(row, "ownedGateway")
		id, _ := row["id"].(string)
		session := m.sessions[id]
		if session != nil {
			row["title"] = historySessionTitle(session)
			row["messageCount"] = len(session.Messages)
		}
		messages, _ := row["messages"].([]any)
		if row["state"] != "running" {
			delete(row, "messages")
		} else {
			row["messages"] = messages
		}
		native, _ := row["nativeId"].(string)
		row["attached"] = m.providers[id] != nil
		if s := m.sessions[id]; m.checkSite != nil && s != nil && s.Source == "newnoa" && m.providers[id] != nil {
			if executor := m.executors[s.Agent]; executor == nil || s.SiteIdentity == "" || s.SiteIdentity != executor.Identity {
				row["source"] = "unknown"
			}
		}
		row["needsResume"] = m.providers[id] == nil && native != ""
		if approvals, ok := row["approvals"].([]any); ok {
			for _, value := range approvals {
				if a, ok := value.(map[string]any); ok {
					delete(a, "nativeId")
					delete(a, "generation")
				}
			}
		}
	}
	for len(raw) > 470000 {
		changed := false
		for _, entry := range copied["sessions"].([]any) {
			s := entry.(map[string]any)
			messages, _ := s["messages"].([]any)
			for _, msg := range messages {
				v := msg.(map[string]any)
				text, _ := v["text"].(string)
				if len(text) > 512 {
					if _, exists := v["textBytes"]; !exists {
						v["textBytes"] = len(text)
					}
					v["text"] = historyPrefix(text, len(text)/2)
					v["truncated"] = true
					changed = true
				}
			}
		}
		copied["truncated"] = true
		raw, _ = json.Marshal(copied)
		if !changed {
			break
		}
	}
	m.addHistoryProjectionLocked(copied)
	return copied
}
func stopRemoteSessionProcesses(main remoteOwnedProcess, gateway *remoteOwnedProcess) error {
	var result error
	if main.PID > 1 {
		result = stopRemoteOwnedProcess(main)
	}
	if gateway != nil {
		result = errors.Join(result, stopRemoteOwnedProcess(*gateway))
	}
	return result
}

func (m *remoteManager) shutdown() error {
	m.shutdownMu.Lock()
	defer m.shutdownMu.Unlock()
	m.mu.Lock()
	m.closed = true
	m.enabled = false
	m.cancel()
	if m.relayCancel != nil {
		m.relayCancel()
	}
	cleanup := map[string]func() error{}
	for id, provider := range m.providers {
		p := provider
		cleanup[id] = p.Close
	}
	for id, session := range m.sessions {
		session.appendNativeOutput("", true)
		if _, ok := cleanup[id]; !ok {
			record, gateway := session.OwnedProcess, session.OwnedGateway
			if record.PID > 1 || gateway != nil {
				cleanup[id] = func() error { return stopRemoteSessionProcesses(record, gateway) }
			} else if session.NativeID != "" && session.State != "stopped" && session.State != "idle" {
				cleanup[id] = func() error { return errors.New("Previous native process ownership cannot be verified") }
			}
		}
		if session.State != "stopped" && session.State != "idle" {
			session.State = "unknown"
		}
		for _, a := range session.Approvals {
			if a.State == "open" {
				a.State = "invalidated"
			}
		}
	}
	providers := m.providers
	m.providers = map[string]remoteProvider{}
	m.mu.Unlock()
	type result struct {
		id  string
		err error
	}
	results := make(chan result, len(cleanup))
	for id, action := range cleanup {
		go func() { results <- result{id: id, err: action()} }()
	}
	failures := []string{}
	for range cleanup {
		outcome := <-results
		m.mu.Lock()
		s := m.sessions[outcome.id]
		if outcome.err != nil {
			s.State = "unknown"
			s.Error = remoteSafeError(outcome.err)
			failures = append(failures, outcome.id)
			if p := providers[outcome.id]; p != nil {
				m.providers[outcome.id] = p
			}
		} else {
			s.OwnedProcess = remoteOwnedProcess{}
			s.OwnedGateway = nil
			s.State = "stopped"
		}
		m.mu.Unlock()
	}

	m.mu.Lock()
	err := m.saveLocked()
	for _, a := range m.attachments {
		_ = os.Remove(a.Path)
	}
	m.attachments = map[string]*remoteAttachment{}
	m.mu.Unlock()
	if err != nil {
		return err
	}
	if len(failures) > 0 {
		return errors.New("Some owned remote agent processes could not be stopped")
	}
	if m.lock != nil {
		if e := m.lock.Close(); e != nil {
			return e
		}
		m.lock = nil
	}
	return nil
}

// Keep the OS account binding separate from the NEWNOA account and native provider login.
func (m *remoteManager) identityValid() bool {
	current, err := user.Current()
	return err == nil && current.Uid == m.uid
}
