package network

import (
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type peerEvidenceEvent struct {
	At     time.Time `json:"at"`
	Stage  string    `json:"stage"`
	Code   string    `json:"code"`
	Detail string    `json:"detail"`
}

type peerFallback struct {
	ID           string              `json:"id"`
	DeploymentID string              `json:"deploymentId"`
	NodeID       string              `json:"nodeId"`
	Epoch        int64               `json:"epoch"`
	StartedAt    time.Time           `json:"startedAt"`
	Status       string              `json:"status"`
	Sequence     int                 `json:"sequence"`
	RequestID    string              `json:"requestId,omitempty"`
	Events       []peerEvidenceEvent `json:"events"`
}

var evidenceSecrets = regexp.MustCompile(`(?i)(?:nn_|permit_)[a-z0-9_-]+|[a-f0-9]{32,}|-----BEGIN [^-]+-----[\s\S]*?-----END [^-]+-----`)
var evidenceAssignments = regexp.MustCompile(`(?i)(authorization|consumerKey|secretKey|network_secret|password|token|secret)\s*[=:]\s*[^\s,;]+`)
var evidenceURLCredentials = regexp.MustCompile(`(://)[^\s/@]+@|\?[^\s]+`)

func scrubPeerEvidence(value string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[redacted]")
		}
	}
	value = evidenceSecrets.ReplaceAllString(value, "[redacted]")
	value = evidenceAssignments.ReplaceAllString(value, "$1=[redacted]")
	value = evidenceURLCredentials.ReplaceAllString(value, "$1[redacted]")
	value = strings.Map(func(r rune) rune {
		if r < 32 && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, value)
	for len(value) > 2048 {
		_, size := utf8.DecodeRuneInString(value)
		value = value[size:]
	}
	return value
}

// Never retain the unbounded output or write subprocess configuration to logs.
type peerLogTail struct {
	mu      sync.Mutex
	text    string
	secrets []string
	limit   int
}

func (l *peerLogTail) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Keep a raw bounded tail until snapshot so credentials split across writes are redacted too.
	l.text += string(p)
	limit := l.limit
	if limit == 0 {
		limit = 8192
	}
	if len(l.text) > limit {
		l.text = l.text[len(l.text)-limit:]
		// Discard a partial first line so a truncated credential cannot lose its recognizable prefix.
		if end := strings.IndexByte(l.text, '\n'); end >= 0 {
			l.text = l.text[end+1:]
		} else {
			l.text = ""
		}
	}
	return len(p), nil
}
func (l *peerLogTail) snapshot() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return scrubPeerEvidence(l.text, l.secrets...)
}
func (d *peerFallback) event(stage, code, detail string) {
	if len(d.Events) < 16 {
		d.Events = append(d.Events, peerEvidenceEvent{time.Now().UTC(), stage, code, scrubPeerEvidence(detail)})
	}
}
