package main

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

const remoteOutputLimit = 65536

type remoteCredentialForm struct {
	prefixes      []string
	foldCase      bool
	completeBody  string
	remainingBody string
}

var remoteCredentialForms = []remoteCredentialForm{
	{[]string{"sk-", "nn_", "ra_", "rla_", "nref_"}, false, `[A-Za-z0-9_-]{16,}`, `[A-Za-z0-9_-]*`},
	{[]string{"Bearer"}, true, `\s+[A-Za-z0-9._~+/-]{16,}`, `(?:\s+[A-Za-z0-9._~+/-]*)?`},
	{[]string{"token", "api_key", "apiKey", "password", "signature"}, true, `=[^\s&"']+`, `(?:=[^\s&"']*)?`},
}

func compileRemoteCredentialPattern(suffix bool) *regexp.Regexp {
	var alternatives []string
	for _, form := range remoteCredentialForms {
		group := "(?:"
		if form.foldCase {
			group = "(?i:"
		}
		body := form.completeBody
		if suffix {
			body = form.remainingBody
			var partial []string
			for _, prefix := range form.prefixes {
				for end := 1; end < len(prefix); end++ {
					partial = append(partial, regexp.QuoteMeta(prefix[:end]))
				}
			}
			alternatives = append(alternatives, group+strings.Join(partial, "|")+")")
		}
		alternatives = append(alternatives, group+strings.Join(form.prefixes, "|")+")"+body)
	}
	pattern := "(?:" + strings.Join(alternatives, "|") + ")"
	if suffix {
		pattern += `\z`
	}
	return regexp.MustCompile(pattern)
}

var remoteCredentialPattern = compileRemoteCredentialPattern(false)
var remoteCredentialSuffix = compileRemoteCredentialPattern(true)

func remoteRedact(s string) string { return remoteCredentialPattern.ReplaceAllString(s, "[redacted]") }

// Hold a potential credential until the stream supplies its boundary. Pending bytes
// are kept in memory only and are absent from both the journal and remote projections.
func (s *remoteSession) appendNativeOutput(delta string, final bool) {
	if s.outputTruncated {
		s.outputTruncated = !final
		return
	}
	raw := s.outputTail + delta
	s.outputTail = ""
	if !final {
		if suffix := remoteCredentialSuffix.FindStringIndex(raw); suffix != nil {
			boundary := suffix[0]
			// A potential credential can start inside an earlier complete match.
			for _, match := range remoteCredentialPattern.FindAllStringIndex(raw, -1) {
				if match[0] < boundary && boundary < match[1] {
					boundary = match[0]
					break
				}
			}
			s.outputTail, raw = raw[boundary:], raw[:boundary]
		}
	}
	if len(s.outputTail) > remoteOutputLimit {
		s.outputTail = ""
		s.outputTruncated = true
	} else {
		s.outputTail = strings.Clone(s.outputTail)
	}
	text := remoteRedact(raw)
	if text == "" && !s.outputTruncated {
		return
	}
	last := len(s.Messages) - 1
	if last < 0 || s.Messages[last].Role != "assistant" {
		s.Messages = append(s.Messages, remoteMessage{ID: uuid.NewString(), Role: "assistant", CreatedAt: remoteNow()})
		last++
	}
	message := &s.Messages[last]
	remaining := max(0, remoteOutputLimit-len(message.Text))
	if len(text) > remaining {
		text = historyPrefix(text, remaining)
		message.Truncated = true
	}
	message.Text += text
	message.Truncated = message.Truncated || s.outputTruncated
}

func (s *remoteSession) redactStoredOutput() {
	for i := range s.Messages {
		if s.Messages[i].Role == "assistant" {
			s.Messages[i].Text = remoteRedact(s.Messages[i].Text)
		}
	}
}

func remotePublicData(raw json.RawMessage) json.RawMessage {
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return nil
	}
	var clean func(any) any
	clean = func(v any) any {
		switch x := v.(type) {
		case map[string]any:
			for k, y := range x {
				switch strings.ToLower(strings.ReplaceAll(k, "_", "")) {
				case "apikey", "token", "authorization", "password", "secret", "hostsecret", "credential", "credentials", "accesstoken", "refreshtoken":
					delete(x, k)
				default:
					x[k] = clean(y)
				}
			}
			return x
		case []any:
			for i, y := range x {
				x[i] = clean(y)
			}
			return x
		case string:
			return remoteRedact(x)
		default:
			return v
		}
	}
	out, _ := json.Marshal(clean(value))
	return out
}
