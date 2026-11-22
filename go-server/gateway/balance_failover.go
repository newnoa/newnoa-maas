package main

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

const balanceErrorLimit = 8192

var balanceCode = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,79}$`)

type balanceRule struct {
	Origin    string `json:"origin"`
	Status    int    `json:"status"`
	Code      string `json:"code"`
	CodeField string `json:"codeField"`
}

type balanceRules []balanceRule

func parseBalanceRules(value string) (balanceRules, error) {
	if value == "" {
		return nil, nil
	}
	var rules balanceRules
	if err := json.Unmarshal([]byte(value), &rules); err != nil || len(rules) > 100 {
		return nil, errors.New("invalid upstream balance rejection rules")
	}
	for _, rule := range rules {
		origin := balanceOrigin(rule.Origin)
		if origin == "" || origin != rule.Origin || rule.Status < 400 || rule.Status >= 500 ||
			!balanceCode.MatchString(rule.Code) ||
			(rule.CodeField != "" && rule.CodeField != "code" && rule.CodeField != "error.code") {
			return nil, errors.New("invalid upstream balance rejection rule")
		}
	}
	return rules, nil
}

func balanceOrigin(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return ""
	}
	return u.Scheme + "://" + strings.ToLower(u.Host)
}

func (rules balanceRules) mayMatch(endpoint string, status int) bool {
	origin := balanceOrigin(endpoint)
	for _, rule := range rules {
		if rule.Origin == origin && rule.Status == status {
			return true
		}
	}
	return false
}

func (rules balanceRules) match(endpoint string, status int, header http.Header, body []byte) string {
	mediaType, _, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" || header.Get("X-Newnoa-Outcome") == "unknown" || len(body) > balanceErrorLimit {
		return ""
	}
	// Require the supplier's structured refusal, with no response or metering evidence.
	var envelope map[string]json.RawMessage
	if json.Unmarshal(body, &envelope) != nil {
		return ""
	}
	for _, field := range []string{"usage", "choices", "output", "data", "content"} {
		if _, exists := envelope[field]; exists {
			return ""
		}
	}
	origin := balanceOrigin(endpoint)
	for _, rule := range rules {
		if rule.Origin != origin || rule.Status != status {
			continue
		}
		var code string
		if rule.CodeField == "code" {
			if _, nested := envelope["error"]; nested || json.Unmarshal(envelope["code"], &code) != nil {
				continue
			}
		} else {
			var detail struct {
				Code string `json:"code"`
			}
			if _, topLevel := envelope["code"]; topLevel || json.Unmarshal(envelope["error"], &detail) != nil {
				continue
			}
			code = detail.Code
		}
		if rule.Code == code {
			return code
		}
	}
	return ""
}

type prefixedResponseBody struct {
	io.Reader
	io.Closer
}
