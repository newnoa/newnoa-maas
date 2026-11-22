package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const serviceCookie = "newnoa_core_session"

type dockerSessionState struct {
	SessionID        string         `json:"sessionId"`
	Token            string         `json:"token"`
	RefreshToken     string         `json:"refreshToken"`
	ExpiresAt        time.Time      `json:"expiresAt"`
	Account          map[string]any `json:"account"`
	CookieHash       string         `json:"cookieHash"`
	PendingOperation string         `json:"pendingOperation,omitempty"`
	AckOperation     string         `json:"ackOperation,omitempty"`
	Closing          bool           `json:"closing,omitempty"`
}
type dockerServiceSession struct {
	mu      sync.Mutex
	account *browserAccount
	control string
	state   dockerSessionState
}

func newDockerServiceSession(account *browserAccount, control string) *dockerServiceSession {
	return &dockerServiceSession{account: account, control: control}
}
func (s *dockerServiceSession) path() string {
	return filepath.Join(s.account.core.home, ".newnoa", "service-session.json")
}
func (s *dockerServiceSession) save() error {
	if err := s.account.core.safeConfigFile(s.path()); err != nil {
		return err
	}
	raw, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	return atomicFile(s.path(), raw)
}
func (s *dockerServiceSession) load() error {
	if !dockerRuntime() {
		return nil
	}
	if err := s.account.core.safeConfigFile(s.path()); err != nil {
		return err
	}
	raw, err := os.ReadFile(s.path())
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	info, err := os.Stat(s.path())
	if err != nil || info.Mode().Perm()&0077 != 0 || len(raw) > 65536 {
		return errors.New("unsafe service session state")
	}
	if json.Unmarshal(raw, &s.state) != nil || !s.valid(s.state) || len(s.state.CookieHash) != 64 {
		return errors.New("invalid service session state")
	}
	s.bind()
	return nil
}
func (s *dockerServiceSession) valid(state dockerSessionState) bool {
	id, _ := state.Account["id"].(string)
	return nodePattern.MatchString(state.SessionID) && nodePattern.MatchString(id) && state.Account["role"] == "user" && strings.HasPrefix(state.Token, "ses_") && len(state.Token) == 47 && strings.HasPrefix(state.RefreshToken, "ref_") && len(state.RefreshToken) == 47 && !state.ExpiresAt.IsZero()
}
func (s *dockerServiceSession) bind() {
	a := s.account
	a.switchMu.Lock()
	defer a.switchMu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	previousOwner := a.ownerID
	a.ownerID, _ = s.state.Account["id"].(string)
	a.claimed = true
	a.active = !s.state.Closing
	a.cleanupPending = s.state.Closing
	a.serviceManaged = true
	a.tokenHash = sha256.Sum256([]byte(s.state.Token))
	if a.core != nil {
		if a.active {
			if previousOwner == a.ownerID {
				a.core.renewProxySession(s.state.Token)
			} else {
				a.core.setProxySession(s.state.Token)
			}
		} else {
			a.core.setProxySession("")
		}
	}
}
func (s *dockerServiceSession) call(ctx context.Context, method, path, token string, body any, out any) error {
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.control, "/")+"/api/v1"+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("service authentication unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode == 401 || response.StatusCode == 403 {
		return errBrowserAccountUnauthorized
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("service authentication unavailable: HTTP %d", response.StatusCode)
	}
	if out != nil && json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(out) != nil {
		return errors.New("invalid service authentication response")
	}
	return nil
}
func (s *dockerServiceSession) clear() error {
	token := s.state.Token
	s.state.Closing = true
	if err := s.save(); err != nil {
		return err
	}
	if err := s.account.expire(token); err != nil {
		return err
	}
	if err := os.Remove(s.path()); err != nil && !os.IsNotExist(err) {
		return err
	}
	s.state = dockerSessionState{}
	return nil
}

// The operation is persisted before sending; an interrupted request retries exactly that operation.
func (s *dockerServiceSession) current(ctx context.Context) (dockerSessionState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.SessionID == "" || s.state.Closing {
		return dockerSessionState{}, errBrowserAccountUnauthorized
	}
	if s.state.PendingOperation != "" || time.Until(s.state.ExpiresAt) < 2*time.Minute {
		if s.state.PendingOperation == "" {
			s.state.PendingOperation = serviceOperationID()
			if err := s.save(); err != nil {
				s.state.PendingOperation = ""
				return dockerSessionState{}, err
			}
		}
		operation := s.state.PendingOperation
		var next dockerSessionState
		err := s.call(ctx, "POST", "/auth/service-sessions/refresh", "", map[string]any{"sessionId": s.state.SessionID, "refreshToken": s.state.RefreshToken, "operationId": operation}, &next)
		if err != nil {
			if errors.Is(err, errBrowserAccountUnauthorized) {
				if clearErr := s.clear(); clearErr != nil {
					return dockerSessionState{}, clearErr
				}
			}
			return dockerSessionState{}, err
		}
		if !s.valid(next) || next.SessionID != s.state.SessionID || next.Account["id"] != s.state.Account["id"] {
			return dockerSessionState{}, errors.New("service identity changed during refresh")
		}
		previous := s.state
		next.CookieHash = previous.CookieHash
		next.AckOperation = operation
		s.state = next
		if err := s.save(); err != nil {
			s.state = previous
			return dockerSessionState{}, err
		}
		s.bind()
	}
	if s.state.AckOperation != "" {
		if err := s.call(ctx, "POST", "/auth/service-sessions/ack", s.state.Token, map[string]any{"operationId": s.state.AckOperation}, nil); err == nil {
			s.state.AckOperation = ""
			if err := s.save(); err != nil {
				return dockerSessionState{}, err
			}
		} else if errors.Is(err, errBrowserAccountUnauthorized) {
			if clearErr := s.clear(); clearErr != nil {
				return dockerSessionState{}, clearErr
			}
			return dockerSessionState{}, err
		}
	}
	return s.state, nil
}
func (s *dockerServiceSession) maintain(ctx context.Context) { s.maintainEvery(ctx, 30*time.Second) }
func (s *dockerServiceSession) maintainEvery(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		s.mu.Lock()
		if s.state.Closing {
			err := s.revoke(ctx, s.state)
			if err == nil || errors.Is(err, errBrowserAccountUnauthorized) {
				_ = s.clear()
			}
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()
		state, err := s.current(ctx)
		if err != nil {
			continue
		}
		if err = s.call(ctx, "GET", "/me", state.Token, nil, nil); errors.Is(err, errBrowserAccountUnauthorized) {
			s.mu.Lock()
			if s.state.Token == state.Token {
				_ = s.clear()
			}
			s.mu.Unlock()
		}
	}
}
func (s *dockerServiceSession) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if !dockerRuntime() || r.URL.Path != "/api/local/session" || r.URL.RawQuery != "" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("X-Newnoa-Session") != "1" || !serviceSameOrigin(r) {
			accountError(w, r, 403, "Untrusted service session origin")
			return
		}
		if r.Method == "POST" {
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if token == "" || token == r.Header.Get("Authorization") || len(token) > 512 {
				accountError(w, r, 401, "Login required")
				return
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			var next dockerSessionState
			if err := s.call(r.Context(), "POST", "/auth/service-sessions", token, nil, &next); err != nil {
				accountError(w, r, 401, err.Error())
				return
			}
			if !s.valid(next) {
				accountError(w, r, 503, "Invalid authentication response")
				return
			}
			if s.state.SessionID != "" {
				if err := s.revoke(r.Context(), s.state); err != nil && !errors.Is(err, errBrowserAccountUnauthorized) {
					_ = s.revoke(r.Context(), next)
					accountError(w, r, 503, "Local account switch failed")
					return
				}
			}
			if err := s.account.authenticate(r, next.Token, s.control); err != nil {
				_ = s.revoke(r.Context(), next)
				accountError(w, r, 503, "Local account switch failed")
				return
			}
			cookie := randomToken()
			next.CookieHash = hash([]byte(cookie))
			s.state = next
			if err := s.save(); err != nil {
				_ = s.revoke(r.Context(), next)
				_ = s.clear()
				accountError(w, r, 503, "Local account switch failed")
				return
			}
			s.bind()
			http.SetCookie(w, &http.Cookie{Name: serviceCookie, Value: cookie, Path: "/api/local/session", HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteStrictMode, MaxAge: 365 * 24 * 3600})
			writeServiceState(w, next)
			return
		}
		if r.Method != "GET" && r.Method != "DELETE" {
			w.WriteHeader(405)
			return
		}
		cookie, err := r.Cookie(serviceCookie)
		if err != nil {
			accountError(w, r, 401, "Login required")
			return
		}
		s.mu.Lock()
		expected := s.state.CookieHash
		s.mu.Unlock()
		actual := hash([]byte(cookie.Value))
		if expected == "" || subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) != 1 {
			accountError(w, r, 401, "Login required")
			return
		}
		if r.Method == "DELETE" {
			s.mu.Lock()
			defer s.mu.Unlock()
			if subtle.ConstantTimeCompare([]byte(actual), []byte(s.state.CookieHash)) != 1 {
				accountError(w, r, 401, "Login required")
				return
			}
			previous := s.state
			s.state.Closing = true
			if err := s.save(); err != nil {
				s.state = previous
				accountError(w, r, 503, "Local logout failed")
				return
			}
			if err := s.account.expire(s.state.Token); err != nil {
				accountError(w, r, 503, "Local logout failed")
				return
			}
			if err := s.revoke(r.Context(), s.state); err != nil && !errors.Is(err, errBrowserAccountUnauthorized) {
				accountError(w, r, 503, "Local logout failed")
				return
			}
			if err := s.clear(); err != nil {
				accountError(w, r, 503, "Local logout failed")
				return
			}
			http.SetCookie(w, &http.Cookie{Name: serviceCookie, Path: "/api/local/session", HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: -1})
			json.NewEncoder(w).Encode(map[string]bool{"loggedOut": true})
			return
		}
		state, err := s.current(r.Context())
		if err != nil {
			status := 503
			if errors.Is(err, errBrowserAccountUnauthorized) {
				status = 401
			}
			accountError(w, r, status, err.Error())
			return
		}
		if subtle.ConstantTimeCompare([]byte(actual), []byte(state.CookieHash)) != 1 {
			accountError(w, r, 401, "Login required")
			return
		}
		writeServiceState(w, state)
	})
}
func writeServiceState(w http.ResponseWriter, state dockerSessionState) {
	json.NewEncoder(w).Encode(map[string]any{"token": state.Token, "expiresAt": state.ExpiresAt, "account": state.Account})
}
func serviceSameOrigin(r *http.Request) bool {
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		scheme := "http"
		if r.TLS != nil || browserPublicHost(r.Host) {
			scheme = "https"
		}
		return err == nil && u.Scheme == scheme && u.Host == r.Host && u.Path == "" && u.RawQuery == "" && u.Fragment == "" && u.User == nil
	}
	return true
}
func browserCapabilities(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" || r.URL.RawQuery != "" {
		http.NotFound(w, r)
		return
	}
	mode := "dev"
	if dockerRuntime() {
		mode = "docker"
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"runtimeMode": mode})
}

func serviceOperationID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("random source unavailable")
	}
	raw[6] = (raw[6] & 15) | 64
	raw[8] = (raw[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[:4], raw[4:6], raw[6:8], raw[8:10], raw[10:])
}

func (s *dockerServiceSession) revoke(ctx context.Context, state dockerSessionState) error {
	return s.call(ctx, "POST", "/auth/service-sessions/revoke", "", map[string]any{"sessionId": state.SessionID, "refreshToken": state.RefreshToken}, nil)
}
