package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/newnoa/newnoa-maas/go-server/network/configuredhost"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type browserAccount struct {
	core           *core
	switchMu       sync.Mutex
	mu             sync.Mutex
	ownerID        string
	tokenHash      [32]byte
	active         bool
	claimed        bool
	cleanupPending bool
	runtimeCleanup chan error
	serviceManaged bool
	requests       map[uint64]browserAccountRequest
	next           uint64
}
type browserAccountRequest struct {
	cancel context.CancelFunc
	done   chan struct{}
}

var errBrowserAccountUnauthorized = errors.New("Login required")
var errBrowserAccountConflict = errors.New("This Go Core is already signed in to another account")

func (a *browserAccount) identify(ctx context.Context, token, control string) (string, error) {
	if token == "" {
		return "", errBrowserAccountUnauthorized
	}
	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, strings.TrimRight(control, "/")+"/api/v1/me", nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := (&http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(request)
	if err != nil {
		return "", fmt.Errorf("account verification unavailable: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return "", errBrowserAccountUnauthorized
	}
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("account verification unavailable: HTTP %d", response.StatusCode)
	}
	var account struct {
		ID string `json:"id"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&account) != nil || !nodePattern.MatchString(account.ID) {
		return "", errors.New("invalid authentication response")
	}
	return account.ID, nil
}

func (a *browserAccount) statePath() string {
	return filepath.Join(a.core.home, ".newnoa", "browser-account.json")
}
func (a *browserAccount) persist(claimed bool, accountID string) error {
	if a.core == nil {
		return nil
	}
	raw, err := json.Marshal(map[string]any{"claimed": claimed, "accountId": accountID})
	if err != nil {
		return err
	}
	return atomicFile(a.statePath(), raw)
}
func (a *browserAccount) load() error {
	if a.core == nil {
		return nil
	}
	path := a.statePath()
	if err := a.core.safeConfigFile(path); err != nil {
		return err
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var saved struct {
		Claimed   bool   `json:"claimed"`
		AccountID string `json:"accountId"`
	}
	if json.Unmarshal(raw, &saved) != nil || saved.AccountID != "" && !nodePattern.MatchString(saved.AccountID) {
		return errors.New("invalid browser account state")
	}
	a.claimed = saved.Claimed
	a.ownerID = saved.AccountID
	return nil
}

// Failed verification preserves the operator; failed cleanup keeps both accounts fenced.
func (a *browserAccount) authenticate(r *http.Request, token, control string) error {
	accountID, err := a.identify(r.Context(), token, control)
	if err != nil {
		return err
	}
	a.switchMu.Lock()
	defer a.switchMu.Unlock()
	tokenHash := sha256.Sum256([]byte(token))
	a.mu.Lock()
	same := a.claimed && a.ownerID == accountID && !a.cleanupPending
	if same {
		a.active = true
		a.tokenHash = tokenHash
		a.serviceManaged = false
		a.mu.Unlock()
		if a.core != nil {
			a.core.renewProxySession(token)
		}
		return nil
	}
	a.mu.Unlock()
	if err := a.cleanupRuntime(r.Context()); err != nil {
		return err
	}
	if err := r.Context().Err(); err != nil {
		return err
	}
	if err := a.persist(true, accountID); err != nil {
		return err
	}
	a.mu.Lock()
	a.ownerID = accountID
	a.tokenHash = tokenHash
	a.active = true
	a.claimed = true
	a.cleanupPending = false
	a.serviceManaged = false
	a.mu.Unlock()
	if a.core != nil {
		a.core.setProxySession(token)
	}
	return nil
}

func (a *browserAccount) begin(token, control string, localHistory ...bool) (context.Context, func(), error) {
	return a.beginRequest(token, control, len(localHistory) > 0 && localHistory[0], false)
}

func (a *browserAccount) beginRequest(token, control string, localHistory, remoteObserver bool) (context.Context, func(), error) {
	if a.core != nil && a.core.historySessionRevoked(token) {
		if err := a.expire(token); err != nil {
			return nil, nil, err
		}
		return nil, nil, errBrowserAccountUnauthorized
	}
	var accountID string
	var err error
	if localHistory {
		hash := sha256.Sum256([]byte(token))
		a.mu.Lock()
		if a.active && !a.cleanupPending && subtle.ConstantTimeCompare(hash[:], a.tokenHash[:]) == 1 {
			accountID = a.ownerID
		}
		a.mu.Unlock()
	}
	if accountID == "" {
		accountID, err = a.identify(context.Background(), token, control)
	}
	if err != nil {
		if errors.Is(err, errBrowserAccountUnauthorized) {
			a.mu.Lock()
			managed := a.serviceManaged
			a.mu.Unlock()
			// Service maintenance validates revocation after any pending refresh commits locally.
			if !managed {
				if cleanupErr := a.expire(token); cleanupErr != nil {
					return nil, nil, cleanupErr
				}
			}
		}
		return nil, nil, err
	}
	a.switchMu.Lock()
	defer a.switchMu.Unlock()
	tokenHash := sha256.Sum256([]byte(token))
	a.mu.Lock()
	if a.cleanupPending {
		a.mu.Unlock()
		return nil, nil, errors.New("Local account cleanup is still pending")
	}
	if a.serviceManaged && subtle.ConstantTimeCompare(tokenHash[:], a.tokenHash[:]) != 1 {
		a.mu.Unlock()
		return nil, nil, errBrowserAccountUnauthorized
	}
	if remoteObserver {
		if a.claimed && a.ownerID != accountID {
			a.mu.Unlock()
			return nil, nil, errBrowserAccountConflict
		}
	} else if !a.active && !a.claimed {
		if err := a.persist(true, accountID); err != nil {
			a.mu.Unlock()
			return nil, nil, err
		}
		a.ownerID = accountID
		a.claimed = true
	}
	if !remoteObserver && a.ownerID != accountID {
		a.mu.Unlock()
		return nil, nil, errBrowserAccountConflict
	}
	// Readers participate in account-switch cancellation, but never take over its credentials.
	if !remoteObserver {
		a.active = true
		a.tokenHash = tokenHash
	}
	ctx, cancel := context.WithCancel(context.Background())
	id := a.next
	a.next++
	done := make(chan struct{})
	if a.requests == nil {
		a.requests = map[uint64]browserAccountRequest{}
	}
	a.requests[id] = browserAccountRequest{cancel, done}
	a.mu.Unlock()
	if a.core != nil && !remoteObserver {
		a.core.renewProxySession(token)
		a.core.historyIdentityMu.Lock()
		previousHistory := a.core.historyIdentityHash
		a.core.historyIdentityHash = tokenHash
		a.core.historyIdentityAccount = accountID
		a.core.historyIdentityMu.Unlock()
		if previousHistory != tokenHash {
			a.core.historyEvents.revoke(previousHistory)
		}
	}
	return ctx, func() {
		a.mu.Lock()
		if _, ok := a.requests[id]; ok {
			delete(a.requests, id)
			close(done)
		}
		a.mu.Unlock()
		cancel()
	}, nil
}

func (a *browserAccount) expire(token string) error {
	a.switchMu.Lock()
	defer a.switchMu.Unlock()
	tokenHash := sha256.Sum256([]byte(token))
	a.mu.Lock()
	if (!a.active && !a.cleanupPending) || subtle.ConstantTimeCompare(tokenHash[:], a.tokenHash[:]) != 1 {
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()
	if err := a.cleanupRuntime(context.Background()); err != nil {
		return err
	}
	a.mu.Lock()
	a.cleanupPending = false
	a.tokenHash = [32]byte{}
	a.mu.Unlock()
	return nil
}

func (a *browserAccount) logout(token, control string) error {
	a.switchMu.Lock()
	defer a.switchMu.Unlock()
	a.mu.Lock()
	if (a.active || a.cleanupPending) && sha256.Sum256([]byte(token)) == a.tokenHash {
		a.mu.Unlock()
		if err := a.cleanupRuntime(context.Background()); err != nil {
			return err
		}
		if err := a.persist(true, ""); err != nil {
			return err
		}
		a.mu.Lock()
		a.ownerID = ""
		a.tokenHash = [32]byte{}
		a.cleanupPending = false
		a.mu.Unlock()
		return nil
	}
	a.mu.Unlock()
	return errors.New("This Go Core is already signed in to another account")
}

// Caller holds switchMu. A timed-out runtime stop is reused, never run concurrently.
func (a *browserAccount) cleanupRuntime(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	a.mu.Lock()
	a.active = false
	a.claimed = true
	a.cleanupPending = true
	requests := make([]browserAccountRequest, 0, len(a.requests))
	for _, request := range a.requests {
		requests = append(requests, request)
		request.cancel()
	}
	a.mu.Unlock()
	if a.core != nil {
		cleanupErr := a.core.stopRemote()
		a.core.setProxySession("")
		if cleanupErr != nil {
			return fmt.Errorf("Local account cleanup still pending: %w", cleanupErr)
		}
	}
	for _, request := range requests {
		select {
		case <-request.done:
		case <-ctx.Done():
			return fmt.Errorf("Local account cleanup still pending: %w", ctx.Err())
		}
	}
	previousAttempt := a.runtimeCleanup != nil
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("Local account cleanup still pending: %w", err)
		}
		if a.runtimeCleanup == nil {
			a.runtimeCleanup = make(chan error, 1)
			done := a.runtimeCleanup
			go func() { done <- a.stopRuntime() }()
		}
		select {
		case err := <-a.runtimeCleanup:
			a.runtimeCleanup = nil
			if err != nil && previousAttempt {
				previousAttempt = false
				continue
			}
			return err
		case <-ctx.Done():
			return fmt.Errorf("Local account cleanup still pending: %w", ctx.Err())
		}
	}
}

func (a *browserAccount) stopRuntime() error {
	if a.core == nil {
		return nil
	}
	if err := a.core.stopRemote(); err != nil {
		return fmt.Errorf("Local account cleanup still pending: %w", err)
	}
	a.core.setProxySession("")
	a.core.svgWorkers.Wait()
	a.core.close()
	if a.core.connector != nil {
		connector := a.core.connector
		connector.stop()
		select {
		case <-connector.done:
		case <-time.After(5 * time.Second):
			return errors.New("Local connector did not stop; account remains locked")
		}
		if a.core.connector == connector {
			a.core.connector = nil
		}
	}
	if a.core.startupReady {
		return a.core.startProxyOn("", a.core.proxyPortValue(), a.core.proxyLAN)
	}
	return nil
}

func (a *browserAccount) handler(next http.Handler, control string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/local/account" || r.URL.Path == "/api/local/startup" || r.URL.Path == "/api/local/session" || r.URL.Path == "/api/local/capabilities" {
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/api/v1/auth/login" || r.URL.Path == "/api/v1/auth/register" {
			result := httptest.NewRecorder()
			next.ServeHTTP(result, r)
			body := result.Body.Bytes()
			if result.Code >= 200 && result.Code < 300 {
				var response struct {
					Token string `json:"token"`
				}
				if json.Unmarshal(body, &response) == nil && response.Token != "" {
					if err := a.authenticate(r, response.Token, control); err != nil {
						accountError(w, r, http.StatusServiceUnavailable, "Local account switch failed")
						return
					}
				}
			}
			copyBrowserResponse(w, result, body)
			return
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			next.ServeHTTP(w, r)
			return
		}
		localHistory, remoteObserver := false, false
		if r.URL.Path == "/api/local/core" {
			raw, readErr := io.ReadAll(io.LimitReader(r.Body, browserLocalCommandMaxBytes+1))
			r.Body = io.NopCloser(bytes.NewReader(raw))
			if readErr == nil && len(raw) <= browserLocalCommandMaxBytes {
				var command struct {
					Method string `json:"method"`
				}
				if json.Unmarshal(raw, &command) == nil {
					localHistory = strings.HasPrefix(command.Method, "history.")
					switch command.Method {
					case "agent.remote.status", "remote.snapshot", "remote.operation.get", "remote.events.ticket", "history.events.ticket", "proxy.status", "agent.detect", "history.database.status":
						remoteObserver = true
					}
				}
			}
		}
		ctx, release, err := a.beginRequest(token, control, localHistory, remoteObserver)
		if err != nil {
			status := http.StatusUnauthorized
			if !errors.Is(err, errBrowserAccountUnauthorized) && !errors.Is(err, errBrowserAccountConflict) {
				status = http.StatusServiceUnavailable
			}
			accountError(w, r, status, err.Error())
			return
		}
		requestContext, cancelRequest := context.WithCancel(ctx)
		stopIncoming := context.AfterFunc(r.Context(), cancelRequest)
		defer stopIncoming()
		defer cancelRequest()
		request := r.WithContext(requestContext)
		if r.URL.Path == "/api/v1/auth/logout" {
			result := httptest.NewRecorder()
			next.ServeHTTP(result, request)
			release()
			if result.Code >= 200 && result.Code < 300 {
				if err := a.logout(token, control); err != nil {
					accountError(w, r, http.StatusServiceUnavailable, "Local logout failed")
					return
				}
			}
			copyBrowserResponse(w, result, result.Body.Bytes())
			return
		}
		defer release()
		next.ServeHTTP(w, request)
	})
}

func accountError(w http.ResponseWriter, r *http.Request, status int, message string) {
	method := "account.verify"
	if r.URL.Path == "/api/local/session" {
		switch r.Method {
		case http.MethodPost:
			method = "account.connect"
		case http.MethodDelete:
			method = "account.logout"
		default:
			method = "account.refresh"
		}
	}
	writeLocalError(w, status, browserLanguage(r), browserLocalMessage(method, nil, errors.New(message)))
}
func copyBrowserResponse(w http.ResponseWriter, result *httptest.ResponseRecorder, body []byte) {
	for key, values := range result.Header() {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(result.Code)
	_, _ = w.Write(body)
}

// Browser development shares the Java API contract with the Electron client.
func browserAPI(control string) (http.Handler, error) {
	target, err := url.Parse(control)
	if err != nil || target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" || (target.Path != "" && target.Path != "/") || (target.Scheme != "http" && target.Scheme != "https") {
		return nil, fmt.Errorf("invalid control origin")
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.FlushInterval = -1
	proxy.Transport = telemetryTransport{next: http.DefaultTransport, propagate: true, route: "desktop.control"}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		writeLocalError(w, http.StatusBadGateway, browserLanguage(r), browserLocalMessage("control.api", nil, errors.New("Control API unavailable")))
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		allowed := strings.HasPrefix(p, "/api/v1/auth/") || p == "/api/v1/me" || strings.HasPrefix(p, "/api/v1/me/") || p == "/api/v1/deployments" || strings.HasPrefix(p, "/api/v1/deployments/") || p == "/api/v1/model-categories" || p == "/api/v1/model-descriptions/translate" && r.Method == http.MethodPost
		if !allowed || path.Clean(p) != p {
			http.NotFound(w, r)
			return
		}
		// No browser-controlled forwarding headers cross the local trust boundary.
		for _, h := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
			r.Header.Del(h)
		}
		w.Header().Set("Cache-Control", "no-store")
		proxy.ServeHTTP(w, r)
	}), nil
}

func browserBoundary(next http.Handler) http.Handler {
	localHost := func(host string) bool {
		ip := net.ParseIP(host)
		return host == "localhost" || (ip != nil && ip.IsLoopback())
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if strings.Contains(host, ":") {
			var err error
			host, _, err = net.SplitHostPort(host)
			if err != nil {
				http.Error(w, "Invalid browser core host", http.StatusForbidden)
				return
			}
		}
		dockerHost := dockerRuntime() && browserHostIP(host)
		publicHost := browserPublicHost(host)
		allowed := localHost(host) || publicHost || dockerHost
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			valid := err == nil && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
			if valid {
				trusted := (u.Scheme == "http" || u.Scheme == "https") && localHost(u.Hostname()) || u.Scheme == "https" && browserPublicHost(u.Host)
				if publicHost {
					trusted = origin == "https://"+host
				} else if dockerHost {
					scheme := "http"
					if r.TLS != nil {
						scheme = "https"
					}
					trusted = u.Scheme == scheme && u.Host == r.Host
				}
				valid = trusted
			}
			allowed = allowed && valid
		}
		if !allowed {
			http.Error(w, "Untrusted browser core origin", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func dockerRuntime() bool { return os.Getenv("NEWNOA_RUNTIME_MODE") == "docker" }

func browserHostIP(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && !ip.IsLoopback() && ip.IsGlobalUnicast()
}

func serveBrowserAPI(address, control string, port int) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil || net.ParseIP(host) == nil || (!net.ParseIP(host).IsLoopback() && !dockerRuntime()) {
		return fmt.Errorf("browser core must bind to a loopback IP outside Docker mode")
	}
	gateway := os.Getenv("NEWNOA_GATEWAY_URL")
	if gateway == "" {
		gateway = "https://server.example.invalid"
	}
	mux := http.NewServeMux()
	home, err := os.UserHomeDir()
	if isolated := os.Getenv("NEWNOA_CORE_TEST_HOME"); isolated != "" {
		home = isolated
		_, err = os.Stat(home)
	}
	if err != nil {
		return err
	}
	local := newCore(home, gateway)
	local.proxyPort = port
	local.control = control
	defer local.shutdown()
	if _, err = local.historyStore(); err != nil {
		return fmt.Errorf("call record maintenance could not start: %w", err)
	}
	account := &browserAccount{core: local}
	if err := account.load(); err != nil {
		return fmt.Errorf("browser account ownership state unavailable: %w", err)
	}
	_ = local.bindStartupPort(port)
	inference, err := privateInferenceWithSelector(control, gateway, local.gatewaySelector)
	if err != nil {
		return err
	}
	service := newDockerServiceSession(account, control)
	if err := service.load(); err != nil {
		return fmt.Errorf("service session state unavailable: %w", err)
	}
	serviceContext, stopService := context.WithCancel(context.Background())
	defer stopService()
	if dockerRuntime() {
		go service.maintain(serviceContext)
	}
	mux.Handle("/api/local/session", service.handler())
	mux.HandleFunc("/api/local/capabilities", browserCapabilities)
	mux.Handle("/api/local/core", browserLocal(local, control))
	mux.HandleFunc(remoteEventsPath, local.remoteEventsHTTP)
	mux.HandleFunc(historyEventsPath, local.historyEventsHTTP)
	mux.Handle("/api/local/startup", browserStartup(local))
	mux.Handle("/api/local/account", account.controlHandler(control))
	modelTest, err := local.modelTestHandler()
	if err != nil {
		return err
	}
	mux.Handle("/api/local/model-test", modelTest)
	mux.Handle("/v1/", local.localInference(local.hostedInference(inference)))
	if webRoot := os.Getenv("NEWNOA_BROWSER_WEB_ROOT"); webRoot != "" {
		info, err := os.Stat(webRoot + "/index.html")
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("browser renderer index unavailable")
		}
		mux.Handle("/", http.FileServer(http.Dir(webRoot)))
	}
	server := &http.Server{Addr: address, Handler: instrumentCoreHTTP(browserBoundary(account.handler(mux, control))), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() { <-ctx.Done(); server.Close() }()
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runtimeMode() string {
	if dockerRuntime() {
		return "docker"
	}
	return "dev"
}

func browserPublicHost(host string) bool {
	return configuredhost.Allowed("NEWNOA_BROWSER_HOSTS", host, "h5.example.invalid,dev-h5.example.invalid,sit-h5.example.invalid,dev-h5-cn.example.invalid")
}
