package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type publicCallerSessionContext struct{}
type publicCallerProofContext struct{}
type publicCallerProofInput struct{ token, bodySHA256, callerIP string }
type publicControlHTTPError struct{ status int }

func (*publicControlHTTPError) Error() string { return "public control rejected request" }

// Public mode uses request-scoped caller identity, independent of the desktop account.
func validatePublicProxyAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	number, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || number < 1 || number > 65535 || ip == nil || (!ip.IsLoopback() && !ip.IsPrivate()) {
		return errors.New("NEWNOA_PUBLIC_PROXY_LISTEN requires a literal loopback or private IP and port")
	}
	return nil
}

func (c *core) publicControl(ctx context.Context, path string, body, result any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.control, "/")+"/api/v1/internal/public"+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+c.publicInternalToken)
	request.Header.Set("Content-Type", "application/json")
	client := connectorControlClient()
	client.Timeout = 10 * time.Second
	response, err := client.Do(request)
	if err != nil {
		return errors.New("public control unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return &publicControlHTTPError{status: response.StatusCode}
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 || json.Unmarshal(data, result) != nil {
		return errors.New("public control response invalid")
	}
	return nil
}

func validatePublicControlOrigin(controlURL string) error {
	control, err := url.Parse(controlURL)
	if err != nil || control.User != nil || control.RawQuery != "" || control.ForceQuery || control.Fragment != "" || (control.Path != "" && control.Path != "/") || (control.Scheme != "http" && control.Scheme != "https") {
		return errors.New("public proxy control requires a private API origin")
	}
	ip := net.ParseIP(control.Hostname())
	if ip == nil || (!ip.IsLoopback() && !ip.IsPrivate()) {
		return errors.New("public proxy control requires a literal loopback or private IP")
	}
	return nil
}

func (c *core) publicProxyHandler(secret, internalToken string) (http.Handler, error) {
	if len(secret) < 32 || len(secret) > 4096 || strings.TrimSpace(internalToken) == "" || strings.ContainsAny(secret+internalToken, "\r\n") {
		return nil, errors.New("public proxy service and internal API credentials are required")
	}
	if err := validatePublicControlOrigin(c.control); err != nil {
		return nil, err
	}
	preferred, err := parsePublicGoServer(os.Getenv("NEWNOA_PUBLIC_GO_SERVER"))
	if err != nil {
		return nil, err
	}
	if preferred != nil {
		selector := newGatewaySelector(c.control)
		if c.gatewaySelector != nil {
			selector.discover, selector.probe, selector.now = c.gatewaySelector.discover, c.gatewaySelector.probe, c.gatewaySelector.now
		}
		selector.preferred = preferred
		c.gatewaySelector = selector
	}
	inference, err := inferenceWithSelector(c.gateway, c.gatewaySelector)
	if err != nil {
		return nil, err
	}
	c.publicStateless = true
	c.publicInternalToken = internalToken
	c.publicResources = &publicResourceSampler{}
	c.publicResources.snapshot("")
	c.publicSVGContext, c.publicSVGCancel = context.WithCancel(context.Background())
	slots := make(chan struct{}, 128)
	var handler http.Handler
	handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("X-Newnoa-Public-Proxy")
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(secret)) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/internal/www/health" {
			if r.Method != http.MethodGet || r.URL.RawQuery != "" || r.URL.ForceQuery {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "no-store")
			local := ""
			if address, ok := r.Context().Value(http.LocalAddrContextKey).(net.Addr); ok {
				local = address.String()
			}
			json.NewEncoder(w).Encode(c.publicResources.snapshot(local))
			return
		}
		if len(r.Header.Values("Authorization")) != 1 || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") || !validAPIKey(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		defer c.publicResources.requests.Add(1)
		if r.URL.Path == "/internal/www/core" {
			c.servePublicSVGCommand(w, r)
			return
		}
		callerSession := ""
		if values := r.Header.Values("X-Newnoa-Caller-Session"); len(values) > 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		} else if len(values) == 1 {
			callerSession = values[0]
			if !strings.HasPrefix(callerSession, "ses_") || len(callerSession) > 256 || !validAPIKey(callerSession) {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}
		if r.URL.RawQuery != "" || r.URL.ForceQuery || !((r.Method == http.MethodGet && r.URL.Path == "/v1/models") || (r.Method == http.MethodPost && historyInferencePath(r.URL.Path))) {
			http.NotFound(w, r)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			http.Error(w, "public proxy is busy", http.StatusTooManyRequests)
			return
		}
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		out := r.Clone(ctx)
		if callerSession != "" {
			out = out.WithContext(context.WithValue(out.Context(), publicCallerSessionContext{}, callerSession))
		}
		out.Header = make(http.Header)
		for _, name := range []string{"Authorization", "Content-Type", "Accept", "User-Agent", "Anthropic-Version", "Anthropic-Beta", "Idempotency-Key"} {
			out.Header[name] = append([]string(nil), r.Header.Values(name)...)
		}
		if values := r.Header.Values("X-Newnoa-Caller-IP"); len(values) > 1 {
			http.Error(w, "invalid caller address", 400)
			return
		} else if len(values) == 1 {
			ip := net.ParseIP(values[0])
			if ip == nil {
				http.Error(w, "invalid caller address", 400)
				return
			}
			out.RemoteAddr = net.JoinHostPort(ip.String(), "0")
		}
		bearer := strings.TrimPrefix(out.Header.Get("Authorization"), "Bearer ")
		if callerSession != "" && !strings.HasPrefix(bearer, "nn_test_") {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		if r.Method == http.MethodGet {
			var models []hostedModel
			params := map[string]any{"secret": bearer}
			if callerSession != "" {
				params["session"] = callerSession
			}
			if err := c.publicControl(ctx, "/models", params, &models); err != nil {
				status, message := http.StatusBadGateway, "model authorization unavailable"
				var rejected *publicControlHTTPError
				if errors.As(err, &rejected) && (rejected.status == http.StatusUnauthorized || rejected.status == http.StatusForbidden) {
					status, message = rejected.status, http.StatusText(rejected.status)
				}
				http.Error(w, message, status)
				return
			}
			rows := make([]map[string]any, 0, len(models))
			for _, model := range models {
				if model.ID != "" {
					callable := model.Model
					if callable == "" {
						callable = model.ID
					}
					rows = append(rows, map[string]any{"id": callable, "deploymentId": model.ID, "object": "model", "name": model.Name, "model": model.Model, "modelType": model.ModelType})
				}
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": rows})
			return
		}
		release := reserveInferenceUpload(w, r, out.RemoteAddr)
		if release == nil {
			return
		}
		defer release()
		raw, err := readInferenceBody(r)
		if err != nil {
			http.Error(w, "request too large", 413)
			return
		}
		// Upload is separately bounded by ReadTimeout; this is only a safety envelope.
		ctx, stopInference := context.WithTimeout(out.Context(), inferenceWrapperTimeout)
		defer stopInference()
		out = out.WithContext(ctx)
		model := hostedRequestedModel(out, raw)
		if isImagePath(r.URL.Path) && validateImageRequest(r.URL.Path, raw) != nil {
			http.Error(w, "invalid image generation request", http.StatusBadRequest)
			return
		}
		if model == "" || len(model) > 200 {
			http.Error(w, "model is required", 400)
			return
		}
		setInferenceBody(out, raw)
		var admission struct {
			Hosted       *bool  `json:"hosted"`
			DeploymentID string `json:"deploymentId"`
		}
		params := map[string]any{"secret": bearer, "model": model}
		if callerSession != "" {
			params["session"] = callerSession
		}
		if c.publicControl(ctx, "/authorize", params, &admission) != nil || admission.Hosted == nil {
			http.Error(w, "model authorization rejected", http.StatusForbidden)
			return
		}
		if *admission.Hosted {
			if !nodePattern.MatchString(admission.DeploymentID) {
				http.Error(w, "hosted authorization response invalid", http.StatusBadGateway)
				return
			}
			defer c.publicResources.models.Add(1)
			c.executeHosted(w, out, admission.DeploymentID, raw)
			return
		}
		if callerSession != "" {
			out.Header.Set("Authorization", "Bearer "+callerSession)
			out.Header.Set("X-Api-Key", bearer)
		} else if validAPIKey(bearer) && !strings.HasPrefix(bearer, "ses_") && !strings.HasPrefix(bearer, "nn_test_") {
			if out.Header.Get("Idempotency-Key") == "" {
				out.Header.Set("Idempotency-Key", uuid.NewString())
			}
			caller, _, splitErr := net.SplitHostPort(out.RemoteAddr)
			if splitErr != nil {
				caller = out.RemoteAddr
			}
			out = out.WithContext(context.WithValue(out.Context(), publicCallerProofContext{}, publicCallerProofInput{token: c.publicInternalToken, bodySHA256: hash(raw), callerIP: caller}))
		}
		defer c.publicResources.models.Add(1)
		inference.ServeHTTP(w, out)
	})
	// Internal SVG generation enters the same authenticated, metered inference handler.
	c.publicSVGInference = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-Newnoa-Public-Proxy", secret)
		handler.ServeHTTP(w, r)
	})
	return handler, nil
}

func servePublicProxy() error {
	address := os.Getenv("NEWNOA_PUBLIC_PROXY_LISTEN")
	if address == "" {
		address = "127.0.0.1:18223"
	}
	if err := validatePublicProxyAddress(address); err != nil {
		return err
	}
	control := os.Getenv("NEWNOA_CONTROL_URL")
	if control == "" {
		control = "https://api.example.invalid"
	}
	gateway := os.Getenv("NEWNOA_GATEWAY_URL")
	if gateway == "" {
		gateway = "https://server.example.invalid"
	}
	home, err := os.UserHomeDir()
	if isolated := os.Getenv("NEWNOA_CORE_TEST_HOME"); isolated != "" {
		home = isolated
	}
	if err != nil {
		return err
	}
	c := &core{home: home, control: control, gateway: gateway, gatewaySelector: newGatewaySelector(control)}
	handler, err := c.publicProxyHandler(os.Getenv("NEWNOA_PUBLIC_PROXY_SECRET"), os.Getenv("NEWNOA_INTERNAL_TOKEN"))
	if err != nil {
		return err
	}
	defer func() { c.publicSVGCancel(); c.cancelSVGTests(); c.svgWorkers.Wait() }()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("public proxy listener unavailable: %w", err)
	}
	server := &http.Server{Handler: instrumentCoreHTTP(handler), ConnState: c.publicResources.connectionState, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Minute, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); server.Close() }()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
