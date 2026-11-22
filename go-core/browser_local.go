package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const browserLocalCommandMaxBytes = 65536

// H5 exposes authenticated bounded local capabilities without arbitrary filesystem RPCs.
func browserLocal(c *core, control string) http.Handler {
	client := connectorControlClient()
	client.Timeout = 8 * time.Second
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		language := browserLanguage(r)
		var command struct {
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		reject := func(code int, message string) {
			writeLocalError(w, code, language, browserLocalMessage(command.Method, command.Params, errors.New(message)))
		}

		if r.Method != "POST" || r.URL.Path != "/api/local/core" || r.URL.RawQuery != "" || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			reject(404, "Not found")
			return
		}

		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, browserLocalCommandMaxBytes))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&command) != nil || decoder.Decode(new(any)) != io.EOF {
			reject(400, "Invalid local command")
			return
		}
		switch command.Method {
		case "agent.detect", "agent.providers", "agent.version", "agent.preview", "agent.apply", "agent.restore", "agent.remote.status", "agent.remote.authorize", "remote.host.enable", "remote.snapshot", "remote.command", "remote.shutdown", "remote.operation.get", "remote.events.ticket":
			if dockerRuntime() {
				reject(403, "Agent operations are unavailable in Docker")
				return
			}
		case "noa.start", "noa.poll", "noa.cancel", "svg.tests.questions", "svg.tests.create", "svg.tests.list", "svg.tests.get", "svg.tests.cancel", "svg.tests.delete", "connector.ensure", "connector.start", "connector.stop", "connector.status", "proxy.start", "proxy.stop", "proxy.status", "proxy.diagnose", "proxy.endpoint", "proxy.compression.get", "proxy.compression.set", "proxy.compression.stats", "model.list", "model.assess", "model.service.check", "deployment.context_probe", "history.events.ticket", "history.database.status", "history.database.test", "history.database.configure", "history.query", "history.filters", "history.accounts", "history.sync.status", "history.export", "history.export.status", "history.export.cancel", "history.export.directory", "history.export.configure", "history.settings", "history.configure", "history.list", "history.conversation", "history.get", "history.content", "history.delete", "history.sync":
		default:
			reject(403, "Local operation unavailable in H5")
			return
		}
		if command.Method == "proxy.compression.stats" || strings.HasPrefix(command.Method, "history.") || strings.HasPrefix(command.Method, "svg.tests.") {
			if _, err := c.historyIdentity(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")); err != nil {
				reject(401, "Login required")
				return
			}
		} else {
			request, err := http.NewRequestWithContext(r.Context(), "GET", strings.TrimRight(control, "/")+"/api/v1/me", nil)
			if err != nil {
				reject(503, "Authentication unavailable")
				return
			}
			request.Header.Set("Authorization", r.Header.Get("Authorization"))
			response, err := client.Do(request)
			if err != nil {
				reject(503, "Authentication unavailable")
				return
			}
			defer response.Body.Close()
			if response.StatusCode != 200 {
				reject(401, "Login required")
				return
			}
			var account struct {
				ID       string `json:"id"`
				Language string `json:"language"`
			}
			if json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&account) != nil || !nodePattern.MatchString(account.ID) {
				reject(503, "Invalid authentication response")
				return
			}
			if account.Language == "en" || account.Language == "zh-CN" {
				language = account.Language
			}
		}
		if command.Method == "connector.ensure" {
			var params map[string]any
			if len(command.Params) > 0 && (json.Unmarshal(command.Params, &params) != nil || params == nil || len(params) != 0) {
				reject(400, "Invalid local supply command")
				return
			}
			command.Params = []byte(`{}`)
		}
		if strings.HasPrefix(command.Method, "noa.") || command.Method == "agent.preview" || command.Method == "agent.apply" || command.Method == "agent.providers" || command.Method == "agent.restore" || command.Method == "proxy.compression.stats" || strings.HasPrefix(command.Method, "remote.") || strings.HasPrefix(command.Method, "agent.remote.") || strings.HasPrefix(command.Method, "history.") || strings.HasPrefix(command.Method, "svg.tests.") || command.Method == "connector.ensure" || command.Method == "model.list" || command.Method == "model.assess" || command.Method == "model.service.check" || command.Method == "deployment.context_probe" {
			var fields map[string]any
			if json.Unmarshal(command.Params, &fields) != nil || fields == nil {
				reject(400, "Invalid history command")
				return
			}
			// Match encoding/json's Unicode case folding before inserting trusted identity.
			for name := range fields {
				if strings.EqualFold(name, "session") {
					delete(fields, name)
				}
			}
			fields["session"] = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			command.Params, _ = json.Marshal(fields)
		}
		var value any
		var err error
		if command.Method == "remote.events.ticket" || command.Method == "history.events.ticket" {
			origin := r.Header.Get("Origin")
			if origin == "" {
				scheme := "http"
				if r.TLS != nil {
					scheme = "https"
				}
				origin = scheme + "://" + r.Host
			}
			if !remoteBrowserOrigin(origin) && !(command.Method == "history.events.ticket" && dockerRuntime() && (origin == "http://"+r.Host || origin == "https://"+r.Host)) {
				reject(403, "Untrusted browser core origin")
				return
			}
			if !c.updateGate.begin() {
				reject(503, "Client update installation is starting")
				return
			}
			defer c.updateGate.end()
			if command.Method == "history.events.ticket" {
				value, err = c.historyEventTicket(command.Params, origin)
			} else {
				value, err = c.remoteRPCOrigin(command.Method, command.Params, origin)
			}
		} else {
			value, err = c.handle(command.Method, command.Params)
		}
		if err != nil {
			writeLocalError(w, http.StatusBadRequest, language, browserLocalMessage(command.Method, command.Params, err))
			return
		}
		if dockerRuntime() && (command.Method == "proxy.status" || command.Method == "proxy.start") {
			status := value.(map[string]any)
			status["runtimeMode"] = "docker"
			status["baseUrls"] = []string{}
			host, _, _ := net.SplitHostPort(r.Host)
			if c.endpointIP != "" && c.proxyLAN {
				status["baseUrls"] = c.proxyEndpoints()
			} else if browserHostIP(host) && c.proxyLAN {
				status["baseUrls"] = []string{proxyEndpointURL(host, fmt.Sprint(c.proxyPortValue()))}
			}
		}
		json.NewEncoder(w).Encode(value)
	})
}
