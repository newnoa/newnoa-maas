package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

func (c *core) startupStatus() map[string]any {
	state := map[string]any{"ready": c.startupReady, "port": c.proxyPortValue(), "message": ""}
	if c.startupError != "" {
		msg := browserLocalMessage("startup.bind", nil, errors.New(c.startupError))
		state["message"], state["publicMessage"], state["localizedMessages"] = msg.English, true, msg
	}
	return state
}

func (c *core) bindStartupPort(port int) error {
	if port < 1024 || port > 65535 {
		return errors.New("proxy port must be an integer from 1024 to 65535")
	}
	if c.startupReady {
		if port == c.proxyPortValue() {
			return nil
		}
		return errors.New("local port is already configured")
	}
	if err := c.canChangeProxyPort(port); err != nil {
		c.startupError = err.Error()
		return err
	}
	c.proxyPort = port
	allowLAN := dockerRuntime() || c.proxyLAN
	listener, err := reserveProxyListener(port, allowLAN)
	if err != nil {
		c.startupError = err.Error()
		return err
	}
	c.proxyListener, c.proxyReserved = listener, true
	if err = c.startProxyOn("", port, allowLAN); err != nil {
		listener.Close()
		c.proxyListener = nil
		c.proxyReserved = false
		c.startupError = err.Error()
		return err
	}
	c.startupReady = true
	c.startupError = ""
	return nil
}

func (c *core) startupCommand(method string, raw json.RawMessage) (any, error) {
	if method == "startup.status" {
		if len(raw) > 0 && string(raw) != "{}" {
			return nil, errors.New("startup status accepts no parameters")
		}
	} else {
		var command struct {
			Port int `json:"port"`
		}
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&command) != nil || decoder.Decode(new(any)) != io.EOF {
			return nil, errors.New("invalid startup port")
		}
		if err := c.bindStartupPort(command.Port); err != nil {
			return nil, err
		}
	}
	return c.startupStatus(), nil
}

// Start listening before login; Server/API validate inference credentials.
func browserStartup(c *core) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path != "/api/local/startup" || r.URL.RawQuery != "" {
			http.NotFound(w, r)
			return
		}
		var state any
		var err error
		switch r.Method {
		case "GET":
			state, err = c.handle("startup.status", nil)
		case "POST":
			if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
				w.WriteHeader(http.StatusUnsupportedMediaType)
				return
			}
			raw, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 256))
			if readErr != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			state, err = c.handle("startup.bind", raw)
		default:
			w.Header().Set("Allow", "GET, POST")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err != nil {
			writeLocalError(w, http.StatusBadRequest, browserLanguage(r), browserLocalMessage("startup.bind", nil, err))
			return
		}
		if result, ok := state.(map[string]any); ok {
			if m, ok := result["localizedMessages"].(localMessage); ok {
				result["message"] = m.in(browserLanguage(r))
			}
		}
		json.NewEncoder(w).Encode(state)
	})
}
