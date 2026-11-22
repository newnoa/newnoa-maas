package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

func (a *browserAccount) controlHandler(control string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != "POST" || r.URL.Path != "/api/local/account" || r.URL.RawQuery != "" || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			w.WriteHeader(404)
			return
		}
		var input struct {
			Action string `json:"action"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || (input.Action != "claim" && input.Action != "release") {
			w.WriteHeader(400)
			return
		}
		authorization := r.Header.Get("Authorization")
		if !strings.HasPrefix(authorization, "Bearer ") || len(authorization) < 8 || len(authorization) > 512 {
			accountError(w, r, 401, "Login required")
			return
		}
		token := strings.TrimPrefix(authorization, "Bearer ")
		var err error
		if input.Action == "claim" {
			err = a.authenticate(r, token, control)
		} else {
			err = a.logout(token, control)
		}
		if err != nil {
			method := "account.connect"
			if input.Action == "release" {
				method = "account.logout"
			}
			writeLocalError(w, 401, browserLanguage(r), browserLocalMessage(method, nil, err))
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	})
}
