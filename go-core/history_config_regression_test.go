package main

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHistoryConfigFailureIsVisibleWithoutBlockingInference(t *testing.T) {
	for _, fault := range []string{"malformed", "unreadable", "off"} {
		t.Run(fault, func(t *testing.T) {
			c := newCore(t.TempDir(), "")
			h, err := c.historyStore()
			if err != nil {
				t.Fatal(err)
			}
			defer c.shutdown()
			historyPolicyFixture(t, h, "local")
			account := "00000000-0000-0000-0000-000000000001"
			config := (&historyManager{home: c.home, owner: account}).configPath()
			if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
				t.Fatal(err)
			}
			if fault == "unreadable" {
				if err := os.Mkdir(config, 0700); err != nil {
					t.Fatal(err)
				}
			} else {
				raw := `{"engine": "secret-invalid-json`
				if fault == "off" {
					raw = `{"engine":"off"}`
				}
				if err := os.WriteFile(config, []byte(raw), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var logs bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(previous)
			response := httptest.NewRecorder()
			request := httptest.NewRequest("POST", "/v1/responses", strings.NewReader(`{"model":"m","input":"hello"}`))
			h.capture(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("inference completed")) }), account, "key").ServeHTTP(response, request)
			if response.Code != 200 || response.Body.String() != "inference completed" {
				t.Fatal("history failure blocked inference")
			}
			if fault == "off" {
				if response.Header().Get("X-Newnoa-History") != "" || logs.Len() != 0 {
					t.Fatal("intentional off reported as error")
				}
			} else if response.Header().Get("X-Newnoa-History") != "unavailable" || !strings.Contains(logs.String(), "history_config_unavailable") {
				t.Fatalf("silent config error: headers=%v logs=%q", response.Header(), logs.String())
			}
			if strings.Contains(logs.String(), "secret-invalid-json") {
				t.Fatal("configuration content leaked")
			}
			var count int
			if err := h.db.QueryRow("SELECT count(*) FROM history_pending WHERE viewer=?", account).Scan(&count); err != nil || count != 0 {
				t.Fatal("unexpected history recording", count, err)
			}
		})
	}
}
