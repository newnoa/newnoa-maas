package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLocalSupplyRegistersOnePrivateDeviceAndReusesIt(t *testing.T) {
	const account = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	const node = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
	const credential = "node_abcdefghijklmnopqrstuvwxyz0123456789ABCDE"
	created := 0
	var api *httptest.Server
	api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ready" {
			w.Write([]byte(`{}`))
			return
		}
		if r.Header.Get("Authorization") != "Bearer ses_local_supply_fixture" {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/api/v1/me":
			json.NewEncoder(w).Encode(map[string]any{"id": account})
		case "/api/v1/me/frp-owners":
			json.NewEncoder(w).Encode([]gatewayEndpoint{{ID: "loaded", Address: strings.TrimPrefix(api.URL, "http://"), ConnectionCount: 20, LatencyTolerancePercent: 10}, {ID: "light", Address: strings.TrimPrefix(api.URL, "http://"), ConnectionCount: 1, LatencyTolerancePercent: 10}})
		case "/api/v1/me/nodes":
			if r.Method == "POST" {
				created++
				json.NewEncoder(w).Encode(map[string]any{"id": node, "credential": credential})
			} else if created > 0 {
				json.NewEncoder(w).Encode([]map[string]any{{"id": node, "revoked_at": nil}})
			} else {
				w.Write([]byte(`[]`))
			}
		default:
			w.WriteHeader(404)
		}
	}))
	defer api.Close()
	home := t.TempDir()
	c := newCore(home, api.URL)
	c.control = api.URL
	c.gatewaySelector.probe = func(_ context.Context, endpoint gatewayEndpoint) (time.Duration, error) {
		if endpoint.ID == "loaded" {
			return 50 * time.Millisecond, nil
		}
		return 54 * time.Millisecond, nil
	}
	first, err := c.localSupplyIdentity(context.Background(), "ses_local_supply_fixture")
	if err != nil {
		t.Fatal(err)
	}
	if first.NodeID != node || first.Credential != credential || created != 1 || first.OwnerID != "light" {
		t.Fatalf("identity not registered correctly; created=%d", created)
	}
	file := c.localSupplyPath(api.URL, account)
	info, err := os.Stat(file)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("device file permissions: %v", err)
	}
	raw, _ := os.ReadFile(file)
	if strings.Contains(string(raw), "ses_local_supply_fixture") {
		t.Fatal("account bearer was persisted")
	}
	c.connector = &connector{nodeID: node, credential: credential, ownerID: "light", purpose: "serving", state: "online"}
	c.gatewaySelector.probe = func(_ context.Context, endpoint gatewayEndpoint) (time.Duration, error) {
		if endpoint.ID == "loaded" {
			return time.Millisecond, nil
		}
		return 54 * time.Millisecond, nil
	}
	active, err := c.localSupplyIdentity(context.Background(), "ses_local_supply_fixture")
	if err != nil || active.OwnerID != "light" {
		t.Fatalf("active publisher binding silently migrated: owner=%s err=%v", active.OwnerID, err)
	}
	c.connector = nil
	reopened := newCore(home, api.URL)
	reopened.control = api.URL
	second, err := reopened.localSupplyIdentity(context.Background(), "ses_local_supply_fixture")
	if err != nil || second.NodeID != node || created != 1 {
		t.Fatalf("device not reused; created=%d err=%v", created, err)
	}
}

func TestLocalSupplyDoesNotEnrollWithoutAuthenticationOrCoordinator(t *testing.T) {
	for _, authorized := range []bool{false, true} {
		t.Run(map[bool]string{false: "unauthenticated", true: "no coordinator"}[authorized], func(t *testing.T) {
			posts := 0
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					posts++
					t.Error("must not enroll device")
				}
				if !authorized {
					w.WriteHeader(401)
					return
				}
				if r.URL.Path == "/api/v1/me" {
					w.Write([]byte(`{"id":"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}`))
				} else {
					w.Write([]byte(`[]`))
				}
			}))
			defer api.Close()
			c := newCore(t.TempDir(), api.URL)
			c.control = api.URL
			if _, err := c.localSupplyIdentity(context.Background(), "ses_local_supply_fixture"); err == nil || posts != 0 {
				t.Fatal("unavailable enrollment was accepted")
			}
		})
	}
}

func TestLocalSupplyRejectsUnsafeCredentialFile(t *testing.T) {
	c := newCore(t.TempDir(), "https://gateway.example.test")
	file := c.localSupplyPath("https://api.example.test", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "private")
	os.WriteFile(target, []byte("must not be read or modified"), 0600)
	if err := os.Symlink(target, file); err != nil {
		t.Fatal(err)
	}
	if _, err := c.readLocalSupply(file); err == nil {
		t.Fatal("credential symlink was accepted")
	}
	data, _ := os.ReadFile(target)
	if string(data) != "must not be read or modified" {
		t.Fatal("foreign file changed")
	}
}
