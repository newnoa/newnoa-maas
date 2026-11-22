package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
)

func testHanaBrowser(t *testing.T, home string) (*core, func(string, map[string]any, int) map[string]any) {
	t.Helper()
	const key = "11111111-1111-4111-8111-111111111111"
	const model = "22222222-2222-4222-8222-222222222222"
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture-session" {
			w.WriteHeader(401)
			return
		}
		switch r.URL.Path {
		case "/api/v1/me":
			json.NewEncoder(w).Encode(map[string]string{"id": key, "language": "zh-CN"})
		case "/api/v1/me/api-keys":
			json.NewEncoder(w).Encode([]any{map[string]any{"id": key, "deploymentIds": []string{model}}})
		case "/api/v1/me/api-keys/" + key + "/copy-secret":
			json.NewEncoder(w).Encode(map[string]string{"secret": "nn_hana_spark_fixture_only"})
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(api.Close)
	c := newCore(home, api.URL)
	c.control = api.URL
	c.proxyPort = 18224
	handler := browserLocal(c, api.URL)
	return c, func(method string, params map[string]any, status int) map[string]any {
		t.Helper()
		data, _ := json.Marshal(map[string]any{"method": method, "params": params})
		req := httptest.NewRequest("POST", "/api/local/core", strings.NewReader(string(data)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer fixture-session")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != status {
			t.Fatalf("%s returned %d (%s)", method, response.Code, response.Body)
		}
		var value map[string]any
		if json.Unmarshal(response.Body.Bytes(), &value) != nil || strings.Contains(response.Body.String(), "nn_hana_spark_fixture_only") {
			t.Fatal("invalid or unredacted H5 payload")
		}
		return value
	}
}

func TestEncryptedHanaProviderFromH5(t *testing.T) {
	home := t.TempDir()
	var restoreLegacyKey func(string)
	// An isolated packaged Hana server can be supplied for native encrypted readback.
	if fixture := os.Getenv("NEWNOA_TEST_HANA_ISOLATED_HOME"); fixture != "" {
		// A harness creates this marker only inside its disposable home.
		if raw, err := os.ReadFile(filepath.Join(fixture, "test-hana-home")); err != nil || string(raw) != "disposable fixture" {
			t.Fatal("expected marked test home")
		}
		home = fixture
	} else {
		state := map[string]map[string]any{}
		var secret string
		var mu sync.Mutex
		restoreLegacyKey = func(value string) {
			mu.Lock()
			defer mu.Unlock()
			secret = value
		}
		h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			if r.Header.Get("Authorization") != "Bearer fixture-hana-token" {
				w.WriteHeader(403)
				return
			}
			switch {
			case r.Method == "GET" && r.URL.Path == "/api/server/identity":
				json.NewEncoder(w).Encode(map[string]string{"serverId": "test-server"})
			case r.Method == "GET" && r.URL.Path == "/api/providers/summary":
				json.NewEncoder(w).Encode(map[string]any{"providers": state})
			case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/api-key"):
				json.NewEncoder(w).Encode(map[string]string{"api_key": secret})
			case r.Method == "PUT" && r.URL.Path == "/api/config":
				var body struct{ Providers map[string]map[string]any }
				if json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Providers) != 1 {
					w.WriteHeader(400)
					return
				}
				for id, patch := range body.Providers {
					if patch == nil {
						delete(state, id)
						continue
					}
					secret, _ = patch["api_key"].(string)
					delete(patch, "api_key")
					state[id] = patch
				}
				json.NewEncoder(w).Encode(map[string]bool{"ok": true})
			default:
				w.WriteHeader(404)
			}
		}))
		defer h.Close()
		u, _ := url.Parse(h.URL)
		if err := atomicFile(filepath.Join(home, ".hanako", "server-info.json"), []byte(`{"serverId":"test-server","token":"fixture-hana-token","port":`+u.Port()+`}`)); err != nil {
			t.Fatal(err)
		}
		if err := atomicFile(filepath.Join(home, ".hanako", "provider-catalog.json"), []byte("HANA-SECRET-1.AES-256-GCM.fixture")); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("HANA_HOME", filepath.Join(home, ".hanako"))
	c, call := testHanaBrowser(t, home)
	const key = "11111111-1111-4111-8111-111111111111"
	const model = "22222222-2222-4222-8222-222222222222"
	const secret = "nn_hana_spark_fixture_only"
	base := map[string]any{"agent": "hana-agent"}
	service, err := c.hanaService()
	if err != nil {
		t.Fatal(err)
	}
	before, err := service.summary()
	if err != nil {
		t.Fatal(err)
	}
	preview := func(action string) string {
		params := map[string]any{"agent": "hana-agent", "providerAction": action, "keyId": key}
		if action == "add" {
			params["apiKey"] = secret
			params["providerModels"] = []providerModel{{model, "spark-2.6-4b"}}
		}
		return call("agent.preview", params, 200)["digest"].(string)
	}
	call("agent.providers", base, 200)
	call("agent.apply", map[string]any{"digest": preview("add")}, 200)
	catalog := call("agent.providers", base, 200)
	if catalog["state"] != "applied" || len(catalog["providers"].([]any)) != 1 {
		t.Fatal("provider missing in H5 list")
	}
	j, err := c.readHanaJournal()
	if err != nil || j == nil || !j.Service {
		t.Fatal("missing live-provider journal", err)
	}
	native := j.ProviderCatalog.Providers[0].Native
	state, err := service.summary()
	if err != nil {
		t.Fatal(err)
	}
	added := state[native]
	if added.URL != "http://127.0.0.1:18224/v1" || added.API != "openai-completions" || len(added.ModelList) != 1 || added.ModelList[0].ID != "spark-2.6-4b" {
		t.Fatal("native provider/model mismatch")
	}
	if val, err := service.match(j, native, state); err != nil || val != managedAgentKey(secret) {
		t.Fatal("native credential mismatch")
	}
	if restoreLegacyKey != nil {
		restoreLegacyKey(secret)
		legacy := *j
		legacy.LocalKeyReferences = false
		if err := c.writeHanaServiceJournal(&legacy); err != nil {
			t.Fatal(err)
		}
		if err := c.migrateAgentFileKeys("hana-agent"); err != nil {
			t.Fatal(err)
		}
		migrated, err := c.readHanaJournal()
		if err != nil || migrated == nil || !migrated.LocalKeyReferences {
			t.Fatal("Hana credential migration did not commit", err)
		}
		if val, err := service.match(migrated, native, state); err != nil || val != managedAgentKey(secret) {
			t.Fatal("Hana credential migration did not preserve provider binding", err)
		}
		if err := c.migrateAgentFileKeys("hana-agent"); err != nil {
			t.Fatal("Hana credential migration is not idempotent", err)
		}
	}
	// Other native providers remain untouched across an add/remove cycle.
	delete(state, native)
	if !reflect.DeepEqual(state, before) {
		t.Fatal("unrelated provider changed")
	}
	if r := newCore(home, c.gateway).modelAliases["hana-agent\nspark-2.6-4b"]; r.Model != model || !r.Keys[hash([]byte(secret))] {
		t.Fatal("lost restart alias/credential binding")
	}
	// Simulate a crash after Hana accepted the PUT, before the applied journal was saved.
	unchanged := *j
	unchanged.Stage = "prepared"
	unchanged.Transition = &hanaServiceTransition{ID: native}
	if err := c.writeHanaServiceJournal(&unchanged); err != nil {
		t.Fatal(err)
	}
	if recovered, err := c.readHanaJournal(); err != nil || recovered == nil || recovered.Stage != "applied" {
		t.Fatal("could not reconcile interrupted add", err)
	}
	// A subsequent, unsubmitted removal must resolve back to the existing provider.
	empty := *j
	empty.Stage = "prepared"
	empty.ProviderCatalog = &providerCatalog{Owner: j.ProviderCatalog.Owner}
	empty.Connections = nil
	empty.Transition = &hanaServiceTransition{ID: native, Before: j}
	if err := c.writeHanaServiceJournal(&empty); err != nil {
		t.Fatal(err)
	}
	if recovered, err := c.readHanaJournal(); err != nil || recovered == nil || len(recovered.ProviderCatalog.Providers) != 1 {
		t.Fatal("discarded profile after unsubmitted removal", err)
	}
	call("agent.apply", map[string]any{"digest": preview("remove")}, 200)
	after, err := service.summary()
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("native removal changed other profiles")
	}
	raw, err := os.ReadFile(filepath.Join(home, ".hanako", "provider-catalog.json"))
	if err != nil || !strings.HasPrefix(string(raw), "HANA-SECRET-1.") {
		t.Fatal("encrypted native catalog was replaced with plaintext")
	}
}

func TestEncryptedHanaClosedAndPrivateErrors(t *testing.T) {
	home := t.TempDir()
	c, call := testHanaBrowser(t, home)
	path := filepath.Join(home, ".hanako", "provider-catalog.json")
	if err := atomicFile(path, []byte("HANA-SECRET-1.AES-256-GCM.fixture")); err != nil {
		t.Fatal(err)
	}
	params := map[string]any{"agent": "hana-agent", "providerAction": "add", "keyId": "11111111-1111-4111-8111-111111111111", "apiKey": "nn_hana_spark_fixture_only", "providerModels": []providerModel{{"22222222-2222-4222-8222-222222222222", "spark-2.6-4b"}}}
	v := call("agent.preview", params, 400)
	if !strings.Contains(v["error"].(map[string]any)["message"].(string), "启动 HanaAgent") {
		t.Fatal("missing actionable launch error")
	}
	if err := atomicFile(filepath.Join(home, ".hanako", "server-info.json"), []byte(`{"token":"fixture-sensitive","port":1234}`)); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(filepath.Join(home, ".hanako", "server-info.json"), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := c.hanaService(); !errors.Is(err, errHanaPrivate) {
			t.Fatal("accepted public bearer file")
		}
	}
	for _, lang := range []string{"zh-CN", "en"} {
		if strings.Contains(browserLocalMessage("agent.apply", json.RawMessage(`{"agent":"hana-agent"}`), errors.New("fixture-sensitive")).in(lang), "fixture-sensitive") {
			t.Fatal("untrusted error escaped")
		}
	}
}
