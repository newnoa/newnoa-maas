package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectorRequestsAnotherOwnerAfterFailedTunnel(t *testing.T) {
	var leases atomic.Int32
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/drain") {
			w.Write([]byte(`{}`))
			return
		}
		var body struct {
			OwnerID  string `json:"ownerId"`
			Failover bool   `json:"failover"`
			Purpose  string `json:"purpose"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		i := leases.Add(1)
		wantOwner := "owner-a"
		if i > 1 {
			wantOwner = "owner-b"
		}
		if body.OwnerID != wantOwner || body.Failover != (i > 1) || body.Purpose != "evaluation" {
			t.Errorf("lease %d: %+v", i, body)
		}
		if i >= 3 {
			w.WriteHeader(401)
			return
		}
		// An invalid tunnel config makes the attempt fail without launching FRPC.
		json.NewEncoder(w).Encode(map[string]any{"ownerId": "owner-b", "lease_purpose": "evaluation", "epoch": i})
	}))
	defer control.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n := &connector{ctx: ctx, cancel: cancel, done: make(chan struct{}), control: control.URL, ownerID: "owner-a", purpose: "evaluation", credential: "node_fixture", client: connectorControlClient(), upstream: control.Client(), server: &http.Server{}}
	n.run()
	if leases.Load() != 3 {
		t.Fatalf("lease attempts=%d", leases.Load())
	}
	if n.status()["ownerId"] != "owner-b" {
		t.Fatal("selected owner missing from status")
	}
}

func TestMalformedPermitResponseNeverExecutesLocalModel(t *testing.T) {
	for _, suffix := range []string{`{}`, `garbage`, strings.Repeat(" ", maxFrame)} {
		var calls atomic.Int32
		model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.Write([]byte(`{}`)) }))
		target, _ := json.Marshal(map[string]any{"endpoint": model.URL, "model": "fixture"})
		control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(append(target, []byte(suffix)...)) }))
		n := &connector{control: control.URL, credential: "node_fixture", client: connectorControlClient(), upstream: model.Client()}
		request := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"fixture","messages":[{"role":"user","content":"test"}]}`))
		request.Header.Set("X-Newnoa-Request", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
		request.Header.Set("X-Newnoa-Permit", "permit_fixture")
		request.Header.Set("X-Newnoa-Gateway", "fixture")
		request.Header.Set("X-Newnoa-Epoch", "1")
		response := httptest.NewRecorder()
		n.ServeHTTP(response, request)
		control.Close()
		model.Close()
		if response.Code != 409 || calls.Load() != 0 {
			t.Fatalf("malformed permit: status=%d executions=%d", response.Code, calls.Load())
		}
	}
}

func TestHeartbeatResponseLimitIsEnforcedWithoutResult(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(strings.Repeat(" ", maxFrame+1))) }))
	defer control.Close()
	n := &connector{control: control.URL, credential: "node_fixture", client: connectorControlClient()}
	if n.call(context.Background(), "/connector/fixture/heartbeat", map[string]any{}, nil) == nil {
		t.Fatal("oversized acknowledgement accepted")
	}
}

func TestFRPStatusRequiresSuccessfulCompleteMatchingResponse(t *testing.T) {
	valid := `{"tcp":[{"name":"fixture","status":"running"}]}`
	for _, tc := range []struct {
		name, body string
		code       int
		want       bool
	}{
		{"running", valid, 200, true},
		{"whitespace", valid + "\n", 200, true},
		{"server failure", valid, 500, false},
		{"unauthorized", valid, 401, false},
		{"trailing JSON", valid + `{}`, 200, false},
		{"truncated", valid[:len(valid)-1], 200, false},
		{"oversized", valid + strings.Repeat(" ", maxFrame), 200, false},
		{"wrong proxy", strings.ReplaceAll(valid, "fixture", "other"), 200, false},
		{"offline", strings.ReplaceAll(valid, "running", "error"), 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
				w.Write([]byte(tc.body))
			}))
			defer server.Close()
			res, err := server.Client().Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			if got := frpProxyRunning(res, "fixture"); got != tc.want {
				t.Fatalf("running=%v, want %v", got, tc.want)
			}
		})
	}
}
