package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	peernet "github.com/newnoa/newnoa-maas/go-server/network"
	rpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDevLoopbackAcceptsSignedProofButProductionAndRemoteHTTPDoNot(t *testing.T) {
	const token = "fixture-public-control-token-at-least32chars"
	const raw = `{"model":"fixture","prompt":"circle"}`
	calls := 0
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(403); io.WriteString(w, `{}`) }))
	defer api.Close()
	g := newGateway(api.URL, token, "dev-local", nil)
	for _, tc := range []struct {
		environment, peer string
		want              int
	}{
		{"dev", "127.0.0.1:1234", 403}, {"prd", "127.0.0.1:1234", 401}, {"dev", "192.168.1.2:1234", 401},
	} {
		t.Setenv("NEWNOA_ENV", tc.environment)
		r := httptest.NewRequest("POST", "/v1/images/generations", strings.NewReader(raw))
		r.RemoteAddr = tc.peer
		r.Header.Set("Authorization", "Bearer sk_fixture_consumer")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "fixture")
		sum := sha256.Sum256([]byte(raw))
		proof, err := peernet.SignPublicCaller(token, "dev-local", r, hex.EncodeToString(sum[:]), "203.0.113.9", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set(peernet.PublicCallerProofHeader, proof)
		r.Header.Set(peernet.PublicCallerIPHeader, "203.0.113.9")
		out := httptest.NewRecorder()
		g.ServeHTTP(out, r)
		if out.Code != tc.want {
			t.Fatalf("env=%s peer=%s status=%d", os.Getenv("NEWNOA_ENV"), tc.peer, out.Code)
		}
	}
	if calls != 1 {
		t.Fatalf("unauthenticated proof reached API %d times", calls)
	}
}

func TestPublicCallerProofKeepsDirectKeyDeniedAndAPIAuthority(t *testing.T) {
	const token = "fixture-public-control-token-at-least32chars"
	const body = `{"model":"fixture","messages":[{"role":"user","content":"test"}]}`
	calls := 0
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("API internal auth lost")
		}
		w.WriteHeader(403)
		io.WriteString(w, `{"error":{"message":"API refuses original caller"}}`)
	}))
	defer api.Close()
	g := newGateway(api.URL, token, "node-a", []string{"127.0.0.1"})
	request := func() *http.Request {
		r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
		r.ProtoMajor = 2
		r.Header.Set("Authorization", "Bearer nn_fixture_consumer")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", "fixture-idempotency")
		return r
	}
	sign := func(r *http.Request, at time.Time) {
		sum := sha256.Sum256([]byte(body))
		proof, err := peernet.SignPublicCaller(token, "node-a", r, hex.EncodeToString(sum[:]), "203.0.113.9", at)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set(peernet.PublicCallerProofHeader, proof)
		r.Header.Set(peernet.PublicCallerIPHeader, "203.0.113.9")
	}
	for _, kind := range []string{"raw-key", "forged", "expired", "wrong-destination", "wrong-body", "duplicate-proof", "duplicate-ip", "wrong-content-type", "wrong-idempotency", "wrong-key", "http-not-rpc"} {
		t.Run(kind, func(t *testing.T) {
			r := request()
			if kind != "raw-key" {
				sign(r, time.Now())
			}
			switch kind {
			case "forged":
				r.Header.Set(peernet.PublicCallerProofHeader, "v1.fake")
			case "expired":
				sign(r, time.Now().Add(-time.Minute))
			case "wrong-destination":
				g.id = "node-b"
				defer func() { g.id = "node-a" }()
			case "wrong-body":
				r.Body = io.NopCloser(strings.NewReader(body + " "))
			case "duplicate-proof":
				r.Header.Add(peernet.PublicCallerProofHeader, r.Header.Get(peernet.PublicCallerProofHeader))
			case "duplicate-ip":
				r.Header.Add(peernet.PublicCallerIPHeader, "203.0.113.8")
			case "wrong-content-type":
				r.Header.Set("Content-Type", "text/plain")
			case "wrong-idempotency":
				r.Header.Set("Idempotency-Key", "other")
			case "wrong-key":
				r.Header.Set("Authorization", "Bearer nn_other")
			case "http-not-rpc":
				r.ProtoMajor = 1
			}
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			if w.Code != 401 || calls != 0 {
				t.Fatalf("untrusted request reached API: status=%d calls=%d", w.Code, calls)
			}
		})
	}
	r := request()
	sign(r, time.Now())
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 401 || calls != 0 {
		t.Fatalf("caller-controlled HTTP2 protocol escaped actual RPC boundary: status=%d calls=%d", w.Code, calls)
	}
}

func TestPublicCallerProofAcrossRealRPCStillRequiresAPI(t *testing.T) {
	const token = "fixture-public-control-token-at-least32chars"
	const body = `{"model":"fixture","messages":[{"role":"user","content":"test"}]}`
	calls := 0
	api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var value map[string]any
		json.NewDecoder(r.Body).Decode(&value)
		if value["secret"] != "nn_fixture_consumer" || value["session"] != nil {
			t.Error("public API identity was replaced")
		}
		w.WriteHeader(403)
		io.WriteString(w, `{"error":{"message":"API refuses caller"}}`)
	}))
	defer api.Close()
	g := newGateway(api.URL, token, "node-a", []string{"127.0.0.1"})
	server := rpc.NewServer(g)
	defer server.Stop()
	edge := httptest.NewUnstartedServer(rpc.Multiplex(server, g))
	edge.EnableHTTP2 = true
	edge.StartTLS()
	defer edge.Close()
	roots := x509.NewCertPool()
	roots.AddCert(edge.Certificate())
	conn, err := grpc.NewClient(strings.TrimPrefix(edge.URL, "https://"), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots})), grpc.WithNoProxy(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, kind := range []string{"valid", "body-changed", "expired", "duplicate-proof", "missing-caller", "with-desktop-key"} {
		t.Run(kind, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer nn_fixture_consumer")
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "fixture-"+kind)
			sum := sha256.Sum256([]byte(body))
			at := time.Now()
			if kind == "expired" {
				at = at.Add(-time.Minute)
			}
			proof, err := peernet.SignPublicCaller(token, "node-a", r, hex.EncodeToString(sum[:]), "203.0.113.9", at)
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set(peernet.PublicCallerProofHeader, proof)
			r.Header.Set(peernet.PublicCallerIPHeader, "203.0.113.9")
			raw := body
			switch kind {
			case "body-changed":
				raw += " "
			case "duplicate-proof":
				r.Header.Add(peernet.PublicCallerProofHeader, proof)
			case "missing-caller":
				r.Header.Del(peernet.PublicCallerIPHeader)
			case "with-desktop-key":
				r.Header.Set("X-Api-Key", "unexpected")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			stream, err := rpc.Open(ctx, conn)
			if err != nil {
				t.Fatal(err)
			}
			head, err := rpc.HeadFrame(rpc.RequestHead, rpc.Head{Method: "POST", Path: r.URL.Path, Headers: r.Header, ContentLength: int64(len(raw))})
			if err != nil {
				t.Fatal(err)
			}
			if err = stream.Send(head); err != nil {
				t.Fatal(err)
			}
			if err = stream.Send(rpc.DataFrame([]byte(raw))); err != nil {
				t.Fatal(err)
			}
			end, _ := rpc.EndFrame(rpc.RequestEnd, nil)
			stream.Send(end)
			stream.CloseSend()
			frame, err := stream.Recv()
			if err != nil {
				t.Fatal(err)
			}
			response, err := rpc.ParseHead(frame)
			if err != nil {
				t.Fatal(err)
			}
			want := 401
			if kind == "valid" {
				want = 403
			}
			if response.Status != want {
				t.Fatalf("%s status%d want%d", kind, response.Status, want)
			}
			for {
				if _, err = stream.Recv(); err != nil {
					break
				}
			}
		})
	}
	if calls != 1 {
		t.Fatalf("untrusted RPC reached independent API: calls%d", calls)
	}
}
