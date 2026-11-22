package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestGatewayGRPCPreservesStreamTerminalIntegrity(t *testing.T) {
	const usage = `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":12,"output_tokens":3}}}`
	for _, tc := range []struct {
		name, tail string
		valid      bool
		event      string
	}{
		{"complete", "", true, usage},
		{"keepalive", ": keepalive\n\n", true, usage},
		{"late data", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"late\"}\n\n", false, usage},
		{"late usage", "data: {\"usage\":{\"input_tokens\":999,\"output_tokens\":999}}\n\n", false, usage},
		{"duplicate end", "data: " + usage + "\n\n", false, usage},
		{"incomplete tail", "data: {", false, usage},
		{"large completion", "", true, strings.Replace(usage, `"status":"completed"`, `"status":"completed","tools":[{"description":"`+strings.Repeat("x", 128*1024)+`"}]`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: "+tc.event+"\n\n"+tc.tail)
			}))
			defer upstream.Close()
			finalized := make(chan map[string]any, 1)
			api := meterFixtureServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/authorize"):
					io.WriteString(w, `{"model":"actual"}`)
				case strings.HasSuffix(r.URL.Path, "/dispatches"):
					json.NewEncoder(w).Encode(map[string]any{"requestId": "grpc-stream-integrity", "permit": "fixture", "endpoint": upstream.URL})
				case strings.HasSuffix(r.URL.Path, "/finalize"):
					var final map[string]any
					if err := json.NewDecoder(r.Body).Decode(&final); err != nil {
						t.Error(err)
					}
					finalized <- meterFixtureSelectedEvidence(r, final)
					io.WriteString(w, `{}`)
				default:
					io.WriteString(w, `{}`)
				}
			}))
			defer api.Close()
			gateway := newGateway(api.URL, "internal", "fixture", []string{"127.0.0.1"})
			server := rpc.NewServer(gateway)
			defer server.Stop()
			host := httptest.NewUnstartedServer(rpc.Multiplex(server, gateway))
			host.EnableHTTP2 = true
			host.StartTLS()
			defer host.Close()
			roots := x509.NewCertPool()
			roots.AddCert(host.Certificate())
			conn, err := grpc.NewClient(host.Listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})), grpc.WithDisableRetry())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, err := rpc.Open(ctx, conn)
			if err != nil {
				t.Fatal(err)
			}
			body := []byte(`{"model":"fixture","input":"hello","stream":true}`)
			head, err := rpc.HeadFrame(rpc.RequestHead, rpc.Head{Method: "POST", Path: "/v1/responses", ContentLength: int64(len(body)), Headers: http.Header{"Authorization": {"Bearer ses_fixture"}, "X-Api-Key": {"nn_fixture"}, "Content-Type": {"application/json"}}})
			if err != nil {
				t.Fatal(err)
			}
			end, _ := rpc.EndFrame(rpc.RequestEnd, nil)
			for _, frame := range []*anypb.Any{head, rpc.DataFrame(body), end} {
				if err := stream.Send(frame); err != nil {
					t.Fatal(err)
				}
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			var data strings.Builder
			ended := false
			for {
				frame, err := stream.Recv()
				if err != nil {
					if tc.valid && err != io.EOF || !tc.valid && err == io.EOF {
						t.Fatalf("stream termination: %v valid=%v", err, tc.valid)
					}
					break
				}
				switch frame.TypeUrl {
				case rpc.ResponseHead:
					head, err := rpc.ParseHead(frame)
					if err != nil || head.Status != 200 {
						t.Fatalf("response head=%+v err=%v", head, err)
					}
				case rpc.Data:
					data.Write(frame.Value)
				case rpc.ResponseEnd:
					ended = true
				}
			}
			if ended != tc.valid {
				t.Fatalf("response end=%v valid=%v", ended, tc.valid)
			}
			if tc.valid && data.String() != "data: "+tc.event+"\n\n"+tc.tail {
				t.Fatal("valid upstream stream changed")
			}
			select {
			case final := <-finalized:
				if final["executionClosed"] != true {
					t.Fatal("closed execution was not confirmed", final)
				}
				want := "unknown"
				if tc.valid {
					want = "succeeded"
				}
				if final["status"] != want || final["inputTokens"] != float64(12) || final["outputTokens"] != float64(3) {
					t.Fatalf("final=%v want=%s input=12 output=3", final, want)
				}
			case <-ctx.Done():
				t.Fatal("missing finalize")
			}
			if calls.Load() != 1 {
				t.Fatal("request replayed", calls.Load())
			}
		})
	}
}
