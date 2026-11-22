package main

import (
	"context"
	"errors"
	rpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type malformedGatewayRPC interface {
	Exchange(grpc.BidiStreamingServer[anypb.Any, anypb.Any]) error
}
type malformedGatewayHandler struct {
	run func(grpc.BidiStreamingServer[anypb.Any, anypb.Any]) error
}

func (handler *malformedGatewayHandler) Exchange(stream grpc.BidiStreamingServer[anypb.Any, anypb.Any]) error {
	return handler.run(stream)
}

func gatewayRPCFixture(t *testing.T, run func(grpc.BidiStreamingServer[anypb.Any, anypb.Any]) error) gatewayEndpoint {
	t.Helper()
	server := grpc.NewServer(grpc.MaxRecvMsgSize(rpc.MaxMessage), grpc.MaxSendMsgSize(rpc.MaxMessage))
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "newnoa.rpc.ModelTransport", HandlerType: (*malformedGatewayRPC)(nil), Streams: []grpc.StreamDesc{{StreamName: "Exchange", ServerStreams: true, ClientStreams: true, Handler: func(service any, stream grpc.ServerStream) error {
		return service.(malformedGatewayRPC).Exchange(&grpc.GenericServerStream[anypb.Any, anypb.Any]{ServerStream: stream})
	}}}}, &malformedGatewayHandler{run: run})
	t.Cleanup(server.Stop)
	host, ca := gatewayTLSFixture(t, server, false)
	endpoint := gatewayTrustFixture(t, "node", host.URL, ca)
	endpoint.Transport = "grpc"
	return endpoint
}

func finishGatewayUpload(stream grpc.BidiStreamingServer[anypb.Any, anypb.Any]) error {
	for {
		frame, err := stream.Recv()
		if err != nil {
			return err
		}
		if frame.TypeUrl == rpc.RequestEnd {
			_, err = stream.Recv()
			if err == io.EOF {
				return nil
			}
			return errors.New("request did not terminate")
		}
	}
}

func TestGatewayGRPCRejectsMalformedResponseFramesWithoutReplay(t *testing.T) {
	goodHead, _ := rpc.HeadFrame(rpc.ResponseHead, rpc.Head{Status: 200, ContentLength: -1})
	goodEnd, _ := rpc.EndFrame(rpc.ResponseEnd, nil)
	badStatus, _ := rpc.HeadFrame(rpc.ResponseHead, rpc.Head{Status: 700})
	for _, tc := range []struct {
		name      string
		frames    []*anypb.Any
		terminal  error
		headError bool
		clean     bool
	}{
		{name: "empty-stream", headError: true},
		{name: "non-head-first", frames: []*anypb.Any{rpc.DataFrame([]byte("secret"))}, headError: true},
		{name: "malformed-head", frames: []*anypb.Any{{TypeUrl: rpc.ResponseHead, Value: []byte("not JSON")}}, headError: true},
		{name: "invalid-status", frames: []*anypb.Any{badStatus}, headError: true},
		{name: "premature-eof", frames: []*anypb.Any{goodHead, rpc.DataFrame([]byte("partial"))}},
		{name: "transport-error", frames: []*anypb.Any{goodHead}, terminal: status.Error(codes.Unavailable, "upstream failed")},
		{name: "oversized-data", frames: []*anypb.Any{goodHead, rpc.DataFrame(make([]byte, rpc.MaxData+1))}},
		{name: "bad-end", frames: []*anypb.Any{goodHead, {TypeUrl: rpc.ResponseEnd, Value: []byte("invalid")}}},
		{name: "trailing-frame", frames: []*anypb.Any{goodHead, goodEnd, rpc.DataFrame([]byte("late"))}},
		{name: "error-after-end", frames: []*anypb.Any{goodHead, goodEnd}, terminal: status.Error(codes.Aborted, "incomplete")},
		{name: "duplicate-head", frames: []*anypb.Any{goodHead, goodHead}},
		{name: "clean-empty", frames: []*anypb.Any{goodHead, goodEnd}, clean: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			endpoint := gatewayRPCFixture(t, func(stream grpc.BidiStreamingServer[anypb.Any, anypb.Any]) error {
				calls.Add(1)
				if err := finishGatewayUpload(stream); err != nil {
					return err
				}
				for _, frame := range tc.frames {
					if err := stream.Send(frame); err != nil {
						return err
					}
				}
				return tc.terminal
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			request, _ := http.NewRequestWithContext(ctx, "GET", endpoint.Address+"/v1/models", nil)
			response, err := grpcGatewayRoundTrip(request, endpoint)
			if tc.headError {
				if err == nil {
					response.Body.Close()
					t.Fatal("malformed initial frame accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if n, err := response.Body.Read(nil); n != 0 || err != nil {
				t.Fatalf("zero-length read=%d %v", n, err)
			}
			_, err = io.ReadAll(response.Body)
			if tc.clean {
				if err != nil {
					t.Fatal(err)
				}
				if n, err := response.Body.Read(make([]byte, 1)); n != 0 || err != io.EOF {
					t.Fatalf("terminal read=%d %v", n, err)
				}
			} else if err == nil {
				t.Fatal("broken response accepted as complete")
			}
			if calls.Load() != 1 {
				t.Fatal("failed request replayed")
			}
		})
	}
}

type failingGatewayUpload struct {
	closed chan struct{}
	once   sync.Once
}

func (body *failingGatewayUpload) Read([]byte) (int, error) {
	return 0, errors.New("fixture read failure")
}
func (body *failingGatewayUpload) Close() error {
	body.once.Do(func() { close(body.closed) })
	return nil
}

func TestGatewayGRPCUploadErrorsAndMalformedMetadataCancelWithoutReplay(t *testing.T) {
	for _, mode := range []string{"read-error", "bad-head", "bad-trailer", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			endpoint := gatewayRPCFixture(t, func(stream grpc.BidiStreamingServer[anypb.Any, anypb.Any]) error {
				for {
					if _, err := stream.Recv(); err != nil {
						return err
					}
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			failed := &failingGatewayUpload{closed: make(chan struct{})}
			var body io.ReadCloser = failed
			if mode != "read-error" {
				body = io.NopCloser(strings.NewReader("body"))
			}
			if mode == "oversized" {
				body = io.NopCloser(io.LimitReader(repeatingGatewayReader{}, rpc.MaxBody+1))
			}
			request, _ := http.NewRequestWithContext(ctx, "POST", endpoint.Address+"/v1/chat/completions", body)
			request.ContentLength = -1
			if mode == "bad-head" {
				request.Header.Set("X-Invalid", "line\nbreak")
			}
			if mode == "bad-trailer" {
				request.Trailer = http.Header{"Bad\nName": {"x"}}
			}
			response, err := grpcGatewayRoundTrip(request, endpoint)
			if err == nil {
				response.Body.Close()
				t.Fatal("invalid upload accepted")
			}
			if mode == "read-error" {
				select {
				case <-failed.closed:
				case <-time.After(time.Second):
					t.Fatal("upload failure left body open")
				}
			}
		})
	}
}

type repeatingGatewayReader struct{}

func (repeatingGatewayReader) Read(destination []byte) (int, error) {
	for i := range destination {
		destination[i] = 'a'
	}
	return len(destination), nil
}
