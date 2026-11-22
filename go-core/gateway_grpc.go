package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	rpc "github.com/newnoa/newnoa-maas/go-server/network/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/anypb"
)

func newGRPCGatewayConnection(endpoint gatewayEndpoint) (*grpc.ClientConn, error) {
	origin, err := url.Parse(endpoint.Address)
	if err != nil || origin.Scheme != "https" {
		return nil, errors.New("gRPC Go Server requires TLS")
	}
	transport, err := gatewayHTTPTransport(endpoint)
	if err != nil {
		return nil, err
	}
	config := transport.TLSClientConfig
	if config == nil {
		config = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	config.ServerName = origin.Hostname()
	options := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(config)), grpc.WithNoProxy(), grpc.WithDisableRetry()}
	if endpoint.dialAddress != "" {
		options = append(options, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return transport.DialContext(ctx, "tcp", endpoint.dialAddress)
		}))
	}
	return grpc.NewClient(origin.Host, options...)
}

func grpcGatewayRoundTrip(request *http.Request, endpoint gatewayEndpoint) (*http.Response, error) {
	connection, release, err := gatewayGRPCConnections.acquire(endpoint)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(request.Context())
	var closeOnce sync.Once
	var bodyOnce sync.Once
	closeRequestBody := func() {
		bodyOnce.Do(func() {
			if request.Body != nil {
				request.Body.Close()
			}
		})
	}
	closeConnection := func() {
		closeOnce.Do(func() {
			cancel()
			release()
			closeRequestBody()
		})
	}
	context.AfterFunc(ctx, closeConnection)
	stream, err := rpc.Open(ctx, connection)
	if err != nil {
		closeConnection()
		return nil, err
	}
	head, err := rpc.HeadFrame(rpc.RequestHead, rpc.Head{Method: request.Method, Path: request.URL.Path, RawQuery: request.URL.RawQuery, Headers: request.Header.Clone(), ContentLength: request.ContentLength})
	if err != nil {
		closeConnection()
		return nil, err
	}
	if err = stream.Send(head); err != nil {
		closeConnection()
		return nil, err
	}
	go func() {
		defer closeRequestBody()
		var total int64
		if request.Body != nil {
			buffer := make([]byte, rpc.MaxData)
			for {
				count, readErr := request.Body.Read(buffer)
				total += int64(count)
				if total > rpc.MaxBody {
					cancel()
					return
				}
				if count > 0 {
					if err := stream.Send(rpc.DataFrame(buffer[:count])); err != nil {
						cancel()
						return
					}
				}
				if readErr == io.EOF {
					break
				}
				if readErr != nil {
					cancel()
					return
				}
			}
		}
		end, err := rpc.EndFrame(rpc.RequestEnd, request.Trailer)
		if err != nil {
			cancel()
			return
		}
		if err = stream.Send(end); err != nil {
			cancel()
			return
		}
		if err = stream.CloseSend(); err != nil {
			cancel()
		}
	}()
	frame, err := stream.Recv()
	if err != nil {
		closeConnection()
		return nil, err
	}
	responseHead, err := rpc.ParseHead(frame)
	if err != nil || frame.TypeUrl != rpc.ResponseHead || responseHead.Status < 200 || responseHead.Status > 599 {
		closeConnection()
		return nil, errors.New("invalid Go Server RPC response")
	}
	trailers := make(http.Header)
	body := &grpcGatewayBody{stream: stream, close: closeConnection, trailers: trailers}
	return &http.Response{StatusCode: responseHead.Status, Status: strconv.Itoa(responseHead.Status) + " " + http.StatusText(responseHead.Status), Header: responseHead.Headers, Body: body, ContentLength: -1, Trailer: trailers, Request: request, Proto: "HTTP/2.0", ProtoMajor: 2, ProtoMinor: 0}, nil
}

type grpcGatewayBody struct {
	stream   grpc.BidiStreamingClient[anypb.Any, anypb.Any]
	pending  []byte
	trailers http.Header
	close    func()
	once     sync.Once
	done     bool
}

func (body *grpcGatewayBody) Read(destination []byte) (int, error) {
	if len(destination) == 0 {
		return 0, nil
	}
	for len(body.pending) == 0 {
		if body.done {
			return 0, io.EOF
		}
		frame, err := body.stream.Recv()
		if err != nil {
			body.Close()
			if err == io.EOF {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}
		switch frame.TypeUrl {
		case rpc.Data:
			if len(frame.Value) > rpc.MaxData {
				body.Close()
				return 0, errors.New("Go Server RPC data exceeds limit")
			}
			body.pending = frame.Value
		case rpc.ResponseEnd:
			trailers, err := rpc.ParseEnd(frame)
			if err != nil {
				body.Close()
				return 0, err
			}
			_, err = body.stream.Recv()
			if err != io.EOF {
				body.Close()
				if err == nil {
					err = errors.New("unexpected Go Server RPC frame after end")
				}
				return 0, err
			}
			for name, values := range trailers {
				body.trailers[name] = values
			}
			body.done = true
			body.Close()
			return 0, io.EOF
		default:
			body.Close()
			return 0, errors.New("invalid Go Server RPC response frame order")
		}
	}
	count := copy(destination, body.pending)
	body.pending = body.pending[count:]
	return count, nil
}

func (body *grpcGatewayBody) Close() error { body.once.Do(body.close); return nil }
