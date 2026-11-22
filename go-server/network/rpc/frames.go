// Package rpc carries HTTP model requests over a bounded gRPC bidirectional stream.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	FullMethod   = "/newnoa.rpc.ModelTransport/Exchange"
	RequestHead  = "newnoa.rpc/request-head"
	ResponseHead = "newnoa.rpc/response-head"
	Data         = "newnoa.rpc/data"
	RequestEnd   = "newnoa.rpc/request-end"
	ResponseEnd  = "newnoa.rpc/response-end"
	MaxData      = 64 << 10
	MaxMetadata  = 16 << 10
	MaxMessage   = 128 << 10
	MaxBody      = 32 << 20
)

type Head struct {
	Method        string      `json:"method,omitempty"`
	Path          string      `json:"path,omitempty"`
	RawQuery      string      `json:"rawQuery,omitempty"`
	Headers       http.Header `json:"headers,omitempty"`
	ContentLength int64       `json:"contentLength"`
	Status        int         `json:"status,omitempty"`
}

func metadataFrame(kind string, value any) (*anypb.Any, error) {
	data, err := json.Marshal(value)
	if err != nil || len(data) > MaxMetadata {
		return nil, errors.New("RPC metadata exceeds limit")
	}
	return &anypb.Any{TypeUrl: kind, Value: data}, nil
}
func HeadFrame(kind string, head Head) (*anypb.Any, error) {
	if kind != RequestHead && kind != ResponseHead {
		return nil, errors.New("invalid RPC head type")
	}
	if !validHeaders(head.Headers) {
		return nil, errors.New("invalid RPC headers")
	}
	return metadataFrame(kind, head)
}
func ParseHead(frame *anypb.Any) (Head, error) {
	var head Head
	if frame == nil || (frame.TypeUrl != RequestHead && frame.TypeUrl != ResponseHead) {
		return head, errors.New("RPC head required")
	}
	if err := parseMetadata(frame.Value, &head); err != nil {
		return head, err
	}
	if !validHeaders(head.Headers) {
		return head, errors.New("invalid RPC headers")
	}
	head.Headers = canonicalHeaders(head.Headers)
	return head, nil
}
func EndFrame(kind string, trailers http.Header) (*anypb.Any, error) {
	if (kind != RequestEnd && kind != ResponseEnd) || !validHeaders(trailers) {
		return nil, errors.New("invalid RPC trailers")
	}
	return metadataFrame(kind, trailers)
}
func ParseEnd(frame *anypb.Any) (http.Header, error) {
	var trailers http.Header
	if frame == nil || (frame.TypeUrl != RequestEnd && frame.TypeUrl != ResponseEnd) {
		return nil, errors.New("RPC end required")
	}
	if err := parseMetadata(frame.Value, &trailers); err != nil {
		return nil, err
	}
	if !validHeaders(trailers) {
		return nil, errors.New("invalid RPC trailers")
	}
	return canonicalHeaders(trailers), nil
}
func DataFrame(data []byte) *anypb.Any { return &anypb.Any{TypeUrl: Data, Value: data} }
func parseMetadata(data []byte, target any) error {
	if len(data) > MaxMetadata {
		return errors.New("RPC metadata exceeds limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("invalid RPC metadata")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("invalid RPC metadata")
	}
	return nil
}
func validHeaders(headers http.Header) bool {
	for name, values := range headers {
		if name == "" {
			return false
		}
		for _, c := range name {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
				return false
			}
		}
		for _, value := range values {
			for _, c := range value {
				if c == '\r' || c == '\n' || c == 0 || c == 127 || c < 32 && c != '\t' {
					return false
				}
			}
		}
	}
	return true
}
func canonicalHeaders(headers http.Header) http.Header {
	result := make(http.Header, len(headers))
	for name, values := range headers {
		key := http.CanonicalHeaderKey(name)
		result[key] = append(result[key], values...)
	}
	return result
}
func Open(ctx context.Context, connection grpc.ClientConnInterface, options ...grpc.CallOption) (grpc.BidiStreamingClient[anypb.Any, anypb.Any], error) {
	options = append([]grpc.CallOption{grpc.MaxCallRecvMsgSize(MaxMessage), grpc.MaxCallSendMsgSize(MaxMessage)}, options...)
	stream, err := connection.NewStream(ctx, &grpc.StreamDesc{StreamName: "Exchange", ServerStreams: true, ClientStreams: true}, FullMethod, options...)
	if err != nil {
		return nil, err
	}
	return &grpc.GenericClientStream[anypb.Any, anypb.Any]{ClientStream: stream}, nil
}
