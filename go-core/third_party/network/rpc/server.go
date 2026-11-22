package rpc

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type exchangeService interface {
	Exchange(grpc.BidiStreamingServer[anypb.Any, anypb.Any]) error
}
type bridge struct{ handler http.Handler }
type httpControllerKey struct{}
type modelTransportKey struct{}

// IsModelTransport identifies requests decoded by the authenticated TLS transport, not a caller header.
func IsModelTransport(r *http.Request) bool {
	value, _ := r.Context().Value(modelTransportKey{}).(bool)
	return value
}

func NewServer(handler http.Handler) *grpc.Server {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(MaxMessage), grpc.MaxSendMsgSize(MaxMessage), grpc.MaxConcurrentStreams(128), grpc.StaticStreamWindowSize(64<<10), grpc.StaticConnWindowSize(128<<10))
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "newnoa.rpc.ModelTransport", HandlerType: (*exchangeService)(nil), Streams: []grpc.StreamDesc{{StreamName: "Exchange", ServerStreams: true, ClientStreams: true, Handler: func(service any, stream grpc.ServerStream) error {
		return service.(exchangeService).Exchange(&grpc.GenericServerStream[anypb.Any, anypb.Any]{ServerStream: stream})
	}}}}, &bridge{handler: handler})
	return server
}

// Multiplex keeps the HTTP/1 FRP upgrade and health routes on the same TLS listener.
func Multiplex(server *grpc.Server, fallback http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			ctx := context.WithValue(r.Context(), httpControllerKey{}, http.NewResponseController(w))
			server.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		fallback.ServeHTTP(w, r)
	})
}

func permitted(head Head) bool {
	if head.Status != 0 || head.ContentLength < -1 || head.ContentLength > MaxBody || len(head.RawQuery) > 4096 || strings.ContainsAny(head.RawQuery, "\r\n#") {
		return false
	}
	switch head.Path {
	case "/ready", "/health", "/v1/models":
		return head.Method == http.MethodGet
	case "/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/embeddings", "/v1/rerank", "/v1/audio/transcriptions", "/v1/systemone", "/v1/images/generations", "/v1/images/edits", "/private/target", "/private/dispatches":
		return head.Method == http.MethodPost
	}
	return false
}

func (b *bridge) Exchange(stream grpc.BidiStreamingServer[anypb.Any, anypb.Any]) (result error) {
	frame, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "request head required")
	}
	head, err := ParseHead(frame)
	if err != nil || frame.TypeUrl != RequestHead || !permitted(head) {
		return status.Error(codes.InvalidArgument, "invalid request head")
	}
	ctx, cancel := context.WithCancel(context.WithValue(stream.Context(), modelTransportKey{}, true))
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	remoteAddr := ""
	if observed, ok := peer.FromContext(stream.Context()); ok && observed.Addr != nil {
		remoteAddr = observed.Addr.String()
	}
	request := (&http.Request{Method: head.Method, URL: &url.URL{Path: head.Path, RawQuery: head.RawQuery}, Header: head.Headers.Clone(), Body: reader, ContentLength: head.ContentLength, Proto: "HTTP/2.0", ProtoMajor: 2, ProtoMinor: 0, RemoteAddr: remoteAddr, RequestURI: head.Path}).WithContext(ctx)
	if head.RawQuery != "" {
		request.RequestURI += "?" + head.RawQuery
	}
	received := make(chan error, 1)
	go func() {
		var total int64
		for {
			frame, err := stream.Recv()
			if err != nil {
				writer.CloseWithError(io.ErrUnexpectedEOF)
				received <- status.Error(codes.InvalidArgument, "request ended without end frame")
				return
			}
			switch frame.TypeUrl {
			case Data:
				total += int64(len(frame.Value))
				if len(frame.Value) > MaxData || total > MaxBody || head.ContentLength >= 0 && total > head.ContentLength {
					writer.CloseWithError(io.ErrUnexpectedEOF)
					received <- status.Error(codes.ResourceExhausted, "request body exceeds limit")
					return
				}
				if _, err = writer.Write(frame.Value); err != nil {
					received <- err
					return
				}
			case RequestEnd:
				trailers, err := ParseEnd(frame)
				if err != nil || head.ContentLength >= 0 && total != head.ContentLength {
					writer.CloseWithError(io.ErrUnexpectedEOF)
					received <- status.Error(codes.InvalidArgument, "invalid request end")
					return
				}
				if _, err = stream.Recv(); err != io.EOF {
					writer.CloseWithError(io.ErrUnexpectedEOF)
					received <- status.Error(codes.InvalidArgument, "frames after request end")
					return
				}
				request.Trailer = trailers
				writer.Close()
				received <- nil
				return
			default:
				writer.CloseWithError(io.ErrUnexpectedEOF)
				received <- status.Error(codes.InvalidArgument, "invalid request frame order")
				return
			}
		}
	}()
	finished := make(chan error, 1)
	response := &responseWriter{stream: stream, header: make(http.Header), deadlineExpired: make(chan struct{}, 1), cancel: cancel}
	response.httpController, _ = stream.Context().Value(httpControllerKey{}).(*http.ResponseController)
	defer func() {
		if response.deadlineReached() {
			response.timedOut.Store(true)
		}
		response.stopDeadline()
		if response.timedOut.Load() {
			result = writeDeadlineError()
		}
	}()
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				if recovered == http.ErrAbortHandler {
					finished <- status.Error(codes.Aborted, "response interrupted")
				} else {
					finished <- status.Error(codes.Internal, "request handler failed")
				}
			}
		}()
		b.handler.ServeHTTP(response, request)
		if response.err != nil {
			finished <- response.err
			return
		}
		finished <- response.finish()
	}()
	for {
		select {
		case err := <-received:
			if err != nil {
				return err
			}
			received = nil
		case err := <-finished:
			return err
		case <-response.deadlineExpired:
			return writeDeadlineError()
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		}
	}
}

type responseWriter struct {
	stream             grpc.BidiStreamingServer[anypb.Any, anypb.Any]
	header             http.Header
	sent               bool
	err                error
	deadlineMu         sync.Mutex
	deadlineTimer      *time.Timer
	deadlineGeneration uint64
	deadline           time.Time
	deadlineExpired    chan struct{}
	timedOut           atomic.Bool
	cancel             context.CancelFunc
	httpController     *http.ResponseController
}

func writeDeadlineError() error {
	return status.Error(codes.DeadlineExceeded, "response write deadline exceeded")
}

func (w *responseWriter) SetReadDeadline(deadline time.Time) error {
	if w.httpController == nil {
		return http.ErrNotSupported
	}
	return w.httpController.SetReadDeadline(deadline)
}

func (w *responseWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlineMu.Lock()
	defer w.deadlineMu.Unlock()
	w.deadlineGeneration++
	w.deadline = deadline
	if w.deadlineTimer != nil {
		w.deadlineTimer.Stop()
		w.deadlineTimer = nil
	}
	if w.timedOut.Load() {
		return writeDeadlineError()
	}
	if w.httpController != nil {
		if err := w.httpController.SetWriteDeadline(deadline); err != nil {
			return err
		}
	}
	if deadline.IsZero() {
		return nil
	}
	generation := w.deadlineGeneration
	w.deadlineTimer = time.AfterFunc(time.Until(deadline), func() {
		w.deadlineMu.Lock()
		defer w.deadlineMu.Unlock()
		if w.deadlineGeneration != generation {
			return
		}
		w.timedOut.Store(true)
		select {
		case w.deadlineExpired <- struct{}{}:
		default:
		}
		w.cancel()
	})
	return nil
}

func (w *responseWriter) stopDeadline() {
	w.deadlineMu.Lock()
	defer w.deadlineMu.Unlock()
	w.deadlineGeneration++
	if w.deadlineTimer != nil {
		w.deadlineTimer.Stop()
		w.deadlineTimer = nil
	}
}

func (w *responseWriter) deadlineReached() bool {
	w.deadlineMu.Lock()
	defer w.deadlineMu.Unlock()
	return !w.deadline.IsZero() && !time.Now().Before(w.deadline)
}

func (w *responseWriter) send(frame *anypb.Any) error {
	if w.timedOut.Load() {
		return writeDeadlineError()
	}
	err := w.stream.Send(frame)
	if w.deadlineReached() {
		w.timedOut.Store(true)
	}
	if w.timedOut.Load() {
		return writeDeadlineError()
	}
	return err
}

func (w *responseWriter) Header() http.Header { return w.header }
func (w *responseWriter) WriteHeader(code int) {
	if w.sent || w.err != nil {
		return
	}
	if code < 200 || code > 599 {
		w.err = status.Error(codes.Internal, "invalid response status")
		return
	}
	head, err := HeadFrame(ResponseHead, Head{Status: code, Headers: w.header.Clone(), ContentLength: -1})
	if err != nil {
		w.err = status.Error(codes.ResourceExhausted, "response headers exceed limit")
		return
	}
	w.sent = true
	w.err = w.send(head)
}
func (w *responseWriter) Write(data []byte) (int, error) {
	if !w.sent {
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return 0, w.err
	}
	total := 0
	for len(data) > 0 {
		size := min(len(data), MaxData)
		if err := w.send(DataFrame(data[:size])); err != nil {
			w.err = err
			return total, err
		}
		total += size
		data = data[size:]
	}
	return total, nil
}
func (w *responseWriter) Flush() { _ = w.FlushError() }
func (w *responseWriter) FlushError() error {
	if !w.sent {
		w.WriteHeader(http.StatusOK)
	}
	return w.err
}
func (w *responseWriter) finish() error {
	if !w.sent {
		w.WriteHeader(http.StatusOK)
	}
	if w.err != nil {
		return w.err
	}
	trailers := make(http.Header)
	for _, line := range w.header.Values("Trailer") {
		for _, name := range strings.Split(line, ",") {
			name = http.CanonicalHeaderKey(strings.TrimSpace(name))
			if name != "" {
				trailers[name] = append([]string(nil), w.header.Values(name)...)
			}
		}
	}
	for name, values := range w.header {
		if strings.HasPrefix(name, http.TrailerPrefix) {
			trailers[strings.TrimPrefix(name, http.TrailerPrefix)] = values
		}
	}
	frame, err := EndFrame(ResponseEnd, trailers)
	if err != nil {
		return status.Error(codes.ResourceExhausted, "response trailers exceed limit")
	}
	return w.send(frame)
}

var _ http.Flusher = (*responseWriter)(nil)
