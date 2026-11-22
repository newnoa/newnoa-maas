package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestSVGStreamReasoningDoesNotConsumeDrawingLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &svgResponse{header: http.Header{"Content-Type": []string{"text/event-stream"}}, status: 200, cancel: cancel}
	frame := "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"" + strings.Repeat("x", 900) + "\"}}]}\n\n"
	for i := 0; i < 5000; i++ {
		if _, err := io.WriteString(w, frame); err != nil {
			t.Fatalf("reasoning overhead cancelled drawing: %v", err)
		}
	}
	if ctx.Err() != nil {
		t.Fatal("drawing cancelled before any SVG content")
	}
	io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"<svg></svg>\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
	got, err := parseSVGResponse(w)
	if err != nil || got.Content != "<svg></svg>" {
		t.Fatalf("complete drawing lost: %#v %v", got, err)
	}
}
func TestSVGStreamPreservesContentOnProviderError(t *testing.T) {
	w := &svgResponse{header: http.Header{"Content-Type": []string{"text/event-stream"}}, status: 200, cancel: func() {}}
	io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"<svg><circle/>\"}}]}\n\ndata: {\"error\":{\"message\":\"private upstream secret\"}}\n\n")
	got, err := parseSVGResponse(w)
	if err == nil || got.Content != "<svg><circle/>" || strings.Contains(err.Error(), "secret") {
		t.Fatalf("partial drawing/error boundary: %#v %v", got, err)
	}
}
func TestSVGStreamRejectsDataAfterDone(t *testing.T) {
	w := &svgResponse{header: http.Header{"Content-Type": []string{"text/event-stream"}}, status: 200, cancel: func() {}}
	io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"<svg></svg>\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\ndata: {}\n\n")
	if _, err := parseSVGResponse(w); err == nil {
		t.Fatal("data after terminal marker accepted")
	}
}

func TestSVGStreamTransportAndContentRemainBounded(t *testing.T) {
	w := &svgResponse{header: http.Header{"Content-Type": []string{"text/event-stream"}}, status: 200, cancel: func() {}, size: svgStreamLimit - 1}
	if _, err := io.WriteString(w, "xx"); err == nil {
		t.Fatal("unbounded SVG transport")
	}
	w = &svgResponse{header: http.Header{"Content-Type": []string{"text/event-stream"}}, status: 200, cancel: func() {}}
	io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"<svg>\"}}]}\n\n")
	w.content.WriteString(strings.Repeat("a", svgContentLimit-5))
	if _, err := io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"); err == nil {
		t.Fatal("unbounded SVG content")
	}
	got, err := parseSVGResponse(w)
	if err == nil || !strings.HasPrefix(got.Content, "<svg>") || len(got.Content) != svgContentLimit {
		t.Fatal("bounded partial content lost")
	}
}

func TestSVGOuterWriterPreservesLimitDiagnostic(t *testing.T) {
	w := &svgResponse{header: http.Header{"Content-Type": []string{"text/event-stream"}}, status: 200, cancel: func() {}}
	outer := &modelTestWriter{limit: svgStreamLimit, size: svgStreamLimit - 1, ResponseWriter: w, header: make(http.Header), status: 200, cancel: func() {}}
	if _, err := io.WriteString(outer, "xx"); err == nil || w.err == nil || w.err.Error() != "SVG response exceeds limit" {
		t.Fatalf("limit diagnostic lost: %v / %v", err, w.err)
	}
}
func TestSVGStreamAcceptsFragmentedMultilineEvents(t *testing.T) {
	w := &svgResponse{header: http.Header{"Content-Type": []string{"text/event-stream"}}, status: 200, cancel: func() {}}
	wire := "data: {\"choices\":[{\"delta\":\r\ndata: {\"content\":\"<svg>中文</svg>\"},\"finish_reason\":\"stop\"}]}\r\n\r\ndata: [DONE]\r\n\r\n"
	for i := 0; i < len(wire); i++ {
		if _, err := w.Write([]byte{wire[i]}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := parseSVGResponse(w)
	if err != nil || got.Content != "<svg>中文</svg>" {
		t.Fatal(got, err)
	}
}
