package metering

import (
	"errors"
	"strings"
	"testing"
)

func TestRejectionSiteAndEventStaySafeAndFirst(t *testing.T) {
	m, _ := New("unknown", "/v1/responses", nil)
	_, err := m.Feed([]byte("data: {\"type\":\"private-event-sentinel\",\"private-key-sentinel\":\"private-value\"}\n\n"))
	if err != ErrProtocol {
		t.Fatal("ORIGINAL_ERROR_CHANGED")
	}
	d := m.Rejection()
	if d.Site != "unsupported-event" || d.Event != "other" {
		t.Fatal("REJECTION_NOT_SAFE", d)
	}
	// fail/recount can replace a return error, but never the first rejected site.
	_, err = m.fail(ErrLimit)
	if !errors.Is(err, ErrLimit) || m.Rejection() != d {
		t.Fatal("FIRST_REJECTION_LOST")
	}
}
func TestFinishTailDoesNotAttributePreviousEvent(t *testing.T) {
	m, _ := New("unknown", "/v1/responses", nil)
	if _, e := m.Feed([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic\"}\n\ndata: {")); e != nil {
		t.Fatal(e)
	}
	_, err := m.Finish()
	d := m.Rejection()
	if err != ErrTruncated || d.Site != "other" || d.Event != "none" {
		t.Fatal("TAIL_ATTRIBUTION_CHANGED", d)
	}
}
func TestRecountDoesNotOverwriteFirstRejectedSite(t *testing.T) {
	m, _ := New("unknown", "/v1/responses", nil)
	_, _ = m.Feed([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n"))
	m.encoding = "invalid-synthetic"
	m.dirty["response/0/0/response.output_text"] = true
	_, err := m.Feed([]byte("data: {\"type\":\"response.output_text.done\",\"text\":\"different\"}\n\n"))
	if err != ErrProtocol || m.Rejection().Site != "snapshot-mismatch" || m.Rejection().Event != "response.output_text.done" {
		t.Fatal("RECOUNT_OVERWROTE_REJECTION")
	}
}
func TestRejectionClassLimitsKeepNoUnverifiedEvent(t *testing.T) {
	m, _ := New("unknown", "/v1/responses", nil)
	_, err := m.Feed([]byte(":" + strings.Repeat("x", MaxEventBytes)))
	if err != ErrLimit || m.Rejection().Site != "other" || m.Rejection().Event != "none" {
		t.Fatal("LIMIT_CHANGED")
	}
}

func TestPartialOversizedFrameDoesNotReuseGoodEvent(t *testing.T) {
	m, _ := New("unknown", "/v1/responses", nil)
	if _, e := m.Feed([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n")); e != nil {
		t.Fatal(e)
	}
	_, err := m.Feed([]byte(":" + strings.Repeat("x", MaxEventBytes)))
	if err != ErrLimit || m.Rejection().Event != "none" {
		t.Fatal("ATTRIBUTED_GOOD_EVENT_TO_UNPARSED_FRAME")
	}
}
