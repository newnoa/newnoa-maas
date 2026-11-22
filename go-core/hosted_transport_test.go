package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

type hostedCloseFailure struct {
	io.Reader
	ctx                  context.Context
	cancelledBeforeClose bool
}

func (b *hostedCloseFailure) Close() error {
	b.cancelledBeforeClose = b.ctx.Err() != nil
	return errors.New("sensitive upstream close detail")
}

func TestHostedCloseFailureCancelsFirstAndRetainsPartialMeterEvidence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &hostedCloseFailure{Reader: strings.NewReader(""), ctx: ctx}
	report := usageReport{Status: "succeeded", Input: ptrUsage(12), MeterSequence: 7}
	if err := closeHostedTransport(b, cancel, &report); err == nil {
		t.Fatal("body Close failure hidden")
	}
	if !b.cancelledBeforeClose || report.Status != "unknown" || report.Input == nil || *report.Input != 12 || report.Output != nil || report.MeterSequence != 7 {
		t.Fatalf("close failed to cancel/preserve partial evidence: %+v", report)
	}
}
