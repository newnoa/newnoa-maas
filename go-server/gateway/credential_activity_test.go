package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestCredentialReaderReportsOnlyActualResponseReads(t *testing.T) {
	calls := 0
	reader := &credentialReader{ReadCloser: io.NopCloser(strings.NewReader("response")), ctx: context.Background(), interval: time.Minute, report: func(context.Context) error { calls++; return nil }}
	buf := make([]byte, 2)
	for {
		_, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Fatalf("expected one throttled communication update, got %d", calls)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("closing must not renew an idle lease")
	}
}

func TestCredentialReaderDoesNotTurnCommunicationFailureIntoSuccessfulRead(t *testing.T) {
	reader := &credentialReader{ReadCloser: io.NopCloser(strings.NewReader("data")), ctx: context.Background(), report: func(context.Context) error { return context.DeadlineExceeded }}
	if _, err := reader.Read(make([]byte, 4)); err == nil {
		t.Fatal("failed activity authentication must fail the executing read")
	}
}
