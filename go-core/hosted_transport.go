package main

import (
	"context"
	"io"
)

// Cancel provider work before a potentially failing Close. Billing retries run
// later, after this transport is closed, and never keep abandoned inference alive.
func closeHostedTransport(body io.ReadCloser, cancel context.CancelFunc, report *usageReport) error {
	cancel()
	err := body.Close()
	if err != nil && report.Status == "succeeded" {
		report.Status = "unknown"
	}
	return err
}
