package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

const maxExecutionTimeout = 30 * time.Minute
const legacyExecutionTimeout = 5 * time.Minute
const executionClockTolerance = 5 * time.Second
const deviceConsumeAllowance = 30 * time.Second // reserved permit/transit envelope, not a Core execution grant

func executionTimeout(raw json.RawMessage) (time.Duration, error) {
	if len(raw) == 0 {
		return legacyExecutionTimeout, nil
	}
	seconds, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || seconds < 1 || seconds > int64(maxExecutionTimeout/time.Second) {
		return 0, errors.New("invalid execution timeout")
	}
	return time.Duration(seconds) * time.Second, nil
}

func executionDeadline(raw json.RawMessage, budget time.Duration, now time.Time) (time.Time, error) {
	if len(raw) == 0 {
		return now.Add(min(budget, legacyExecutionTimeout)), nil
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return time.Time{}, errors.New("invalid execution deadline")
	}
	deadline, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || !deadline.After(now) || deadline.After(now.Add(budget+executionClockTolerance)) {
		return time.Time{}, errors.New("invalid execution deadline")
	}
	return deadline, nil
}

// Only locally established execution contexts opt into long first-byte waits.
type executionContextKey struct{}

// Admission cancellation stops peer processes; after handoff execution owns their lifetime.
func peerLifecycle(admission context.Context) (context.Context, func(), func()) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(admission))
	stop := context.AfterFunc(admission, cancel)
	return ctx, func() { stop() }, func() { stop(); cancel() }
}

func inferenceHTTPClient(client *http.Client) *http.Client {
	// Peer transports are constructed per request and do not use the gateway's timeout selector.
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		return client
	}
	clone := transport.Clone()
	clone.ResponseHeaderTimeout = maxExecutionTimeout + time.Minute
	result := *client
	result.Transport = clone
	return &result
}
