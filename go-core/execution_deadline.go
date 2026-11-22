package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const legacyExecutionTimeout = 5 * time.Minute
const maximumExecutionTimeout = 30 * time.Minute
const inferenceWrapperTimeout = 36 * time.Minute // admission + execution + settlement, not an execution grant
const executionClockTolerance = 5 * time.Second

type authorizedExecutionContext struct{}

// Only API consume/admission metadata can grant execution time; caller headers and bodies cannot.
func executionContext(parent context.Context, raw json.RawMessage, now time.Time) (context.Context, context.CancelFunc, error) {
	deadline := now.Add(legacyExecutionTimeout)
	if len(raw) != 0 {
		var value string
		if json.Unmarshal(raw, &value) != nil || value == "" {
			return nil, nil, errors.New("invalid API execution deadline")
		}
		var err error
		deadline, err = time.Parse(time.RFC3339Nano, value)
		if err != nil || !deadline.After(now) || deadline.After(now.Add(maximumExecutionTimeout+executionClockTolerance)) {
			return nil, nil, errors.New("invalid API execution deadline")
		}
	}
	ctx, cancel := context.WithDeadline(context.WithValue(parent, authorizedExecutionContext{}, true), deadline)
	return ctx, cancel, nil
}
