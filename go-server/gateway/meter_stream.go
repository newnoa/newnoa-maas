package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	metering "github.com/newnoa/newnoa-maas/go-server/network/metering"
)

var errMeterCheckpoint = errors.New("meter checkpoint failed")

func checkpointMeterBatch(ctx context.Context, meter *metering.Meter, reporter *meterCheckpoint, raw []byte, eof bool) ([]byte, error) {
	ready, err := meter.Feed(raw)
	if err != nil {
		if reporter != nil {
			reporter.failureStage = "feed"
		}
		return nil, err
	}
	if eof {
		tail, finishErr := meter.Finish()
		if finishErr != nil {
			if reporter != nil {
				reporter.failureStage = "finish"
			}
			return nil, finishErr
		}
		ready = append(ready, tail...)
	}
	if len(ready) > 0 {
		snapshot := meter.Snapshot()
		rawSnapshot, marshalErr := json.Marshal(snapshot)
		if marshalErr != nil {
			return nil, fmt.Errorf("%w: %w", errMeterCheckpoint, marshalErr)
		}
		if bytes.Equal(rawSnapshot, reporter.lastSnapshot) {
			return ready, nil
		}
		if err = reporter.persist(ctx, snapshot); err != nil {
			return nil, fmt.Errorf("%w: %w", errMeterCheckpoint, err)
		}
	}
	return ready, nil
}

func meterErrorClass(err error) string {
	switch {
	case errors.Is(err, metering.ErrProtocol):
		return "protocol"
	case errors.Is(err, metering.ErrLimit):
		return "limit"
	case errors.Is(err, metering.ErrTruncated):
		return "truncated"
	case errors.Is(err, metering.ErrFinished):
		return "finished"
	default:
		return "other"
	}
}
