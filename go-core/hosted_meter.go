package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime"
	"strings"

	"github.com/newnoa/newnoa-maas/go-server/network/metering"
)

// Hosted-only adapter. Ordinary device accounting stays at Gateway.
type hostedMeter struct {
	imageMetadata map[string]any
	imageRequest  []byte
	text          *metering.Meter
	usage         *usageCapture
	plain         bool
	observed      bool
	terminal      bool
	sequence      int64
	accepted      []byte
	pending       []byte
}

func newHostedMeter(model, protocol string, request []byte) (*hostedMeter, error) {
	m := &hostedMeter{usage: newUsageCapture(false)}
	m.usage.protocol = protocol
	if isImagePath(protocol) {
		m.imageRequest = append([]byte(nil), request...)
		m.imageMetadata = metering.ObserveImagePayload(protocol, request, nil)
	}
	switch protocol {
	case "/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/audio/transcriptions":
		// Multipart media bytes are never interpreted as text prompt tokens.
		if protocol == "/v1/audio/transcriptions" {
			request = nil
		}
		var err error
		m.text, err = metering.New(model, protocol, request)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *hostedMeter) response(stream bool, contentType string) {
	m.usage.stream = stream
	media, _, _ := mime.ParseMediaType(contentType)
	if m.usage.protocol == "/v1/audio/transcriptions" && !stream && !strings.EqualFold(media, "application/json") {
		m.text = nil
		m.plain = true
	}
}

func (m *hostedMeter) feed(raw []byte) ([]byte, error) {
	if m.text != nil {
		return m.text.Feed(raw)
	}
	if m.plain {
		m.observed = m.observed || len(raw) > 0
		return raw, nil
	}
	// Existing non-text JSON endpoints stay supported, with nullable actual
	// upstream counters only. Their validated JSON is held until complete EOF.
	m.usage.feed(raw)
	if m.usage.overflow {
		return nil, errors.New("hosted observation exceeds limit")
	}
	return nil, nil
}

func (m *hostedMeter) finish() ([]byte, error) {
	if m.text != nil {
		return m.text.Finish()
	}
	if m.plain {
		return nil, nil
	}
	raw := m.usage.pending
	if !json.Valid(raw) {
		return nil, errors.New("hosted observation invalid")
	}
	m.usage.counts()
	if isImagePath(m.usage.protocol) {
		m.imageMetadata = metering.ObserveImagePayload(m.usage.protocol, m.imageRequest, raw)
	}
	if m.usage.invalid {
		return nil, errors.New("hosted observation invalid")
	}
	m.observed = true
	m.terminal = true
	return raw, nil
}

func (m *hostedMeter) snapshot() metering.Snapshot {
	if m.text != nil {
		return m.text.Snapshot()
	}
	s := metering.Snapshot{Estimator: "reported-only-v1", Encoding: "unavailable", ResponseStarted: m.observed}
	u := m.usage.report
	s.ImageUsage = u.ImageUsage
	if m.imageMetadata != nil {
		s.ImageMetadata = map[string]any{}
		for k, v := range m.imageMetadata {
			s.ImageMetadata[k] = v
		}
		for k, v := range u.ImageMetadata {
			s.ImageMetadata[k] = v
		}
	}
	if u.ImageUsage != nil {
		s.TerminalUsage = m.terminal
	}
	if u.Input != nil || u.Output != nil || u.Write != nil || u.Cached != nil {
		s.Reported = &metering.Usage{InputTokens: u.Input, OutputTokens: u.Output, CacheWriteTokens: u.Write, CachedInputTokens: u.Cached}
		s.TerminalUsage = m.terminal
	}
	return s
}

func (m *hostedMeter) copyReported(report *usageReport) {
	snapshot := m.snapshot()
	report.ImageUsage = snapshot.ImageUsage
	report.ImageMetadata = snapshot.ImageMetadata
	if usage := snapshot.Reported; usage != nil {
		report.Input, report.Output, report.Write, report.Cached = usage.InputTokens, usage.OutputTokens, usage.CacheWriteTokens, usage.CachedInputTokens
	}
}

func (m *hostedMeter) checkpoint(ctx context.Context, c *core, h *chatHistory, account string, d hostedDispatch, report *usageReport) error {
	snapshot, err := json.Marshal(m.snapshot())
	if err != nil {
		return err
	}
	if bytes.Equal(snapshot, m.accepted) {
		return nil
	}
	sequence := m.sequence + 1
	if err = c.checkpointHosted(ctx, d, sequence, snapshot); err != nil {
		if errors.Is(err, errHostedCheckpointUnavailable) {
			m.pending = append(m.pending[:0], snapshot...)
		}
		return err
	}
	m.pending = nil
	m.sequence, m.accepted = sequence, append(m.accepted[:0], snapshot...)
	report.MeterSequence = sequence
	m.copyReported(report)
	if h != nil {
		return c.persistHostedProgress(ctx, h, account, d, *report)
	}
	return nil
}

// Recover delivery only after executeHosted's transport-close defer. The API may
// already have committed this exact next sequence before the caller lost its ack.
func (m *hostedMeter) recoverCheckpoint(ctx context.Context, c *core, h *chatHistory, account string, d hostedDispatch, report *usageReport) error {
	if len(m.pending) == 0 {
		return nil
	}
	sequence := m.sequence + 1
	if err := c.checkpointHosted(ctx, d, sequence, json.RawMessage(m.pending)); err != nil {
		if errors.Is(err, errHostedCheckpointRejected) {
			m.pending = nil
		}
		return err
	}
	m.sequence, m.accepted = sequence, append(m.accepted[:0], m.pending...)
	m.pending = nil
	report.MeterSequence = sequence
	m.copyReported(report)
	if h != nil {
		return c.persistHostedProgress(ctx, h, account, d, *report)
	}
	return nil
}
