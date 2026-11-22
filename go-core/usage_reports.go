package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/newnoa/newnoa-maas/go-server/network/metering"
	"io"
	"log"
	"net/http"
	"time"
)

type usageReport struct {
	ImageUsage         *metering.ImageUsage `json:"imageUsage,omitempty"`
	ImageMetadata      map[string]any       `json:"imageMetadata,omitempty"`
	PendingMeter       json.RawMessage      `json:"pendingMeter,omitempty"`
	MeterSequence      int64                `json:"meterSequence,omitempty"`
	DurationMs         int64                `json:"durationMs,omitempty"`
	FirstTokenMs       *int64               `json:"firstTokenMs,omitempty"`
	OutputTps          *float64             `json:"outputTps,omitempty"`
	Status             string               `json:"status,omitempty"`
	TransportConfirmed bool                 `json:"transportConfirmed,omitempty"`
	GatewayID          string               `json:"gatewayId"`
	RequestID          string               `json:"requestId"`
	NodeID             string               `json:"nodeId"`
	Epoch              int64                `json:"epoch"`
	Input              *int64               `json:"inputTokens"`
	Output             *int64               `json:"outputTokens"`
	Cached             *int64               `json:"cachedInputTokens"`
	Write              *int64               `json:"cacheWriteTokens"`
}
type usageCapture struct {
	completed                               bool
	stream                                  bool
	pending                                 []byte
	event                                   []byte
	report                                  usageReport
	nativeInput, nativeRead, nativeCreation *int64
	overflow                                bool
	invalid                                 bool
	protocol                                string
}

func newUsageCapture(stream bool) *usageCapture { return &usageCapture{stream: stream} }
func usageStreamEventLimit(protocol string) int {
	if protocol == "/v1/responses" {
		return historyBodyLimit
	}
	return hostedSSEEventLimit
}
func validUsage(n *int64) bool { return n == nil || *n >= 0 && *n <= 1_000_000_000 }
func (c *usageCapture) invalidate() {
	c.invalid = true
	c.completed = false
	c.report.Status = "failed"
	c.report.Input, c.report.Output, c.report.Cached = nil, nil, nil
	c.report.Write = nil
	c.pending, c.event = nil, nil
}
func (c *usageCapture) parse(raw []byte) {
	raw = bytes.TrimPrefix(bytes.TrimSpace(raw), []byte("\xef\xbb\xbf"))
	if bytes.Equal(raw, []byte("[DONE]")) {
		if c.protocol == "/v1/audio/transcriptions" && c.completed {
			return
		}
		if c.protocol == "/v1/messages" || c.protocol == "/v1/responses" || c.protocol == "/v1/audio/transcriptions" || c.stream && c.completed {
			c.invalidate()
		} else if !c.invalid {
			c.completed = true
		}
		return
	}
	var value struct {
		Type    string          `json:"type"`
		Error   json.RawMessage `json:"error"`
		Choices []struct {
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Usage   json.RawMessage `json:"usage"`
		Message struct {
			Usage json.RawMessage `json:"usage"`
		} `json:"message"`
		Response struct {
			Usage  json.RawMessage `json:"usage"`
			Status string          `json:"status"`
		} `json:"response"`
	}
	if !bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{")) || json.Unmarshal(raw, &value) != nil {
		c.invalidate()
		return
	}
	if isImagePath(c.protocol) {
		c.report.ImageUsage = metering.ParseImageUsage(raw)
		c.report.ImageMetadata = metering.ImageMetadata(c.protocol, nil, raw)
	}
	if len(value.Error) > 0 && string(value.Error) != "null" || value.Type == "error" || value.Type == "response.failed" || value.Type == "response.incomplete" {
		c.invalidate()
		return
	}
	if c.invalid {
		return
	}
	if c.stream {
		if c.completed {
			c.invalidate()
			return
		}
		switch c.protocol {
		case "/v1/messages":
			c.completed = value.Type == "message_stop"
		case "/v1/responses":
			if value.Type == "response.completed" {
				if value.Response.Status != "completed" {
					c.invalidate()
					return
				}
				c.completed = true
			}
		case "/v1/audio/transcriptions":
			c.completed = value.Type == "transcript.text.done"
		}
	} else {
		if value.Type == "message_stop" || value.Type == "response.completed" || value.Type == "transcript.text.done" {
			c.completed = true
		}
		for _, choice := range value.Choices {
			if choice.FinishReason != nil {
				c.completed = true
			}
		}
	}
	usage := value.Usage
	if len(value.Message.Usage) > 0 {
		usage = value.Message.Usage
	}
	if len(value.Response.Usage) > 0 {
		usage = value.Response.Usage
	}
	var u struct {
		Prompt        *int64 `json:"prompt_tokens"`
		Completion    *int64 `json:"completion_tokens"`
		Input         *int64 `json:"input_tokens"`
		Output        *int64 `json:"output_tokens"`
		Read          *int64 `json:"cache_read_input_tokens"`
		Creation      *int64 `json:"cache_creation_input_tokens"`
		PromptDetails struct {
			Cached   *int64 `json:"cached_tokens"`
			Write    *int64 `json:"cache_write_tokens"`
			Creation *int64 `json:"cache_creation_tokens"`
		} `json:"prompt_tokens_details"`
		InputDetails struct {
			Cached   *int64 `json:"cached_tokens"`
			Write    *int64 `json:"cache_write_tokens"`
			Creation *int64 `json:"cache_creation_tokens"`
		} `json:"input_tokens_details"`
	}
	if len(usage) == 0 || bytes.Equal(bytes.TrimSpace(usage), []byte("null")) {
		return
	}
	if json.Unmarshal(usage, &u) != nil {
		c.invalidate()
		return
	}
	for _, n := range []*int64{u.Prompt, u.Completion, u.Input, u.Output, u.Read, u.Creation, u.PromptDetails.Cached, u.InputDetails.Cached, u.PromptDetails.Write, u.InputDetails.Write, u.PromptDetails.Creation, u.InputDetails.Creation} {
		if !validUsage(n) {
			c.invalidate()
			return
		}
	}
	native := c.protocol == "/v1/messages" || c.protocol == "" && (value.Type == "message_start" || value.Type == "message_delta" || u.Prompt == nil && (u.Read != nil || u.Creation != nil))
	if native {
		if u.Input != nil {
			c.nativeInput = u.Input
		}
		if u.Read != nil {
			c.nativeRead = u.Read
		}
		if u.Creation != nil {
			c.nativeCreation = u.Creation
		}
		if u.Output != nil {
			c.report.Output = u.Output
		}
		if c.nativeInput != nil {
			total := *c.nativeInput
			if c.nativeRead != nil {
				total += *c.nativeRead
			}
			if c.nativeCreation != nil {
				total += *c.nativeCreation
			}
			if total > 1_000_000_000 {
				c.invalidate()
				return
			}
			c.report.Input = &total
			c.report.Cached = c.nativeRead
			c.report.Write = c.nativeCreation
		}
		return
	}
	input, output, cached, write := u.Prompt, u.Completion, u.PromptDetails.Cached, u.PromptDetails.Write
	if write == nil {
		write = u.PromptDetails.Creation
	}
	if input == nil {
		input = u.Input
		if output == nil {
			output = u.Output
		}
		if cached == nil {
			cached = u.InputDetails.Cached
		}
		if write == nil {
			write = u.InputDetails.Write
		}
		if write == nil {
			write = u.InputDetails.Creation
		}
	}
	if write == nil {
		write = u.Creation
	}
	var writeCount int64
	if write != nil {
		writeCount = *write
	}
	if (writeCount > 0 || cached != nil) && (input == nil || writeCount > *input || cached != nil && *cached > *input-writeCount) {
		c.invalidate()
		return
	}
	if input != nil {
		c.report.Cached = nil
		c.report.Input = input
		c.report.Write = nil
		if write != nil && *write <= *input {
			c.report.Write = write
		}
	}
	if output != nil {
		c.report.Output = output
	}
	if cached != nil && c.report.Input != nil && *cached+writeCount <= *c.report.Input {
		c.report.Cached = cached
	}
}
func (c *usageCapture) feed(data []byte) {
	if c.overflow || c.invalid {
		return
	}
	if !c.stream {
		c.pending = append(c.pending, data...)
		limit := historyBodyLimit
		if isImagePath(c.protocol) {
			limit = imageResponseLimit
		}
		if len(c.pending) > limit {
			c.pending = nil
			c.overflow = true
			c.invalidate()
		}
		return
	}
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		size := len(data)
		if i >= 0 {
			size = i + 1
		}
		if len(c.pending)+len(c.event)+size > usageStreamEventLimit(c.protocol) {
			c.overflow = true
			c.invalidate()
			return
		}
		c.pending = append(c.pending, data[:size]...)
		data = data[size:]
		if i < 0 {
			return
		}
		line := bytes.TrimSuffix(c.pending[:len(c.pending)-1], []byte("\r"))
		if len(line) == 0 {
			if len(c.event) > 0 {
				c.parse(c.event[:len(c.event)-1])
				c.event = nil
			}
		} else if bytes.HasPrefix(line, []byte("data:")) {
			c.event = append(c.event, bytes.TrimPrefix(line[5:], []byte(" "))...)
			c.event = append(c.event, '\n')
		}
		c.pending = c.pending[:0]
		if c.invalid {
			return
		}
	}
}
func (c *usageCapture) counts() usageReport {
	if !c.stream && !c.overflow {
		c.parse(c.pending)
	} else if len(c.pending) != 0 || len(c.event) != 0 {
		// EOF does not dispatch an unfinished SSE event, even after a done marker.
		c.completed = false
	}
	return c.report
}

func (h *chatHistory) queueUsage(report usageReport) error {
	raw, err := json.Marshal(report)
	if err != nil {
		return err
	}
	_, err = h.db.Exec("INSERT INTO private_usage_outbox(request_id,node_id,payload) VALUES(?,?,?) ON CONFLICT(request_id,node_id) DO NOTHING", report.RequestID, report.NodeID, string(raw))
	return err
}
func (n *connector) usageFailure(err error) {
	n.mu.Lock()
	if err == nil {
		n.usageReportError = ""
	} else {
		n.usageReportError = err.Error()
	}
	n.mu.Unlock()
}
func (n *connector) flushUsage(ctx context.Context) {
	if n.usageStore == nil {
		return
	}
	if !n.usageMu.TryLock() {
		return
	}
	defer n.usageMu.Unlock()
	rows, err := n.usageStore.db.QueryContext(ctx, "SELECT request_id,payload,attempts FROM private_usage_outbox WHERE node_id=? AND next_attempt<=unixepoch() ORDER BY created_at,request_id LIMIT 20", n.nodeID)
	if err != nil {
		n.usageFailure(errors.New("usage_queue_read_failed"))
		return
	}
	type pending struct {
		id, raw  string
		attempts int
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if err = rows.Scan(&p.id, &p.raw, &p.attempts); err != nil {
			break
		}
		batch = append(batch, p)
	}
	readErr := rows.Err()
	rows.Close()
	if err != nil || readErr != nil {
		n.usageFailure(errors.New("usage_queue_read_failed"))
		return
	}
	for _, p := range batch {
		if ctx.Err() != nil {
			return
		}
		var report usageReport
		if json.Unmarshal([]byte(p.raw), &report) != nil {
			n.usageFailure(errors.New("usage_queue_invalid"))
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		err = n.sendUsage(callCtx, report)
		cancel()
		if err == nil {
			_, err = n.usageStore.db.Exec("DELETE FROM private_usage_outbox WHERE request_id=? AND node_id=?", p.id, n.nodeID)
		} else {
			delay := 10 << min(p.attempts, 5)
			_, writeErr := n.usageStore.db.Exec("UPDATE private_usage_outbox SET attempts=attempts+1,next_attempt=unixepoch()+?,last_error=? WHERE request_id=? AND node_id=?", min(delay, 300), err.Error(), p.id, n.nodeID)
			if writeErr != nil {
				err = errors.New("usage_queue_write_failed")
			}
		}
		n.usageFailure(err)
	}
}
func (n *connector) sendUsage(ctx context.Context, report usageReport) error {
	if _, err := controlBase(n.control); err != nil {
		return errors.New("usage_cloud_invalid")
	}
	if report.Status != "" {
		body := map[string]any{"nodeId": report.NodeID, "epoch": report.Epoch, "gatewayId": report.GatewayID, "status": report.Status, "transportConfirmed": report.TransportConfirmed}
		var ack struct {
			Finalized bool `json:"finalized"`
		}
		if err := n.call(ctx, "/connector/dispatches/"+report.RequestID+"/private-finish", body, &ack); err != nil || !ack.Finalized {
			return errors.New("private_completion_pending")
		}
	}
	endpoint := n.control + "/api/v1/connector/usage"
	reportBody := map[string]any{"requestId": report.RequestID, "nodeId": report.NodeID, "epoch": report.Epoch, "inputTokens": report.Input, "outputTokens": report.Output, "cachedInputTokens": report.Cached, "cacheWriteTokens": report.Write, "gatewayId": report.GatewayID, "imageUsage": report.ImageUsage, "imageMetadata": report.ImageMetadata}
	raw, _ := json.Marshal(reportBody)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return errors.New("usage_server_invalid")
	}
	req.Header.Set("Authorization", "Bearer "+n.credential)
	req.Header.Set("Content-Type", "application/json")
	res, err := n.client.Do(req)
	if err != nil {
		return errors.New("usage_server_unavailable")
	}
	defer res.Body.Close()
	var ack struct {
		Accepted bool `json:"accepted"`
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("usage_report_pending_%d", res.StatusCode)
	}
	if json.NewDecoder(io.LimitReader(res.Body, 8192)).Decode(&ack) != nil || !ack.Accepted {
		return errors.New("usage_not_acknowledged")
	}
	return nil
}

// Pause new executions while the durable backlog drains. In-flight reports must still be recorded.
const usageBacklogHighWater = 10000

func (n *connector) checkUsageBacklog(ctx context.Context) {
	if n.usageStore == nil || ctx.Err() != nil {
		return
	}
	var full bool
	err := n.usageStore.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM private_usage_outbox LIMIT 1 OFFSET ?)", usageBacklogHighWater-1).Scan(&full)
	if ctx.Err() != nil {
		return
	}
	paused := full || err != nil
	n.mu.Lock()
	changed := paused != n.usageQueuePaused
	n.usageQueuePaused = paused
	n.mu.Unlock()
	if changed {
		log.Printf("private usage outbox admission paused=%t high_water=%d read_failed=%t", paused, usageBacklogHighWater, err != nil)
	}
}

func (n *connector) runUsage() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	n.flushUsage(n.ctx)
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.checkUsageBacklog(n.ctx)
			n.flushUsage(n.ctx)
		}
	}
}
func (n *connector) saveUsage(c *usageCapture, requestID string, epoch int64, gateway string) {
	report := c.counts()
	report.RequestID, report.NodeID, report.Epoch, report.GatewayID = requestID, n.nodeID, epoch, gateway
	if err := n.usageStore.queueUsage(report); err != nil {
		n.usageFailure(errors.New("usage_queue_write_failed"))
		log.Printf("usage_queue_write_failed request=%s", requestID)
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		n.flushUsage(ctx)
	}()
}
