package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	metering "github.com/newnoa/newnoa-maas/go-server/network/metering"
)

const cloudHistoryLimit = 8 << 20

var cloudHistoryCompression = make(chan struct{}, 2)

type cloudHistoryWriter struct {
	http.ResponseWriter
	body                   bytes.Buffer
	status                 int
	truncated, writeFailed bool
}

func (w *cloudHistoryWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *cloudHistoryWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *cloudHistoryWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	left := cloudHistoryLimit - w.body.Len()
	if len(data) > left {
		w.truncated = true
	} else {
		left = len(data)
	}
	w.body.Write(data[:left])
	n, err := w.ResponseWriter.Write(data)
	if err != nil || n != len(data) {
		w.writeFailed = true
	}
	return n, err
}

func (g *gateway) historyRemote(ctx context.Context, path, secret string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", g.api+"/api/v1/client/history"+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Content-Type", "application/json")
	res, err := g.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return errors.New("cloud history API rejected request")
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(res.Body, maxBody)).Decode(out)
	}
	return nil
}

func (g *gateway) captureCloudHistory(w http.ResponseWriter, r *http.Request, secret, keyID, requestID string, raw []byte) (http.ResponseWriter, func()) {
	noop := func() {}
	if !historyInferencePath(r.URL.Path) {
		return w, noop
	}
	if _, err := uuid.Parse(keyID); err != nil {
		return w, noop
	}
	if _, err := uuid.Parse(requestID); err != nil {
		return w, noop
	}
	body, captured, attachments, captureErr := historyPayload(raw, r.Header.Get("Content-Type"))
	if captureErr != nil {
		return w, noop
	}
	conversation := r.Header.Get("X-Newnoa-Conversation-Id")
	validConversation := len(r.Header.Values("X-Newnoa-Conversation-Id")) <= 1
	if metadata, ok := body["metadata"].(map[string]any); ok {
		if value, exists := metadata["conversation_id"]; exists {
			id, valid := value.(string)
			if !valid || conversation != "" && id != "" && conversation != id {
				validConversation = false
			}
			if valid && id != "" {
				conversation = id
			}
		}
	}
	if !validConversation || len(conversation) > 200 {
		conversation = ""
	}
	key := conversation
	if key == "" {
		key = requestID
	}
	digest := sha256.Sum256([]byte(key))
	var policy struct{ ID, Mode string }
	if err := g.historyRemote(r.Context(), "/policy", secret, map[string]any{"conversationKey": hex.EncodeToString(digest[:]), "requestId": requestID}, &policy); err != nil {
		w.Header().Set("X-Newnoa-History", "unavailable")
		return w, noop
	}
	if _, err := uuid.Parse(policy.ID); err != nil || policy.Mode != "cloud" {
		return w, noop
	}
	w.Header().Add("Trailer", "X-Newnoa-History")
	writer := &cloudHistoryWriter{ResponseWriter: w}
	var wrapped http.ResponseWriter = writer
	if native, ok := w.(*messagesWriter); ok {
		writer.ResponseWriter = native.ResponseWriter
		wrapped = &messagesWriter{writer}
	}
	started := time.Now()
	return wrapped, func() {
		status := writer.status
		if status == 0 {
			status = http.StatusOK
		}
		model, _ := body["model"].(string)
		agent := r.Header.Get("X-Newnoa-Agent")
		if agent == "" {
			agent = r.UserAgent()
		}
		clean := func(value string, limit int) string {
			if len(value) > limit {
				value = value[:limit]
			}
			return strings.ReplaceAll(strings.ToValidUTF8(value, "?"), "\x00", "?")
		}
		response := writer.body.String()
		if imagePath(r.URL.Path) {
			response = imageHistoryResponse(response)
		}
		value := map[string]any{"id": requestID, "requestId": requestID, "storageId": policy.ID, "storageMode": "cloud", "createdAt": started.UTC().Format(time.RFC3339Nano), "finishedAt": time.Now().UTC().Format(time.RFC3339Nano), "durationMs": time.Since(started).Milliseconds(), "model": clean(model, 200), "agent": clean(agent, 200), "keyId": keyID, "protocol": r.URL.Path, "request": clean(string(captured), 32<<20), "response": clean(response, cloudHistoryLimit), "requestTruncated": len(captured) > 32<<20, "responseTruncated": writer.truncated, "httpStatus": status, "interrupted": writer.writeFailed || r.Context().Err() != nil, "contentType": writer.Header().Get("Content-Type"), "transportPath": writer.Header().Get("X-Newnoa-Path")}
		if imagePath(r.URL.Path) {
			value["imageUsage"] = metering.ParseImageUsage(writer.body.Bytes())
			value["imageMetadata"] = metering.ImageMetadata(r.URL.Path, raw, writer.body.Bytes())
		}
		if modelType := historyModelType(writer.Header().Get("X-Newnoa-Model-Type")); modelType != "" {
			value["modelType"] = modelType
			value["modelCategory"] = "text"
			if modelType == "image" {
				value["modelCategory"] = "image"
			}
		}
		if category := writer.Header().Get("X-Newnoa-Model-Category"); category == "text" || category == "image" {
			value["modelCategory"] = category
		}
		if len(attachments) > 0 {
			value["attachments"] = attachments
		}
		if conversation != "" {
			value["conversationId"] = conversation
		}

		packed, err := compressedHistory(value)
		if err != nil {
			writer.Header().Set("X-Newnoa-History", "failed")
			log.Printf("cloud_history_failed request=%s reason=encoding", requestID)
			return
		}
		delivery, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 60*time.Second)
		defer cancel()
		for attempt := 0; attempt < 3; attempt++ {
			var receipt struct {
				Saved  bool `json:"saved"`
				Paused bool `json:"paused"`
			}
			err = g.historyRemote(delivery, "", secret, packed, &receipt)
			if err == nil && receipt.Paused && !receipt.Saved {
				writer.Header().Set("X-Newnoa-History", "paused")
				return
			}
			if err == nil && !receipt.Saved {
				err = errors.New("cloud history was not acknowledged")
			}
			if err == nil {
				writer.Header().Set("X-Newnoa-History", "saved")
				return
			}
			if attempt < 2 {
				select {
				case <-delivery.Done():
					attempt = 3
				case <-time.After(time.Duration(attempt+1) * 150 * time.Millisecond):
				}
			}
		}
		writer.Header().Set("X-Newnoa-History", "pending")
		log.Printf("cloud_history_pending request=%s attempts=3", requestID)

	}
}

func coreCapturesHistory(r *http.Request, path string) bool {
	return false // A caller-controlled header cannot suppress publisher persistence.
}

func compressedHistory(value map[string]any) (map[string]any, error) {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case cloudHistoryCompression <- struct{}{}:
		defer func() { <-cloudHistoryCompression }()
	case <-timer.C:
		return nil, errors.New("history compression capacity unavailable")
	}

	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(raw) > 64<<20 {
		return nil, errors.New("history record exceeds limit")
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(19)), zstd.WithWindowSize(8<<20), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	compressed := encoder.EncodeAll(raw, nil)
	digest := sha256.Sum256(raw)
	return map[string]any{"record": map[string]any{"id": value["id"], "requestId": value["requestId"]}, "archive": map[string]any{"codec": "zstd-json-v1", "rawBytes": len(raw), "compressedBytes": len(compressed), "sha256": hex.EncodeToString(digest[:]), "bodyBase64": base64.StdEncoding.EncodeToString(compressed)}}, nil
}
