package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
)

var errHostedResponseInvalid = errors.New("invalid hosted response")

// Sniff before forwarding: some upstreams return JSON even when streaming was requested.
func hostedResponseReader(response *http.Response, protocol string, requestedStream bool) (io.Reader, bool, error) {
	media, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	stream := requestedStream || strings.EqualFold(media, "text/event-stream")
	reader := bufio.NewReader(response.Body)
	var prefix []byte
	for len(prefix) < 4096 {
		value, err := reader.ReadByte()
		if err != nil {
			if err != io.EOF {
				return nil, false, err
			}
			return nil, false, errHostedResponseInvalid
		}
		prefix = append(prefix, value)
		text := bytes.TrimSpace(bytes.TrimPrefix(bytes.TrimSpace(prefix), []byte("\xef\xbb\xbf")))
		if len(text) == 0 || bytes.HasPrefix([]byte("\xef\xbb\xbf"), text) {
			continue
		}
		body := io.MultiReader(bytes.NewReader(prefix), reader)
		if text[0] == '{' || text[0] == '[' || strings.EqualFold(media, "application/json") {
			limit := historyBodyLimit
			if isImagePath(protocol) {
				limit = imageResponseLimit
			}
			raw, err := io.ReadAll(io.LimitReader(body, int64(limit)+1))
			if err != nil {
				return nil, false, err
			}
			if len(raw) > limit || hostedJSONProtocolError(raw, protocol) || !hostedJSONResponse(raw, protocol, stream) {
				return nil, false, errHostedResponseInvalid
			}
			response.Header.Set("Content-Type", "application/json")
			return bytes.NewReader(raw), false, nil
		}
		if stream && strings.ContainsRune(":deir", rune(text[0])) {
			response.Header.Set("Content-Type", "text/event-stream")
			return body, true, nil
		}
		if !stream && protocol == "/v1/audio/transcriptions" {
			return body, false, nil
		}
		return nil, false, errHostedResponseInvalid
	}
	return nil, false, errHostedResponseInvalid
}

func hostedJSONResponse(raw []byte, protocol string, stream bool) bool {
	var body map[string]json.RawMessage
	if json.Unmarshal(bytes.TrimPrefix(bytes.TrimSpace(raw), []byte("\xef\xbb\xbf")), &body) != nil {
		return false
	}
	field := "choices"
	switch protocol {
	case "/v1/responses":
		field = "output"
	case "/v1/messages":
		field = "content"
	case "/v1/audio/transcriptions":
		var text string
		return !bytes.Equal(bytes.TrimSpace(body["text"]), []byte("null")) && json.Unmarshal(body["text"], &text) == nil
	case imageGenerationPath, imageEditPath:
		field = "data"
	case "/v1/chat/completions":
	default:
		// Other model protocols keep their existing response contracts, not a chat envelope.
		if !stream {
			return true
		}
	}
	var rows []json.RawMessage
	return json.Unmarshal(body[field], &rows) == nil && rows != nil
}
