package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const (
	historyBodyChunkMax = 256 * 1024
	historyBodyMaxBytes = 256 * 1024 * 1024
	historyBodyMarker   = "_newnoaBody"
)

type historyBodyBlock struct {
	Hash    string
	RawSize int
	Data    []byte
}
type historyBodyPart struct {
	Hash string `json:"sha256"`
	Size int    `json:"size"`
}
type historyBodyField struct {
	Size  int               `json:"size"`
	Hash  string            `json:"sha256"`
	Parts []historyBodyPart `json:"parts"`
}
type historyBodyManifest struct {
	Version int                         `json:"version"`
	Fields  map[string]historyBodyField `json:"fields"`
}

func historyBodyHash(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func historyBodyValidHash(hash string) bool {
	return len(hash) == 64 && strings.Trim(hash, "0123456789abcdef") == ""
}

// The rolling gear hash forgets prefix changes, so later conversation blocks can be reused.
var historyBodyGear = func() [256]uint64 {
	var table [256]uint64
	for i := range table {
		x := uint64(i) + 0x9e3779b97f4a7c15
		x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
		x = (x ^ (x >> 27)) * 0x94d049bb133111eb
		table[i] = x ^ (x >> 31)
	}
	return table
}()

func historyBodyEncode(value map[string]any) (string, []historyBodyBlock, error) {
	if _, exists := value[historyBodyMarker]; exists {
		return "", nil, errors.New("reserved history body marker")
	}
	record := make(map[string]any, len(value)+1)
	for key, v := range value {
		record[key] = v
	}
	manifest := historyBodyManifest{Version: 1, Fields: map[string]historyBodyField{}}
	blocks := []historyBodyBlock{}
	seen := map[string]bool{}
	total := 0
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(historyBodyChunkMax))
	if err != nil {
		return "", nil, err
	}
	defer encoder.Close()
	for _, key := range []string{"request", "response"} {
		body, ok := value[key].(string)
		if !ok {
			continue
		}
		total += len(body)
		if total > historyBodyMaxBytes {
			return "", nil, errors.New("history body exceeds expansion limit")
		}
		raw := []byte(body)
		field := historyBodyField{Size: len(raw), Hash: historyBodyHash(raw), Parts: []historyBodyPart{}}
		start := 0
		var rolling uint64
		for end := 0; end < len(raw); end++ {
			rolling = (rolling << 1) + historyBodyGear[raw[end]]
			size := end + 1 - start
			if end+1 != len(raw) && size < historyBodyChunkMax && (size < 16*1024 || rolling&65535 != 0) {
				continue
			}
			part := raw[start : end+1]
			hash := historyBodyHash(part)
			field.Parts = append(field.Parts, historyBodyPart{Hash: hash, Size: len(part)})
			if !seen[hash] {
				blocks = append(blocks, historyBodyBlock{Hash: hash, RawSize: len(part), Data: encoder.EncodeAll(part, nil)})
				seen[hash] = true
			}
			start = end + 1
			rolling = 0
		}
		manifest.Fields[key] = field
		delete(record, key)
	}
	record[historyBodyMarker] = manifest
	raw, err := json.Marshal(record)
	if err != nil {
		return "", nil, err
	}
	if len(raw) > historyBodyMaxBytes {
		return "", nil, errors.New("history header exceeds limit")
	}
	return string(raw), blocks, nil
}

func historyBodyIsPacked(raw string) bool {
	var record map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &record) != nil {
		return false
	}
	_, ok := record[historyBodyMarker]
	return ok
}

func historyBodyDecode(header string, load func(string) (historyBodyBlock, error)) (map[string]any, error) {
	if len(header) > historyBodyMaxBytes {
		return nil, errors.New("history header exceeds limit")
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(header), &record); err != nil {
		return nil, err
	}
	if record == nil {
		return nil, errors.New("invalid history record")
	}
	raw, packed := record[historyBodyMarker]
	if !packed {
		return record, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var manifest historyBodyManifest
	if err = json.Unmarshal(encoded, &manifest); err != nil {
		return nil, err
	}
	if manifest.Version != 1 || manifest.Fields == nil || len(manifest.Fields) > 2 {
		return nil, errors.New("unsupported history body manifest")
	}
	total := 0
	for key, field := range manifest.Fields {
		if key != "request" && key != "response" {
			return nil, errors.New("invalid history body field")
		}
		if _, exists := record[key]; exists {
			return nil, errors.New("ambiguous history body field")
		}
		if field.Size < 0 || field.Size > historyBodyMaxBytes-total || !historyBodyValidHash(field.Hash) || len(field.Parts) > historyBodyMaxBytes/(16*1024)+2 {
			return nil, errors.New("invalid history body size or hash")
		}
		total += field.Size
		size := 0
		for _, part := range field.Parts {
			if part.Size <= 0 || part.Size > historyBodyChunkMax || part.Size > field.Size-size || !historyBodyValidHash(part.Hash) {
				return nil, errors.New("invalid history block manifest")
			}
			size += part.Size
		}
		if size != field.Size {
			return nil, errors.New("history body length mismatch")
		}
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(historyBodyChunkMax*2), zstd.WithDecoderMaxWindow(historyBodyChunkMax), zstd.WithDecodeAllCapLimit(true))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	for key, field := range manifest.Fields {
		body := make([]byte, 0, field.Size)
		for _, part := range field.Parts {
			if load == nil {
				return nil, errors.New("history body block unavailable")
			}
			block, err := load(part.Hash)
			if err != nil {
				return nil, fmt.Errorf("history body block unavailable: %w", err)
			}
			if block.Hash != part.Hash || block.RawSize != part.Size || len(block.Data) > historyBodyChunkMax+1024 {
				return nil, errors.New("history block identity or length mismatch")
			}
			decoded, err := decoder.DecodeAll(block.Data, make([]byte, 0, part.Size))
			if err != nil {
				return nil, fmt.Errorf("history body decompression failed: %w", err)
			}
			if len(decoded) != part.Size || historyBodyHash(decoded) != part.Hash {
				return nil, errors.New("history block integrity check failed")
			}
			body = append(body, decoded...)
		}
		if len(body) != field.Size || historyBodyHash(body) != field.Hash {
			return nil, errors.New("history body integrity check failed")
		}
		record[key] = string(body)
	}
	delete(record, historyBodyMarker)
	return record, nil
}

func historyBodySearch(value map[string]any, query string) bool {
	return strings.Contains(strings.ToLower(historyText(value["request"])+"\n"+historyText(value["response"])+"\n"+historyText(value["model"])+"\n"+historyText(value["modelName"])), strings.ToLower(query))
}
