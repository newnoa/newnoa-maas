package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type historyExportResult struct {
	Path     string `json:"path"`
	Filename string `json:"filename"`
	Count    int64  `json:"count"`
	Bytes    int64  `json:"bytes"`
}
type historyExportJobKey struct{}

func historyUserExportValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(value))
		for key, field := range value {
			switch key {
			case "pointsStatus", "settlementStatus", "settlementReason", "usageSource", "billingPhase", "usageSnapshot":
				continue
			}
			result[key] = historyUserExportValue(field)
		}
		return result
	case []any:
		result := make([]any, len(value))
		for index, field := range value {
			result[index] = historyUserExportValue(field)
		}
		return result
	default:
		return value
	}
}

func exportMemoryBudget() int64 {
	budget := int64(16 << 20)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	// The byte budget shrinks with heap pressure; a single stored record is still bounded by capture limits.
	if mem.HeapAlloc > 256<<20 {
		budget = 2 << 20
	}
	if raw, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			parts := strings.Fields(line)
			if len(parts) > 1 && parts[0] == "MemAvailable:" {
				n, _ := strconv.ParseInt(parts[1], 10, 64)
				budget = min(budget, n*1024/32)
			}
		}
	}
	if maximum, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		if limit, err := strconv.ParseInt(strings.TrimSpace(string(maximum)), 10, 64); err == nil {
			if raw, err := os.ReadFile("/sys/fs/cgroup/memory.current"); err == nil {
				used, _ := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
				budget = min(budget, max(0, limit-used)/32)
			}
		}
	}
	return max(256<<10, budget)
}

func writeHistoryExport(ctx context.Context, store historyRepository, viewer string, q historyQuery, format, directory string, progress func(int), budget func() int64) (result historyExportResult, err error) {
	if format != "csv" && format != "json" {
		return result, errors.New("unsupported history export format")
	}
	if err = historyDirectory(directory); err != nil {
		return result, err
	}
	if err = q.validate(); err != nil {
		return result, err
	}
	q.Before = ""
	q.IncludeContent = false
	q.Limit = 50
	now := time.Now().UTC()
	if q.To == "" {
		q.To = now.Format(time.RFC3339Nano)
	} else if upper, _ := time.Parse(time.RFC3339Nano, q.To); upper.After(now) {
		q.To = now.Format(time.RFC3339Nano)
	}
	first, err := store.Query(ctx, viewer, q)
	if err != nil {
		return result, err
	}
	total := first.Total
	if budget == nil {
		budget = exportMemoryBudget
	}
	if progress == nil {
		progress = func(int) {}
	}
	progress(0)
	prefix := ".newnoa-export-"
	if id, ok := ctx.Value(historyExportJobKey{}).(string); ok && len(id) == 64 && strings.Trim(id, "0123456789abcdef") == "" {
		prefix += id + "-"
	}
	segmentDirectory, e := os.MkdirTemp(directory, prefix+"work-*")
	if e != nil {
		return result, e
	}
	defer os.RemoveAll(segmentDirectory)
	segmentCount := 0
	var current *os.File
	var output *os.File
	defer func() {
		if current != nil {
			current.Close()
		}
		if output != nil {
			output.Close()
			os.Remove(output.Name())
		}
	}()
	var segmentBytes int64
	var encoder *json.Encoder
	openSegment := func() error {
		f, e := os.OpenFile(filepath.Join(segmentDirectory, fmt.Sprintf("%09d.jsonl", segmentCount)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		current = f
		segmentCount++
		encoder = json.NewEncoder(f)
		encoder.SetEscapeHTML(false)
		segmentBytes = 0
		return nil
	}
	flush := func() error {
		if current == nil {
			return nil
		}
		if e := current.Sync(); e != nil {
			return e
		}
		e := current.Close()
		current = nil
		return e
	}
	page := first
	for {
		for _, meta := range page.Items {
			if err = ctx.Err(); err != nil {
				return result, err
			}
			record, e := store.Get(ctx, viewer, q.Scope, historyText(meta["id"]))
			if e != nil {
				return result, e
			}
			if current == nil {
				if err = openSegment(); err != nil {
					return result, err
				}
			}
			before, _ := current.Seek(0, io.SeekCurrent)
			if err = encoder.Encode(historyUserExportValue(record)); err != nil {
				return result, err
			}
			after, _ := current.Seek(0, io.SeekCurrent)
			segmentBytes += after - before
			result.Count++
			if total > 0 {
				progress(int(min(result.Count, total) * 99 / (2 * total)))
			}
			if segmentBytes >= max(1, budget()) {
				if err = flush(); err != nil {
					return result, err
				}
			}
		}
		if page.NextCursor == "" {
			break
		}
		q.Before = page.NextCursor
		page, err = store.Query(ctx, viewer, q)
		if err != nil {
			return result, err
		}
	}
	if err = flush(); err != nil {
		return result, err
	}
	if result.Count != total {
		return result, errors.New("history changed during export; retry")
	}
	output, err = os.CreateTemp(directory, prefix+"merged-*.part")
	if err != nil {
		return result, err
	}
	writer := bufio.NewWriterSize(output, 64<<10)
	columns := []string{"id", "createdAt", "modelName", "model", "callerEmail", "keyName", "protocol", "httpStatus", "durationMs", "callerSource", "reasoningEffort", "clientIp", "firstTokenMs", "outputTps", "interrupted", "inputTokens", "outputTokens", "cacheWriteTokens", "cachedInputTokens", "pointsConsumed", "modelCategory", "imageUsage", "imagePricing", "imageCosts", "imageMetadata", "pointsMultiplier", "effectivePointsMultiplier", "longContextPricing", "longContextApplied", "longContextThreshold", "request", "response"}
	csvWriter := csv.NewWriter(writer)
	if format == "json" {
		_, err = writer.WriteString("[")
	} else {
		_, err = writer.WriteString("\ufeff")
		if err == nil {
			err = csvWriter.Write(columns)
		}
	}
	if err != nil {
		return result, err
	}
	merged := int64(0)
	for index := 0; index < segmentCount; index++ {
		name := filepath.Join(segmentDirectory, fmt.Sprintf("%09d.jsonl", index))
		part, e := os.Open(name)
		if e != nil {
			return result, e
		}
		decoder := json.NewDecoder(bufio.NewReaderSize(part, 64<<10))
		for {
			if err = ctx.Err(); err != nil {
				part.Close()
				return result, err
			}
			var raw json.RawMessage
			err = decoder.Decode(&raw)
			if errors.Is(err, io.EOF) {
				err = nil
				break
			}
			if err != nil {
				part.Close()
				return result, err
			}
			if format == "json" {
				if merged > 0 {
					_, err = writer.WriteString(",")
				}
				if err == nil {
					_, err = writer.Write(raw)
				}
			} else {
				var value map[string]any
				if err = json.Unmarshal(raw, &value); err == nil {
					row := make([]string, len(columns))
					for i, key := range columns {
						if v := value[key]; v != nil {
							if key == "imageUsage" || key == "imagePricing" || key == "imageCosts" || key == "imageMetadata" {
								data, e := json.Marshal(v)
								if e != nil {
									part.Close()
									return result, e
								}
								row[i] = historyCSVCell(string(data))
							} else {
								row[i] = historyCSVCell(fmt.Sprint(v))
							}
						}
					}
					err = csvWriter.Write(row)
				}
			}
			if err != nil {
				part.Close()
				return result, err
			}
			merged++
			if total > 0 {
				progress(int((total + min(merged, total)) * 99 / (2 * total)))
			}
		}
		if err = part.Close(); err != nil {
			return result, err
		}
	}
	if merged != total {
		return result, errors.New("history export segment count mismatch")
	}
	if format == "json" {
		_, err = writer.WriteString("]\n")
	} else {
		csvWriter.Flush()
		err = csvWriter.Error()
	}
	if err != nil {
		return result, err
	}
	if err = writer.Flush(); err != nil {
		return result, err
	}
	if err = output.Sync(); err != nil {
		return result, err
	}
	info, e := output.Stat()
	if e != nil {
		return result, e
	}
	result.Bytes = info.Size()
	if err = ctx.Err(); err != nil {
		return result, err
	}
	name := output.Name()
	if err = output.Close(); err != nil {
		return result, err
	}
	result.Filename = "newnoa-history-" + q.Scope + "-" + time.Now().UTC().Format("20060102T150405") + "-" + strings.TrimPrefix(filepath.Base(name), prefix+"merged-") + "." + format
	result.Path = filepath.Join(directory, result.Filename)
	if err = os.Rename(name, result.Path); err != nil {
		return result, err
	}
	output = nil
	progress(100)
	return result, nil
}
func historyCSVCell(value string) string {
	trimmed := strings.TrimLeftFunc(value, unicode.IsSpace)
	if trimmed != "" && strings.ContainsRune("=+-@", rune(trimmed[0])) || strings.HasPrefix(value, "\t") || strings.HasPrefix(value, "\r") {
		return "'" + value
	}
	return value
}
