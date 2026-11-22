package main

import (
	"encoding/json"
	"math"
)

func embeddingInput(value any) int {
	if text, ok := value.(string); ok {
		if text != "" {
			return 1
		}
		return 0
	}
	items, ok := value.([]any)
	if !ok || len(items) == 0 || len(items) > 2048 {
		return 0
	}
	token := func(value any) bool {
		n, ok := value.(float64)
		return ok && n >= 0 && n <= 2147483647 && math.Trunc(n) == n
	}
	if token(items[0]) {
		for _, item := range items {
			if !token(item) {
				return 0
			}
		}
		return 1
	}
	_, texts := items[0].(string)
	for _, item := range items {
		_, isText := item.(string)
		if isText != texts {
			return 0
		}
		if text, ok := item.(string); ok {
			if text == "" {
				return 0
			}
			continue
		}
		tokens, ok := item.([]any)
		if !ok || len(tokens) == 0 || len(tokens) > 32768 {
			return 0
		}
		for _, value := range tokens {
			if !token(value) {
				return 0
			}
		}
	}
	return len(items)
}

func validEmbeddings(data []byte, count int, dimensions int) bool {
	var result struct {
		Object string `json:"object"`
		Data   []struct {
			Object    string     `json:"object"`
			Index     *int       `json:"index"`
			Embedding []*float64 `json:"embedding"`
		} `json:"data"`
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(data, &result) != nil || len(result.Error) > 0 || result.Object != "list" || len(result.Data) != count {
		return false
	}
	seen := map[int]bool{}
	width := dimensions
	for _, row := range result.Data {
		if row.Object != "embedding" || row.Index == nil || *row.Index < 0 || *row.Index >= count || seen[*row.Index] || len(row.Embedding) == 0 {
			return false
		}
		seen[*row.Index] = true
		if width == 0 {
			width = len(row.Embedding)
		}
		if len(row.Embedding) != width {
			return false
		}
		for _, number := range row.Embedding {
			if number == nil || math.IsNaN(*number) || math.IsInf(*number, 0) {
				return false
			}
		}
	}
	return true
}
