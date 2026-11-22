package main

import (
	"encoding/json"
	"math"
	"strings"
)

func rerankInput(body map[string]any) (int, int, bool) {
	query, ok := body["query"].(string)
	if !ok || strings.TrimSpace(query) == "" {
		return 0, 0, false
	}
	documents, ok := body["documents"].([]any)
	if !ok || len(documents) == 0 || len(documents) > 2048 {
		return 0, 0, false
	}
	for _, document := range documents {
		text, ok := document.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return 0, 0, false
		}
	}
	count := len(documents)
	if value, exists := body["top_n"]; exists {
		number, ok := value.(float64)
		if !ok || number < 1 || number > float64(count) || math.Trunc(number) != number {
			return 0, 0, false
		}
		count = int(number)
	}
	if value, exists := body["return_documents"]; exists {
		if _, ok := value.(bool); !ok {
			return 0, 0, false
		}
	}
	return len(documents), count, true
}

func validRerank(data []byte, documents, count int) bool {
	var result struct {
		Results []struct {
			Index *int     `json:"index"`
			Score *float64 `json:"relevance_score"`
		} `json:"results"`
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(data, &result) != nil || len(result.Error) > 0 || len(result.Results) != count {
		return false
	}
	seen := map[int]bool{}
	previous := math.Inf(1)
	for _, item := range result.Results {
		if item.Index == nil || *item.Index < 0 || *item.Index >= documents || seen[*item.Index] || item.Score == nil || math.IsNaN(*item.Score) || math.IsInf(*item.Score, 0) || *item.Score > previous {
			return false
		}
		seen[*item.Index] = true
		previous = *item.Score
	}
	return true
}

func inputOnlyTokenUsage(data []byte) (*int64, *int64) {
	var result struct {
		Usage *struct {
			Prompt     *int64 `json:"prompt_tokens"`
			Total      *int64 `json:"total_tokens"`
			Completion *int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(data, &result) != nil || result.Usage == nil {
		return nil, nil
	}
	u := result.Usage
	if u.Prompt == nil || u.Total == nil || *u.Prompt < 0 || *u.Prompt > 1000000000 || *u.Total != *u.Prompt || (u.Completion != nil && *u.Completion != 0) {
		return nil, nil
	}
	// Matching prompt and total counts prove zero generated tokens for a score or vector response.
	zero := int64(0)
	return u.Prompt, &zero
}
