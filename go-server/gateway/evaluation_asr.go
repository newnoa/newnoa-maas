package main

import (
	"encoding/json"
	"unicode/utf8"
)

func evaluationTranscriptionUsage(raw []byte, reserve int64) (int64, int64, bool) {
	var result struct {
		Usage *struct {
			Input  *int64 `json:"prompt_tokens"`
			Output *int64 `json:"completion_tokens"`
			Total  *int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if !utf8.Valid(raw) || !validTranscription(raw) || json.Unmarshal(raw, &result) != nil || result.Usage == nil {
		return 0, 0, false
	}
	u := result.Usage
	if u.Input == nil || u.Output == nil || u.Total == nil || *u.Input < 0 || *u.Output < 0 || *u.Input > reserve || *u.Output > reserve-*u.Input || *u.Total != *u.Input+*u.Output {
		return 0, 0, false
	}
	return *u.Input, *u.Output, true
}
