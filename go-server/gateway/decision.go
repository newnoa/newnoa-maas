package main

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

type decisionQuestion struct {
	kind   string
	labels []string
}

func fraction(value any) bool {
	n, ok := value.(float64)
	return ok && !math.IsNaN(n) && !math.IsInf(n, 0) && n >= 0 && n <= 1
}

func decisionInput(body map[string]any) (map[string]decisionQuestion, bool) {
	questions, ok := body["questions"].(map[string]any)
	if !ok || len(questions) < 1 || len(questions) > 128 || body["state"] == nil {
		return nil, false
	}
	// Media needs a separately bounded decoder and is not part of the text contract.
	for _, key := range []string{"images", "assets", "audio", "videos"} {
		if _, exists := body[key]; exists {
			return nil, false
		}
	}
	for _, key := range []string{"allow_none", "none_check", "return_details", "return_thought"} {
		if value, exists := body[key]; exists {
			if _, ok := value.(bool); !ok {
				return nil, false
			}
		}
	}
	for _, key := range []string{"gate", "mix"} {
		if value, exists := body[key]; exists && !fraction(value) {
			return nil, false
		}
	}
	if value, exists := body["think_budget"]; exists {
		n, ok := value.(float64)
		if !ok || n < 1 || n > 2_000_000 || math.Trunc(n) != n {
			return nil, false
		}
	}
	if value, exists := body["think_mode"]; exists {
		switch value {
		case "auto", "never", "always", "hybrid", "plain", "think":
		default:
			return nil, false
		}
	}
	if value, exists := body["thought_generation"]; exists {
		switch value {
		case "auto", "stream", "chunked":
		default:
			return nil, false
		}
	}
	result := map[string]decisionQuestion{}
	for name, value := range questions {
		question, ok := value.(map[string]any)
		if !ok || strings.TrimSpace(name) == "" || len(name) > 256 {
			return nil, false
		}
		kind, _ := question["type"].(string)
		if value, exists := question["allow_none"]; exists {
			if _, ok := value.(bool); !ok {
				return nil, false
			}
		}
		spec := decisionQuestion{kind: kind}
		switch kind {
		case "noul":
			if criteria, exists := question["criteria"]; exists {
				labels, ok := criteria.(map[string]any)
				if !ok {
					return nil, false
				}
				for label := range labels {
					if label != "true" && label != "false" {
						return nil, false
					}
				}
			}
		case "choice":
			labels, ok := question["criteria"].(map[string]any)
			if !ok || len(labels) < 1 || len(labels) > 255 {
				return nil, false
			}
			for label := range labels {
				if strings.TrimSpace(label) == "" || len(label) > 256 {
					return nil, false
				}
				spec.labels = append(spec.labels, label)
			}
		case "score":
			levels, ok := question["criteria"].([]any)
			if !ok || len(levels) < 1 || len(levels) > 10 {
				return nil, false
			}
			for index := range levels {
				spec.labels = append(spec.labels, strconv.Itoa(index))
			}
		default:
			return nil, false
		}
		result[name] = spec
	}
	return result, true
}

func validDecision(raw []byte, questions map[string]decisionQuestion) bool {
	var result map[string]any
	if json.Unmarshal(raw, &result) != nil || result["error"] != nil {
		return false
	}
	answers, ok := result["answers"].(map[string]any)
	if !ok || len(answers) != len(questions) {
		return false
	}
	for name, spec := range questions {
		answer, ok := answers[name].(map[string]any)
		if !ok || answer["type"] != spec.kind {
			return false
		}
		if spec.kind == "noul" {
			if !fraction(answer["noul"]) {
				return false
			}
			continue
		}
		if !fraction(answer["confidence"]) {
			return false
		}
		probabilities, ok := answer["probabilities"].(map[string]any)
		if !ok || len(probabilities) != len(spec.labels) {
			return false
		}
		sum, weighted := 0.0, 0.0
		for index, label := range spec.labels {
			if !fraction(probabilities[label]) {
				return false
			}
			p := probabilities[label].(float64)
			sum += p
			weighted += float64(index) * p
		}
		if math.Abs(sum-1) > 0.0001 {
			return false
		}
		if spec.kind == "choice" {
			if value, exists := answer["none_probability"]; exists && !fraction(value) {
				return false
			}
			choice, exists := answer["choice"]
			if !exists {
				return false
			}
			if choice == nil {
				if !fraction(answer["none_probability"]) {
					return false
				}
			} else {
				label, ok := choice.(string)
				if !ok {
					return false
				}
				if _, ok := probabilities[label]; !ok {
					return false
				}
			}
		} else {
			score, ok := answer["score"].(float64)
			if !ok || math.IsNaN(score) || math.IsInf(score, 0) || score < 0 || score > float64(len(spec.labels)-1) || math.Abs(score-weighted) > 0.0001 {
				return false
			}
		}
	}
	return true
}
