package main

import "encoding/json"

// Only publish fixed admission explanations; control responses may contain private data.
func admissionErrorMessage(body []byte) string {
	var detail struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &detail) != nil {
		return "Too Many Requests"
	}
	switch detail.Error.Code {
	case "ACCOUNT_CONCURRENCY_LIMIT":
		return "Account inference concurrency limit reached; retry after active requests finish"
	case "API_KEY_CONCURRENCY_LIMIT":
		return "NEWNOA API Key inference concurrency limit reached; retry after active requests finish"
	case "NODE_CONCURRENCY_LIMIT":
		return "Go Server node inference concurrency limit reached; retry after active requests finish"
	case "MODEL_CONCURRENCY_LIMIT":
		return "Model inference concurrency limit reached; retry after active requests finish"
	case "UPSTREAM_KEY_CONCURRENCY_LIMIT":
		return "Upstream API Key concurrency limit reached; retry after active requests finish"
	case "POOL_ALLOCATION_LIMIT":
		return "Credential pool allocation threshold reached; new allocation refused"
	case "POOL_QUEUE_FULL":
		return "Credential pool waiting queue is full"
	case "POOL_KEY_UNAVAILABLE":
		return "No available upstream API Key for this caller"
	case "ADMISSION_BUSY":
		return "Concurrent admission in progress; retry shortly"
	default:
		return "Too Many Requests"
	}
}
