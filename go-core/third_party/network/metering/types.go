// Package metering observes protocol content. It owns no billing or persistence policy.
package metering

// Usage contains only counters explicitly reported by the upstream protocol.
// Nil is unavailable, not zero. No hidden reasoning or cache category is inferred.
type Usage struct {
	InputTokens       *int64 `json:"inputTokens,omitempty"`
	OutputTokens      *int64 `json:"outputTokens,omitempty"`
	CacheWriteTokens  *int64 `json:"cacheWriteTokens,omitempty"`
	CachedInputTokens *int64 `json:"cachedInputTokens,omitempty"`
}

// Snapshot is a plaintext-free cumulative observation. BPE counts may decrease
// when a lane grows; reporters own monotonic sequence numbers, not this package.
type Snapshot struct {
	ImageMetadata         map[string]any `json:"imageMetadata,omitempty"`
	ImageUsage            *ImageUsage    `json:"imageUsage,omitempty"`
	Estimator             string         `json:"estimator"`
	Encoding              string         `json:"encoding"`
	EstimatedInputTokens  *int64         `json:"estimatedInputTokens,omitempty"`
	EstimatedOutputTokens *int64         `json:"estimatedOutputTokens,omitempty"`
	ResponseStarted       bool           `json:"responseStarted"`
	Reported              *Usage         `json:"reported,omitempty"`
	TerminalUsage         bool           `json:"terminalUsage"`
}
