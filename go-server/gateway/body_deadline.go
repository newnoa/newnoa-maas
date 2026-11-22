package main

import (
	"net/http"
	"time"
)

const (
	minimumUploadBytesPerSecond = int64(64 << 10)
	maximumBodyReadTimeout      = 10 * time.Minute
)

func bodyReadTimeout(contentLength, maximum int64) time.Duration {
	if contentLength < 0 || contentLength > maximum {
		contentLength = maximum
	}
	seconds := (contentLength + minimumUploadBytesPerSecond - 1) / minimumUploadBytesPerSecond
	return min(15*time.Second+time.Duration(seconds)*time.Second, maximumBodyReadTimeout)
}

func setBodyReadDeadline(w http.ResponseWriter, contentLength, maximum int64) error {
	return http.NewResponseController(w).SetReadDeadline(time.Now().Add(bodyReadTimeout(contentLength, maximum)))
}
