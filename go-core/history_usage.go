package main

import (
	peernet "github.com/newnoa/newnoa-maas/go-server/network"
	"github.com/newnoa/newnoa-maas/go-server/network/metering"
	"strings"
)

func recoverHistoryUsage(value map[string]any) {
	if value["callerSource"] == nil {
		if source := peernet.KnownCallerSource(historyText(value["agent"])); source != "" {
			value["callerSource"] = source
		}
	}
	if value["responseTruncated"] != true {
		capture := newUsageCapture(strings.Contains(historyText(value["contentType"]), "text/event-stream") || strings.HasPrefix(strings.TrimSpace(historyText(value["response"])), "data:"))
		capture.protocol = historyText(value["protocol"])
		capture.feed([]byte(historyText(value["response"])))
		usage := capture.counts()
		for key, count := range map[string]*int64{"inputTokens": usage.Input, "outputTokens": usage.Output, "cachedInputTokens": usage.Cached} {
			if _, exists := value[key]; !exists && count != nil {
				value[key] = *count
			}
		}
		if _, exists := value["cacheWriteTokens"]; !exists && usage.Write != nil && !capture.invalid {
			value["cacheWriteTokens"] = *usage.Write
		}
	}
	if historyText(value["modelType"]) != "" && value["modelCategory"] == nil {
		value["modelCategory"] = historyModelCategory(value)
	}
	if historyText(value["protocol"]) == imageGenerationPath || historyText(value["protocol"]) == "/v1/images/edits" {
		if value["responseTruncated"] != true {
			if _, exists := value["imageUsage"]; !exists {
				value["imageUsage"] = metering.ParseImageUsage([]byte(historyText(value["response"])))
			}
			if _, exists := value["imageMetadata"]; !exists {
				value["imageMetadata"] = metering.ImageMetadata(historyText(value["protocol"]), []byte(historyText(value["request"])), []byte(historyText(value["response"])))
			}
		}
		value["response"] = string(imageResponseMetadata([]byte(historyText(value["response"]))))
	}
	if _, exists := value["pointsStatus"]; !exists {
		value["pointsStatus"] = "unavailable"
		if nodePattern.MatchString(historyText(value["requestId"])) {
			value["pointsStatus"] = "pending"
		}
	}
}

func historyModelCategory(value map[string]any) string {
	if category := historyText(value["modelCategory"]); category == "image" || category == "text" {
		return category
	}
	if historyText(value["modelType"]) == "image" {
		return "image"
	}
	return "text"
}

// Speech uses the existing text storage leaf; reporting separates ASR by its recorded type.
func historyStorageLeaf(category string) string {
	if category == "audio" {
		return "text"
	}
	return category
}
