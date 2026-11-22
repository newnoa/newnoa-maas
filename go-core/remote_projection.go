package main

import (
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
)

func remoteDistribution() string {
	if runtime.GOOS != "linux" {
		return ""
	}
	raw, err := os.ReadFile("/etc/os-release")
	if err != nil || len(raw) > 65536 {
		return ""
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "ID=") {
			id := strings.Trim(strings.TrimPrefix(line, "ID="), "\"'")
			if regexp.MustCompile(`^[a-z0-9_-]{1,40}$`).MatchString(id) {
				return id
			}
		}
	}
	return ""
}
func (m *remoteManager) attachmentProjectionLocked() []map[string]any {
	out := []map[string]any{}
	for _, a := range m.attachments {
		out = append(out, map[string]any{"id": a.ID, "sessionId": a.SessionID, "name": a.Name, "mime": a.Mime, "size": a.Size, "ready": a.Ready})
	}
	sort.Slice(out, func(i, j int) bool { return out[i]["id"].(string) < out[j]["id"].(string) })
	return out
}
