// Package configuredhost matches complete authorities from a configured allowlist.
package configuredhost

import (
	"os"
	"strings"
)

func Allowed(name, authority, defaults string) bool {
	if authority == "" {
		return false
	}
	hosts, present := os.LookupEnv(name)
	if !present {
		hosts = defaults
	}
	for _, host := range strings.Split(hosts, ",") {
		if strings.EqualFold(strings.TrimSpace(host), authority) {
			return true
		}
	}
	return false
}
