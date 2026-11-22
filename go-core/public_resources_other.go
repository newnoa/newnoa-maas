//go:build !linux

package main

func publicOSResources() (map[string]uint64, map[string]any) {
	return map[string]uint64{}, map[string]any{}
}
