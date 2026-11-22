//go:build !linux

package main

// Resource collection runs on the Linux API/Server deployment; other systems retain request rates.
func osResources() (map[string]uint64, map[string]any) { return map[string]uint64{}, map[string]any{} }
