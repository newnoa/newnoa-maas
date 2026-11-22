package main

import "regexp"

var apiKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,256}$`)

func validAPIKey(value string) bool { return apiKeyPattern.MatchString(value) }
